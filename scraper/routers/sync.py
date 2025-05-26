from typing import Optional, Set
from pathlib import Path
import httpx, asyncio

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import JSONResponse

from core.database import get_db, AsyncSessionLocal
from models import Provider
from providers import bunjang
from core.logger import setup_logger
from providers.bunjang import fetch_products, fetch_product_detail, upsert_product
from utils.string import parse_korean_number

logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정

router = APIRouter(prefix="/sync", tags=["Sync"])

keyword_queue: list[str] = []         # 아직 처리하지 않은 키워드
total_keywords: int = 0               # 전체 키워드 수
batch_size: int = 5
processed_count: int = 0             # 현재까지 처리된 수
is_running: bool = False             # 실행 중 여부
all_product_pids: Set[str] = set()    # pid 중복 제거용 set
pid_lock = asyncio.Lock()             # 동시성 제어를 위한 lock

@router.post("/test")
async def test(
    data: str = Query("13500000", description="숫자")
):
    return {"value": parse_korean_number(data)}

@router.post("/sync_categories")
async def sync_categories(
    code: str = Query(..., description="공급자 코드"),
    db: AsyncSession = Depends(get_db)
):
    await bunjang.fetch_categories(code, db)
    return {"message": f"{code} categories synced."}

@router.post("/sync_products")
async def sync_products(
    code: str = Query("BUNJANG", description="공급자 코드"),
    # keyword: Optional[str] = Query(None, description="키워드"),
    category: Optional[str] = Query("750800", description="카테고리"),
    db: AsyncSession = Depends(get_db)
):
    global keyword_queue, total_keywords, processed_count, is_running, all_product_pids

    if is_running:
        return JSONResponse(
            status_code=409,
            content={
                "message": "이미 실행 중입니다.",
                "progress": f"{processed_count}/{total_keywords}"
            }
        )

    result = await db.execute(select(Provider).where(Provider.code == code))
    provider = result.scalars().first()

    # 키워드 로딩
    text = Path("./list.txt").read_text(encoding="utf-8")
    keywords = []
    for line in text.splitlines():
        for item in line.split(","):
            kw = item.strip()
            if kw and kw not in keywords:
                keywords.append(kw)

    keyword_queue = keywords
    total_keywords = len(keyword_queue)
    processed_count = 0
    all_product_pids.clear()

    async def sync_detail_and_save(pid: str):
        async with AsyncSessionLocal() as db:
            detail = await fetch_product_detail(pid)
            if detail:
                await upsert_product(db, detail)

    async def run_keyword_sync():
        global is_running, processed_count, all_product_pids
        is_running = True
        try:
            logger.info("🚀 키워드 동기화 시작")

            # 1. pid 수집
            async def worker(worker_id: int):
                global keyword_queue, processed_count
                while True:
                    try:
                        keyword = keyword_queue.pop(0)
                    except IndexError:
                        break
                    try:
                        if not provider or provider.code == "BUNJANG":
                            all_product_pids.update(await fetch_products(keyword=keyword, category=category))
                            processed_count += 1
                    except Exception:
                        logger.exception(f"[{keyword}] ❌ 작업자 {worker_id} 에러 발생")

            tasks = [asyncio.create_task(worker(i)) for i in range(batch_size)]
            await asyncio.gather(*tasks)
            logger.info(f"✅ pid 수집 완료. 총 {len(all_product_pids)}개")

            # 2. 상세 정보 upsert
            pids = list(all_product_pids)
            pid_chunks = [pids[i:i+batch_size] for i in range(0, len(pids), batch_size)]
            for chunk in pid_chunks:
                detail_tasks = [
                    asyncio.create_task(sync_detail_and_save(pid))
                    for pid in chunk
                ]
                await asyncio.gather(*detail_tasks)

            logger.info("✅ 상세 upsert 완료.")

        except Exception:
            logger.exception("❌ run_keyword_sync 에러 발생")
        finally:
            is_running = False

    asyncio.create_task(run_keyword_sync())
    logger.info(f"total_keywords: {total_keywords}, keyword_queue: {keyword_queue}")
    return {"message": f"{total_keywords}개 키워드 백그라운드 처리 및 상세 upsert 시작"}