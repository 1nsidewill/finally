from typing import Optional
from pathlib import Path
import httpx, asyncio

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import JSONResponse

from core.database import get_db
from providers import bunjang
from core.logger import setup_logger

logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정

router = APIRouter(prefix="/sync", tags=["Sync"])

keyword_queue: list[str] = []         # 아직 처리하지 않은 키워드
total_keywords: int = 0               # 전체 키워드 수
processed_count: int = 0             # 현재까지 처리된 수
is_running: bool = False             # 실행 중 여부

@router.post("/sync_bunjang_categories")
async def sync_bunjang_categories(db: AsyncSession = Depends(get_db)):
    categories = await bunjang.fetch_categories(db)
    await bunjang.save_categories(db, categories)
    return {"message": f"{len(categories)} categories synced."}

@router.post("/sync_bunjang_products")
async def sync_bunjang_products(keyword: Optional[str] = Query(default=None), db: AsyncSession = Depends(get_db)):
    page = 0
    total_processed = 0
    while True:
        try:
            products = await bunjang.fetch_products(keyword, page)
            if not products:
                break
            for product in products:
                pid = product.get("pid")
                if not pid:
                    continue
                if product.get("status") != "0":
                    await bunjang.delete_product_by_code(db, pid)
                    continue
                detail = await bunjang.fetch_product_detail(pid)
                if detail:
                    await bunjang.upsert_product(db, detail)
                    total_processed += 1
            total_processed += len(products)
            page += 1
        except Exception as e:
            logger.exception("에러 발생")
    return {"message": f"{len(products)} products synced."}

@router.post("/sync_bunjang_bike")
async def sync_bunjang_bike(db: AsyncSession = Depends(get_db)):
    global keyword_queue, total_keywords, processed_count, is_running

    if is_running:
        return JSONResponse(
            status_code=409,
            content={
                "message": "이미 실행 중입니다.",
                "progress": f"{processed_count}/{total_keywords}"
            }
        )

    # 키워드 로딩
    text = Path("./list.txt").read_text(encoding="utf-8")
    keywords = set()
    for line in text.splitlines():
        for item in line.split(", "):
            kw = item.strip()
            if kw:
                keywords.add(kw)

    keyword_queue = sorted(keywords)
    total_keywords = len(keyword_queue)
    processed_count = 0

    asyncio.create_task(run_keyword_sync())

    logger.info(f"total_keywords: {total_keywords}, keyword_queue: {keyword_queue}")
    return {"message": f"{total_keywords}개 키워드 백그라운드 처리 시작"}

async def run_keyword_sync():
    global keyword_queue, is_running, processed_count

    is_running = True
    batch_size = 5

    try:
        while keyword_queue:
            batch = [keyword_queue.pop(0) for _ in range(min(batch_size, len(keyword_queue)))]
            for kw in batch:
                await call_sync_bunjang(kw)
                processed_count += 1
    except Exception:
        logger.exception("run_keyword_sync 에러 발생")
    finally:
        is_running = False

async def call_sync_bunjang(keyword: str):
    url = "http://localhost:8000/sync/sync_bunjang_products"
    try:
        async with httpx.AsyncClient(timeout=None) as client:
            res = await client.post(url, params={"keyword": keyword})
            print(f"[{keyword}] ✅ {res.status_code}: {res.json()}")
    except Exception:
        logger.exception(f"[{keyword}] ❌ 요청 중 에러 발생")