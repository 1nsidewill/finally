import httpx
import sys
from core.logger import setup_logger
from core.database import AsyncSessionLocal
from sqlalchemy import select, update, delete
from models import Provider, Category

logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정
PROVIDER = None

async def init():
    global PROVIDER
    logger.info("Init Provider Start")
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Provider).where(Provider.code == 'BUNJANG'))
        provider = result.scalars().first()
        if provider:
            PROVIDER = provider
            logger.info("Init Provider End")
        else:
            logger.info("Init Provider Fail")
            logger.info("❌ Program Exit Now")
            sys.exit(1)


async def sync_categories():
    async with AsyncSessionLocal() as db:
        try:
            logger.info("Sync Categories Start")
            url = f"{PROVIDER.url_api.rstrip('/')}/1/categories/list.json"
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                raw_categories = response.json().get("categories", [])

            # 3. 전처리 (재귀 파싱)
            def flatten(data, depth=1):
                result = []
                for cat in data:
                    result.append(Category(
                        provider_uid=PROVIDER.uid,
                        id=str(cat["id"]),
                        title=cat["title"],
                        depth=depth,
                        order=cat.get("order")
                    ))
                    if isinstance(cat.get("categories"), list):
                        result += flatten(cat["categories"], depth + 1)
                return result

            parsed_categories = flatten(raw_categories)
            logger.info(f"Sync Categories - {PROVIDER.code} / {len(parsed_categories)} List Detected")

            # 4. 기존 데이터 삭제 후 저장
            await db.execute(delete(Category).where(Category.provider_uid == PROVIDER.uid))
            db.add_all(parsed_categories)
            await db.commit()
            logger.info("Sync Categories End")
        except Exception as e:
            await db.rollback()
            logger.exception(f"Sync Categories Error - {e}")
