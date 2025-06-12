import sys
from core.logger import setup_logger
from core.database import AsyncSessionLocal
from sqlalchemy import select
from models import Provider

from . import categories, products

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