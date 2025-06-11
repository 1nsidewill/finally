from core.logger import setup_logger
from .providers import bunjang

logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정
#sync_batch_size = 5

# Sync Categories
async def sync_categories():
    logger.info("Sync Categories Start")
    await bunjang.sync_categories()
    logger.info("Sync Categories End")
    
    
# Sync Products
async def sync_products():
    logger.info("Sync Products Start")
    
    
    logger.info("Sync Products End")
    