from core.logger import setup_logger
from modules.providers import bunjang

logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정
#sync_batch_size = 5

# Sync Categories
async def sync_categories():
    logger.info("Sync Categories Start")
    await bunjang.categories.sync_categories()
    # await danggun.categories.sync_categories()
    logger.info("Sync Categories End")
    
    
# Sync Products
async def sync_products():
    logger.info("Sync Products Start")
    # await bunjang.products.sync_categories()
    # await danggun.products.sync_categories()
    logger.info("Sync Products End")


# Is Init?
async def is_inits():
    print(f"{bunjang.PROVIDER.code} - Success Init")
    