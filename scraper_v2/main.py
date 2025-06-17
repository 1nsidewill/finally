from modules.sync import sync_categories, sync_products
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from core.database import engine, Base
from core.logger import setup_logger
from modules.providers import bunjang
import asyncio

logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정

async def init_db():
    logger.info("Database Create Start")
    async with engine.begin() as conn:
        conn.run_sync(Base.metadata.create_all)
    logger.info("Database Create End")

def scheduler():
    logger.info("Scheduler Create Start")
    scheduler = AsyncIOScheduler(timezone="Asia/Seoul")
    #####################################################################################################
    # scheduler.add_job(my_job, 'cron', minute='*/5') # 5분마다 진행
    # scheduler.add_job(my_job, 'cron', hour=0, minute=0) # 매일 0시마다 실행
    # scheduler.add_job(my_job, 'cron', minute='*') cron 표현식(매분 실행)
    #scheduler.add_job(sync_categories, CronTrigger.from_crontab("0 0 * * *")) # 매일 0시 0분에 1번 실행
    #####################################################################################################
    scheduler.add_job(sync_categories, CronTrigger.from_crontab("0 0 * * *")) # 매일 0시 0분에 1번 실행
    scheduler.add_job(sync_products, CronTrigger.from_crontab("* * * * *")) # 테스트용
    scheduler.start()
    logger.info("Scheduler Create End")

async def main():
    logger.info("🚀 Program Start")
    await init_db()

    # ✅ Provider 초기화 명시적 호출
    await bunjang.init()

    scheduler()
    await asyncio.Event().wait()  # 이벤트 루프를 영원히 유지
    
if __name__ == "__main__":
    asyncio.run(main())