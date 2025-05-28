from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import asyncio
from datetime import datetime

# === 1. 글로벌 크론 문자열 선언 ===
crons = """
0 * * * * functionA
0 * * * * functionB
"""

# === 2. 실행할 함수들 선언 ===
async def functionA():
    print(f"[{datetime.now()}] functionA 실행됨")

async def functionB():
    print(f"[{datetime.now()}] functionB 실행됨")

# === 3. 함수명과 실제 함수 매핑 ===
FUNCTION_MAP = {
    "functionA": functionA,
    "functionB": functionB,
}

# === 4. 크론 문자열 파싱 및 스케줄러 등록 ===
def start_scheduler():
    scheduler = AsyncIOScheduler()
    for line in crons.strip().splitlines():
        parts = line.strip().split()
        if len(parts) != 6:
            print(f"잘못된 형식: {line}")
            continue
        # cron 표현식(앞 5개), 함수명(마지막)
        cron_expr = " ".join(parts[:5])
        func_name = parts[5]
        func = FUNCTION_MAP.get(func_name)
        if not func:
            print(f"등록되지 않은 함수: {func_name}")
            continue
        # APScheduler에 등록
        scheduler.add_job(func, CronTrigger.from_crontab(cron_expr))
        print(f"등록: {cron_expr} -> {func_name}()")
    scheduler.start()
    print("스케줄러 시작됨.")

# === 0. 실행python scheduler.py ===
if __name__ == "__main__":
    start_scheduler()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
