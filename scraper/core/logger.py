import os
import logging
from datetime import datetime

# 로그 디렉토리 생성
os.makedirs("./logs", exist_ok=True)

# 날짜 기반 파일명
today_str = datetime.now().strftime("%Y-%m-%d")
log_file_path = f"./logs/{today_str}.log"

# 포맷터 정의
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] - %(filename)s/%(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# 공통 핸들러 설정 (다른 모듈에서도 재사용 가능)
def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # 콘솔 핸들러
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # 파일 핸들러 (오늘 날짜 파일)
        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

# 외부 라이브러리 로그 억제
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
