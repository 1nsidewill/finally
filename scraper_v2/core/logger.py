import os
import logging
from datetime import datetime

# 로그 디렉토리 생성
os.makedirs("./logs", exist_ok=True)

# 날짜 기반 파일명
today_str = datetime.now().strftime("%Y-%m-%d")
log_file_path = f"./logs/{today_str}.log"

# 포맷터 정의
class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
            if "%f" in datefmt:
                s = s.replace("%f", f"{int(record.msecs):03d}")
            return s
        return super().formatTime(record, datefmt)

formatter = CustomFormatter(
    "%(asctime)s [%(levelname)s] - %(filename)s/%(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S.%f"
)

# 공통 핸들러 설정 (다른 모듈에서도 재사용 가능)
def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # ✅ 중복 출력 방지: 부모(루트)에 전파 안 함

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

