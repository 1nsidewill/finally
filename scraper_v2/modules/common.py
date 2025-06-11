from pathlib import Path
from core.logger import setup_logger

logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정

def read_keywords():
    keywords = []
    try:
        logger.info("Find keywords Start")
        text = Path("./list.txt").read_text(encoding="utf-8")
        for line in text.splitlines():
            for item in line.split(","):
                kw = item.strip()
                if kw and kw not in keywords:
                    keywords.append(kw)
        logger.info("Find keywords End")
    except Exception:
        logger.info(f"Find keywords Fail - {Exception}")

    return keywords
