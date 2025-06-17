import sys
from pathlib import Path
from core.logger import setup_logger
from modules import od

logger = setup_logger(__name__)  # 현재 파일명 기준 이름 지정

def read_keywords():
    keywords = od.odSet()
    try:
        # main.py와 같은 디렉터리 기준으로 파일 경로 지정
        base_dir = Path(sys.argv[0]).resolve().parent
        file_path = base_dir / "list.txt"

        logger.info("Find keywords Start")
        text = file_path.read_text(encoding="utf-8")
        print(text)
        for line in text.splitlines():
            for item in line.split(","):
                kw = item.strip()
                keywords.push(kw, kw)
        logger.info("Find keywords End")
    except Exception as e:
        logger.info(f"Find keywords Fail - {e}")

    return keywords.keyList()