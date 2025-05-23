import logging
import time

class EmojiFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.INFO:
            record.levelemoji = "ℹ️"
        elif record.levelno == logging.WARNING:
            record.levelemoji = "⚠️"
        elif record.levelno == logging.ERROR:
            record.levelemoji = "❌"
        elif record.levelno == logging.DEBUG:
            record.levelemoji = "🐛"
        else:
            record.levelemoji = "🔍"

        return super().format(record)

def configure_logging():
    logging.Formatter.converter = time.localtime  # KST 시간

    formatter = EmojiFormatter("[%(asctime)s] %(levelemoji)s [%(levelname)s] %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = [handler]  # 기존 핸들러 제거 후 설정 