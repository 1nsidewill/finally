import logging
import yaml
from pathlib import Path

# 로거 설정
logger = logging.getLogger("api")

# 프롬프트 YAML 로드
def load_prompts():
    try:
        prompt_path = Path(__file__).parent.parent.parent / "prompt.yaml"
        logger.info(f"📝 프롬프트 파일 로드 중: {prompt_path}")
        with open(prompt_path, "r", encoding="utf-8") as file:
            prompts = yaml.safe_load(file)
        logger.info("✅ 프롬프트 파일 로드 완료")
        return prompts
    except Exception as e:
        logger.error(f"❌ 프롬프트 파일 로드 실패: {str(e)}", exc_info=True)
        raise RuntimeError(f"프롬프트 파일 로드 실패: {str(e)}")
