from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
import os
from typing import ClassVar

class Settings(BaseSettings):
    app_name: str
    # Milvus Connection Info
    MILVUS_URL: str
    MILVUS_COLLECTION_NAME: str
    
    # OpenAPI Connection Info
    OPENAI_API_KEY: str

    secret_key: str

    # 환경 파일 선택 및 로드 경로 출력
    env_file_path: ClassVar[str] = os.path.join(".", f".env.{os.getenv('ENVIRONMENT', 'dev')}")
    print(f"🟢 Loading environment file: {env_file_path}", flush=True)

    model_config = SettingsConfigDict(
        env_file=env_file_path, 
        env_file_encoding='utf-8'
    )

@lru_cache
def get_settings():
    return Settings()