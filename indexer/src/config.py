from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
import os
from typing import ClassVar, Optional

class Settings(BaseSettings):
    app_name: str
    # Milvus Connection Info
    MILVUS_URL: str
    MILVUS_COLLECTION_NAME: str
    
    # OpenAPI Connection Info
    OPENAI_API_KEY: str

    # Qdrant Connection Info
    QDRANT_HOST: str  # GCP 서버 주소
    QDRANT_PORT: int
    QDRANT_GRPC_PORT: int
    QDRANT_PREFER_GRPC: bool
    QDRANT_COLLECTION: str
    QDRANT_USE_MEMORY: bool  # 인메모리 모드 사용 여부 (테스트/개발용)
    VECTOR_SIZE: int # 임베딩 벡터 차원 수

    secret_key: str
    test_username: str
    test_password: str

    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str



    # 배치 처리 관련 설정 (선택적)
    BATCH_SIZE: Optional[int] = 100
    MAX_CONCURRENT_BATCHES: Optional[int] = 3
    EMBEDDING_BATCH_SIZE: Optional[int] = 50
    
    # 재시도 관련 설정 (선택적)
    MAX_RETRIES: Optional[int] = 3
    RETRY_DELAY: Optional[float] = 5.0
    
    # 로깅 설정 (선택적)
    LOG_LEVEL: Optional[str] = "INFO"
    
    # 진행상황 저장 설정 (선택적)
    SAVE_PROGRESS_EVERY: Optional[int] = 10
    LOG_EVERY: Optional[int] = 5

    # 환경 파일 선택 및 로드 경로 출력
    env_file_path: ClassVar[str] = os.path.join(".", f".env.{os.getenv('ENVIRONMENT', 'dev')}")
    print(f"🟢 Loading environment file: {env_file_path}", flush=True)

    model_config = SettingsConfigDict(
        env_file=env_file_path, 
        env_file_encoding='utf-8',
        extra='ignore'  # redis 등 사용하지 않는 환경변수 무시
    )

@lru_cache()
def get_settings():
    return Settings()