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

    # Redis Connection Info
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_PASSWORD: Optional[str] = None
    REDIS_DB: Optional[int] = 0
    REDIS_URL: Optional[str] = None  # 전체 URL로 설정할 경우 사용
    
    # Redis 연결 풀 설정 (벤치마크 최적화됨: 20 → 50으로 12.7% 성능 향상)
    REDIS_MAX_CONNECTIONS: Optional[int] = 50
    REDIS_CONNECTION_TIMEOUT: Optional[float] = 5.0
    REDIS_RETRY_ON_TIMEOUT: Optional[bool] = True
    
    # Redis 큐 설정
    REDIS_QUEUE_NAME: Optional[str] = "indexer_jobs"
    REDIS_BATCH_SIZE: Optional[int] = 30  # 벤치마크 결과 기반 최적화 (10 → 30): 300 jobs/sec, 8.74ms 지연
    REDIS_POLL_INTERVAL: Optional[float] = 1.0  # 초 단위
    REDIS_BLOCKING_TIMEOUT: Optional[int] = 5   # 블로킹 대기 시간(초)

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
        env_file_encoding='utf-8'
    )

@lru_cache()
def get_settings():
    return Settings()