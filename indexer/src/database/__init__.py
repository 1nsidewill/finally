# src/database/__init__.py
"""
데이터베이스 연결 모듈

이 모듈은 PostgreSQL과 Qdrant 데이터베이스에 대한 
프로덕션 레디한 연결 관리자를 제공합니다.

Usage:
    from src.database import postgres_manager, qdrant_manager
    from src.database.postgresql import execute_query
    from src.database.qdrant import generate_embedding
"""

from .postgresql import postgres_manager, execute_query, execute_command
from .qdrant import qdrant_manager, generate_embedding, get_async_client, get_sync_client
from .redis import redis_manager

__all__ = [
    "postgres_manager",
    "qdrant_manager", 
    "execute_query",
    "execute_command",
    "generate_embedding",
    "get_async_client",
    "get_sync_client",
    "redis_manager"
]