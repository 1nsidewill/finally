"""
Prometheus 메트릭 수집 시스템
Redis Queue Worker와 Embedding Service의 성능을 모니터링합니다.
"""

import time
import functools
from typing import Optional, Dict, Any, Callable
from contextlib import asynccontextmanager
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest
import logging

logger = logging.getLogger(__name__)

# 전역 레지스트리 (테스트에서 격리 가능)
REGISTRY = CollectorRegistry()

# =============================================================================
# Redis Queue Worker 메트릭
# =============================================================================

# 작업 처리 카운터
REDIS_JOBS_TOTAL = Counter(
    'redis_jobs_total',
    'Total number of Redis jobs processed',
    ['queue_name', 'status'],  # status: processed, failed, retried
    registry=REGISTRY
)

# 작업 처리 시간 히스토그램
REDIS_JOB_DURATION = Histogram(
    'redis_job_duration_seconds',
    'Time spent processing Redis jobs',
    ['queue_name', 'job_type'],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float('inf')),
    registry=REGISTRY
)

# 큐 크기 게이지
REDIS_QUEUE_SIZE = Gauge(
    'redis_queue_size',
    'Current number of jobs in Redis queue',
    ['queue_name'],
    registry=REGISTRY
)

# Redis 연결 풀 메트릭
REDIS_POOL_CONNECTIONS = Gauge(
    'redis_pool_connections_current',
    'Current number of Redis pool connections',
    ['pool_type'],  # pool_type: active, idle
    registry=REGISTRY
)

# =============================================================================
# Embedding Service 메트릭
# =============================================================================

# 임베딩 생성 카운터
EMBEDDINGS_GENERATED_TOTAL = Counter(
    'embeddings_generated_total',
    'Total number of embeddings generated',
    ['model', 'status'],  # status: success, failed
    registry=REGISTRY
)

# 임베딩 생성 시간
EMBEDDING_GENERATION_DURATION = Histogram(
    'embedding_generation_duration_seconds',
    'Time spent generating embeddings',
    ['model'],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, float('inf')),
    registry=REGISTRY
)

# 배치 크기 히스토그램
EMBEDDING_BATCH_SIZE = Histogram(
    'embedding_batch_size',
    'Number of items processed in each embedding batch',
    buckets=(1, 5, 10, 20, 30, 50, 100, 200, float('inf')),
    registry=REGISTRY
)

# =============================================================================
# Database 메트릭
# =============================================================================

# DB 연결 풀 메트릭
DB_POOL_CONNECTIONS = Gauge(
    'db_pool_connections_current',
    'Current number of database pool connections',
    ['database', 'pool_type'],  # database: postgresql, qdrant
    registry=REGISTRY
)

# DB 쿼리 시간
DB_QUERY_DURATION = Histogram(
    'db_query_duration_seconds',
    'Time spent executing database queries',
    ['database', 'operation'],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, float('inf')),
    registry=REGISTRY
)

# DB 쿼리 카운터
DB_QUERIES_TOTAL = Counter(
    'db_queries_total',
    'Total number of database queries',
    ['database', 'operation', 'status'],
    registry=REGISTRY
)

# =============================================================================
# 시스템 메트릭
# =============================================================================

# 메모리 사용량
MEMORY_USAGE_BYTES = Gauge(
    'memory_usage_bytes',
    'Current memory usage in bytes',
    ['type'],  # type: rss, vms, percent
    registry=REGISTRY
)

# CPU 사용률
CPU_USAGE_PERCENT = Gauge(
    'cpu_usage_percent',
    'Current CPU usage percentage',
    registry=REGISTRY
)

# =============================================================================
# 메트릭 수집 유틸리티
# =============================================================================

class MetricsCollector:
    """메트릭 수집을 위한 유틸리티 클래스"""
    
    @staticmethod
    def track_redis_job(queue_name: str, job_type: str):
        """Redis 작업 처리 시간을 추적하는 데코레이터"""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                status = 'processed'
                
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    status = 'failed'
                    logger.error(f"Redis job failed: {e}")
                    raise
                finally:
                    duration = time.time() - start_time
                    REDIS_JOB_DURATION.labels(
                        queue_name=queue_name,
                        job_type=job_type
                    ).observe(duration)
                    REDIS_JOBS_TOTAL.labels(
                        queue_name=queue_name,
                        status=status
                    ).inc()
                    
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                status = 'processed'
                
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    status = 'failed'
                    logger.error(f"Redis job failed: {e}")
                    raise
                finally:
                    duration = time.time() - start_time
                    REDIS_JOB_DURATION.labels(
                        queue_name=queue_name,
                        job_type=job_type
                    ).observe(duration)
                    REDIS_JOBS_TOTAL.labels(
                        queue_name=queue_name,
                        status=status
                    ).inc()
                    
            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
        return decorator
    
    @staticmethod
    def track_embedding_generation(model: str):
        """임베딩 생성 시간을 추적하는 데코레이터"""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                status = 'success'
                
                try:
                    result = await func(*args, **kwargs)
                    
                    # 배치 크기 기록 (result가 리스트인 경우)
                    if isinstance(result, list):
                        EMBEDDING_BATCH_SIZE.observe(len(result))
                    
                    return result
                except Exception as e:
                    status = 'failed'
                    logger.error(f"Embedding generation failed: {e}")
                    raise
                finally:
                    duration = time.time() - start_time
                    EMBEDDING_GENERATION_DURATION.labels(model=model).observe(duration)
                    EMBEDDINGS_GENERATED_TOTAL.labels(model=model, status=status).inc()
                    
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                status = 'success'
                
                try:
                    result = func(*args, **kwargs)
                    
                    # 배치 크기 기록
                    if isinstance(result, list):
                        EMBEDDING_BATCH_SIZE.observe(len(result))
                    
                    return result
                except Exception as e:
                    status = 'failed'
                    logger.error(f"Embedding generation failed: {e}")
                    raise
                finally:
                    duration = time.time() - start_time
                    EMBEDDING_GENERATION_DURATION.labels(model=model).observe(duration)
                    EMBEDDINGS_GENERATED_TOTAL.labels(model=model, status=status).inc()
                    
            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
        return decorator
    
    @staticmethod
    def track_db_query(database: str, operation: str):
        """데이터베이스 쿼리 시간을 추적하는 데코레이터"""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                status = 'success'
                
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    status = 'failed'
                    logger.error(f"DB query failed: {e}")
                    raise
                finally:
                    duration = time.time() - start_time
                    DB_QUERY_DURATION.labels(
                        database=database,
                        operation=operation
                    ).observe(duration)
                    DB_QUERIES_TOTAL.labels(
                        database=database,
                        operation=operation,
                        status=status
                    ).inc()
                    
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                status = 'success'
                
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    status = 'failed'
                    logger.error(f"DB query failed: {e}")
                    raise
                finally:
                    duration = time.time() - start_time
                    DB_QUERY_DURATION.labels(
                        database=database,
                        operation=operation
                    ).observe(duration)
                    DB_QUERIES_TOTAL.labels(
                        database=database,
                        operation=operation,
                        status=status
                    ).inc()
                    
            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
        return decorator

    @staticmethod
    async def update_queue_size(queue_name: str, size: int):
        """큐 크기 업데이트"""
        REDIS_QUEUE_SIZE.labels(queue_name=queue_name).set(size)
    
    @staticmethod
    async def update_pool_connections(pool_type: str, active: int, idle: int = None):
        """연결 풀 상태 업데이트"""
        REDIS_POOL_CONNECTIONS.labels(pool_type='active').set(active)
        if idle is not None:
            REDIS_POOL_CONNECTIONS.labels(pool_type='idle').set(idle)
    
    @staticmethod
    async def update_system_metrics():
        """시스템 메트릭 업데이트 (psutil 필요)"""
        try:
            import psutil
            
            # 메모리 정보
            memory = psutil.virtual_memory()
            MEMORY_USAGE_BYTES.labels(type='rss').set(memory.used)
            MEMORY_USAGE_BYTES.labels(type='percent').set(memory.percent)
            
            # CPU 정보
            cpu_percent = psutil.cpu_percent()
            CPU_USAGE_PERCENT.set(cpu_percent)
            
        except ImportError:
            logger.warning("psutil not installed, system metrics not available")
        except Exception as e:
            logger.error(f"Failed to update system metrics: {e}")

# =============================================================================
# 메트릭 내보내기
# =============================================================================

def get_metrics() -> str:
    """Prometheus 형식으로 메트릭 반환"""
    return generate_latest(REGISTRY).decode('utf-8')

def get_metrics_bytes() -> bytes:
    """Prometheus 형식으로 메트릭 반환 (바이트)"""
    return generate_latest(REGISTRY)

# =============================================================================
# 초기화
# =============================================================================

import asyncio

# asyncio import 추가
__all__ = [
    'REGISTRY',
    'MetricsCollector',
    'get_metrics',
    'get_metrics_bytes',
    # 개별 메트릭들
    'REDIS_JOBS_TOTAL',
    'REDIS_JOB_DURATION',
    'REDIS_QUEUE_SIZE',
    'REDIS_POOL_CONNECTIONS',
    'EMBEDDINGS_GENERATED_TOTAL',
    'EMBEDDING_GENERATION_DURATION',
    'EMBEDDING_BATCH_SIZE',
    'DB_POOL_CONNECTIONS',
    'DB_QUERY_DURATION',
    'DB_QUERIES_TOTAL',
    'MEMORY_USAGE_BYTES',
    'CPU_USAGE_PERCENT',
] 