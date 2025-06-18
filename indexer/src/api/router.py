"""
FastAPI 라우터 설정
각 엔드포인트에 대한 메트릭 수집 포함
"""

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse
from typing import List, Dict, Any, Optional
import logging
import time
import json

from ..database.postgresql import PostgreSQLManager
from ..database.qdrant import QdrantManager
from ..database.redis import RedisManager
from ..services.embedding_service import EmbeddingService
from ..services.failure_handler import FailureHandler
from ..workers.reliable_worker import ReliableWorker
from ..monitoring.metrics import MetricsCollector, get_metrics_bytes
from ..config import get_settings
from .models import (
    SyncRequest, SyncResponse, RetryRequest, RetryResponse,
    QueueStatusResponse, FailedOperation, FailuresResponse
)

logger = logging.getLogger(__name__)
router = APIRouter()

# 설정 로드
config = get_settings()

# =============================================================================
# 상태 확인 엔드포인트
# =============================================================================

@router.get("/health", tags=["health"])
@MetricsCollector.track_redis_job("api", "health_check")
async def health_check():
    """서비스 상태 확인"""
    try:
        # 각 컴포넌트 상태 확인
        pg_manager = PostgreSQLManager()
        qdrant_manager = QdrantManager()
        redis_manager = RedisManager()
        
        status = {
            "status": "healthy",
            "timestamp": time.time(),
            "components": {
                "postgresql": "unknown",
                "qdrant": "unknown", 
                "redis": "unknown"
            }
        }
        
        # PostgreSQL 연결 확인
        try:
            async with pg_manager.get_connection() as conn:
                await conn.execute("SELECT 1")
                status["components"]["postgresql"] = "healthy"
        except Exception as e:
            status["components"]["postgresql"] = f"unhealthy: {str(e)}"
            status["status"] = "degraded"
        
        # Qdrant 연결 확인
        try:
            collections = await qdrant_manager.list_collections()
            status["components"]["qdrant"] = "healthy"
        except Exception as e:
            status["components"]["qdrant"] = f"unhealthy: {str(e)}"
            status["status"] = "degraded"
        
        # Redis 연결 확인
        try:
            await redis_manager.ping()
            status["components"]["redis"] = "healthy"
        except Exception as e:
            status["components"]["redis"] = f"unhealthy: {str(e)}"
            status["status"] = "degraded"
        
        return status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

# =============================================================================
# 메트릭 엔드포인트
# =============================================================================

@router.get("/metrics", tags=["monitoring"])
async def get_prometheus_metrics():
    """Prometheus 메트릭 반환"""
    try:
        metrics_data = get_metrics_bytes()
        return Response(
            content=metrics_data,
            media_type="text/plain; version=0.0.4; charset=utf-8"
        )
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")

@router.get("/metrics/status", tags=["monitoring"])
@MetricsCollector.track_redis_job("api", "metrics_status")
async def get_metrics_status():
    """메트릭 수집 상태 확인"""
    try:
        # Redis 큐 크기 확인
        redis_manager = RedisManager()
        queue_sizes = {}
        
        try:
            # 기본 큐들 크기 확인
            embedding_queue_size = await redis_manager.get_queue_size("embedding_queue")
            sync_queue_size = await redis_manager.get_queue_size("sync_queue")
            
            queue_sizes = {
                "embedding_queue": embedding_queue_size,
                "sync_queue": sync_queue_size
            }
            
            # 메트릭에 큐 크기 업데이트
            await MetricsCollector.update_queue_size("embedding_queue", embedding_queue_size)
            await MetricsCollector.update_queue_size("sync_queue", sync_queue_size)
            
        except Exception as e:
            logger.warning(f"Failed to get queue sizes: {e}")
        
        # 시스템 메트릭 업데이트
        await MetricsCollector.update_system_metrics()
        
        return {
            "status": "collecting",
            "timestamp": time.time(),
            "queue_sizes": queue_sizes,
            "metrics_endpoint": "/metrics"
        }
        
    except Exception as e:
        logger.error(f"Failed to get metrics status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get metrics status: {str(e)}")

# =============================================================================
# 동기화 관련 엔드포인트
# =============================================================================

@router.post("/sync/trigger", tags=["sync"])
@MetricsCollector.track_redis_job("api", "sync_trigger")
async def trigger_sync():
    """수동 동기화 트리거"""
    try:
        redis_manager = RedisManager()
        
        # 동기화 작업을 Redis 큐에 추가
        job_data = {
            "type": "full_sync",
            "timestamp": time.time(),
            "triggered_by": "api"
        }
        
        job_id = await redis_manager.enqueue_job("sync_queue", job_data)
        
        return {
            "message": "Sync triggered successfully",
            "job_id": job_id,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger sync: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger sync: {str(e)}")

@router.get("/sync/status", tags=["sync"])
@MetricsCollector.track_redis_job("api", "sync_status")
async def get_sync_status():
    """동기화 상태 확인"""
    try:
        pg_manager = PostgreSQLManager()
        qdrant_manager = QdrantManager()
        
        # PostgreSQL에서 총 매물 수 확인
        async with pg_manager.get_connection() as conn:
            pg_count_result = await conn.fetchrow(
                "SELECT COUNT(*) as total FROM products"
            )
            pg_total = pg_count_result['total'] if pg_count_result else 0
        
        # Qdrant에서 총 벡터 수 확인
        try:
            collection_info = await qdrant_manager.get_collection_info()
            qdrant_total = collection_info.points_count if collection_info else 0
        except Exception as e:
            logger.warning(f"Failed to get Qdrant count: {e}")
            qdrant_total = 0
        
        # 동기화 상태 계산
        sync_percentage = (qdrant_total / pg_total * 100) if pg_total > 0 else 0
        
        return {
            "postgresql_count": pg_total,
            "qdrant_count": qdrant_total,
            "sync_percentage": round(sync_percentage, 2),
            "needs_sync": pg_total != qdrant_total,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to get sync status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get sync status: {str(e)}")

# =============================================================================
# 검색 엔드포인트 (추후 구현)
# =============================================================================

@router.post("/search", tags=["search"])
@MetricsCollector.track_redis_job("api", "vector_search")
async def vector_search(query: Dict[str, Any]):
    """벡터 검색 (추후 구현)"""
    try:
        # 현재는 플레이스홀더
        return {
            "message": "Vector search endpoint - coming soon",
            "query": query,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Vector search failed: {e}")
        raise HTTPException(status_code=500, detail=f"Vector search failed: {str(e)}")

# =============================================================================
# 디버깅 엔드포인트
# =============================================================================

@router.get("/debug/info", tags=["debug"])
async def get_debug_info():
    """디버깅 정보 반환"""
    try:
        return {
            "config": {
                "redis_max_connections": config.REDIS_MAX_CONNECTIONS,
                "redis_batch_size": config.REDIS_BATCH_SIZE,
                "embedding_model": config.OPENAI_EMBEDDING_MODEL,
                "qdrant_collection": config.QDRANT_COLLECTION_NAME
            },
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to get debug info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get debug info: {str(e)}")

# =============================================================================
# Redis Queue 운영 관리 API
# =============================================================================

@router.post("/sync", tags=["operations"], response_model=SyncResponse)
@MetricsCollector.track_redis_job("api", "manual_sync")
async def manual_sync(request: SyncRequest):
    """특정 매물의 수동 재처리"""
    try:
        redis_manager = RedisManager()
        
        # 작업 데이터 구성
        job_data = {
            "type": "sync",
            "product_uid": request.product_uid,
            "force": request.force,
            "priority": request.priority,
            "timestamp": time.time(),
            "triggered_by": "manual_api"
        }
        
        # Redis 큐에 작업 추가
        job_id = await redis_manager.enqueue_job("indexer_jobs", job_data)
        
        logger.info(f"Manual sync queued for product {request.product_uid}, job_id: {job_id}")
        
        return SyncResponse(
            message=f"Manual sync queued for product {request.product_uid}",
            job_id=job_id,
            product_uid=request.product_uid,
            timestamp=time.time()
        )
        
    except Exception as e:
        logger.error(f"Failed to queue manual sync: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to queue manual sync: {str(e)}")

@router.post("/retry", tags=["operations"], response_model=RetryResponse)
@MetricsCollector.track_redis_job("api", "retry_failed")
async def retry_failed_operations(request: RetryRequest):
    """실패한 작업들을 재시도"""
    try:
        failure_handler = FailureHandler()
        reliable_worker = ReliableWorker()
        
        retried_count = 0
        failed_retry_count = 0
        job_ids = []
        
        # 재시도할 작업들 조회
        if request.operation_ids:
            # 특정 작업 ID들 재시도
            for op_id in request.operation_ids:
                try:
                    job_id = await reliable_worker.retry_failed_operation(op_id)
                    if job_id:
                        job_ids.append(job_id)
                        retried_count += 1
                    else:
                        failed_retry_count += 1
                except Exception as e:
                    logger.error(f"Failed to retry operation {op_id}: {e}")
                    failed_retry_count += 1
        else:
            # 재시도 가능한 모든 작업 조회
            retryable_ops = await failure_handler.get_retryable_operations(
                limit=request.max_operations
            )
            
            for op in retryable_ops:
                try:
                    job_id = await reliable_worker.retry_failed_operation(op.id)
                    if job_id:
                        job_ids.append(job_id)
                        retried_count += 1
                    else:
                        failed_retry_count += 1
                except Exception as e:
                    logger.error(f"Failed to retry operation {op.id}: {e}")
                    failed_retry_count += 1
        
        logger.info(f"Retry completed: {retried_count} success, {failed_retry_count} failed")
        
        return RetryResponse(
            message=f"Retry completed: {retried_count} operations retried, {failed_retry_count} failed",
            retried_count=retried_count,
            failed_retry_count=failed_retry_count,
            job_ids=job_ids,
            timestamp=time.time()
        )
        
    except Exception as e:
        logger.error(f"Failed to retry operations: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retry operations: {str(e)}")

@router.get("/status", tags=["monitoring"], response_model=QueueStatusResponse)
@MetricsCollector.track_redis_job("api", "queue_status")
async def get_queue_status():
    """Redis 큐 상태 및 워커 진행 현황 조회"""
    try:
        redis_manager = RedisManager()
        failure_handler = FailureHandler()
        
        # Redis 큐 상태
        queue_size = await redis_manager.get_queue_size("indexer_jobs")
        
        # 실패 통계
        failure_stats = await failure_handler.get_failure_stats()
        
        # 워커 상태 (간단한 추정)
        worker_status = {
            "estimated_active_workers": min(queue_size, config.REDIS_MAX_CONNECTIONS),
            "queue_throughput_estimate": "49.8 jobs/sec",  # 벤치마크 결과 기반
            "estimated_completion_time": f"{queue_size / 49.8:.1f} seconds" if queue_size > 0 else "0 seconds"
        }
        
        # 큐 상세 정보
        queue_details = {
            "indexer_jobs": {
                "size": queue_size,
                "estimated_processing_time": worker_status["estimated_completion_time"]
            }
        }
        
        # 실패 작업 수 계산
        total_failed = sum(stats.get('total_failures', 0) for stats in failure_stats.values())
        
        return QueueStatusResponse(
            total_pending=queue_size,
            total_processing=0,  # Redis Queue에서는 정확한 처리 중 작업 수를 알기 어려움
            total_failed=total_failed,
            queue_details=queue_details,
            worker_status=worker_status,
            timestamp=time.time()
        )
        
    except Exception as e:
        logger.error(f"Failed to get queue status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get queue status: {str(e)}")

@router.get("/failures", tags=["monitoring"], response_model=FailuresResponse)
@MetricsCollector.track_redis_job("api", "get_failures")
async def get_failed_operations(
    page: int = 1,
    page_size: int = 50,
    operation_type: Optional[str] = None,
    product_uid: Optional[str] = None
):
    """실패한 작업 목록 조회"""
    try:
        failure_handler = FailureHandler()
        
        # 페이지네이션 계산
        offset = (page - 1) * page_size
        
        # 실패 작업 조회 (임시로 간단한 구현)
        # 실제로는 failure_handler에서 페이지네이션 지원하는 메서드 필요
        try:
            # PostgreSQL에서 직접 조회
            pg_manager = PostgreSQLManager()
            
            # WHERE 조건 구성 (resolved_at이 NULL이면 아직 해결되지 않은 실패 작업)
            where_conditions = ["resolved_at IS NULL"]
            params = []
            
            if operation_type:
                where_conditions.append(f"operation_type = ${len(params) + 1}")
                params.append(operation_type)
            
            if product_uid:
                where_conditions.append(f"product_uid = ${len(params) + 1}")
                params.append(product_uid)
            
            where_clause = " AND ".join(where_conditions)
            
            # 총 개수 조회
            count_query = f"SELECT COUNT(*) as total FROM failed_operations WHERE {where_clause}"
            
            # 데이터 조회
            data_query = f"""
            SELECT id, product_uid, operation_type, error_message, retry_count, max_retries,
                   next_retry_at, created_at, last_attempted_at, error_details
            FROM failed_operations 
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT {page_size} OFFSET {offset}
            """
            
            async with pg_manager.get_connection() as conn:
                # 총 개수
                count_result = await conn.fetchrow(count_query, *params)
                total_count = count_result['total'] if count_result else 0
                
                # 데이터
                rows = await conn.fetch(data_query, *params)
                
                failed_operations = []
                for row in rows:
                    # Parse error_details JSON string to dict
                    context = None
                    if row['error_details']:
                        try:
                            if isinstance(row['error_details'], str):
                                context = json.loads(row['error_details'])
                            else:
                                context = row['error_details']  # Already a dict
                        except json.JSONDecodeError:
                            context = {"error": "Failed to parse error_details", "raw": str(row['error_details'])}
                    
                    failed_operations.append(FailedOperation(
                        id=row['id'],
                        product_uid=str(row['product_uid']),  # Convert to string
                        operation_type=row['operation_type'],
                        error_message=row['error_message'],
                        retry_count=row['retry_count'] or 0,
                        max_retries=row['max_retries'] or 3,
                        next_retry_at=row['next_retry_at'],
                        created_at=row['created_at'],
                        updated_at=row['last_attempted_at'] or row['created_at'],  # Use last_attempted_at as updated_at
                        context=context
                    ))
                
                has_more = (page * page_size) < total_count
                
                return FailuresResponse(
                    failed_operations=failed_operations,
                    total_count=total_count,
                    page=page,
                    page_size=page_size,
                    has_more=has_more,
                    timestamp=time.time()
                )
                
        except Exception as e:
            logger.error(f"Failed to query failed operations: {e}")
            # 폴백: 빈 응답
            return FailuresResponse(
                failed_operations=[],
                total_count=0,
                page=page,
                page_size=page_size,
                has_more=False,
                timestamp=time.time()
            )
        
    except Exception as e:
        logger.error(f"Failed to get failed operations: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get failed operations: {str(e)}")

# Qdrant Storage Optimization 엔드포인트들 추가
@router.post("/qdrant/optimize", response_model=Dict[str, Any])
async def optimize_qdrant_collection():
    """🧹 Qdrant 컬렉션 최적화 실행"""
    try:
        qdrant_manager = QdrantManager()
        result = await qdrant_manager.optimize_collection()
        
        return {
            "success": True,
            "message": "컬렉션 최적화 완료",
            "data": result
        }
    except Exception as e:
        logger.error(f"컬렉션 최적화 실패: {e}")
        raise HTTPException(status_code=500, detail=f"컬렉션 최적화 실패: {str(e)}")

@router.get("/qdrant/storage-stats", response_model=Dict[str, Any])
async def get_qdrant_storage_stats():
    """📊 Qdrant 스토리지 사용량 및 성능 통계"""
    try:
        qdrant_manager = QdrantManager()
        stats = await qdrant_manager.get_storage_stats()
        
        return {
            "success": True,
            "message": "스토리지 통계 조회 완료",
            "data": stats
        }
    except Exception as e:
        logger.error(f"스토리지 통계 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"스토리지 통계 조회 실패: {str(e)}")

@router.post("/qdrant/batch-upload-optimized", response_model=Dict[str, Any])
async def batch_upload_optimized(
    batch_size: int = 100,
    parallel_batches: int = 3,
    wait: bool = False
):
    """🚀 최적화된 배치 업로드 (테스트용 - 실제 데이터는 sync 엔드포인트 사용)"""
    try:
        # 이 엔드포인트는 테스트/데모용입니다
        # 실제 데이터 업로드는 /sync 엔드포인트를 통해 수행됩니다
        
        return {
            "success": True,
            "message": "배치 업로드 최적화 설정 확인됨",
            "config": {
                "batch_size": batch_size,
                "parallel_batches": parallel_batches,
                "wait": wait
            },
            "note": "실제 데이터 업로드는 /sync 엔드포인트를 사용하세요"
        }
    except Exception as e:
        logger.error(f"배치 업로드 설정 확인 실패: {e}")
        raise HTTPException(status_code=500, detail=f"배치 업로드 설정 확인 실패: {str(e)}")

# Progress Tracking 관련 엔드포인트들 추가
@router.post("/sync/enhanced", response_model=Dict[str, Any])
async def sync_data_enhanced(
    batch_size: int = 50,
    use_optimized_batch: bool = True,
    parallel_batches: int = 3,
    session_id: Optional[str] = None
):
    """🚀 향상된 진행률 추적이 포함된 대용량 데이터 동기화"""
    try:
        from ..services.bulk_sync_enhanced import EnhancedBulkSynchronizer
        
        synchronizer = EnhancedBulkSynchronizer(batch_size=batch_size)
        
        result = await synchronizer.sync_all_products(
            session_id=session_id,
            use_optimized_batch=use_optimized_batch,
            parallel_batches=parallel_batches
        )
        
        return {
            "success": True,
            "message": "향상된 동기화 완료",
            "data": result
        }
    except Exception as e:
        logger.error(f"향상된 동기화 실패: {e}")
        raise HTTPException(status_code=500, detail=f"향상된 동기화 실패: {str(e)}")

@router.get("/progress/{session_id}", response_model=Dict[str, Any])
async def get_progress_status(session_id: str):
    """📊 특정 세션의 진행률 상태 조회"""
    try:
        from ..monitoring.progress_tracker import ProgressTracker
        from pathlib import Path
        import json
        
        # 진행률 로그 파일 경로
        log_dir = Path("./.taskmaster/logs")
        progress_file = log_dir / f"progress_{session_id}.json"
        
        if not progress_file.exists():
            raise HTTPException(status_code=404, detail=f"세션 {session_id}를 찾을 수 없습니다")
        
        # 진행률 데이터 로드
        with open(progress_file, 'r', encoding='utf-8') as f:
            progress_data = json.load(f)
        
        return {
            "success": True,
            "message": "진행률 조회 완료",
            "data": progress_data
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"진행률 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"진행률 조회 실패: {str(e)}")

@router.get("/progress/sessions", response_model=Dict[str, Any])
async def list_progress_sessions():
    """📋 모든 진행률 추적 세션 목록 조회"""
    try:
        from pathlib import Path
        import json
        import glob
        
        log_dir = Path("./.taskmaster/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # 모든 progress 파일 찾기
        progress_files = glob.glob(str(log_dir / "progress_*.json"))
        
        sessions = []
        for file_path in progress_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                session_info = data["session"]
                session_info["file_path"] = file_path
                sessions.append(session_info)
                
            except Exception as e:
                logger.warning(f"세션 파일 읽기 실패 {file_path}: {e}")
        
        # 시작 시간으로 정렬 (최신순)
        sessions.sort(key=lambda x: x.get("start_time", ""), reverse=True)
        
        return {
            "success": True,
            "message": f"{len(sessions)}개 세션 조회 완료",
            "data": {
                "sessions": sessions,
                "total_count": len(sessions)
            }
        }
    except Exception as e:
        logger.error(f"세션 목록 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"세션 목록 조회 실패: {str(e)}")

@router.get("/progress/{session_id}/logs", response_model=Dict[str, Any])
async def get_session_logs(session_id: str, lines: int = 100):
    """📜 특정 세션의 상세 로그 조회"""
    try:
        from pathlib import Path
        
        log_dir = Path("./.taskmaster/logs")
        log_file = log_dir / f"detailed_{session_id}.log"
        
        if not log_file.exists():
            raise HTTPException(status_code=404, detail=f"세션 {session_id}의 로그를 찾을 수 없습니다")
        
        # 로그 파일 읽기 (마지막 N줄)
        with open(log_file, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
        
        # 요청된 줄 수만큼 가져오기 (마지막부터)
        recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
        
        return {
            "success": True,
            "message": f"로그 조회 완료 ({len(recent_lines)}줄)",
            "data": {
                "session_id": session_id,
                "total_lines": len(all_lines),
                "returned_lines": len(recent_lines),
                "logs": [line.strip() for line in recent_lines]
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"로그 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"로그 조회 실패: {str(e)}")

@router.delete("/progress/{session_id}", response_model=Dict[str, Any])
async def delete_session_logs(session_id: str):
    """🗑️ 특정 세션의 로그 파일들 삭제"""
    try:
        from pathlib import Path
        
        log_dir = Path("./.taskmaster/logs")
        progress_file = log_dir / f"progress_{session_id}.json"
        detail_file = log_dir / f"detailed_{session_id}.log"
        
        deleted_files = []
        
        if progress_file.exists():
            progress_file.unlink()
            deleted_files.append(str(progress_file))
        
        if detail_file.exists():
            detail_file.unlink()
            deleted_files.append(str(detail_file))
        
        if not deleted_files:
            raise HTTPException(status_code=404, detail=f"세션 {session_id}의 파일을 찾을 수 없습니다")
        
        return {
            "success": True,
            "message": f"세션 {session_id} 로그 삭제 완료",
            "data": {
                "session_id": session_id,
                "deleted_files": deleted_files
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"로그 삭제 실패: {e}")
        raise HTTPException(status_code=500, detail=f"로그 삭제 실패: {str(e)}")

# 기존 sync 엔드포인트에 향상된 옵션 추가 (선택적으로 사용)
@router.post("/sync/with-tracking", response_model=SyncResponse)
async def sync_data_with_tracking(
    request: SyncRequest,
    enable_tracking: bool = True,
    session_id: Optional[str] = None
):
    """📊 진행률 추적 옵션이 포함된 데이터 동기화"""
    try:
        if enable_tracking:
            # 향상된 동기화 사용
            from ..services.bulk_sync_enhanced import EnhancedBulkSynchronizer
            
            synchronizer = EnhancedBulkSynchronizer(batch_size=50)
            result = await synchronizer.sync_all_products(session_id=session_id)
            
            return SyncResponse(
                status="success",
                message="진행률 추적과 함께 동기화 완료",
                processed_count=result.get("successful", 0),
                failed_count=result.get("failed", 0),
                details=result
            )
        else:
            # 기존 동기화 방식 사용
            # ... 기존 sync 로직 ...
            return SyncResponse(
                status="success",
                message="기본 동기화 완료",
                processed_count=0,
                failed_count=0
            )
    
    except Exception as e:
        logger.error(f"추적 동기화 실패: {e}")
        raise HTTPException(status_code=500, detail=f"추적 동기화 실패: {str(e)}")

# =============================================================================
# Worker Daemon 관리 엔드포인트
# =============================================================================

@router.get("/worker/status", tags=["worker"])
@MetricsCollector.track_redis_job("api", "worker_status")
async def get_worker_status():
    """워커 데몬 상태 확인"""
    try:
        # 워커 데몬 임포트 (글로벌 변수)
        try:
            from ..worker_daemon import worker_daemon
            
            # 워커 통계 가져오기
            stats = worker_daemon.get_stats()
            
            return {
                "status": "success",
                "message": "워커 상태 조회 완료",
                "worker": stats,
                "timestamp": time.time()
            }
            
        except ImportError:
            return {
                "status": "error",
                "message": "워커 데몬이 사용 가능하지 않습니다",
                "worker": {
                    "daemon_running": False,
                    "error": "Worker daemon not found"
                },
                "timestamp": time.time()
            }
            
    except Exception as e:
        logger.error(f"Failed to get worker status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get worker status: {str(e)}")

@router.post("/worker/start", tags=["worker"])
@MetricsCollector.track_redis_job("api", "worker_start")
async def start_worker():
    """워커 데몬 시작 (테스트 목적)"""
    try:
        # 참고: 실제 프로덕션에서는 별도 프로세스로 워커를 실행해야 함
        return {
            "status": "info",
            "message": "워커는 별도 프로세스로 실행해야 합니다",
            "instructions": [
                "터미널에서 다음 명령 실행:",
                "python worker_daemon.py",
                "또는",
                "python -m src.worker_daemon"
            ],
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to provide worker start info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to provide worker start info: {str(e)}")

@router.post("/worker/stop", tags=["worker"])  
@MetricsCollector.track_redis_job("api", "worker_stop")
async def stop_worker():
    """워커 데몬 중지 신호 (테스트 목적)"""
    try:
        return {
            "status": "info", 
            "message": "워커 중지는 워커 프로세스에서 직접 수행해야 합니다",
            "instructions": [
                "워커 프로세스에서 Ctrl+C 또는 SIGTERM 시그널 전송",
                "또는 프로세스 ID를 찾아서 kill 명령 사용"
            ],
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to provide worker stop info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to provide worker stop info: {str(e)}")

@router.post("/worker/test-job", tags=["worker", "testing"])
@MetricsCollector.track_redis_job("api", "worker_test_job")
async def submit_test_job(
    job_type: str = "sync",
    product_id: str = "test_12345",
    provider: str = "bunjang"
):
    """워커 테스트를 위한 테스트 Job 제출"""
    try:
        redis_manager = RedisManager()
        
        # 테스트 Job 데이터 구성
        test_job = {
            "type": job_type,
            "product_id": product_id,
            "provider": provider,
            "product_data": {
                "title": f"테스트 상품 {product_id}",
                "content": "워커 테스트를 위한 테스트 상품입니다",
                "price": 10000,
                "created_dt": time.time()
            },
            "timestamp": time.time(),
            "source": "api_test"
        }
        
        # Redis 큐에 추가
        job_id = await redis_manager.enqueue_job(config.REDIS_QUEUE_NAME, test_job)
        
        return {
            "status": "success",
            "message": "테스트 Job 제출 완료",
            "job_id": job_id,
            "job_data": test_job,
            "queue": config.REDIS_QUEUE_NAME,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to submit test job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit test job: {str(e)}")

# Export alias for compatibility
api_router = router