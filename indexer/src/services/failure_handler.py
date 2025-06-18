# src/services/failure_handler.py
import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from enum import Enum
import traceback
from dataclasses import dataclass

from src.database.postgresql import postgres_manager

logger = logging.getLogger(__name__)

class OperationType(Enum):
    """작업 타입 정의"""
    SYNC = "sync"
    UPDATE = "update" 
    DELETE = "delete"
    EMBEDDING = "embedding"

class RetryStrategy(Enum):
    """재시도 전략 정의"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_INTERVAL = "fixed_interval"

@dataclass
class FailedOperation:
    """실패한 작업 정보"""
    id: Optional[int]
    operation_type: OperationType
    product_uid: int
    error_message: str
    error_details: Dict[str, Any]
    retry_count: int
    max_retries: int
    next_retry_at: datetime
    created_at: datetime
    last_attempted_at: datetime

class FailureHandler:
    """실패한 작업 로깅 및 재시도 메커니즘 처리"""
    
    def __init__(self, max_retries: int = 3, initial_delay: int = 60):
        self.max_retries = max_retries
        self.initial_delay = initial_delay  # 초 단위
        
    async def log_failure(
        self,
        operation_type: OperationType,
        product_uid: int,
        error: Exception,
        context: Optional[Dict[str, Any]] = None
    ) -> int:
        """실패한 작업을 데이터베이스에 로깅"""
        try:
            error_details = {
                "exception_type": type(error).__name__,
                "traceback": traceback.format_exc(),
                "context": context or {}
            }
            
            next_retry_at = datetime.now() + timedelta(seconds=self.initial_delay)
            
            query = """
                INSERT INTO failed_operations (
                    operation_type, product_uid, error_message, error_details,
                    retry_count, max_retries, next_retry_at, created_at, last_attempted_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING id
            """
            
            result = await postgres_manager.execute_single(
                query,
                operation_type.value,
                product_uid,
                str(error),
                json.dumps(error_details),
                0,
                self.max_retries,
                next_retry_at,
                datetime.now(),
                datetime.now()
            )
            
            failure_id = result['id'] if result else None
            logger.warning(
                f"실패한 작업 로깅됨 - ID: {failure_id}, "
                f"타입: {operation_type.value}, 제품: {product_uid}, "
                f"에러: {str(error)[:100]}"
            )
            
            return failure_id
            
        except Exception as log_error:
            logger.error(f"실패 로깅 중 오류 발생: {log_error}")
            raise

    async def get_retryable_operations(self, limit: int = 50) -> List[FailedOperation]:
        """재시도 가능한 실패 작업들 조회"""
        query = """
            SELECT id, operation_type, product_uid, error_message, error_details,
                   retry_count, max_retries, next_retry_at, created_at, last_attempted_at
            FROM failed_operations
            WHERE retry_count < max_retries
              AND next_retry_at <= NOW()
              AND resolved_at IS NULL
            ORDER BY next_retry_at ASC
            LIMIT $1
        """
        
        rows = await postgres_manager.execute_query(query, limit)
        
        operations = []
        for row in rows:
            operations.append(FailedOperation(
                id=row['id'],
                operation_type=OperationType(row['operation_type']),
                product_uid=row['product_uid'],
                error_message=row['error_message'],
                error_details=json.loads(row['error_details']),
                retry_count=row['retry_count'],
                max_retries=row['max_retries'],
                next_retry_at=row['next_retry_at'],
                created_at=row['created_at'],
                last_attempted_at=row['last_attempted_at']
            ))
        
        return operations

    async def update_retry_attempt(
        self,
        failure_id: int,
        success: bool = False,
        new_error: Optional[Exception] = None,
        strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    ):
        """재시도 시도 후 상태 업데이트"""
        if success:
            # 성공한 경우 resolved_at 설정
            query = """
                UPDATE failed_operations
                SET resolved_at = NOW(), last_attempted_at = NOW()
                WHERE id = $1
            """
            await postgres_manager.execute_command(query, failure_id)
            logger.info(f"실패 작업 {failure_id} 성공적으로 해결됨")
            
        else:
            # 실패한 경우 재시도 카운트 증가 및 다음 재시도 시간 계산
            operation = await self._get_operation_by_id(failure_id)
            if not operation:
                logger.error(f"실패 작업 {failure_id}를 찾을 수 없음")
                return
                
            new_retry_count = operation.retry_count + 1
            next_retry_at = self._calculate_next_retry(
                new_retry_count, strategy
            )
            
            error_details = operation.error_details.copy()
            if new_error:
                error_details['latest_error'] = {
                    "message": str(new_error),
                    "type": type(new_error).__name__,
                    "timestamp": datetime.now().isoformat()
                }
            
            query = """
                UPDATE failed_operations
                SET retry_count = $1, next_retry_at = $2, 
                    last_attempted_at = NOW(), error_details = $3,
                    error_message = $4
                WHERE id = $5
            """
            
            await postgres_manager.execute_command(
                query,
                new_retry_count,
                next_retry_at,
                json.dumps(error_details),
                str(new_error) if new_error else operation.error_message,
                failure_id
            )
            
            logger.warning(
                f"실패 작업 {failure_id} 재시도 실패 - "
                f"시도 횟수: {new_retry_count}/{operation.max_retries}, "
                f"다음 재시도: {next_retry_at}"
            )

    async def mark_permanently_failed(self, failure_id: int, reason: str):
        """영구 실패로 표시"""
        query = """
            UPDATE failed_operations
            SET retry_count = max_retries, last_attempted_at = NOW(),
                error_message = error_message || ' [PERMANENT FAILURE: ' || $1 || ']'
            WHERE id = $2
        """
        
        await postgres_manager.execute_command(query, reason, failure_id)
        logger.error(f"실패 작업 {failure_id} 영구 실패로 표시됨: {reason}")

    def _calculate_next_retry(
        self, 
        retry_count: int, 
        strategy: RetryStrategy
    ) -> datetime:
        """재시도 전략에 따른 다음 재시도 시간 계산"""
        if strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            # 지수 백오프: 60초, 120초, 240초, 480초...
            delay = self.initial_delay * (2 ** (retry_count - 1))
            # 최대 1시간으로 제한
            delay = min(delay, 3600)
            
        elif strategy == RetryStrategy.LINEAR_BACKOFF:
            # 선형 백오프: 60초, 120초, 180초, 240초...
            delay = self.initial_delay * retry_count
            
        else:  # FIXED_INTERVAL
            # 고정 간격: 60초마다
            delay = self.initial_delay
        
        return datetime.now() + timedelta(seconds=delay)

    async def _get_operation_by_id(self, failure_id: int) -> Optional[FailedOperation]:
        """ID로 실패 작업 조회"""
        query = """
            SELECT id, operation_type, product_uid, error_message, error_details,
                   retry_count, max_retries, next_retry_at, created_at, last_attempted_at
            FROM failed_operations
            WHERE id = $1
        """
        
        row = await postgres_manager.execute_single(query, failure_id)
        if not row:
            return None
            
        return FailedOperation(
            id=row['id'],
            operation_type=OperationType(row['operation_type']),
            product_uid=row['product_uid'],
            error_message=row['error_message'],
            error_details=json.loads(row['error_details']),
            retry_count=row['retry_count'],
            max_retries=row['max_retries'],
            next_retry_at=row['next_retry_at'],
            created_at=row['created_at'],
            last_attempted_at=row['last_attempted_at']
        )

    async def get_failure_stats(self) -> Dict[str, Any]:
        """실패 작업 통계 조회"""
        query = """
            SELECT 
                operation_type,
                COUNT(*) as total_failures,
                COUNT(CASE WHEN resolved_at IS NOT NULL THEN 1 END) as resolved,
                COUNT(CASE WHEN retry_count >= max_retries AND resolved_at IS NULL THEN 1 END) as permanent_failures,
                COUNT(CASE WHEN retry_count < max_retries AND resolved_at IS NULL THEN 1 END) as pending_retries,
                AVG(retry_count) as avg_retry_count
            FROM failed_operations
            GROUP BY operation_type
        """
        
        rows = await postgres_manager.execute_query(query)
        
        stats = {}
        for row in rows:
            stats[row['operation_type']] = {
                "total_failures": row['total_failures'],
                "resolved": row['resolved'],
                "permanent_failures": row['permanent_failures'],
                "pending_retries": row['pending_retries'],
                "avg_retry_count": float(row['avg_retry_count']) if row['avg_retry_count'] else 0
            }
        
        return stats

    async def cleanup_old_resolved_failures(self, days_old: int = 30):
        """해결된 오래된 실패 기록 정리"""
        query = """
            DELETE FROM failed_operations
            WHERE resolved_at IS NOT NULL 
              AND resolved_at < NOW() - INTERVAL '%s days'
        """
        
        result = await postgres_manager.execute_command(query % days_old)
        logger.info(f"오래된 해결된 실패 기록 정리 완료: {result}")

# 전역 인스턴스
failure_handler = FailureHandler() 