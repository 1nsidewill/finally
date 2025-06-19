"""
Robust Error Handling and Logging Service

Redis Queue Worker의 강력한 오류 처리, 로깅, 실패 작업 추적 모듈
"""

import asyncio
import logging
import json
import traceback
import uuid
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime, timezone
import sys

from src.config import get_settings
from src.database.postgresql import PostgreSQLManager

logger = logging.getLogger(__name__)

class ErrorSeverity(Enum):
    """오류 심각도 레벨"""
    LOW = "low"           # 재시도 가능한 일시적 오류
    MEDIUM = "medium"     # 데이터 관련 오류 (복구 가능)
    HIGH = "high"         # 시스템 오류 (즉시 대응 필요)
    CRITICAL = "critical" # 서비스 중단 위험

class ErrorCategory(Enum):
    """오류 카테고리"""
    DATABASE = "database"           # PostgreSQL/Qdrant 연결/쿼리 오류
    NETWORK = "network"             # API 호출, 네트워크 연결 오류
    VALIDATION = "validation"       # 데이터 검증 오류
    PROCESSING = "processing"       # 데이터 처리/변환 오류
    AUTHENTICATION = "authentication" # API 키, 인증 오류
    RATE_LIMIT = "rate_limit"       # API 속도 제한
    UNKNOWN = "unknown"             # 분류되지 않은 오류

@dataclass
class ErrorContext:
    """오류 컨텍스트 정보"""
    job_id: str
    job_type: str
    product_id: str
    operation_step: str  # 어떤 단계에서 오류 발생
    additional_data: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.additional_data is None:
            self.additional_data = {}

@dataclass
class FailedOperation:
    """실패한 작업 기록"""
    id: str
    job_id: str
    job_type: str
    product_id: str
    error_category: ErrorCategory
    error_severity: ErrorSeverity
    error_message: str
    error_details: str
    operation_step: str
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = None
    last_retry_at: datetime = None
    resolved_at: datetime = None
    additional_data: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.additional_data is None:
            self.additional_data = {}

class ErrorHandler:
    """강력한 오류 처리 및 로깅 시스템"""
    
    def __init__(self):
        self.settings = get_settings()
        self.postgresql_manager = None
        
        # 오류 통계
        self.error_stats = {
            'total_errors': 0,
            'by_category': {cat.value: 0 for cat in ErrorCategory},
            'by_severity': {sev.value: 0 for sev in ErrorSeverity},
            'retry_success_count': 0,
            'permanent_failures': 0,
        }
        
        # 로깅 설정
        self._setup_logging()
    
    def _setup_logging(self):
        """구조화된 로깅 설정"""
        # JSON 로거 생성
        self.error_logger = logging.getLogger('indexer.errors')
        self.error_logger.setLevel(logging.ERROR)
        
        # 파일 핸들러 (오류 전용)
        error_handler = logging.FileHandler('logs/errors.log')
        error_handler.setLevel(logging.ERROR)
        
        # JSON 포맷터
        json_formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
            '"logger": "%(name)s", "message": %(message)s}'
        )
        error_handler.setFormatter(json_formatter)
        
        self.error_logger.addHandler(error_handler)
        
        # 성능 로거
        self.perf_logger = logging.getLogger('indexer.performance')
        perf_handler = logging.FileHandler('logs/performance.log')
        perf_handler.setFormatter(json_formatter)
        self.perf_logger.addHandler(perf_handler)
    
    async def initialize(self):
        """데이터베이스 매니저 초기화"""
        try:
            # PostgreSQLManager가 없으면 새로 생성
            if self.postgresql_manager is None:
                self.postgresql_manager = PostgreSQLManager()
                # PostgreSQL은 lazy loading이므로 pool 생성 테스트
                await self.postgresql_manager.get_pool()
            
            # failed_operations 테이블 생성 (존재하지 않으면)
            await self._ensure_failed_operations_table()
            
            logger.info("✅ ErrorHandler 초기화 완료")
            
        except Exception as e:
            logger.error(f"❌ ErrorHandler 초기화 실패: {e}")
            raise
    
    async def close(self):
        """리소스 정리"""
        if self.postgresql_manager:
            await self.postgresql_manager.close()
        logger.info("🔹 ErrorHandler 리소스 정리 완료")
    
    async def _ensure_failed_operations_table(self):
        """failed_operations 테이블 생성 - failure_handler.py와 호환되는 스키마"""
        try:
            async with self.postgresql_manager.get_connection() as conn:
                create_table_sql = """
                    CREATE TABLE IF NOT EXISTS failed_operations (
                        id SERIAL PRIMARY KEY,
                        operation_type VARCHAR(50) NOT NULL,
                        product_uid INTEGER NOT NULL,
                        error_message TEXT NOT NULL,
                        error_details JSONB,
                        retry_count INTEGER DEFAULT 0,
                        max_retries INTEGER DEFAULT 3,
                        next_retry_at TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_attempted_at TIMESTAMP WITH TIME ZONE,
                        resolved_at TIMESTAMP WITH TIME ZONE
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_failed_ops_product_uid ON failed_operations(product_uid);
                    CREATE INDEX IF NOT EXISTS idx_failed_ops_operation_type ON failed_operations(operation_type);
                    CREATE INDEX IF NOT EXISTS idx_failed_ops_created_at ON failed_operations(created_at);
                    CREATE INDEX IF NOT EXISTS idx_failed_ops_retry ON failed_operations(retry_count, next_retry_at);
                """
                
                await conn.execute(create_table_sql)
                logger.debug("🔧 failed_operations 테이블 확인/생성 완료")
            
        except Exception as e:
            logger.error(f"❌ failed_operations 테이블 생성 실패: {e}")
            raise
    
    async def handle_error(self, 
                          exception: Exception, 
                          context: ErrorContext,
                          auto_categorize: bool = True) -> FailedOperation:
        """오류 처리 및 기록"""
        try:
            # 오류 분류
            category, severity = self._categorize_error(exception) if auto_categorize else (ErrorCategory.UNKNOWN, ErrorSeverity.MEDIUM)
            
            # 오류 세부 정보 수집
            error_details = self._extract_error_details(exception)
            
            # FailedOperation 객체 생성
            failed_op = FailedOperation(
                id=str(uuid.uuid4()),
                job_id=context.job_id,
                job_type=context.job_type,
                product_id=context.product_id,
                error_category=category,
                error_severity=severity,
                error_message=str(exception),
                error_details=error_details,
                operation_step=context.operation_step,
                additional_data=context.additional_data
            )
            
            # 데이터베이스에 기록
            await self._record_failed_operation(failed_op)
            
            # 구조화된 로깅
            await self._log_error(failed_op, exception)
            
            # 통계 업데이트
            self._update_error_stats(category, severity)
            
            logger.error(f"❌ 오류 처리 완료: {context.job_id} - {category.value}({severity.value})")
            
            return failed_op
            
        except Exception as e:
            # 오류 처리 중 오류 발생 - 로그만 남기고 계속 진행
            logger.critical(f"🚨 오류 처리 중 예외 발생: {e}")
            traceback.print_exc()
            
            # 최소한의 기본 객체 반환
            return FailedOperation(
                id=str(uuid.uuid4()),
                job_id=context.job_id,
                job_type=context.job_type,
                product_id=context.product_id,
                error_category=ErrorCategory.UNKNOWN,
                error_severity=ErrorSeverity.HIGH,
                error_message=str(exception),
                error_details="오류 처리 중 예외 발생",
                operation_step=context.operation_step
            )
    
    def _categorize_error(self, exception: Exception) -> tuple[ErrorCategory, ErrorSeverity]:
        """오류 자동 분류"""
        error_msg = str(exception).lower()
        error_type = type(exception).__name__
        
        # 데이터베이스 관련 오류
        if any(keyword in error_msg for keyword in ['connection', 'database', 'postgresql', 'qdrant', 'sql']):
            return ErrorCategory.DATABASE, ErrorSeverity.HIGH
        
        # 네트워크 관련 오류
        if any(keyword in error_msg for keyword in ['network', 'timeout', 'connection refused', 'dns']):
            return ErrorCategory.NETWORK, ErrorSeverity.MEDIUM
        
        # 인증 관련 오류
        if any(keyword in error_msg for keyword in ['api key', 'authentication', 'unauthorized', 'forbidden']):
            return ErrorCategory.AUTHENTICATION, ErrorSeverity.HIGH
        
        # 속도 제한 오류
        if any(keyword in error_msg for keyword in ['rate limit', 'too many requests', 'quota']):
            return ErrorCategory.RATE_LIMIT, ErrorSeverity.LOW
        
        # 데이터 검증 오류
        if any(keyword in error_msg for keyword in ['validation', 'invalid', 'missing', 'required']):
            return ErrorCategory.VALIDATION, ErrorSeverity.MEDIUM
        
        # 타입별 분류
        if error_type in ['ValueError', 'TypeError', 'KeyError']:
            return ErrorCategory.VALIDATION, ErrorSeverity.MEDIUM
        elif error_type in ['ConnectionError', 'TimeoutError']:
            return ErrorCategory.NETWORK, ErrorSeverity.MEDIUM
        else:
            return ErrorCategory.UNKNOWN, ErrorSeverity.MEDIUM
    
    def _extract_error_details(self, exception: Exception) -> str:
        """오류 세부 정보 추출"""
        details = {
            'exception_type': type(exception).__name__,
            'exception_args': exception.args,
            'traceback': traceback.format_exc(),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # 특정 예외 타입별 추가 정보
        if hasattr(exception, 'response'):
            details['http_status'] = getattr(exception.response, 'status_code', None)
            details['http_response'] = getattr(exception.response, 'text', None)
        
        return json.dumps(details, ensure_ascii=False, indent=2)
    
    async def _record_failed_operation(self, failed_op: FailedOperation):
        """failed_operations 테이블에 기록"""
        try:
            conn = await self.postgresql_manager.get_connection()
            
            insert_sql = """
                INSERT INTO failed_operations (
                    id, job_id, job_type, product_id, error_category, error_severity,
                    error_message, error_details, operation_step, retry_count, max_retries,
                    created_at, additional_data
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            """
            
            await conn.execute(
                insert_sql,
                failed_op.id,
                failed_op.job_id,
                failed_op.job_type,
                failed_op.product_id,
                failed_op.error_category.value,
                failed_op.error_severity.value,
                failed_op.error_message,
                failed_op.error_details,
                failed_op.operation_step,
                failed_op.retry_count,
                failed_op.max_retries,
                failed_op.created_at,
                json.dumps(failed_op.additional_data)
            )
            
            logger.debug(f"📝 실패 작업 기록 완료: {failed_op.id}")
            
        except Exception as e:
            logger.error(f"❌ 실패 작업 기록 실패: {e}")
            # 데이터베이스 기록 실패해도 처리는 계속
    
    async def _log_error(self, failed_op: FailedOperation, exception: Exception):
        """구조화된 오류 로깅"""
        log_data = {
            'failed_operation_id': failed_op.id,
            'job_id': failed_op.job_id,
            'job_type': failed_op.job_type,
            'product_id': failed_op.product_id,
            'error_category': failed_op.error_category.value,
            'error_severity': failed_op.error_severity.value,
            'error_message': failed_op.error_message,
            'operation_step': failed_op.operation_step,
            'retry_count': failed_op.retry_count,
            'exception_type': type(exception).__name__,
            'timestamp': failed_op.created_at.isoformat(),
            'additional_data': failed_op.additional_data
        }
        
        # JSON 로그 기록
        self.error_logger.error(json.dumps(log_data, ensure_ascii=False))
        
        # 심각도별 추가 로깅
        if failed_op.error_severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            logger.critical(f"🚨 심각한 오류 발생: {failed_op.job_id} - {failed_op.error_message}")
    
    def _update_error_stats(self, category: ErrorCategory, severity: ErrorSeverity):
        """오류 통계 업데이트"""
        self.error_stats['total_errors'] += 1
        self.error_stats['by_category'][category.value] += 1
        self.error_stats['by_severity'][severity.value] += 1
    
    async def retry_failed_operation(self, failed_operation_id: str) -> bool:
        """실패한 작업 재시도"""
        try:
            conn = await self.postgresql_manager.get_connection()
            
            # 실패한 작업 조회
            select_sql = """
                SELECT * FROM failed_operations 
                WHERE id = $1 AND resolved_at IS NULL
            """
            result = await conn.fetch(select_sql, failed_operation_id)
            
            if not result:
                logger.warning(f"⚠️ 재시도할 실패 작업을 찾을 수 없음: {failed_operation_id}")
                return False
            
            failed_op_data = result[0]
            
            # 최대 재시도 횟수 확인
            if failed_op_data['retry_count'] >= failed_op_data['max_retries']:
                logger.warning(f"⚠️ 최대 재시도 횟수 초과: {failed_operation_id}")
                await self._mark_permanent_failure(failed_operation_id)
                return False
            
            # 재시도 횟수 증가
            update_sql = """
                UPDATE failed_operations 
                SET retry_count = retry_count + 1, last_retry_at = NOW()
                WHERE id = $1
            """
            await conn.execute(update_sql, failed_operation_id)
            
            logger.info(f"🔄 실패 작업 재시도 준비: {failed_operation_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 재시도 처리 실패: {e}")
            return False
    
    async def _mark_permanent_failure(self, failed_operation_id: str):
        """영구 실패로 표시"""
        try:
            conn = await self.postgresql_manager.get_connection()
            
            update_sql = """
                UPDATE failed_operations 
                SET resolved_at = NOW()
                WHERE id = $1
            """
            await conn.execute(update_sql, failed_operation_id)
            
            self.error_stats['permanent_failures'] += 1
            logger.warning(f"⚠️ 영구 실패로 표시: {failed_operation_id}")
            
        except Exception as e:
            logger.error(f"❌ 영구 실패 표시 실패: {e}")
    
    async def mark_resolved(self, failed_operation_id: str):
        """실패 작업을 해결됨으로 표시"""
        try:
            conn = await self.postgresql_manager.get_connection()
            
            update_sql = """
                UPDATE failed_operations 
                SET resolved_at = NOW()
                WHERE id = $1
            """
            await conn.execute(update_sql, failed_operation_id)
            
            self.error_stats['retry_success_count'] += 1
            logger.info(f"✅ 실패 작업 해결 완료: {failed_operation_id}")
            
        except Exception as e:
            logger.error(f"❌ 해결 표시 실패: {e}")
    
    async def get_failed_operations(self, 
                                   limit: int = 100,
                                   category: Optional[ErrorCategory] = None,
                                   severity: Optional[ErrorSeverity] = None,
                                   unresolved_only: bool = True) -> List[Dict[str, Any]]:
        """실패한 작업 목록 조회"""
        try:
            conn = await self.postgresql_manager.get_connection()
            
            conditions = []
            params = []
            param_counter = 1
            
            if unresolved_only:
                conditions.append("resolved_at IS NULL")
            
            if category:
                conditions.append(f"error_category = ${param_counter}")
                params.append(category.value)
                param_counter += 1
            
            if severity:
                conditions.append(f"error_severity = ${param_counter}")
                params.append(severity.value)
                param_counter += 1
            
            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            
            select_sql = f"""
                SELECT * FROM failed_operations 
                {where_clause}
                ORDER BY created_at DESC 
                LIMIT ${param_counter}
            """
            params.append(limit)
            
            results = await conn.fetch(select_sql, *params)
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"❌ 실패 작업 조회 실패: {e}")
            return []
    
    def get_error_stats(self) -> Dict[str, Any]:
        """오류 통계 반환"""
        total = self.error_stats['total_errors']
        
        return {
            **self.error_stats,
            'error_rate_by_category': {
                cat: (count / total * 100) if total > 0 else 0
                for cat, count in self.error_stats['by_category'].items()
            },
            'retry_success_rate': (
                self.error_stats['retry_success_count'] / total * 100
                if total > 0 else 0
            )
        }
    
    def reset_stats(self):
        """통계 초기화"""
        for key in self.error_stats:
            if isinstance(self.error_stats[key], dict):
                for subkey in self.error_stats[key]:
                    self.error_stats[key][subkey] = 0
            else:
                self.error_stats[key] = 0 