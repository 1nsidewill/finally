# src/workers/reliable_worker.py
import asyncio
import logging
import json
from typing import Dict, Any, Callable, Optional
from contextlib import asynccontextmanager
import uuid

from src.services.failure_handler import (
    failure_handler, 
    OperationType, 
    RetryStrategy
)
from src.database.postgresql import postgres_manager
from src.database.qdrant import qdrant_manager

logger = logging.getLogger(__name__)

class ReliableWorker:
    """실패 처리가 통합된 안정적인 작업 처리 워커"""
    
    def __init__(self, retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF):
        self.retry_strategy = retry_strategy
        self.processing = False
        
    @asynccontextmanager
    async def failure_context(
        self, 
        operation_type: OperationType, 
        product_uid: int,
        context: Optional[Dict[str, Any]] = None
    ):
        """실패 처리 컨텍스트 매니저"""
        failure_id = None
        try:
            yield
        except Exception as error:
            # 실패 로깅
            failure_id = await failure_handler.log_failure(
                operation_type=operation_type,
                product_uid=product_uid,
                error=error,
                context=context
            )
            logger.error(
                f"작업 실패 (ID: {failure_id}) - "
                f"타입: {operation_type.value}, 제품: {product_uid}, "
                f"에러: {str(error)}"
            )
            raise
    
    async def safe_sync_operation(self, product_uid: int, product_data: Dict[str, Any]) -> bool:
        """안전한 동기화 작업"""
        context = {
            "operation": "sync_to_qdrant",
            "product_data_keys": list(product_data.keys())
        }
        
        async with self.failure_context(OperationType.SYNC, product_uid, context):
            # 실제 동기화 로직 (Qdrant에 데이터 업서트)
            await qdrant_manager.upsert_product(product_uid, product_data)
            
            # PostgreSQL에서 is_conversion = True로 업데이트
            await postgres_manager.update_conversion_status([product_uid], True)
            
            logger.info(f"제품 {product_uid} 동기화 성공")
            return True
    
    async def safe_update_operation(self, product_uid: int, updates: Dict[str, Any]) -> bool:
        """안전한 업데이트 작업"""
        context = {
            "operation": "update_qdrant", 
            "updates": updates
        }
        
        async with self.failure_context(OperationType.UPDATE, product_uid, context):
            # Qdrant에서 데이터 업데이트
            await qdrant_manager.update_product(product_uid, updates)
            
            logger.info(f"제품 {product_uid} 업데이트 성공")
            return True
    
    async def safe_delete_operation(self, product_uid: int) -> bool:
        """안전한 삭제 작업"""
        context = {"operation": "delete_from_qdrant"}
        
        async with self.failure_context(OperationType.DELETE, product_uid, context):
            # Qdrant에서 데이터 삭제
            await qdrant_manager.delete_product(product_uid)
            
            logger.info(f"제품 {product_uid} 삭제 성공")
            return True
    
    async def safe_embedding_operation(
        self, 
        product_uid: int, 
        text_data: str
    ) -> bool:
        """안전한 임베딩 생성 작업"""
        context = {
            "operation": "generate_embedding",
            "text_length": len(text_data),
            "text_preview": text_data[:100] + "..." if len(text_data) > 100 else text_data
        }
        
        async with self.failure_context(OperationType.EMBEDDING, product_uid, context):
            # 임베딩 생성 및 저장
            embedding = await qdrant_manager.generate_embedding(text_data)
            await qdrant_manager.store_embedding(product_uid, embedding, text_data)
            
            logger.info(f"제품 {product_uid} 임베딩 생성 성공")
            return True
    
    async def process_failed_operations(self, max_operations: int = 50):
        """실패한 작업들을 재시도"""
        if self.processing:
            logger.warning("이미 실패 작업 처리가 진행 중입니다")
            return
        
        self.processing = True
        try:
            operations = await failure_handler.get_retryable_operations(max_operations)
            
            if not operations:
                logger.info("재시도할 실패 작업이 없습니다")
                return
            
            logger.info(f"재시도할 실패 작업 {len(operations)}개 발견")
            
            success_count = 0
            failure_count = 0
            
            for operation in operations:
                try:
                    success = await self._retry_operation(operation)
                    
                    if success:
                        await failure_handler.update_retry_attempt(
                            operation.id, 
                            success=True
                        )
                        success_count += 1
                        logger.info(
                            f"실패 작업 {operation.id} 재시도 성공 "
                            f"(타입: {operation.operation_type.value}, 제품: {operation.product_uid})"
                        )
                    else:
                        failure_count += 1
                        
                except Exception as retry_error:
                    await failure_handler.update_retry_attempt(
                        operation.id,
                        success=False,
                        new_error=retry_error,
                        strategy=self.retry_strategy
                    )
                    failure_count += 1
                    logger.warning(
                        f"실패 작업 {operation.id} 재시도 실패: {retry_error}"
                    )
                
                # 과부하 방지를 위한 짧은 대기
                await asyncio.sleep(0.1)
            
            logger.info(
                f"실패 작업 재시도 완료 - 성공: {success_count}, 실패: {failure_count}"
            )
            
        finally:
            self.processing = False
    
    async def _retry_operation(self, operation) -> bool:
        """개별 실패 작업 재시도"""
        try:
            if operation.operation_type == OperationType.SYNC:
                # 제품 데이터 다시 조회 후 동기화
                products = await postgres_manager.execute_query(
                    "SELECT uid, title, content, price, created_dt FROM product WHERE uid = $1",
                    operation.product_uid
                )
                
                if not products:
                    await failure_handler.mark_permanently_failed(
                        operation.id, 
                        "제품이 더 이상 존재하지 않음"
                    )
                    return False
                
                product_data = dict(products[0])
                return await self.safe_sync_operation(operation.product_uid, product_data)
                
            elif operation.operation_type == OperationType.UPDATE:
                # 업데이트는 컨텍스트에서 변경 사항 추출
                updates = operation.error_details.get('context', {}).get('updates', {})
                if not updates:
                    await failure_handler.mark_permanently_failed(
                        operation.id,
                        "업데이트 데이터를 찾을 수 없음"
                    )
                    return False
                
                return await self.safe_update_operation(operation.product_uid, updates)
                
            elif operation.operation_type == OperationType.DELETE:
                return await self.safe_delete_operation(operation.product_uid)
                
            elif operation.operation_type == OperationType.EMBEDDING:
                # 제품 데이터 다시 조회 후 임베딩 생성
                products = await postgres_manager.execute_query(
                    "SELECT title, content FROM product WHERE uid = $1",
                    operation.product_uid
                )
                
                if not products:
                    await failure_handler.mark_permanently_failed(
                        operation.id,
                        "제품이 더 이상 존재하지 않음"
                    )
                    return False
                
                # 텍스트 전처리 후 임베딩 생성
                from src.services.text_preprocessor import text_preprocessor
                
                product = products[0]
                processed_text = text_preprocessor.create_embedding_text({
                    'title': product['title'],
                    'content': product['content']
                })
                
                return await self.safe_embedding_operation(
                    operation.product_uid, 
                    processed_text
                )
            
            return False
            
        except Exception as error:
            logger.error(f"재시도 작업 실행 중 오류: {error}")
            return False
    
    async def retry_failed_operation(self, operation_id: int) -> Optional[str]:
        """특정 실패 작업을 재시도"""
        try:
            # Redis job ID 생성 (임시)
            job_id = str(uuid.uuid4())
            
            # 실제로는 Redis 큐에 재시도 작업을 넣어야 하지만
            # 여기서는 바로 실행
            operations = await failure_handler.get_retryable_operations(100)
            target_operation = None
            
            for op in operations:
                if op.id == operation_id:
                    target_operation = op
                    break
            
            if not target_operation:
                logger.warning(f"재시도할 작업을 찾을 수 없음: {operation_id}")
                return None
            
            # 재시도 실행
            success = await self._retry_operation(target_operation)
            
            if success:
                await failure_handler.update_retry_attempt(
                    target_operation.id, 
                    success=True
                )
                logger.info(f"작업 {operation_id} 재시도 성공")
                return job_id
            else:
                logger.warning(f"작업 {operation_id} 재시도 실패")
                return None
                
        except Exception as e:
            logger.error(f"작업 {operation_id} 재시도 중 오류: {e}")
            return None
    
    async def get_failure_statistics(self) -> Dict[str, Any]:
        """실패 통계 조회"""
        return await failure_handler.get_failure_stats()
    
    async def cleanup_old_failures(self, days: int = 30):
        """오래된 해결된 실패 기록 정리"""
        await failure_handler.cleanup_old_resolved_failures(days)

# 전역 인스턴스
reliable_worker = ReliableWorker() 