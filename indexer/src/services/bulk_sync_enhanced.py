#!/usr/bin/env python3
"""
🚀 Enhanced Bulk Synchronizer with Advanced Progress Tracking
향상된 진행률 추적이 포함된 대용량 동기화 시스템
"""

import asyncio
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from ..database.postgresql import PostgreSQLManager
from ..database.qdrant import QdrantManager
from ..monitoring.progress_tracker import ProgressTracker, track_progress
from ..config import get_settings

logger = logging.getLogger(__name__)

class EnhancedBulkSynchronizer:
    """🚀 향상된 대용량 동기화 시스템"""
    
    def __init__(self, batch_size: int = 50, max_retries: int = 3):
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.config = get_settings()
        
        # 데이터베이스 매니저들
        self.pg_manager = PostgreSQLManager()
        self.qdrant_manager = QdrantManager()
        
        logger.info(f"🚀 Enhanced Bulk Synchronizer 초기화 (배치 크기: {batch_size})")
    
    async def sync_all_products(
        self, 
        session_id: Optional[str] = None,
        use_optimized_batch: bool = True,
        parallel_batches: int = 3
    ) -> Dict[str, Any]:
        """모든 제품을 Qdrant와 동기화 (향상된 진행률 추적 포함)"""
        
        if not session_id:
            session_id = f"bulk_sync_{int(time.time())}"
        
        logger.info(f"🎯 대용량 동기화 시작: {session_id}")
        
        try:
            # 1. 총 처리할 제품 수 조회
            total_products = await self._get_total_products()
            if total_products == 0:
                return {
                    "status": "success",
                    "message": "처리할 제품이 없습니다",
                    "processed": 0
                }
            
            total_batches = (total_products + self.batch_size - 1) // self.batch_size
            logger.info(f"📊 처리 계획: {total_products}개 제품, {total_batches}개 배치")
            
            # 2. 진행률 추적 시작
            async with track_progress(session_id, total_products, total_batches) as tracker:
                
                # 진행률 콜백 설정
                def on_progress_update(session):
                    logger.info(
                        f"📈 전체 진행률: {session.completion_percentage:.1f}% "
                        f"({session.processed_items}/{session.total_items})"
                    )
                    if session.estimated_time_remaining:
                        logger.info(f"⏱️ 예상 남은 시간: {session.estimated_time_remaining}")
                
                def on_batch_complete(batch):
                    logger.info(
                        f"✅ 배치 {batch.batch_id} 완료: "
                        f"{batch.successful_items}/{batch.total_items} 성공 "
                        f"({batch.success_rate:.1f}%), 속도: {batch.processing_rate:.2f}/s"
                    )
                
                tracker.add_progress_callback(on_progress_update)
                tracker.add_batch_callback(on_batch_complete)
                
                # 3. 배치별 처리 실행
                total_successful = 0
                total_failed = 0
                
                for batch_id in range(total_batches):
                    offset = batch_id * self.batch_size
                    
                    # 배치 시작
                    current_batch_size = min(self.batch_size, total_products - offset)
                    batch = tracker.start_batch(batch_id, current_batch_size)
                    
                    try:
                        # 배치 처리
                        batch_result = await self._process_batch_enhanced(
                            batch_id, offset, current_batch_size, tracker,
                            use_optimized_batch, parallel_batches
                        )
                        
                        total_successful += batch_result["successful"]
                        total_failed += batch_result["failed"]
                        
                        # 배치 완료
                        tracker.complete_batch(batch_id)
                        
                        # 배치 간 잠시 대기 (시스템 부하 조절)
                        if batch_id < total_batches - 1:
                            await asyncio.sleep(0.5)
                    
                    except Exception as e:
                        logger.error(f"배치 {batch_id} 처리 실패: {e}")
                        tracker.update_batch_progress(
                            batch_id, current_batch_size, 0, current_batch_size,
                            [{"batch_id": batch_id, "error": str(e)}]
                        )
                        tracker.complete_batch(batch_id)
                        total_failed += current_batch_size
                
                # 4. 최종 결과
                final_result = {
                    "status": "success",
                    "session_id": session_id,
                    "total_products": total_products,
                    "successful": total_successful,
                    "failed": total_failed,
                    "success_rate": (total_successful / total_products * 100) if total_products > 0 else 0,
                    "processing_time_seconds": tracker.session.duration_seconds if hasattr(tracker.session, 'duration_seconds') else 0,
                    "average_rate": tracker.session.average_processing_rate,
                    "peak_memory_mb": tracker.session.peak_memory_usage_mb
                }
                
                logger.info(f"🎉 대용량 동기화 완료: {final_result}")
                return final_result
        
        except Exception as e:
            logger.error(f"대용량 동기화 실패: {e}")
            return {
                "status": "error",
                "session_id": session_id,
                "error": str(e),
                "processed": 0
            }
    
    async def _process_batch_enhanced(
        self, 
        batch_id: int, 
        offset: int, 
        batch_size: int, 
        tracker: ProgressTracker,
        use_optimized_batch: bool = True,
        parallel_batches: int = 3
    ) -> Dict[str, Any]:
        """향상된 배치 처리"""
        
        logger.debug(f"🔄 배치 {batch_id} 처리 시작 (오프셋: {offset}, 크기: {batch_size})")
        
        successful = 0
        failed = 0
        error_details = []
        
        try:
            # 1. 배치 데이터 조회
            products = await self._get_products_batch(offset, batch_size)
            if not products:
                logger.warning(f"배치 {batch_id}: 조회된 제품이 없음")
                return {"successful": 0, "failed": 0}
            
            # 2. 개별 제품 처리 또는 최적화된 배치 처리
            if use_optimized_batch and len(products) > 10:
                # 최적화된 배치 업로드 사용
                result = await self._process_optimized_batch(
                    products, batch_id, tracker, parallel_batches
                )
                successful = result["successful"]
                failed = result["failed"]
                error_details = result.get("errors", [])
            else:
                # 개별 처리 (소규모 배치 또는 디버깅용)
                for i, product in enumerate(products):
                    try:
                        await self._process_single_product(product)
                        successful += 1
                        
                        # 진행률 업데이트
                        tracker.update_batch_progress(
                            batch_id, i + 1, successful, failed
                        )
                        
                    except Exception as e:
                        failed += 1
                        error_detail = {
                            "product_uid": product.get("uid"),
                            "error": str(e),
                            "timestamp": datetime.now().isoformat()
                        }
                        error_details.append(error_detail)
                        
                        # 진행률 업데이트
                        tracker.update_batch_progress(
                            batch_id, i + 1, successful, failed, [error_detail]
                        )
                        
                        logger.warning(f"제품 {product.get('uid')} 처리 실패: {e}")
            
            logger.info(f"✅ 배치 {batch_id} 완료: 성공 {successful}, 실패 {failed}")
            
        except Exception as e:
            logger.error(f"배치 {batch_id} 처리 중 오류: {e}")
            failed = batch_size
            error_details.append({
                "batch_id": batch_id,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
        
        return {
            "successful": successful,
            "failed": failed,
            "errors": error_details
        }
    
    async def _process_optimized_batch(
        self, 
        products: List[Dict[str, Any]], 
        batch_id: int, 
        tracker: ProgressTracker,
        parallel_batches: int = 3
    ) -> Dict[str, Any]:
        """최적화된 배치 처리"""
        
        logger.debug(f"🚀 최적화된 배치 처리: {len(products)}개 제품")
        
        try:
            # 1. 임베딩 생성 (병렬 처리)
            embeddings_data = []
            successful = 0
            failed = 0
            errors = []
            
            # 임베딩 생성을 위한 세마포어
            embedding_semaphore = asyncio.Semaphore(parallel_batches)
            
            async def generate_embedding_for_product(product, index):
                async with embedding_semaphore:
                    try:
                        # 텍스트 생성
                        text = f"{product.get('title', '')} {product.get('brand', '')} {product.get('content', '')}"
                        
                        # 임베딩 생성
                        embedding = await self.qdrant_manager.generate_embedding(text)
                        
                        return {
                            "product": product,
                            "embedding": embedding,
                            "index": index,
                            "success": True
                        }
                    except Exception as e:
                        return {
                            "product": product,
                            "error": str(e),
                            "index": index,
                            "success": False
                        }
            
            # 모든 제품에 대해 병렬로 임베딩 생성
            embedding_tasks = [
                generate_embedding_for_product(product, i)
                for i, product in enumerate(products)
            ]
            
            embedding_results = await asyncio.gather(*embedding_tasks)
            
            # 결과 분류
            successful_embeddings = []
            for result in embedding_results:
                if result["success"]:
                    embeddings_data.append({
                        "product": result["product"],
                        "embedding": result["embedding"]
                    })
                    successful += 1
                else:
                    failed += 1
                    errors.append({
                        "product_uid": result["product"].get("uid"),
                        "error": f"임베딩 생성 실패: {result['error']}",
                        "timestamp": datetime.now().isoformat()
                    })
                
                # 진행률 업데이트
                tracker.update_batch_progress(
                    batch_id, result["index"] + 1, successful, failed
                )
            
            # 2. Qdrant에 배치 업로드 (성공한 임베딩만)
            if embeddings_data:
                await self._batch_upload_to_qdrant(embeddings_data)
                
                # 3. PostgreSQL 상태 업데이트
                successful_uids = [item["product"]["uid"] for item in embeddings_data]
                await self._batch_update_postgresql(successful_uids)
            
            return {
                "successful": successful,
                "failed": failed,
                "errors": errors
            }
            
        except Exception as e:
            logger.error(f"최적화된 배치 처리 실패: {e}")
            return {
                "successful": 0,
                "failed": len(products),
                "errors": [{
                    "batch_error": str(e),
                    "timestamp": datetime.now().isoformat()
                }]
            }
    
    async def _batch_upload_to_qdrant(self, embeddings_data: List[Dict[str, Any]]):
        """Qdrant에 배치 업로드"""
        from qdrant_client.http.models import PointStruct
        from ..database.qdrant import generate_product_vector_id
        
        points = []
        for item in embeddings_data:
            product = item["product"]
            embedding = item["embedding"]
            
            # 새로운 UUID 생성 로직: uid + provider 기반
            point_id = generate_product_vector_id(str(product["uid"]), "bunjang")
            
            payload = {
                'uid': product['uid'],
                'title': product.get('title', ''),
                'brand': product.get('brand', ''),
                'content': product.get('content', ''),
                'price': str(product.get('price', '')),
                'status': product.get('status', '')
            }
            
            point = PointStruct(
                id=point_id,
                vector=embedding,
                payload=payload
            )
            points.append(point)
        
        # 최적화된 배치 업로드 사용
        await self.qdrant_manager.upsert_points_batch_optimized(
            points, batch_size=100, wait=False, parallel_batches=3
        )
        
        logger.debug(f"✅ Qdrant 배치 업로드 완료: {len(points)}개 포인트")
    
    async def _batch_update_postgresql(self, successful_uids: List[int]):
        """PostgreSQL 배치 상태 업데이트"""
        if not successful_uids:
            return
        
        # 배치 업데이트 쿼리
        uid_list = ','.join(map(str, successful_uids))
        query = f"""
        UPDATE product 
        SET is_conversion = true 
        WHERE uid IN ({uid_list})
        """
        
        await self.pg_manager.execute_query(query)
        logger.debug(f"✅ PostgreSQL 배치 업데이트 완료: {len(successful_uids)}개 제품")
    
    async def _get_total_products(self) -> int:
        """총 처리할 제품 수 조회"""
        query = "SELECT COUNT(*) as total FROM product WHERE is_conversion = false"
        result = await self.pg_manager.execute_query(query)
        return result[0]['total'] if result else 0
    
    async def _get_products_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        """배치 단위로 제품 조회"""
        query = """
        SELECT uid, pid, title, brand, content, price, status
        FROM product 
        WHERE is_conversion = false
        ORDER BY uid
        LIMIT $1 OFFSET $2
        """
        return await self.pg_manager.execute_query(query, limit, offset)
    
    async def _process_single_product(self, product: Dict[str, Any]):
        """단일 제품 처리 (기존 방식)"""
        # 기존 단일 제품 처리 로직
        text = f"{product.get('title', '')} {product.get('brand', '')} {product.get('content', '')}"
        embedding = await self.qdrant_manager.generate_embedding(text)
        
        # Qdrant 저장
        await self.qdrant_manager.upsert_vector_async(
            product["uid"], product["pid"], embedding, product
        )
        
        # PostgreSQL 업데이트
        update_query = "UPDATE product SET is_conversion = true WHERE uid = $1"
        await self.pg_manager.execute_query(update_query, product["uid"])

# 사용 예시
async def run_enhanced_bulk_sync():
    """향상된 대용량 동기화 실행"""
    synchronizer = EnhancedBulkSynchronizer(batch_size=50)
    
    result = await synchronizer.sync_all_products(
        session_id="enhanced_sync_test",
        use_optimized_batch=True,
        parallel_batches=3
    )
    
    print(f"🎉 동기화 완료: {result}")

if __name__ == "__main__":
    asyncio.run(run_enhanced_bulk_sync()) 