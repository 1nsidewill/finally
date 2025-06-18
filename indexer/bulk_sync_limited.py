#!/usr/bin/env python3
"""
제한된 범위의 Bulk Sync 스크립트
PID < 338683404인 제품들만 처리하여 400개를 Redis Queue 테스트용으로 남겨둡니다.
"""

import asyncio
import logging
import time
from src.config import get_settings
from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager, generate_product_vector_id
from src.services.text_preprocessor import ProductTextPreprocessor
from src.services.embedding_service import EmbeddingService

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LimitedBulkSync:
    """제한된 범위의 대량 동기화"""
    
    def __init__(self, pid_limit: int = 338683404, batch_size: int = 50):
        self.pid_limit = pid_limit
        self.batch_size = batch_size
        
        # 서비스 초기화
        self.pg_manager = PostgreSQLManager()
        self.qdrant_manager = QdrantManager()
        self.preprocessor = ProductTextPreprocessor()
        self.embedding_service = EmbeddingService()
        
        # 통계
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.start_time = None
        
    async def get_target_products(self) -> int:
        """처리 대상 제품 수 확인"""
        async with self.pg_manager.get_connection() as conn:
            result = await conn.fetchrow("""
                SELECT COUNT(*) as count
                FROM product 
                WHERE status = 1 
                AND is_conversion = false 
                AND CAST(pid AS INTEGER) < $1
            """, self.pid_limit)
            
            return result['count']
    
    async def process_batch(self, products: list) -> tuple[int, int]:
        """배치 처리"""
        success = 0
        errors = 0
        
        for product in products:
            try:
                # 텍스트 전처리
                processed_text = self.preprocessor.preprocess_product_data(product)
                
                # 임베딩 생성
                embeddings = await self.embedding_service.create_embeddings_async([processed_text])
                embedding = embeddings[0] if embeddings else None
                
                if embedding is None:
                    raise Exception("임베딩 생성 실패")
                
                # 벡터 ID 생성 (새로운 UUID 로직)
                vector_id = generate_product_vector_id(product['pid'], 'bunjang')
                
                # Qdrant에 저장
                await self.qdrant_manager.upsert_vector_async(
                    vector_id=vector_id,
                    vector=embedding,
                    metadata={
                        'pid': product['pid'],
                        'title': product['title'],
                        'price': float(product['price']) if product['price'] else 0.0,
                        'year': product['year'] if product['year'] else 0,
                        'provider': 'bunjang'
                    }
                )
                
                # PostgreSQL 업데이트
                async with self.pg_manager.get_connection() as conn:
                    await conn.execute("""
                        UPDATE product 
                        SET is_conversion = true 
                        WHERE pid = $1
                    """, product['pid'])
                
                success += 1
                logger.debug(f"✅ 처리 완료: {product['pid']} - {product['title'][:50]}")
                
            except Exception as e:
                errors += 1
                logger.error(f"❌ 처리 실패 {product['pid']}: {e}")
        
        return success, errors
    
    async def run_sync(self):
        """동기화 실행"""
        logger.info("🚀 제한된 범위 대량 동기화 시작")
        self.start_time = time.time()
        
        # 처리 대상 확인
        target_count = await self.get_target_products()
        logger.info(f"📊 처리 대상: {target_count:,}개 제품 (PID < {self.pid_limit:,})")
        
        if target_count == 0:
            logger.info("✅ 처리할 제품이 없습니다.")
            return True
        
        # 배치 처리
        offset = 0
        
        while True:
            # 배치 데이터 가져오기
            async with self.pg_manager.get_connection() as conn:
                products = await conn.fetch("""
                    SELECT pid, title, content, price, year
                    FROM product 
                    WHERE status = 1 
                    AND is_conversion = false 
                    AND CAST(pid AS INTEGER) < $1
                    ORDER BY pid
                    LIMIT $2 OFFSET $3
                """, self.pid_limit, self.batch_size, offset)
            
            if not products:
                break
            
            # 배치 처리
            batch_success, batch_errors = await self.process_batch(products)
            
            # 통계 업데이트
            self.processed_count += len(products)
            self.success_count += batch_success
            self.error_count += batch_errors
            
            # 진행 상황 출력
            elapsed = time.time() - self.start_time
            rate = self.processed_count / elapsed if elapsed > 0 else 0
            remaining = target_count - self.processed_count
            eta = remaining / rate if rate > 0 else 0
            
            logger.info(f"📈 진행: {self.processed_count:,}/{target_count:,} ({self.processed_count/target_count*100:.1f}%) | "
                       f"성공: {self.success_count:,} | 실패: {self.error_count:,} | "
                       f"속도: {rate:.1f}/초 | ETA: {eta/60:.1f}분")
            
            offset += self.batch_size
            
            # 짧은 대기 (시스템 부하 완화)
            await asyncio.sleep(0.1)
        
        # 최종 통계
        total_time = time.time() - self.start_time
        logger.info(f"🎉 동기화 완료!")
        logger.info(f"  • 처리된 제품: {self.processed_count:,}개")
        logger.info(f"  • 성공: {self.success_count:,}개")
        logger.info(f"  • 실패: {self.error_count:,}개") 
        logger.info(f"  • 성공률: {(self.success_count/self.processed_count*100):.2f}%")
        logger.info(f"  • 총 처리 시간: {total_time:.2f}초")
        logger.info(f"  • 평균 속도: {self.processed_count/total_time:.2f} 제품/초")
        
        return self.error_count == 0
    
    async def close(self):
        """리소스 정리"""
        if self.pg_manager:
            await self.pg_manager.close()
        if self.qdrant_manager:
            await self.qdrant_manager.close()

async def main():
    """메인 함수"""
    sync = LimitedBulkSync(pid_limit=338683404, batch_size=50)
    
    try:
        success = await sync.run_sync()
        if success:
            logger.info("✅ 제한된 범위 동기화가 성공적으로 완료되었습니다.")
        else:
            logger.warning("⚠️ 일부 오류가 있었지만 동기화가 완료되었습니다.")
    except Exception as e:
        logger.error(f"❌ 동기화 중 오류 발생: {e}")
    finally:
        await sync.close()

if __name__ == "__main__":
    asyncio.run(main())