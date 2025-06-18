#!/usr/bin/env python3
"""
간단한 Redis Queue 테스트 - 핵심 기능만 테스트
"""

import asyncio
import json
import logging
from typing import Dict, Any, List
from src.database.redis import RedisManager
from src.database.postgresql import PostgreSQLManager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleRedisQueueTester:
    """간단한 Redis Queue 테스트 클래스"""
    
    def __init__(self):
        self.redis_manager = RedisManager()
        
    async def create_test_jobs(self) -> List[Dict[str, Any]]:
        """테스트용 Job 생성"""
        
        # 실제 DB에서 테스트용 데이터 가져오기
        pg_manager = PostgreSQLManager()
        async with pg_manager.get_connection() as conn:
            test_products = await conn.fetch('''
                SELECT pid, title, price, content 
                FROM product 
                WHERE status = 1 AND is_conversion = false 
                AND CAST(pid AS INTEGER) >= 338683404
                LIMIT 3
            ''')
        
        jobs = []
        
        # Job 1: SYNC (새 제품 추가)
        if test_products:
            product = test_products[0]
            jobs.append({
                "id": "test_sync_001",
                "type": "sync",
                "product_id": str(product['pid']),
                "provider": "bunjang",
                "product_data": {
                    "pid": str(product['pid']),
                    "title": product['title'],
                    "price": int(product['price']) if product['price'] else None,
                    "content": product['content'] or "테스트 제품 설명",
                    "year": 2020,
                    "mileage": 15000,
                    "images": []
                },
                "timestamp": "2025-06-18T06:30:00Z",
                "metadata": {
                    "source": "redis_queue_test",
                    "test_version": "1.0"
                }
            })
        
        # Job 2: UPDATE (기존 제품 업데이트)
        if len(test_products) > 1:
            product = test_products[1]
            jobs.append({
                "id": "test_update_001", 
                "type": "update",
                "product_id": str(product['pid']),
                "provider": "bunjang",
                "product_data": {
                    "pid": str(product['pid']),
                    "title": f"{product['title']} [가격인하]",
                    "price": int(float(product['price']) * 0.9) if product['price'] else 1000000,
                    "content": f"{product['content'] or ''} 빠른 판매를 위해 가격을 인하했습니다!",
                    "year": 2020
                },
                "timestamp": "2025-06-18T06:31:00Z",
                "metadata": {
                    "source": "redis_queue_test",
                    "price_change": -100000
                }
            })
        
        # Job 3: DELETE (제품 삭제)
        if len(test_products) > 2:
            product = test_products[2]
            jobs.append({
                "id": "test_delete_001",
                "type": "delete", 
                "product_id": str(product['pid']),
                "provider": "bunjang",
                "timestamp": "2025-06-18T06:32:00Z",
                "metadata": {
                    "source": "redis_queue_test",
                    "reason": "test_deletion"
                }
            })
        
        logger.info(f"📋 테스트 Job {len(jobs)}개 생성 완료")
        return jobs
    
    async def test_redis_operations(self):
        """기본 Redis 연산 테스트"""
        try:
            logger.info("🔄 Redis 기본 연산 테스트 시작")
            
            # 1. Redis 연결 테스트
            ping_result = await self.redis_manager.health_check()
            logger.info(f"✅ Redis Ping: {ping_result}")
            
            # 2. Queue 길이 확인 및 정리
            initial_length = await self.redis_manager.get_queue_length()
            logger.info(f"📊 초기 Queue 길이: {initial_length}개")
            
            # 테스트를 위해 Queue 비우기
            if initial_length > 0:
                await self.redis_manager.clear_queue()
                logger.info(f"🧹 테스트를 위해 Queue 비움")
                initial_length = 0
            
            # 3. 테스트 Job 생성
            jobs = await self.create_test_jobs()
            if not jobs:
                logger.warning("⚠️ 테스트용 데이터가 없습니다")
                return
            
            # 4. Job 제출 테스트
            logger.info("📤 Job들을 Queue에 제출 중...")
            for i, job in enumerate(jobs, 1):
                success = await self.redis_manager.push_job(job)
                if success:
                    logger.info(f"✅ Job {i}/{len(jobs)} 제출 완료: {job['type']} - {job['id']}")
                else:
                    logger.error(f"❌ Job {i}/{len(jobs)} 제출 실패: {job['id']}")
                    return False
            
            # 5. Queue 길이 재확인
            after_submit_length = await self.redis_manager.get_queue_length()
            logger.info(f"📊 제출 후 Queue 길이: {after_submit_length}개")
            
            # 6. Job 미리보기
            peeked_jobs = await self.redis_manager.peek_jobs(count=5)
            logger.info(f"👀 Queue 미리보기: {len(peeked_jobs)}개 Job 확인")
            for i, job in enumerate(peeked_jobs, 1):
                logger.info(f"   Job {i}: {job.get('type')} - {job.get('id')}")
            
            # 7. Job 팝 테스트
            logger.info("📥 Job 팝 테스트 중...")
            popped_jobs = []
            for i in range(len(jobs)):
                job_data = await self.redis_manager.pop_job(timeout=2)
                if job_data:
                    popped_jobs.append(job_data)
                    logger.info(f"✅ Job {i+1} 팝 성공: {job_data.get('type')} - {job_data.get('id')}")
                else:
                    logger.warning(f"⏰ Job {i+1} 팝 타임아웃")
                    break
            
            # 8. 최종 Queue 길이 확인
            final_length = await self.redis_manager.get_queue_length()
            logger.info(f"📊 최종 Queue 길이: {final_length}개")
            
            # 9. 결과 분석
            logger.info("📊 테스트 결과:")
            logger.info(f"   - 제출된 Job: {len(jobs)}개")
            logger.info(f"   - 팝된 Job: {len(popped_jobs)}개")
            logger.info(f"   - Queue 변화: {initial_length} → {after_submit_length} → {final_length}")
            
            # 10. Job 데이터 검증
            logger.info("🔍 Job 데이터 검증:")
            for i, (original, popped) in enumerate(zip(jobs, popped_jobs), 1):
                try:
                    original_id = original.get('id', 'N/A')
                    popped_id = popped.get('id', 'N/A')
                    original_type = original.get('type', 'N/A')
                    popped_type = popped.get('type', 'N/A')
                    
                    if original_id == popped_id and original_type == popped_type:
                        logger.info(f"   ✅ Job {i}: 데이터 일치 ({original_type} - {original_id})")
                    else:
                        logger.error(f"   ❌ Job {i}: 데이터 불일치 (원본: {original_type}-{original_id}, 팝: {popped_type}-{popped_id})")
                except Exception as e:
                    logger.error(f"   ❌ Job {i}: 검증 중 오류 - {e}")
            
            logger.info("🎉 Redis Queue 기본 테스트 완료!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Redis 테스트 중 오류: {e}")
            return False

async def main():
    """메인 함수"""
    tester = SimpleRedisQueueTester()
    success = await tester.test_redis_operations()
    
    if success:
        logger.info("✅ 모든 테스트 통과!")
    else:
        logger.error("❌ 테스트 실패!")

if __name__ == "__main__":
    asyncio.run(main())