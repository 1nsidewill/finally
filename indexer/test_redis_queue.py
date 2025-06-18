#!/usr/bin/env python3
"""
Redis Queue 테스트 스크립트
명세서의 예시 Job을 실제로 테스트해봅니다.
"""

import asyncio
import json
import logging
from typing import Dict, Any, List
from src.database.redis import RedisManager
from src.workers.job_processor import JobProcessor

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RedisQueueTester:
    """Redis Queue 테스트 클래스"""
    
    def __init__(self):
        self.redis_manager = RedisManager()
        self.job_processor = None
        
    async def initialize(self):
        """초기화"""
        try:
            # Redis 연결 확인
            if not await self.redis_manager.health_check():
                raise Exception("Redis 연결 실패")
            
            # Job Processor 초기화
            self.job_processor = JobProcessor()
            await self.job_processor.initialize()
            
            logger.info("✅ 테스트 환경 초기화 완료")
            
        except Exception as e:
            logger.error(f"❌ 초기화 실패: {e}")
            raise
    
    async def create_test_jobs(self) -> List[Dict[str, Any]]:
        """테스트용 Job 생성 (명세서 예시 기반)"""
        
        # 실제 DB에서 테스트용 데이터 가져오기
        from src.database.postgresql import PostgreSQLManager
        
        pg_manager = PostgreSQLManager()
        async with pg_manager.get_connection() as conn:
            test_products = await conn.fetch('''
                SELECT pid, title, price, content, year 
                FROM product 
                WHERE status = 1 AND is_conversion = false 
                AND pid >= 338683404
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
                    "year": product['year'] if product['year'] else 2020,
                    "mileage": 15000,
                    "images": []
                },
                "timestamp": "2025-06-18T06:30:00Z",
                "metadata": {
                    "source": "redis_queue_test",
                    "test_version": "1.0"
                }
            })
        
        # Job 2: UPDATE (기존 제품 업데이트) - 이미 처리된 제품 중 하나 사용
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
                    "price": int(product['price'] * 0.9) if product['price'] else 1000000,
                    "content": f"{product['content'] or ''} 빠른 판매를 위해 가격을 인하했습니다!",
                    "year": product['year'] if product['year'] else 2020
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
    
    async def submit_jobs_to_queue(self, jobs: List[Dict[str, Any]]) -> bool:
        """Job들을 Redis Queue에 제출"""
        try:
            for i, job in enumerate(jobs, 1):
                success = await self.redis_manager.push_job(job)
                if success:
                    logger.info(f"✅ Job {i}/{len(jobs)} 제출 완료: {job['type']} - {job['id']}")
                else:
                    logger.error(f"❌ Job {i}/{len(jobs)} 제출 실패: {job['id']}")
                    return False
            
            # Queue 길이 확인
            queue_length = await self.redis_manager.get_queue_length()
            logger.info(f"📊 현재 Queue 길이: {queue_length}개")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Job 제출 중 오류: {e}")
            return False
    
    async def process_jobs(self, max_jobs: int = 10) -> List[Dict]:
        """Queue에서 Job을 처리"""
        results = []
        
        try:
            for i in range(max_jobs):
                # Queue에서 Job 가져오기
                job_data = await self.redis_manager.pop_job(timeout=5)
                
                if not job_data:
                    logger.info(f"⏰ Queue에서 Job을 찾을 수 없음 (타임아웃)")
                    break
                
                logger.info(f"🔄 Job 처리 중: {job_data.get('type')} - {job_data.get('id')}")
                
                # Job 처리
                result = await self.job_processor.process_job(job_data)
                results.append({
                    "job": job_data,
                    "result": result.__dict__
                })
                
                logger.info(f"✅ Job 처리 완료: {result.success} - {result.message}")
        
        except Exception as e:
            logger.error(f"❌ Job 처리 중 오류: {e}")
        
        return results
    
    async def run_full_test(self):
        """전체 테스트 실행"""
        try:
            logger.info("🧪 Redis Queue 전체 테스트 시작")
            
            # 1. 초기화
            await self.initialize()
            
            # 2. 테스트 Job 생성
            jobs = await self.create_test_jobs()
            if not jobs:
                logger.warning("⚠️ 테스트용 데이터가 없습니다")
                return
            
            # 3. Job 제출
            logger.info("📤 Job들을 Queue에 제출 중...")
            submit_success = await self.submit_jobs_to_queue(jobs)
            if not submit_success:
                logger.error("❌ Job 제출 실패")
                return
            
            # 4. Job 처리
            logger.info("🔄 Queue에서 Job 처리 중...")
            results = await self.process_jobs(len(jobs))
            
            # 5. 결과 분석
            logger.info("📊 테스트 결과 분석:")
            
            success_count = sum(1 for r in results if r['result']['success'])
            total_count = len(results)
            
            logger.info(f"   - 총 처리된 Job: {total_count}개")
            logger.info(f"   - 성공: {success_count}개")
            logger.info(f"   - 실패: {total_count - success_count}개")
            logger.info(f"   - 성공률: {(success_count/total_count*100):.1f}%" if total_count > 0 else "   - 성공률: 0%")
            
            # 개별 결과 출력
            for i, result in enumerate(results, 1):
                job = result['job']
                res = result['result']
                status = "✅" if res['success'] else "❌"
                logger.info(f"   Job {i}: {status} {job['type']} - {res['message']}")
            
            logger.info("🎉 Redis Queue 테스트 완료!")
            
        except Exception as e:
            logger.error(f"❌ 테스트 실행 중 오류: {e}")
        
        finally:
            # 정리
            if self.job_processor:
                await self.job_processor.close()

async def main():
    """메인 함수"""
    tester = RedisQueueTester()
    await tester.run_full_test()

if __name__ == "__main__":
    asyncio.run(main())