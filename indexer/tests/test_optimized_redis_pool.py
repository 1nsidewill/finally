#!/usr/bin/env python3
"""
최적화된 Redis 연결 풀 성능 검증 테스트
50개 연결로 최적화된 설정의 성능을 빠르게 확인합니다.
"""

import asyncio
import time
import json
from datetime import datetime
from src.config import get_settings
from src.database.redis import RedisManager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_optimized_pool_performance():
    """최적화된 연결 풀 성능 테스트"""
    
    config = get_settings()
    redis_manager = RedisManager()
    
    print("🚀 최적화된 Redis 연결 풀 성능 테스트")
    print(f"📊 연결 풀 설정: 최대 {config.REDIS_MAX_CONNECTIONS}개 연결")
    print(f"⏱️ 타임아웃: {config.REDIS_CONNECTION_TIMEOUT}초")
    print(f"📦 배치 크기: {config.REDIS_BATCH_SIZE}")
    print("-" * 60)
    
    # 테스트 데이터 생성
    test_jobs = 1000
    test_data = [
        {
            "id": i,
            "action": "sync",
            "product_uid": f"optimized-test-{i}",
            "data": {"timestamp": time.time(), "index": i}
        }
        for i in range(test_jobs)
    ]
    
    try:
        # Redis 연결 확인
        if not await redis_manager.health_check():
            raise Exception("Redis 헬스체크 실패")
        
        # 테스트 큐 이름
        test_queue = "optimized_pool_test"
        
        # === Push 성능 테스트 ===
        print("📤 Push 성능 테스트 시작...")
        push_start = time.time()
        
        batch_size = config.REDIS_BATCH_SIZE
        for i in range(0, test_jobs, batch_size):
            batch = test_data[i:i + batch_size]
            await redis_manager.push_jobs_batch(batch, test_queue)
        
        push_time = time.time() - push_start
        push_jobs_per_sec = test_jobs / push_time
        
        print(f"✅ Push 완료: {test_jobs}개 작업을 {push_time:.2f}초에 처리 ({push_jobs_per_sec:.1f} jobs/sec)")
        
        # === Pop 성능 테스트 ===
        print("📥 Pop 성능 테스트 시작...")
        pop_start = time.time()
        popped_jobs = 0
        
        while popped_jobs < test_jobs:
            batch = await redis_manager.pop_jobs_batch(batch_size, test_queue)
            if not batch:
                break
            popped_jobs += len(batch)
        
        pop_time = time.time() - pop_start
        pop_jobs_per_sec = popped_jobs / pop_time if pop_time > 0 else 0
        
        print(f"✅ Pop 완료: {popped_jobs}개 작업을 {pop_time:.2f}초에 처리 ({pop_jobs_per_sec:.1f} jobs/sec)")
        
        # === 전체 성능 계산 ===
        total_time = push_time + pop_time
        overall_jobs_per_sec = test_jobs / total_time
        
        print("-" * 60)
        print("🎯 최종 성능 결과:")
        print(f"   총 처리 시간: {total_time:.2f}초")
        print(f"   전체 처리량: {overall_jobs_per_sec:.1f} jobs/sec")
        print(f"   Push 효율: {push_jobs_per_sec:.1f} jobs/sec")
        print(f"   Pop 효율: {pop_jobs_per_sec:.1f} jobs/sec")
        
        # 예상 성능과 비교
        expected_performance = 2288  # 벤치마크 결과 기준
        improvement = (overall_jobs_per_sec / expected_performance) * 100
        
        print(f"\n📈 성능 비교:")
        print(f"   벤치마크 예상 성능: {expected_performance} jobs/sec")
        print(f"   실제 측정 성능: {overall_jobs_per_sec:.1f} jobs/sec")
        print(f"   달성률: {improvement:.1f}%")
        
        if improvement >= 90:
            print("🎉 성능 최적화 성공! 예상 성능의 90% 이상 달성")
        elif improvement >= 80:
            print("✅ 성능 최적화 양호! 예상 성능의 80% 이상 달성")
        else:
            print("⚠️ 성능 최적화 재검토 필요 (예상 성능의 80% 미만)")
        
        # 30k 매물 처리 시간 예측
        estimated_30k_time = 30000 / overall_jobs_per_sec
        print(f"\n🏍️ 30k 매물 처리 예상 시간: {estimated_30k_time:.1f}초 ({estimated_30k_time/60:.1f}분)")
        
        # 정리
        await redis_manager.clear_queue(test_queue)
        
    except Exception as e:
        logger.error(f"테스트 실패: {e}")
        raise
    
    finally:
        # 연결 정리
        try:
            await redis_manager.close()
        except:
            pass

async def main():
    """메인 실행 함수"""
    try:
        await test_optimized_pool_performance()
    except Exception as e:
        print(f"❌ 테스트 실행 실패: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 