# test_redis_connection.py

"""
Redis 연결 풀 테스트
Redis 매니저의 연결 풀과 기본 기능들을 테스트합니다.
"""

import asyncio
import logging
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.database import redis_manager
from src.config import get_settings

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_redis_connection_pool():
    """Redis 연결 풀 테스트"""
    print("\n🔌 Redis 연결 풀 테스트")
    
    try:
        # 헬스체크
        health = await redis_manager.health_check()
        print(f"✅ Redis 헬스체크: {health}")
        
        if not health:
            print("❌ Redis 서버에 연결할 수 없습니다. Redis가 실행 중인지 확인하세요.")
            return False
        
        # 연결 풀 생성 확인
        pool = await redis_manager.get_connection_pool()
        print(f"✅ 연결 풀 생성: {pool}")
        
        # Redis 클라이언트 가져오기
        client = await redis_manager.get_redis_client()
        print(f"✅ Redis 클라이언트: {client}")
        
        return True
        
    except Exception as e:
        print(f"❌ Redis 연결 풀 테스트 실패: {e}")
        return False

async def test_basic_redis_operations():
    """기본 Redis 연산 테스트"""
    print("\n📝 기본 Redis 연산 테스트")
    
    try:
        # 키-값 저장
        test_key = "test:connection:basic"
        test_value = {"message": "Hello Redis!", "timestamp": "2024-01-01"}
        
        success = await redis_manager.set_value(test_key, test_value, expire=60)
        print(f"✅ 값 저장: {success}")
        
        # 값 조회
        retrieved_value = await redis_manager.get_value(test_key)
        print(f"✅ 값 조회: {retrieved_value}")
        
        # 값 비교
        if retrieved_value == test_value:
            print("✅ 저장/조회 값 일치")
        else:
            print(f"❌ 값 불일치: {retrieved_value} != {test_value}")
        
        # 키 삭제
        deleted = await redis_manager.delete_key(test_key)
        print(f"✅ 키 삭제: {deleted}")
        
        return True
        
    except Exception as e:
        print(f"❌ 기본 Redis 연산 테스트 실패: {e}")
        return False

async def test_queue_operations():
    """큐 연산 테스트"""
    print("\n📬 큐 연산 테스트")
    
    try:
        test_queue = "test_connection_queue"
        
        # 큐 클리어 (기존 데이터 정리)
        await redis_manager.clear_queue(test_queue)
        
        # 단일 작업 추가
        job1 = {"type": "sync", "product_id": 1, "action": "update"}
        success = await redis_manager.push_job(job1, test_queue)
        print(f"✅ 단일 작업 추가: {success}")
        
        # 배치 작업 추가
        jobs = [
            {"type": "sync", "product_id": 2, "action": "create"},
            {"type": "sync", "product_id": 3, "action": "delete"},
            {"type": "update", "product_id": 4, "action": "modify"}
        ]
        
        count = await redis_manager.push_jobs_batch(jobs, test_queue)
        print(f"✅ 배치 작업 추가: {count}개")
        
        # 큐 길이 확인
        queue_length = await redis_manager.get_queue_length(test_queue)
        print(f"✅ 큐 길이: {queue_length}")
        
        # 큐 미리보기
        preview_jobs = await redis_manager.peek_jobs(5, test_queue)
        print(f"✅ 큐 미리보기: {len(preview_jobs)}개")
        for i, job in enumerate(preview_jobs):
            print(f"    {i+1}. {job}")
        
        # 단일 작업 가져오기 (논블로킹)
        job = await redis_manager.pop_job(test_queue, timeout=1)
        print(f"✅ 작업 팝: {job}")
        
        # 배치 작업 가져오기
        batch_jobs = await redis_manager.pop_jobs_batch(2, test_queue)
        print(f"✅ 배치 작업 팝: {len(batch_jobs)}개")
        for i, job in enumerate(batch_jobs):
            print(f"    {i+1}. {job}")
        
        # 최종 큐 길이 확인
        final_length = await redis_manager.get_queue_length(test_queue)
        print(f"✅ 최종 큐 길이: {final_length}")
        
        # 큐 클리어
        await redis_manager.clear_queue(test_queue)
        print("✅ 테스트 큐 클리어 완료")
        
        return True
        
    except Exception as e:
        print(f"❌ 큐 연산 테스트 실패: {e}")
        return False

async def test_concurrent_operations():
    """동시 연산 테스트"""
    print("\n🚀 동시 연산 테스트")
    
    try:
        test_queue = "test_concurrent_queue"
        
        # 큐 클리어
        await redis_manager.clear_queue(test_queue)
        
        # 동시에 여러 작업 추가
        async def add_jobs(worker_id: int):
            jobs = [
                {"type": "sync", "worker_id": worker_id, "job_id": f"{worker_id}_{i}"}
                for i in range(5)
            ]
            return await redis_manager.push_jobs_batch(jobs, test_queue)
        
        # 3개 워커가 동시에 작업 추가
        tasks = [add_jobs(worker_id) for worker_id in range(1, 4)]
        results = await asyncio.gather(*tasks)
        
        total_added = sum(results)
        print(f"✅ 동시 작업 추가 완료: {total_added}개")
        
        # 큐 길이 확인
        queue_length = await redis_manager.get_queue_length(test_queue)
        print(f"✅ 큐 길이: {queue_length}")
        
        # 동시에 작업 처리
        async def process_jobs(worker_id: int):
            processed = 0
            while True:
                job = await redis_manager.pop_job(test_queue, timeout=1)
                if job is None:
                    break
                processed += 1
                # 간단한 처리 시뮬레이션
                await asyncio.sleep(0.01)
            return processed
        
        # 3개 워커가 동시에 작업 처리
        process_tasks = [process_jobs(worker_id) for worker_id in range(1, 4)]
        process_results = await asyncio.gather(*process_tasks)
        
        total_processed = sum(process_results)
        print(f"✅ 동시 작업 처리 완료: {total_processed}개")
        print(f"    워커별 처리량: {process_results}")
        
        # 최종 큐 길이 확인
        final_length = await redis_manager.get_queue_length(test_queue)
        print(f"✅ 최종 큐 길이: {final_length}")
        
        # 큐 클리어
        await redis_manager.clear_queue(test_queue)
        
        return True
        
    except Exception as e:
        print(f"❌ 동시 연산 테스트 실패: {e}")
        return False

async def display_config():
    """Redis 설정 정보 표시"""
    print("\n⚙️ Redis 설정 정보")
    
    config = get_settings()
    print(f"Redis Host: {config.REDIS_HOST}")
    print(f"Redis Port: {config.REDIS_PORT}")
    print(f"Redis DB: {config.REDIS_DB}")
    print(f"Redis Password: {'***' if config.REDIS_PASSWORD else 'None'}")
    print(f"Queue Name: {config.REDIS_QUEUE_NAME}")
    print(f"Batch Size: {config.REDIS_BATCH_SIZE}")
    print(f"Max Connections: {config.REDIS_MAX_CONNECTIONS}")
    print(f"Connection Timeout: {config.REDIS_CONNECTION_TIMEOUT}s")

async def main():
    """전체 테스트 실행"""
    print("🔥 Redis 연결 풀 및 큐 테스트 시작!")
    
    # 설정 정보 표시
    await display_config()
    
    tests = [
        ("Redis 연결 풀", test_redis_connection_pool),
        ("기본 Redis 연산", test_basic_redis_operations),
        ("큐 연산", test_queue_operations),
        ("동시 연산", test_concurrent_operations),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} 테스트 중 예외 발생: {e}")
            results.append((test_name, False))
    
    # 최종 결과 요약
    print(f"\n{'='*50}")
    print("📊 테스트 결과 요약")
    print(f"{'='*50}")
    
    passed = 0
    for test_name, result in results:
        status = "✅ 통과" if result else "❌ 실패"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n총 {len(results)}개 테스트 중 {passed}개 통과")
    
    # 연결 정리
    try:
        await redis_manager.close()
        print("✅ Redis 연결 정리 완료")
    except Exception as e:
        print(f"❌ Redis 연결 정리 실패: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 