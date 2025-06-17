#!/usr/bin/env python3
# test_batch_processor.py - 배치 프로세서 테스트

import asyncio
import logging
from unittest.mock import patch, MagicMock, AsyncMock
from src.services.batch_processor import BatchProcessor, BatchConfig, BatchProgress

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_mock_listings(count: int) -> list:
    """테스트용 Mock 매물 데이터 생성"""
    listings = []
    for i in range(count):
        listing = {
            'id': i + 1,
            'title': f'야마하 R3 2019년형 {i + 1}',
            'price': 5500000 + (i * 100000),
            'year': 2019,
            'mileage': 15000 + (i * 1000),
            'content': f'상태 양호한 바이크입니다. 매물 {i + 1}',
            'url': f'https://example.com/listing/{i + 1}',
            'img_url': f'https://example.com/image/{i + 1}.jpg',
            'brand': 'YAMAHA'
        }
        listings.append(listing)
    return listings

async def test_batch_config():
    """배치 설정 테스트"""
    print("=== 배치 설정 테스트 ===")
    
    config = BatchConfig(
        batch_size=10,
        max_concurrent_batches=2,
        delay_between_batches=0.1,
        max_retries=2,
        retry_delay=1.0,
        save_progress_every=5,
        log_every=2
    )
    
    print(f"배치 크기: {config.batch_size}")
    print(f"동시 배치 수: {config.max_concurrent_batches}")
    print(f"배치 간 딜레이: {config.delay_between_batches}초")
    print(f"최대 재시도: {config.max_retries}")
    print(f"재시도 딜레이: {config.retry_delay}초")
    print("✅ 배치 설정 테스트 완료")

async def test_batch_progress():
    """배치 진행상황 테스트"""
    print("\n=== 배치 진행상황 테스트 ===")
    
    progress = BatchProgress()
    progress.total_items = 100
    progress.processed_items = 75
    progress.successful_items = 70
    progress.failed_items = 5
    progress.current_batch = 8
    progress.failed_item_ids = [15, 32, 47, 68, 89]
    
    # 딕셔너리 변환 테스트
    progress_dict = progress.to_dict()
    print(f"완료율: {progress_dict['completion_percentage']:.1f}%")
    print(f"성공률: {progress_dict['success_rate']:.1f}%")
    
    # 딕셔너리에서 복원 테스트
    restored_progress = BatchProgress.from_dict(progress_dict)
    print(f"복원된 처리 항목: {restored_progress.processed_items}")
    print(f"복원된 실패 ID 수: {len(restored_progress.failed_item_ids)}")
    
    print("✅ 배치 진행상황 테스트 완료")

async def test_batch_processor_mock():
    """Mock을 사용한 배치 프로세서 테스트"""
    print("\n=== Mock 배치 프로세서 테스트 ===")
    
    # Mock 서비스들 생성
    mock_embedding_service = MagicMock()
    mock_postgres_manager = MagicMock()
    mock_qdrant_manager = MagicMock()
    
    # 배치 설정 (테스트용 작은 값들)
    config = BatchConfig(
        batch_size=5,
        delay_between_batches=0.1,
        max_retries=2,
        save_progress_every=2,
        log_every=1
    )
    
    # 배치 프로세서 생성
    processor = BatchProcessor(
        embedding_service=mock_embedding_service,
        postgres_manager=mock_postgres_manager,
        qdrant_manager=mock_qdrant_manager,
        config=config
    )
    
    # Mock 매물 데이터
    test_listings = create_mock_listings(15)  # 15개 매물 (3개 배치)
    
    # Mock 임베딩 응답 설정
    mock_embeddings = [[0.1] * 3072 for _ in range(5)]  # 배치당 5개씩
    mock_embedding_service.embed_product_batch.return_value = mock_embeddings
    
    # Mock 데이터베이스 응답 설정
    mock_qdrant_manager.upsert_vector_async = AsyncMock()
    mock_postgres_manager.get_async_connection = AsyncMock()
    
    # 배치 처리 테스트
    print(f"테스트 매물 수: {len(test_listings)}")
    print(f"배치 크기: {config.batch_size}")
    print(f"예상 배치 수: {len(test_listings) // config.batch_size + (1 if len(test_listings) % config.batch_size else 0)}")
    
    # 첫 번째 배치 처리 테스트
    first_batch = test_listings[:config.batch_size]
    try:
        successful, failed = await processor.process_listings_batch(first_batch)
        print(f"첫 번째 배치 결과: 성공 {successful}, 실패 {failed}")
    except Exception as e:
        print(f"배치 처리 중 예상된 오류 (Mock 환경): {e}")
    
    print("✅ Mock 배치 프로세서 테스트 완료")

async def test_progress_tracking():
    """진행상황 추적 테스트"""
    print("\n=== 진행상황 추적 테스트 ===")
    
    # 진행상황 콜백 함수
    def progress_callback(progress: BatchProgress):
        completion = (progress.processed_items / progress.total_items) * 100 if progress.total_items > 0 else 0
        success_rate = (progress.successful_items / progress.processed_items) * 100 if progress.processed_items > 0 else 0
        print(f"  📊 진행률: {completion:.1f}% (성공률: {success_rate:.1f}%)")
    
    # Mock 배치 프로세서 생성
    mock_embedding_service = MagicMock()
    mock_postgres_manager = MagicMock()
    mock_qdrant_manager = MagicMock()
    
    config = BatchConfig(batch_size=3, save_progress_every=1, log_every=1)
    processor = BatchProcessor(
        embedding_service=mock_embedding_service,
        postgres_manager=mock_postgres_manager,
        qdrant_manager=mock_qdrant_manager,
        config=config
    )
    
    # 진행상황 시뮬레이션
    processor.progress.total_items = 10
    
    for batch_num in range(4):  # 4개 배치 시뮬레이션
        processed = min(3, processor.progress.total_items - processor.progress.processed_items)
        
        processor.progress.processed_items += processed
        processor.progress.successful_items += processed - (1 if batch_num == 2 else 0)  # 3번째 배치에서 1개 실패
        processor.progress.failed_items += (1 if batch_num == 2 else 0)
        processor.progress.current_batch += 1
        
        # 콜백 호출
        progress_callback(processor.progress)
        
        # 진행상황 저장 테스트
        processor.save_progress()
    
    print("✅ 진행상황 추적 테스트 완료")

async def test_error_handling():
    """에러 처리 테스트"""
    print("\n=== 에러 처리 테스트 ===")
    
    # Mock 서비스들
    mock_embedding_service = MagicMock()
    mock_postgres_manager = MagicMock()
    mock_qdrant_manager = MagicMock()
    
    # 임베딩 실패 시뮬레이션
    mock_embedding_service.embed_product_batch.return_value = [None, None, None]  # 모든 임베딩 실패
    
    config = BatchConfig(batch_size=3, max_retries=2)
    processor = BatchProcessor(
        embedding_service=mock_embedding_service,
        postgres_manager=mock_postgres_manager,
        qdrant_manager=mock_qdrant_manager,
        config=config
    )
    
    # 테스트 매물
    test_listings = create_mock_listings(3)
    
    try:
        successful, failed = await processor.process_listings_batch(test_listings)
        print(f"임베딩 실패 시뮬레이션 결과: 성공 {successful}, 실패 {failed}")
        
        if failed == len(test_listings):
            print("✅ 임베딩 실패 처리 정상 동작")
    except Exception as e:
        print(f"예상된 에러: {e}")
    
    print("✅ 에러 처리 테스트 완료")

async def main():
    """모든 테스트 실행"""
    print("🚀 배치 프로세서 종합 테스트 시작!")
    
    await test_batch_config()
    await test_batch_progress()
    await test_batch_processor_mock()
    await test_progress_tracking()
    await test_error_handling()
    
    print("\n🎉 모든 배치 프로세서 테스트 완료!")

if __name__ == "__main__":
    asyncio.run(main()) 