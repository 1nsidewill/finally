"""
Job Processor 테스트

sync, update, delete 연산과 데이터베이스 통합을 테스트
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock
from typing import List, Dict, Any

from src.workers.job_processor import JobProcessor, JobType, JobResult, ProductData
from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager
from src.services.text_preprocessor import ProductTextPreprocessor
from src.services.embedding_service import EmbeddingService

async def test_product_data_structure():
    """ProductData 데이터 구조 테스트"""
    print("🔥 ProductData 구조 테스트!")
    
    # 기본 데이터 생성
    product = ProductData(
        pid="12345",
        title="2019 야마하 SR400",
        price=2500,
        content="깔끔한 상태의 야마하 SR400 판매합니다.",
        year=2019,
        mileage=15000,
        images=["image1.jpg", "image2.jpg"]
    )
    
    # 페이지 URL 자동 생성 확인
    expected_url = "https://m.bunjang.co.kr/products/12345"
    assert product.page_url == expected_url, f"페이지 URL 불일치: {product.page_url}"
    
    # 이미지 리스트 확인
    assert len(product.images) == 2, f"이미지 수 불일치: {len(product.images)}"
    
    print(f"  ✅ ProductData 구조 검증 완료")
    print(f"  - PID: {product.pid}")
    print(f"  - 제목: {product.title}")
    print(f"  - 페이지 URL: {product.page_url}")
    print(f"  - 이미지: {len(product.images)}개")

async def test_job_processor_initialization():
    """JobProcessor 초기화 테스트"""
    print("\n🔥 JobProcessor 초기화 테스트!")
    
    # Mock 매니저들 생성
    mock_postgresql = AsyncMock(spec=PostgreSQLManager)
    mock_qdrant = AsyncMock(spec=QdrantManager)
    
    # JobProcessor 생성
    processor = JobProcessor()
    
    # Mock 매니저 주입
    processor.postgresql_manager = mock_postgresql
    processor.qdrant_manager = mock_qdrant
    
    # 초기화 (Mock이므로 실제 초기화는 생략)
    await processor.initialize()
    
    # 통계 초기값 확인
    stats = processor.get_stats()
    assert stats['total_processed'] == 0
    assert stats['success_count'] == 0
    assert stats['success_rate'] == 0.0
    
    print(f"  ✅ 초기화 완료, 초기 통계: {stats}")

async def test_sync_operation():
    """SYNC 연산 테스트"""
    print("\n🔥 SYNC 연산 테스트!")
    
    # JobProcessor 생성 및 Mock 설정
    processor = JobProcessor()
    
    # Mock PostgreSQL 매니저
    mock_postgresql = AsyncMock(spec=PostgreSQLManager)
    mock_conn = AsyncMock()
    
    # Mock 제품 데이터
    product_result = [{
        'pid': '12345',
        'title': '2019 야마하 SR400',
        'price': 2500,
        'content': '깔끔한 상태',
        'year': 2019,
        'mileage': 15000
    }]
    
    # Mock 이미지 데이터
    file_results = [
        {'url': 'https://example.com/image_{cnt}.jpg', 'count': 1},
        {'url': 'https://example.com/image_{cnt}.jpg', 'count': 2}
    ]
    
    mock_conn.fetch.side_effect = [product_result, file_results]
    mock_postgresql.get_connection.return_value = mock_conn
    
    # Mock Qdrant 매니저
    mock_qdrant = AsyncMock(spec=QdrantManager)
    mock_qdrant.upsert_vector_async = AsyncMock()
    
    # Mock 텍스트 전처리기와 임베딩 서비스
    mock_preprocessor = MagicMock(spec=ProductTextPreprocessor)
    mock_preprocessor.preprocess_product_data.return_value = "[야마하 SR400] 2019 야마하 SR400 스펙: 2019|2500만원|15000km 상세: 깔끔한 상태"
    
    mock_embedding = AsyncMock(spec=EmbeddingService)
    mock_embedding.get_embedding_async.return_value = [0.1] * 3072  # Mock 임베딩
    
    # Mock 주입
    processor.postgresql_manager = mock_postgresql
    processor.qdrant_manager = mock_qdrant
    processor.text_preprocessor = mock_preprocessor
    processor.embedding_service = mock_embedding
    
    # SYNC 작업 데이터
    job_data = {
        'id': 'test_sync_1',
        'type': 'sync',
        'product_id': '12345'
    }
    
    # SYNC 작업 처리
    result = await processor.process_job(job_data)
    
    # 결과 검증
    assert result.success == True, f"SYNC 실패: {result.message}"
    assert result.job_type == JobType.SYNC
    assert result.product_id == '12345'
    assert result.vector_id is not None
    
    # Mock 호출 검증
    assert mock_conn.fetch.call_count == 2  # product + file 쿼리
    mock_preprocessor.preprocess_product_data.assert_called_once()
    mock_embedding.get_embedding_async.assert_called_once()
    mock_qdrant.upsert_vector_async.assert_called_once()
    
    print(f"  ✅ SYNC 연산 성공: {result.message}")
    print(f"  - 처리 시간: {result.processing_time:.3f}초")
    print(f"  - 벡터 ID: {result.vector_id}")

async def test_update_operation():
    """UPDATE 연산 테스트"""
    print("\n🔥 UPDATE 연산 테스트!")
    
    processor = JobProcessor()
    
    # Mock 설정 (SYNC와 유사하지만 기존 벡터 검색 추가)
    mock_postgresql = AsyncMock(spec=PostgreSQLManager)
    mock_conn = AsyncMock()
    
    product_result = [{
        'pid': '12345',
        'title': '2019 야마하 SR400 (가격수정)',
        'price': 2300,  # 가격 변경됨
        'content': '가격 인하했습니다',
        'year': 2019,
        'mileage': 15000
    }]
    
    mock_conn.fetch.side_effect = [product_result, []]  # 이미지는 없음
    mock_postgresql.get_connection.return_value = mock_conn
    
    # Mock Qdrant (기존 벡터 존재)
    mock_qdrant = AsyncMock(spec=QdrantManager)
    existing_vectors = [{'id': 'existing_vector_123', 'score': 0.9}]
    mock_qdrant.search_vectors.return_value = existing_vectors
    mock_qdrant.upsert_vector_async = AsyncMock()
    
    # Mock 서비스들
    mock_preprocessor = MagicMock(spec=ProductTextPreprocessor)
    mock_preprocessor.preprocess_product_data.return_value = "[야마하 SR400] 2019 야마하 SR400 (가격수정) 스펙: 2019|2300만원|15000km 상세: 가격 인하했습니다"
    
    mock_embedding = AsyncMock(spec=EmbeddingService)
    mock_embedding.get_embedding_async.return_value = [0.2] * 3072  # 다른 임베딩
    
    # Mock 주입
    processor.postgresql_manager = mock_postgresql
    processor.qdrant_manager = mock_qdrant
    processor.text_preprocessor = mock_preprocessor
    processor.embedding_service = mock_embedding
    
    # UPDATE 작업 데이터
    job_data = {
        'id': 'test_update_1',
        'type': 'update',
        'product_id': '12345'
    }
    
    # UPDATE 작업 처리
    result = await processor.process_job(job_data)
    
    # 결과 검증
    assert result.success == True, f"UPDATE 실패: {result.message}"
    assert result.job_type == JobType.UPDATE
    assert result.vector_id == 'existing_vector_123'  # 기존 벡터 ID 사용
    
    # Mock 호출 검증
    mock_qdrant.search_vectors.assert_called_once()  # 기존 벡터 검색
    mock_qdrant.upsert_vector_async.assert_called_once()  # 벡터 업데이트
    
    print(f"  ✅ UPDATE 연산 성공: {result.message}")
    print(f"  - 기존 벡터 ID 사용: {result.vector_id}")

async def test_delete_operation():
    """DELETE 연산 테스트"""
    print("\n🔥 DELETE 연산 테스트!")
    
    processor = JobProcessor()
    
    # Mock Qdrant (삭제할 벡터들 존재)
    mock_qdrant = AsyncMock(spec=QdrantManager)
    existing_vectors = [
        {'id': 'vector_1', 'score': 0.9},
        {'id': 'vector_2', 'score': 0.8}  # 중복 벡터가 있다고 가정
    ]
    mock_qdrant.search_vectors.return_value = existing_vectors
    mock_qdrant.delete_vector = AsyncMock()
    
    processor.qdrant_manager = mock_qdrant
    
    # DELETE 작업 데이터
    job_data = {
        'id': 'test_delete_1',
        'type': 'delete',
        'product_id': '12345'
    }
    
    # DELETE 작업 처리
    result = await processor.process_job(job_data)
    
    # 결과 검증
    assert result.success == True, f"DELETE 실패: {result.message}"
    assert result.job_type == JobType.DELETE
    assert "2개 벡터 삭제" in result.message
    
    # Mock 호출 검증
    mock_qdrant.search_vectors.assert_called_once()
    assert mock_qdrant.delete_vector.call_count == 2  # 2개 벡터 삭제
    
    print(f"  ✅ DELETE 연산 성공: {result.message}")

async def test_batch_processing():
    """배치 작업 처리 테스트"""
    print("\n🔥 배치 작업 처리 테스트!")
    
    processor = JobProcessor()
    
    # 여러 작업 데이터
    jobs = [
        {'id': 'batch_1', 'type': 'sync', 'product_id': '100'},
        {'id': 'batch_2', 'type': 'update', 'product_id': '101'},
        {'id': 'batch_3', 'type': 'delete', 'product_id': '102'},
        {'id': 'batch_4', 'type': 'invalid', 'product_id': '103'},  # 잘못된 타입
    ]
    
    # Mock process_job 메서드
    async def mock_process_job(job_data):
        job_type = job_data.get('type', '')
        product_id = job_data.get('product_id', '')
        
        if job_type == 'invalid':
            # 잘못된 작업 타입 에러
            return JobResult(
                job_id=job_data['id'],
                job_type=JobType.SYNC,
                product_id=product_id,
                success=False,
                message="잘못된 작업 타입",
                processing_time=0.1,
                error="ValueError"
            )
        else:
            # 성공적인 처리
            return JobResult(
                job_id=job_data['id'],
                job_type=JobType(job_type),
                product_id=product_id,
                success=True,
                message=f"{job_type} 성공",
                processing_time=0.1
            )
    
    # Mock 주입
    processor.process_job = mock_process_job
    
    # 배치 처리
    start_time = time.time()
    results = await processor.process_jobs_batch(jobs)
    batch_time = time.time() - start_time
    
    # 결과 검증
    assert len(results) == 4, f"결과 수 불일치: {len(results)}"
    
    success_count = sum(1 for r in results if r.success)
    assert success_count == 3, f"성공 수 불일치: {success_count}"
    
    # 개별 결과 검증
    assert results[0].job_type == JobType.SYNC
    assert results[1].job_type == JobType.UPDATE
    assert results[2].job_type == JobType.DELETE
    assert results[3].success == False  # 잘못된 타입
    
    print(f"  ✅ 배치 처리 완료: {success_count}/4 성공")
    print(f"  - 배치 처리 시간: {batch_time:.3f}초")
    print(f"  - 개별 작업 결과:")
    for i, result in enumerate(results):
        status = "✅" if result.success else "❌"
        print(f"    {status} {result.job_type.value}: {result.message}")

async def test_error_handling():
    """오류 처리 테스트"""
    print("\n🔥 오류 처리 테스트!")
    
    processor = JobProcessor()
    
    # 잘못된 작업 데이터들
    error_jobs = [
        {'id': 'error_1'},  # type 누락
        {'id': 'error_2', 'type': 'sync'},  # product_id 누락
        {'id': 'error_3', 'type': 'invalid_type', 'product_id': '123'},  # 잘못된 타입
    ]
    
    error_results = []
    for job in error_jobs:
        result = await processor.process_job(job)
        error_results.append(result)
        assert result.success == False, f"오류 작업이 성공으로 처리됨: {job}"
    
    print(f"  ✅ 오류 처리 테스트 완료: {len(error_results)}개 오류 정상 처리")
    for result in error_results:
        print(f"    ❌ {result.job_id}: {result.message}")

async def test_statistics():
    """통계 기능 테스트"""
    print("\n🔥 통계 기능 테스트!")
    
    processor = JobProcessor()
    
    # 초기 통계
    initial_stats = processor.get_stats()
    assert initial_stats['total_processed'] == 0
    
    # Mock으로 통계 데이터 조작
    processor.stats['total_processed'] = 10
    processor.stats['sync_count'] = 6
    processor.stats['update_count'] = 2
    processor.stats['delete_count'] = 2
    processor.stats['success_count'] = 8
    processor.stats['error_count'] = 2
    
    # 통계 확인
    stats = processor.get_stats()
    assert stats['total_processed'] == 10
    assert stats['success_rate'] == 80.0  # 8/10 * 100
    
    print(f"  ✅ 통계 기능 검증 완료:")
    print(f"    - 총 처리: {stats['total_processed']}개")
    print(f"    - SYNC: {stats['sync_count']}개")
    print(f"    - UPDATE: {stats['update_count']}개") 
    print(f"    - DELETE: {stats['delete_count']}개")
    print(f"    - 성공률: {stats['success_rate']:.1f}%")
    
    # 통계 초기화
    processor.reset_stats()
    reset_stats = processor.get_stats()
    assert reset_stats['total_processed'] == 0
    assert reset_stats['success_rate'] == 0.0
    
    print(f"  ✅ 통계 초기화 완료")

async def main():
    """모든 테스트 실행"""
    print("🚀 JobProcessor 종합 테스트 시작!\n")
    
    test_functions = [
        test_product_data_structure,
        test_job_processor_initialization,
        test_sync_operation,
        test_update_operation,
        test_delete_operation,
        test_batch_processing,
        test_error_handling,
        test_statistics,
    ]
    
    passed = 0
    total = len(test_functions)
    
    for test_func in test_functions:
        try:
            await test_func()
            passed += 1
        except Exception as e:
            print(f"❌ {test_func.__name__} 실패: {e}")
    
    print(f"\n🎯 JobProcessor 테스트 완료: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 테스트 통과! JobProcessor 구현이 완벽합니다!")
    else:
        print(f"⚠️ {total - passed}개 테스트 실패")

if __name__ == "__main__":
    asyncio.run(main()) 