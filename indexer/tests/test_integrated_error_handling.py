"""
통합된 오류 처리 시스템 테스트

JobProcessor + ErrorHandler 통합 테스트
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any

from src.workers.job_processor import JobProcessor, JobType, JobResult

class MockContextManager:
    """Mock Context Manager"""
    def __init__(self, conn):
        self.conn = conn
    
    async def __aenter__(self):
        return self.conn
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

async def test_jobprocessor_with_error_handler():
    """JobProcessor와 ErrorHandler 통합 테스트"""
    print("🔥 JobProcessor + ErrorHandler 통합 테스트!")
    
    # JobProcessor 생성
    processor = JobProcessor()
    
    # Mock 설정 (실제 DB 연결 없이)
    processor.postgresql_manager = AsyncMock()
    processor.qdrant_manager = AsyncMock()
    processor.error_handler = AsyncMock()
    
    # Mock 데이터베이스 초기화
    processor.postgresql_manager.initialize = AsyncMock()
    processor.qdrant_manager.initialize = AsyncMock()
    processor.error_handler.initialize = AsyncMock()
    
    # 초기화
    await processor.initialize()
    
    # Mock 데이터베이스 연결 (Context Manager)
    mock_conn = AsyncMock()
    
    # Context Manager Mock 설정
    def mock_get_connection():
        return MockContextManager(mock_conn)
    
    processor.postgresql_manager.get_connection = mock_get_connection
    
    # 성공 케이스 Mock 데이터
    mock_product_row = {
        'pid': 'test_123',
        'title': '테스트 오토바이',
        'price': 5000000,
        'content': '좋은 상태의 오토바이입니다',
        'year': 2020,
        'mileage': 15000
    }
    
    mock_conn.fetchrow.return_value = mock_product_row
    mock_conn.fetch.return_value = []  # 이미지 없음
    
    # Mock 임베딩 서비스
    processor.embedding_service.get_embedding_async = AsyncMock(return_value=[0.1] * 3072)
    
    # Mock Qdrant 작업
    processor.qdrant_manager.upsert_vector_async = AsyncMock()
    
    # 성공 케이스 테스트
    print("  📝 성공 케이스 테스트...")
    job_data = {
        'id': 'test_job_1',
        'type': 'sync',
        'product_id': 'test_123'
    }
    
    result = await processor.process_job(job_data)
    
    print(f"    📊 결과: success={result.success}, message={result.message}, error={result.error}")
    
    assert result.success == True, f"작업이 실패했습니다: {result.message}"
    assert result.job_type == JobType.SYNC
    assert result.product_id == 'test_123'
    
    print(f"    ✅ 성공 케이스: {result.message}")
    
    # 오류 케이스 테스트 (데이터베이스 오류)
    print("  📝 데이터베이스 오류 케이스 테스트...")
    
    # DB 연결 오류 시뮬레이션
    def error_get_connection():
        raise ConnectionError("PostgreSQL 연결 실패")
    
    processor.postgresql_manager.get_connection = error_get_connection
    
    # ErrorHandler Mock 설정
    mock_failed_op = MagicMock()
    mock_failed_op.id = 'error_123'
    processor.error_handler.handle_error = AsyncMock(return_value=mock_failed_op)
    
    error_job_data = {
        'id': 'test_job_error',
        'type': 'sync',
        'product_id': 'error_product'
    }
    
    error_result = await processor.process_job(error_job_data)
    
    assert error_result.success == False
    assert 'PostgreSQL 연결 실패' in error_result.message
    
    # ErrorHandler가 호출되었는지 확인
    processor.error_handler.handle_error.assert_called_once()
    
    print(f"    ✅ 오류 케이스: {error_result.message}")
    print(f"    ✅ ErrorHandler 호출됨")
    
    # 통계 확인
    stats = processor.get_stats()
    assert stats['total_processed'] == 2
    assert stats['success_count'] == 1
    assert stats['error_count'] == 1
    
    print(f"    ✅ 통계: 총 {stats['total_processed']}개, 성공 {stats['success_count']}개, 오류 {stats['error_count']}개")
    
    # 정리
    await processor.close()
    
    print("  🎯 JobProcessor + ErrorHandler 통합 테스트 완료!")

async def main():
    """모든 통합 테스트 실행"""
    print("🚀 통합된 오류 처리 시스템 테스트 시작!\n")
    
    try:
        await test_jobprocessor_with_error_handler()
        print("🎉 통합 테스트 통과! 시스템이 완벽하게 통합되었습니다!")
    except Exception as e:
        print(f"❌ 통합 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())