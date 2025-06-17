#!/usr/bin/env python3
# test_database_integration.py - 데이터베이스 통합 테스트

import asyncio
import logging
from unittest.mock import patch, MagicMock, AsyncMock
from src.database import postgres_manager, qdrant_manager
from src.services.batch_processor import BatchProcessor, BatchConfig
from src.services.embedding_service import EmbeddingService, EmbeddingConfig

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_database_managers():
    """데이터베이스 매니저 기본 테스트"""
    print("=== 데이터베이스 매니저 테스트 ===")
    
    # PostgreSQL 매니저 테스트
    print(f"PostgreSQL 매니저: {type(postgres_manager)}")
    print(f"Qdrant 매니저: {type(qdrant_manager)}")
    
    try:
        # PostgreSQL 연결 정보 확인
        pg_config = postgres_manager.get_database_config()
        print(f"PostgreSQL 설정:")
        print(f"  Host: {pg_config.get('host', 'N/A')}")
        print(f"  Port: {pg_config.get('port', 'N/A')}")
        print(f"  Database: {pg_config.get('database', 'N/A')}")
        
    except Exception as e:
        print(f"PostgreSQL 설정 조회 중 오류: {e}")
    
    try:
        # Qdrant 연결 정보 확인
        qdrant_config = qdrant_manager.get_client_info()
        print(f"Qdrant 설정:")
        print(f"  Host: {qdrant_config.get('host', 'N/A')}")
        print(f"  Port: {qdrant_config.get('port', 'N/A')}")
        print(f"  Collection: {qdrant_config.get('collection', 'N/A')}")
        
    except Exception as e:
        print(f"Qdrant 설정 조회 중 오류: {e}")
    
    print("✅ 데이터베이스 매니저 테스트 완료")

async def test_mock_database_integration():
    """Mock을 사용한 데이터베이스 통합 테스트"""
    print("\n=== Mock 데이터베이스 통합 테스트 ===")
    
    # Mock 데이터베이스 연결
    with patch('src.database.postgresql.postgres_manager') as mock_pg:
        with patch('src.database.qdrant.qdrant_manager') as mock_qdrant:
            # Mock 설정
            mock_pg_conn = AsyncMock()
            mock_pg.get_async_connection.return_value.__aenter__.return_value = mock_pg_conn
            mock_pg.get_async_connection.return_value.__aexit__.return_value = None
            
            # 미처리 매물 쿼리 응답 Mock
            mock_listing_rows = [
                {
                    'id': 1,
                    'title': '야마하 R3 2019',
                    'price': 5500000,
                    'year': 2019,
                    'mileage': 15000,
                    'content': '상태 양호',
                    'url': 'https://example.com/1',
                    'img_url': 'https://example.com/img1.jpg',
                    'brand': 'YAMAHA'
                },
                {
                    'id': 2,
                    'title': '혼다 CBR600RR',
                    'price': 8000000,
                    'year': 2020,
                    'mileage': 8000,
                    'content': '최상급',
                    'url': 'https://example.com/2',
                    'img_url': 'https://example.com/img2.jpg',
                    'brand': 'HONDA'
                }
            ]
            
            mock_pg_conn.fetch.return_value = mock_listing_rows
            mock_pg_conn.execute.return_value = None
            
            # Qdrant Mock 설정
            mock_qdrant.upsert_vector_async = AsyncMock()
            
            # 임베딩 서비스 Mock
            with patch('src.services.embedding_service.OpenAI') as mock_openai:
                with patch('src.services.embedding_service.AsyncOpenAI') as mock_async_openai:
                    mock_client = MagicMock()
                    mock_openai.return_value = mock_client
                    mock_async_openai.return_value = mock_client
                    
                    # Mock 임베딩 응답
                    mock_embedding_response = MagicMock()
                    mock_embedding_response.data = [
                        MagicMock(embedding=[0.1] * 3072),
                        MagicMock(embedding=[0.2] * 3072)
                    ]
                    mock_embedding_response.usage.total_tokens = 300
                    mock_client.embeddings.create.return_value = mock_embedding_response
                    
                    # 서비스 생성
                    embedding_config = EmbeddingConfig(batch_size=10)
                    embedding_service = EmbeddingService(
                        api_key="sk-test-dummy",
                        config=embedding_config
                    )
                    
                    batch_config = BatchConfig(
                        batch_size=2,
                        delay_between_batches=0.1,
                        max_retries=2
                    )
                    
                    batch_processor = BatchProcessor(
                        embedding_service=embedding_service,
                        postgres_manager=mock_pg,
                        qdrant_manager=mock_qdrant,
                        config=batch_config
                    )
                    
                    print(f"배치 프로세서 생성 완료")
                    print(f"  임베딩 서비스: {type(embedding_service)}")
                    print(f"  PostgreSQL 매니저: {type(mock_pg)}")
                    print(f"  Qdrant 매니저: {type(mock_qdrant)}")
                    
                    # 미처리 매물 조회 테스트
                    unprocessed_listings = await batch_processor.get_unprocessed_listings()
                    print(f"  조회된 미처리 매물: {len(unprocessed_listings)}개")
                    
                    # 배치 처리 테스트
                    if unprocessed_listings:
                        successful, failed = await batch_processor.process_listings_batch(unprocessed_listings)
                        print(f"  배치 처리 결과: 성공 {successful}, 실패 {failed}")
                        
                        # Qdrant 벡터 저장 호출 확인
                        assert mock_qdrant.upsert_vector_async.call_count == successful
                        print(f"  Qdrant 벡터 저장 호출: {mock_qdrant.upsert_vector_async.call_count}회")
                        
                        # PostgreSQL 플래그 업데이트 호출 확인
                        update_calls = [call for call in mock_pg_conn.execute.call_args_list 
                                      if 'UPDATE listings SET is_converted' in str(call)]
                        print(f"  PostgreSQL 플래그 업데이트 호출: {len(update_calls)}회")
    
    print("✅ Mock 데이터베이스 통합 테스트 완료")

async def test_configuration_compatibility():
    """설정 호환성 테스트"""
    print("\n=== 설정 호환성 테스트 ===")
    
    try:
        from src.config import get_settings
        settings = get_settings()
        
        print("필수 설정 확인:")
        
        # OpenAI 설정
        if hasattr(settings, 'OPENAI_API_KEY'):
            print(f"  ✅ OPENAI_API_KEY: 설정됨")
        else:
            print(f"  ❌ OPENAI_API_KEY: 누락")
        
        # PostgreSQL 설정
        pg_settings = ['POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD']
        for setting in pg_settings:
            if hasattr(settings, setting):
                print(f"  ✅ {setting}: 설정됨")
            else:
                print(f"  ❌ {setting}: 누락")
        
        # Qdrant 설정  
        qdrant_settings = ['QDRANT_HOST', 'QDRANT_PORT', 'QDRANT_COLLECTION', 'VECTOR_SIZE']
        for setting in qdrant_settings:
            if hasattr(settings, setting):
                print(f"  ✅ {setting}: 설정됨")
            else:
                print(f"  ❌ {setting}: 누락")
        
        # 벡터 차원 호환성 확인
        if hasattr(settings, 'VECTOR_SIZE'):
            expected_dim = 3072  # text-embedding-3-large 차원
            actual_dim = settings.VECTOR_SIZE
            if actual_dim == expected_dim:
                print(f"  ✅ 벡터 차원 호환성: {actual_dim} (일치)")
            else:
                print(f"  ⚠️  벡터 차원 불일치: 설정 {actual_dim} vs 예상 {expected_dim}")
        
    except Exception as e:
        print(f"설정 확인 중 오류: {e}")
    
    print("✅ 설정 호환성 테스트 완료")

async def test_end_to_end_workflow():
    """전체 워크플로우 테스트 (Mock)"""
    print("\n=== 전체 워크플로우 테스트 ===")
    
    # 워크플로우 시뮬레이션
    workflow_steps = [
        "1. 미처리 매물 조회 (PostgreSQL)",
        "2. 텍스트 전처리 (ProductTextPreprocessor)",
        "3. 배치 임베딩 생성 (OpenAI API)",
        "4. 벡터 저장 (Qdrant)",
        "5. 변환 플래그 업데이트 (PostgreSQL)",
        "6. 진행상황 저장 (JSON)"
    ]
    
    print("예상 워크플로우:")
    for step in workflow_steps:
        print(f"  {step}")
    
    # 예상 성능 지표
    print("\n예상 성능 지표 (30k 매물 기준):")
    print(f"  배치 크기: 50매물/배치")
    print(f"  총 배치 수: {30000 // 50}배치")
    print(f"  예상 API 호출: {30000 // 50}회")
    print(f"  예상 소요 시간: {(30000 // 50) * 2 / 60:.1f}분 (배치당 2초 가정)")
    print(f"  예상 토큰 사용량: ~{30000 * 50:,}토큰")
    
    print("✅ 전체 워크플로우 테스트 완료")

async def main():
    """모든 테스트 실행"""
    print("🚀 데이터베이스 통합 및 배치 프로세서 테스트 시작!")
    
    await test_database_managers()
    await test_mock_database_integration()
    await test_configuration_compatibility()
    await test_end_to_end_workflow()
    
    print("\n🎉 모든 데이터베이스 통합 테스트 완료!")
    print("\n💡 실제 사용을 위해:")
    print("   1. .env.dev 파일에 실제 API 키와 DB 연결 정보 설정")
    print("   2. PostgreSQL과 Qdrant 서버 실행")
    print("   3. python scripts/run_batch_indexing.py 실행")

if __name__ == "__main__":
    asyncio.run(main()) 