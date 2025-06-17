#!/usr/bin/env python3
# test_embedding_service_mock.py - Mock 임베딩 서비스 테스트

import numpy as np
from unittest.mock import patch, MagicMock
from src.services.embedding_service import (
    EmbeddingService, 
    EmbeddingConfig,
    ProductTextPreprocessor
)

def mock_openai_response():
    """OpenAI API 응답을 시뮬레이션하는 Mock"""
    mock_response = MagicMock()
    mock_response.data = [
        MagicMock(embedding=np.random.rand(3072).tolist())
    ]
    mock_response.usage.total_tokens = 150
    return mock_response

def test_embedding_service_mock():
    print("=== Mock 임베딩 서비스 테스트 ===")
    
    try:
        # Mock OpenAI API 호출
        with patch('src.services.embedding_service.OpenAI') as mock_openai:
            with patch('src.services.embedding_service.AsyncOpenAI') as mock_async_openai:
                # Mock 클라이언트 설정
                mock_client = MagicMock()
                mock_openai.return_value = mock_client
                mock_async_openai.return_value = mock_client
                
                # Mock 임베딩 응답 설정
                mock_client.embeddings.create.return_value = mock_openai_response()
                
                # 임베딩 서비스 초기화 (더미 API 키 사용)
                config = EmbeddingConfig(
                    model="text-embedding-3-large",
                    dimensions=3072,
                    batch_size=5,
                    max_retries=2
                )
                
                service = EmbeddingService(api_key="sk-test-dummy", config=config)
                print(f"✅ Mock 임베딩 서비스 초기화 완료")
                print(f"   모델: {config.model}")
                print(f"   차원: {config.dimensions}")
                
                # 1. 텍스트 전처리 테스트
                print("\n=== 텍스트 전처리 테스트 ===")
                preprocessor = ProductTextPreprocessor()
                
                test_product = {
                    'title': '야마하 R3 2019년형 판매합니다',
                    'price': 5500000,
                    'year': 2019,
                    'mileage': 15000,
                    'content': '상태 양호, 사고무, 정기점검 완료.',
                }
                
                processed_text = preprocessor.preprocess_product_data(test_product)
                print(f"원본 매물 데이터: {test_product}")
                print(f"전처리된 텍스트: {processed_text}")
                
                # 2. Mock 단일 임베딩 테스트
                print("\n=== Mock 단일 임베딩 테스트 ===")
                test_text = "야마하 R3 2019년형 550만원"
                
                embedding = service.create_embedding(test_text)
                if embedding is not None:
                    print(f"✅ Mock 임베딩 생성 성공!")
                    print(f"   차원: {embedding.shape}")
                    print(f"   타입: {type(embedding)}")
                    print(f"   첫 5개 값: {embedding[:5]}")
                
                # 3. Mock 배치 임베딩 테스트
                print("\n=== Mock 배치 임베딩 테스트 ===")
                test_texts = [
                    "야마하 R3 2019년형",
                    "혼다 CBR600RR",
                    "가와사키 닌자",
                    ""  # 빈 텍스트
                ]
                
                # 배치 크기만큼 Mock 응답 설정
                mock_client.embeddings.create.side_effect = [
                    mock_openai_response(),
                    mock_openai_response()
                ]
                
                batch_embeddings = service.create_embeddings(test_texts)
                print(f"배치 임베딩 결과:")
                for i, emb in enumerate(batch_embeddings):
                    if emb is not None:
                        print(f"  {i+1}: 성공 (차원: {emb.shape})")
                    else:
                        print(f"  {i+1}: 실패 또는 빈 텍스트")
                
                # 4. Mock 매물 데이터 임베딩 테스트
                print("\n=== Mock 매물 데이터 임베딩 테스트 ===")
                mock_client.embeddings.create.return_value = mock_openai_response()
                
                product_embedding = service.embed_product_data(test_product)
                if product_embedding is not None:
                    print(f"✅ Mock 매물 임베딩 생성 성공!")
                    print(f"   차원: {product_embedding.shape}")
                
                # 5. 설정 및 통계 테스트
                print("\n=== 서비스 설정 테스트 ===")
                config_info = service.get_config()
                print(f"모델: {config_info.model}")
                print(f"차원: {config_info.dimensions}")
                print(f"배치 크기: {config_info.batch_size}")
                
                stats = service.get_stats()
                print(f"서비스 통계: {stats}")
                
                # 6. Rate Limiter 테스트
                print("\n=== Rate Limiter 테스트 ===")
                rate_limiter = service.rate_limiter
                
                can_request = rate_limiter.can_make_request(1000)
                print(f"요청 가능 여부: {can_request}")
                
                # 요청 기록
                rate_limiter.record_request(150)
                print(f"요청 기록 완료 (150 토큰)")
                
                wait_time = rate_limiter.wait_time_needed(1000)
                print(f"다음 요청까지 대기 시간: {wait_time:.2f}초")
                
                print("\n🎉 모든 Mock 테스트 완료!")
                
    except Exception as e:
        print(f"❌ Mock 테스트 중 에러 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_embedding_service_mock() 