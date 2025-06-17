#!/usr/bin/env python3
# test_embedding_service.py - 임베딩 서비스 테스트

import os
import numpy as np
from src.services.embedding_service import (
    EmbeddingService, 
    EmbeddingConfig,
    get_embedding_service,
    embed_text,
    embed_product
)
from src.config import get_settings

def test_embedding_service():
    print("=== OpenAI 임베딩 서비스 테스트 ===")
    
    # API 키 확인 (config.py에서)
    try:
        settings = get_settings()
        api_key = settings.OPENAI_API_KEY
        print(f"✅ API 키 확인 완료: {api_key[:8]}...")
    except Exception as e:
        print(f"❌ config.py에서 OPENAI_API_KEY를 읽을 수 없습니다: {e}")
        print("테스트를 건너뜁니다.")
        return
    
    try:
        # 임베딩 서비스 초기화
        config = EmbeddingConfig(
            model="text-embedding-3-large",
            dimensions=3072,
            batch_size=5,  # 테스트용으로 작게 설정
            max_retries=2
        )
        
        service = EmbeddingService(api_key=api_key, config=config)
        print(f"✅ 임베딩 서비스 초기화 완료")
        print(f"   모델: {config.model}")
        print(f"   차원: {config.dimensions}")
        
        # 1. 단일 텍스트 임베딩 테스트
        print("\n=== 단일 텍스트 임베딩 테스트 ===")
        test_text = "야마하 R3 2019년형 550만원 15,000km 상태 양호"
        
        print(f"입력 텍스트: {test_text}")
        print("임베딩 생성 중...")
        
        embedding = service.create_embedding(test_text)
        
        if embedding is not None:
            print(f"✅ 임베딩 생성 성공!")
            print(f"   차원: {embedding.shape}")
            print(f"   타입: {type(embedding)}")
            print(f"   첫 5개 값: {embedding[:5]}")
            print(f"   벡터 크기: {np.linalg.norm(embedding):.6f}")
        else:
            print("❌ 임베딩 생성 실패")
            return
        
        # 2. 배치 임베딩 테스트
        print("\n=== 배치 임베딩 테스트 ===")
        test_texts = [
            "야마하 R3 2019년형 550만원",
            "혼다 CBR600RR 2020년 780만원",
            "가와사키 닌자 2018년 1200만원",
            ""  # 빈 텍스트 테스트
        ]
        
        print(f"입력 텍스트 {len(test_texts)}개:")
        for i, text in enumerate(test_texts):
            print(f"  {i+1}: {text or '(빈 텍스트)'}")
        
        print("배치 임베딩 생성 중...")
        batch_embeddings = service.create_embeddings(test_texts)
        
        print(f"✅ 배치 임베딩 결과:")
        for i, emb in enumerate(batch_embeddings):
            if emb is not None:
                print(f"  {i+1}: 성공 (차원: {emb.shape})")
            else:
                print(f"  {i+1}: 실패 또는 빈 텍스트")
        
        # 3. 매물 데이터 임베딩 테스트
        print("\n=== 매물 데이터 임베딩 테스트 ===")
        product_data = {
            'title': '야마하 R3 2019년형 판매합니다',
            'price': 5500000,
            'year': 2019,
            'mileage': 15000,
            'content': '상태 양호, 사고무, 정기점검 완료. 성인 1인 라이더만 타던 차량입니다.',
            'brand': 'Yamaha',
            'model': 'R3'
        }
        
        print("매물 데이터:")
        for key, value in product_data.items():
            print(f"  {key}: {value}")
        
        print("매물 임베딩 생성 중...")
        product_embedding = service.embed_product_data(product_data)
        
        if product_embedding is not None:
            print(f"✅ 매물 임베딩 생성 성공!")
            print(f"   차원: {product_embedding.shape}")
        else:
            print("❌ 매물 임베딩 생성 실패")
        
        # 4. 편의 함수 테스트
        print("\n=== 편의 함수 테스트 ===")
        convenience_embedding = embed_text("테스트 텍스트")
        if convenience_embedding is not None:
            print(f"✅ embed_text() 성공: {convenience_embedding.shape}")
        
        convenience_product = embed_product(product_data)
        if convenience_product is not None:
            print(f"✅ embed_product() 성공: {convenience_product.shape}")
        
        # 5. 유사도 테스트
        print("\n=== 유사도 테스트 ===")
        text1 = "야마하 R3 스포츠바이크"
        text2 = "야마하 R3 오토바이"
        text3 = "혼다 CBR 레이싱"
        
        emb1 = service.create_embedding(text1)
        emb2 = service.create_embedding(text2)
        emb3 = service.create_embedding(text3)
        
        if all(e is not None for e in [emb1, emb2, emb3]):
            # 코사인 유사도 계산
            def cosine_similarity(a, b):
                return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
            
            sim_1_2 = cosine_similarity(emb1, emb2)
            sim_1_3 = cosine_similarity(emb1, emb3)
            sim_2_3 = cosine_similarity(emb2, emb3)
            
            print(f"'{text1}' vs '{text2}': {sim_1_2:.4f}")
            print(f"'{text1}' vs '{text3}': {sim_1_3:.4f}")
            print(f"'{text2}' vs '{text3}': {sim_2_3:.4f}")
            
            if sim_1_2 > sim_1_3:
                print("✅ 유사도 테스트 통과: 야마하 R3끼리 더 유사함")
            else:
                print("⚠️ 유사도 테스트 결과 예상과 다름")
        
        # 6. 서비스 통계
        print("\n=== 서비스 통계 ===")
        stats = service.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        print("\n🎉 모든 테스트 완료!")
        
    except Exception as e:
        print(f"❌ 테스트 중 에러 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_embedding_service()