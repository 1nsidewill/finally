"""
데이터베이스 통합 테스트
기존 PostgreSQL과 Qdrant 매니저와의 연동 테스트
"""
import asyncio
import logging
from typing import Dict, Any
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.config import get_settings
from src.database import postgres_manager, qdrant_manager
from src.services.text_preprocessor import ProductTextPreprocessor
from src.services.batch_processor import BatchProcessor, BatchConfig

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_database_connections():
    """데이터베이스 연결 테스트"""
    print("\n🔌 데이터베이스 연결 테스트")
    
    try:
        # PostgreSQL 헬스체크
        pg_health = await postgres_manager.health_check()
        print(f"✅ PostgreSQL 연결: {pg_health}")
        
        # Qdrant 헬스체크
        qdrant_health = await qdrant_manager.health_check()
        print(f"✅ Qdrant 연결: {qdrant_health}")
        
        return True
        
    except Exception as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")
        return False

async def test_text_preprocessing():
    """텍스트 전처리 테스트"""
    print("\n📝 텍스트 전처리 테스트")
    
    try:
        preprocessor = ProductTextPreprocessor()
        
        # 테스트 데이터
        test_product = {
            'title': '야마하 YZF-R3 판매합니다',
            'price': 800,
            'year': 2020,
            'mileage': 15000,
            'content': '깔끔한 상태입니다. 정기점검 받았어요.'
        }
        
        # 전처리 실행
        processed_text = preprocessor.preprocess_product_data(test_product)
        print(f"✅ 전처리 결과: {processed_text}")
        
        return True
        
    except Exception as e:
        print(f"❌ 텍스트 전처리 실패: {e}")
        return False

async def test_embedding_generation():
    """임베딩 생성 테스트"""
    print("\n🤖 임베딩 생성 테스트")
    
    try:
        # 테스트 텍스트 리스트
        test_texts = [
            "[YAMAHA R3] 야마하 YZF-R3 판매합니다 스펙: 2020년 | 800만원 | 15,000km 상세: 깔끔한 상태입니다.",
            "[HONDA CBR] 혼다 CBR600RR 급매 스펙: 2019년 | 1200만원 | 8,500km 상세: 풀 정비 완료된 상태입니다."
        ]
        
        # 배치 임베딩 생성
        embeddings = await qdrant_manager.generate_embeddings_batch(test_texts)
        
        print(f"✅ 임베딩 생성 완료:")
        for i, (text, embedding) in enumerate(zip(test_texts, embeddings)):
            if embedding is not None:
                print(f"  - 텍스트 {i+1}: {len(embedding)}차원 벡터 생성")
            else:
                print(f"  - 텍스트 {i+1}: 임베딩 생성 실패")
        
        return True
        
    except Exception as e:
        print(f"❌ 임베딩 생성 실패: {e}")
        return False

async def test_qdrant_operations():
    """Qdrant 벡터 연산 테스트"""
    print("\n📊 Qdrant 벡터 연산 테스트")
    
    try:
        # 컬렉션 정보 확인
        collection_info = await qdrant_manager.get_collection_info()
        print(f"✅ 컬렉션 정보: {collection_info}")
        
        # 테스트 벡터 데이터
        test_vector = [0.1] * 3072  # 3072차원 더미 벡터
        test_metadata = {
            'uid': 999999,  # 테스트용 ID
            'title': '테스트 매물',
            'price': 1000,
            'content': '테스트 내용입니다.',
            'processed_text': '[TEST] 테스트 매물 스펙: 2023년 | 1000만원 상세: 테스트 내용'
        }
        
        # 벡터 업서트 테스트
        await qdrant_manager.upsert_vector_async(
            vector_id="999999",
            vector=test_vector,
            metadata=test_metadata
        )
        print("✅ 테스트 벡터 업서트 완료")
        
        # 벡터 검색 테스트
        search_results = await qdrant_manager.search_similar_vectors(
            query_vector=test_vector,
            limit=1
        )
        
        if search_results:
            print(f"✅ 벡터 검색 완료: {len(search_results)}개 결과")
            print(f"  - 최상위 결과 스코어: {search_results[0].score}")
        else:
            print("⚠️ 검색 결과 없음")
        
        return True
        
    except Exception as e:
        print(f"❌ Qdrant 연산 실패: {e}")
        return False

async def test_postgresql_operations():
    """PostgreSQL 연산 테스트"""
    print("\n🗄️ PostgreSQL 연산 테스트")
    
    try:
        # 미처리 제품 조회 테스트
        unprocessed_products = await postgres_manager.get_products_by_conversion_status(
            is_conversion=False,
            limit=5
        )
        
        print(f"✅ 미처리 제품 조회: {len(unprocessed_products)}개")
        
        if unprocessed_products:
            # 첫 번째 제품 정보 출력
            first_product = unprocessed_products[0]
            print(f"  - 첫 번째 제품 ID: {first_product.get('uid')}")
            print(f"  - 제목: {first_product.get('title', '')[:50]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL 연산 실패: {e}")
        return False

async def test_batch_processor():
    """배치 프로세서 통합 테스트"""
    print("\n⚙️ 배치 프로세서 통합 테스트")
    
    try:
        # 설정에서 값 가져오기
        settings = get_settings()
        
        # 테스트용 배치 설정
        test_config = BatchConfig(
            batch_size=2,  # 작은 배치로 테스트
            max_concurrent_batches=1,
            delay_between_batches=0.5,
            max_retries=2,
            save_progress_every=1,
            log_every=1
        )
        
        # 배치 프로세서 생성
        processor = BatchProcessor(
            postgres_manager=postgres_manager,
            qdrant_manager=qdrant_manager,
            config=test_config
        )
        
        print(f"✅ 배치 프로세서 생성 완료")
        print(f"  - 배치 크기: {test_config.batch_size}")
        print(f"  - 최대 재시도: {test_config.max_retries}")
        
        # 미처리 제품 조회 테스트
        unprocessed = await processor.get_unprocessed_products(limit=5)
        print(f"✅ 미처리 제품 조회: {len(unprocessed)}개")
        
        if unprocessed:
            # 첫 번째 제품으로 텍스트 전처리 테스트
            first_product = unprocessed[0]
            processed_text = processor.text_preprocessor.preprocess_product_data({
                'title': first_product.get('title', ''),
                'price': first_product.get('price', 0),
                'content': first_product.get('content', '')
            })
            print(f"✅ 전처리 샘플: {processed_text[:100]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ 배치 프로세서 테스트 실패: {e}")
        return False

async def test_config_settings():
    """config.py 설정 테스트"""
    print("\n⚙️ 설정 테스트")
    
    try:
        settings = get_settings()
        
        print(f"✅ 설정 로드 완료:")
        print(f"  - BATCH_SIZE: {settings.BATCH_SIZE}")
        print(f"  - MAX_RETRIES: {settings.MAX_RETRIES}")
        print(f"  - EMBEDDING_BATCH_SIZE: {settings.EMBEDDING_BATCH_SIZE}")
        print(f"  - LOG_LEVEL: {settings.LOG_LEVEL}")
        print(f"  - VECTOR_SIZE: {settings.VECTOR_SIZE}")
        print(f"  - QDRANT_COLLECTION: {settings.QDRANT_COLLECTION}")
        
        return True
        
    except Exception as e:
        print(f"❌ 설정 테스트 실패: {e}")
        return False

async def main():
    """모든 통합 테스트 실행"""
    print("🚀 데이터베이스 통합 테스트 시작")
    
    tests = [
        ("설정 테스트", test_config_settings),
        ("데이터베이스 연결", test_database_connections),
        ("텍스트 전처리", test_text_preprocessing),
        ("임베딩 생성", test_embedding_generation),
        ("PostgreSQL 연산", test_postgresql_operations),
        ("Qdrant 연산", test_qdrant_operations),
        ("배치 프로세서", test_batch_processor),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} 테스트 중 예외 발생: {e}")
            results.append((test_name, False))
    
    # 결과 요약
    print("\n📊 테스트 결과 요약")
    print("=" * 50)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n총 {len(results)}개 테스트 중 {passed}개 통과 ({passed/len(results)*100:.1f}%)")
    
    if passed == len(results):
        print("🎉 모든 통합 테스트 통과!")
    else:
        print("⚠️ 일부 테스트 실패 - 로그를 확인하세요.")

if __name__ == "__main__":
    asyncio.run(main()) 