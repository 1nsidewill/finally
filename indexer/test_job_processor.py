#!/usr/bin/env python3

import asyncio
import json
from src.workers.job_processor import JobProcessor

async def test_updated_job_processor():
    """수정된 JobProcessor 테스트"""
    
    print("🧪 === 수정된 JobProcessor 테스트 ===")
    
    # JobProcessor 초기화
    processor = JobProcessor()
    await processor.initialize()
    
    try:
        # 테스트할 PID (실제 데이터에서 확인했던 것)
        test_pid = "302431298"  # 17년식 PCX125
        
        print(f"\n1️⃣ 제품 데이터 조회 테스트 (PID: {test_pid})")
        product_data = await processor._fetch_product_data(test_pid)
        
        print(f"✅ 제품 데이터 조회 성공:")
        print(f"  - PID: {product_data.pid}")
        print(f"  - Title: {product_data.title}")
        print(f"  - Brand: '{product_data.brand}' (길이: {len(product_data.brand or '')})")
        print(f"  - Price: {product_data.price}")
        print(f"  - Year: {product_data.year}")
        print(f"  - ODO: {product_data.odo}")
        print(f"  - Page URL: {product_data.page_url}")
        print(f"  - Images: {len(product_data.images)}개")
        for i, img in enumerate(product_data.images[:3], 1):
            print(f"    {i}. {img}")
        if len(product_data.images) > 3:
            print(f"    ... 총 {len(product_data.images)}개")
        
        print(f"\n2️⃣ 텍스트 전처리 테스트")
        preprocessed_text = processor.text_preprocessor.preprocess_product_data({
            'title': product_data.title,
            'brand': product_data.brand,
            'year': product_data.year,
            'price': product_data.price,
            'odo': product_data.odo,
            'content': product_data.content
        })
        
        print(f"✅ 전처리된 텍스트 ({len(preprocessed_text)}자):")
        print(f"  {preprocessed_text}")
        
        print(f"\n3️⃣ 실제 Job 처리 테스트 (SYNC)")
        job_data = {
            'id': 'test-job-001',
            'type': 'sync',
            'product_id': test_pid
        }
        
        result = await processor.process_job(job_data)
        
        print(f"✅ Job 처리 결과:")
        print(f"  - Success: {result.success}")
        print(f"  - Message: {result.message}")
        print(f"  - Vector ID: {result.vector_id}")
        print(f"  - Processing Time: {result.processing_time:.3f}s")
        
        if result.error:
            print(f"  - Error: {result.error}")
        
        print(f"\n4️⃣ Qdrant 벡터 확인")
        if result.success and result.vector_id:
            from src.database.qdrant import QdrantManager
            qdrant = QdrantManager()
            
            # 벡터 조회
            vectors = await qdrant.search_vectors(
                query_text="",  # 빈 쿼리
                filter_conditions={"product_id": test_pid},
                limit=1
            )
            
            if vectors:
                vector_data = vectors[0]
                print(f"✅ Qdrant 벡터 확인 성공:")
                print(f"  - Vector ID: {vector_data['id']}")
                print(f"  - Score: {vector_data.get('score', 'N/A')}")
                print(f"  - Metadata:")
                for key, value in vector_data.get('payload', {}).items():
                    if isinstance(value, str) and len(value) > 100:
                        print(f"    {key}: {value[:100]}...")
                    else:
                        print(f"    {key}: {value}")
            else:
                print("❌ Qdrant에서 벡터를 찾을 수 없음")
        
        print(f"\n5️⃣ 통계 확인")
        stats = processor.get_stats()
        print(f"✅ Processor 통계:")
        for key, value in stats.items():
            print(f"  - {key}: {value}")
            
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await processor.close()
        print(f"\n✅ 테스트 완료")

if __name__ == "__main__":
    asyncio.run(test_updated_job_processor()) 