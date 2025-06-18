import asyncio
import sys
import traceback
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from bulk_sync_with_checkpoints import BulkSynchronizer

async def debug_single_product():
    """Debug processing a single product with detailed error information"""
    print("🔍 단일 제품 디버깅 시작")
    print("=" * 50)
    
    # Create synchronizer
    synchronizer = BulkSynchronizer(batch_size=1)
    
    # Initialize
    if not await synchronizer.initialize():
        print("❌ 초기화 실패")
        return False
    
    # Get first product
    products = await synchronizer.get_products_batch(0, 1)
    if not products:
        print("⚠️ 처리할 제품이 없습니다.")
        return True
    
    product = products[0]
    print(f"🏍️ 디버깅 대상 제품:")
    print(f"  • UID: {product['uid']}")
    print(f"  • 제목: {product['title'][:100]}...")
    print(f"  • 컨텐츠: {product['content'][:100] if product['content'] else 'None'}...")
    print(f"  • 브랜드: {product['brand']}")
    print(f"  • 가격: {product['price']}")
    print(f"  • 상태: {product['status']}")
    print(f"  • is_conversion: {product['is_conversion']}")
    
    print(f"\\n🚀 단계별 처리 시작...")
    
    try:
        # Step 1: Create embedding text
        embedding_text = f"{product['title']} {product['content']} 브랜드:{product['brand']} 가격:{product['price']}원"
        print(f"\\n1️⃣ 임베딩 텍스트 생성:")
        print(f"  • 길이: {len(embedding_text)} 문자")
        print(f"  • 텍스트: {embedding_text[:200]}...")
        
        # Step 2: Generate embedding
        print(f"\\n2️⃣ 임베딩 생성 중...")
        embedding_array = synchronizer.embedding_service.create_embedding(embedding_text)
        if embedding_array is None:
            print("❌ 임베딩 생성 실패")
            return False
        
        print(f"  ✅ 임베딩 생성 성공:")
        print(f"  • 타입: {type(embedding_array)}")
        print(f"  • 형태: {embedding_array.shape if hasattr(embedding_array, 'shape') else 'No shape'}")
        print(f"  • 길이: {len(embedding_array)}")
        
        # Convert to list
        embedding = embedding_array.tolist()
        print(f"  • 리스트 변환 성공: {len(embedding)} 차원")
        
        # Step 3: Insert into Qdrant
        print(f"\\n3️⃣ Qdrant에 벡터 삽입 중...")
        qdrant_result = await synchronizer.qdrant_manager.upsert_vector_async(
            vector_id=str(product['uid']),
            vector=embedding,
            metadata={
                "title": product['title'],
                "content": product['content'] or "",
                "brand": product['brand'] or "",
                "price": float(product['price']) if product['price'] else 0.0,
                "status": product['status'],
                "uid": product['uid']
            }
        )
        print(f"  ✅ Qdrant 삽입 성공:")
        print(f"  • 결과: {qdrant_result}")
        
        # Step 4: Update PostgreSQL
        print(f"\\n4️⃣ PostgreSQL 업데이트 중...")
        async with synchronizer.pg_manager.get_connection() as conn:
            result = await conn.execute("""
                UPDATE product 
                SET is_conversion = true 
                WHERE uid = $1
            """, product['uid'])
            print(f"  ✅ PostgreSQL 업데이트 성공:")
            print(f"  • 결과: {result}")
        
        print(f"\\n🎉 모든 단계 성공!")
        return True
        
    except Exception as e:
        print(f"\\n❌ 에러 발생:")
        print(f"  • 타입: {type(e).__name__}")
        print(f"  • 메시지: {str(e)}")
        print(f"  • 상세 스택 트레이스:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    asyncio.run(debug_single_product()) 