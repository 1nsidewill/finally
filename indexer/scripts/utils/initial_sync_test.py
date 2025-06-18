"""
첫 실제 데이터 동기화 테스트
PostgreSQL → 임베딩 생성 → Qdrant 삽입
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager
from src.services.embedding_service import EmbeddingService
from src.config import get_settings

settings = get_settings()

async def extract_and_process_products(limit: int = 30):
    """PostgreSQL에서 제품 추출 및 임베딩 처리"""
    
    # 매니저들 초기화
    pg_manager = PostgreSQLManager()
    qdrant_manager = QdrantManager()
    embedding_service = EmbeddingService()
    
    try:
        print(f"🚀 {limit}개 제품 추출 및 처리 시작...")
        
        # 1. PostgreSQL에서 데이터 추출
        print("📤 PostgreSQL에서 데이터 추출 중...")
        extract_query = """
        SELECT 
            uid,
            title,
            content,
            brand,
            price,
            location,
            category,
            color,
            odo,
            year,
            status,
            is_conversion,
            created_dt,
            updated_dt
        FROM product 
        WHERE is_conversion = false 
        AND status = 1
        ORDER BY created_dt DESC
        LIMIT $1
        """
        
        products = await pg_manager.execute_query(extract_query, limit)
        actual_count = len(products)
        print(f"✅ {actual_count}개 제품 추출 완료")
        
        if actual_count == 0:
            print("⚠️  조건에 맞는 제품이 없습니다")
            return
        
        # 2. 임베딩 생성 및 Qdrant 데이터 준비
        print("🧠 임베딩 생성 중...")
        processed_products = []
        
        for i, product in enumerate(products, 1):
            print(f"   처리 중... {i}/{actual_count}: {product['title'][:30]}...")
            
            # 임베딩을 위한 텍스트 조합
            text_for_embedding = f"""{product['title']}
{product['content'] if product['content'] else ''}
브랜드: {product['brand'] if product['brand'] else 'N/A'}
가격: {product['price'] if product['price'] else 'N/A'}원
위치: {product['location'] if product['location'] else 'N/A'}
카테고리: {product['category'] if product['category'] else 'N/A'}
색상: {product['color'] if product['color'] else 'N/A'}
주행거리: {product['odo'] if product['odo'] else 'N/A'}km
연식: {product['year'] if product['year'] else 'N/A'}년""".strip()
            
            # 임베딩 생성
            try:
                embedding = embedding_service.create_embedding(text_for_embedding)
                
                # Qdrant 포인트 구조 생성
                point_data = {
                    "id": int(product['uid']),
                    "vector": embedding,
                    "payload": {
                        "uid": int(product['uid']),
                        "title": product['title'],
                        "content": product['content'],
                        "brand": product['brand'],
                        "price": float(product['price']) if product['price'] else None,
                        "location": product['location'],
                        "category": product['category'],
                        "color": product['color'],
                        "odo": product['odo'],
                        "year": product['year'],
                        "status": product['status'],
                        "created_dt": product['created_dt'].isoformat() if product['created_dt'] else None,
                        "updated_dt": product['updated_dt'].isoformat() if product['updated_dt'] else None,
                        "text_for_embedding": text_for_embedding[:500] + "..." if len(text_for_embedding) > 500 else text_for_embedding
                    }
                }
                
                processed_products.append(point_data)
                
            except Exception as e:
                print(f"   ❌ 임베딩 생성 실패 (ID: {product['uid']}): {e}")
                continue
        
        print(f"✅ {len(processed_products)}개 임베딩 생성 완료")
        
        # 3. Qdrant 컬렉션 확인 및 생성
        print("🗂️  Qdrant 컬렉션 확인 및 생성 중...")
        collection_created = await qdrant_manager.create_collection_if_not_exists()
        
        if collection_created:
            print("📁 새 컬렉션 생성 완료")
        else:
            print("✅ 컬렉션 이미 존재")
        
        # 4. Qdrant에 데이터 삽입
        print("📥 Qdrant에 데이터 삽입 중...")
        success_count = 0
        
        for point_data in processed_products:
            try:
                result = await qdrant_manager.upsert_vector_async(
                    vector_id=str(point_data["id"]),
                    vector=point_data["vector"],
                    metadata=point_data["payload"]
                )
                success_count += 1
                print(f"   ✅ 삽입 성공: ID {point_data['id']} -> UUID {result.get('uuid', 'N/A')}")
                
            except Exception as e:
                print(f"   ❌ 삽입 실패 (ID: {point_data['id']}): {e}")
                continue
        
        print(f"✅ Qdrant 삽입 완료: {success_count}/{len(processed_products)}개 성공")
        
        # 5. PostgreSQL에서 is_conversion 플래그 업데이트
        if success_count > 0:
            print("🔄 PostgreSQL is_conversion 플래그 업데이트 중...")
            successful_ids = [p["id"] for p in processed_products[:success_count]]
            
            update_query = """
            UPDATE product 
            SET is_conversion = true, updated_dt = NOW()
            WHERE uid = ANY($1)
            """
            
            result = await pg_manager.execute_command(update_query, successful_ids)
            print(f"✅ {success_count}개 제품의 is_conversion 플래그 업데이트 완료")
        
        # 6. 결과 요약
        print(f"""
🎉 첫 실제 데이터 동기화 완료!

📊 처리 결과:
- 추출된 제품: {actual_count}개
- 임베딩 생성: {len(processed_products)}개
- Qdrant 삽입: {success_count}개
- PostgreSQL 업데이트: {success_count}개

💰 예상 임베딩 비용: 약 $0.01-0.03
        """)
        
    except Exception as e:
        print(f"❌ 처리 중 오류 발생: {e}")
        raise
    finally:
        await pg_manager.close()
        await qdrant_manager.close()

async def main():
    print("🔧 첫 실제 데이터 동기화 테스트")
    print("=" * 50)
    
    # 테스트용으로 10개부터 시작
    await extract_and_process_products(limit=10)

if __name__ == "__main__":
    asyncio.run(main())