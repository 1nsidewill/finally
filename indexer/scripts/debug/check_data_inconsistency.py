import asyncio
import asyncpg
from qdrant_client import QdrantClient
from src.config import get_settings
import json

async def check_data_inconsistency():
    """PostgreSQL의 conversion_true 개수와 Qdrant collection point 개수를 비교"""
    settings = get_settings()
    
    # PostgreSQL 연결
    pg_conn = await asyncpg.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        database=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD
    )
    
    try:
        # PostgreSQL에서 conversion_true 개수 확인
        pg_query = "SELECT COUNT(*) FROM product WHERE is_conversion = true"
        pg_count = await pg_conn.fetchval(pg_query)
        print(f"📊 PostgreSQL conversion_true 개수: {pg_count}")
        
        # conversion_true인 product_id들 가져오기 (여기서 pid를 사용)
        pg_ids_query = "SELECT pid FROM product WHERE is_conversion = true ORDER BY pid"
        pg_ids = await pg_conn.fetch(pg_ids_query)
        pg_product_ids = [row['pid'] for row in pg_ids]
        print(f"📋 PostgreSQL product_ids: {len(pg_product_ids)}개")
        
        # 샘플 10개 출력
        print(f"📋 첫 10개 PostgreSQL product_ids: {pg_product_ids[:10]}")
        
        # product 테이블 전체 개수도 확인
        total_query = "SELECT COUNT(*) FROM product"
        total_count = await pg_conn.fetchval(total_query)
        print(f"📊 PostgreSQL 전체 product 개수: {total_count}")
        
    finally:
        await pg_conn.close()
    
    # Qdrant 연결
    qdrant_client = QdrantClient(
        host=settings.QDRANT_HOST,
        port=settings.QDRANT_PORT,
        prefer_grpc=settings.QDRANT_PREFER_GRPC
    )
    
    try:
        # Qdrant collection 정보 확인
        collection_info = qdrant_client.get_collection(settings.QDRANT_COLLECTION)
        qdrant_count = collection_info.points_count
        print(f"🎯 Qdrant collection points 개수: {qdrant_count}")
        
        # Qdrant에서 실제 points 스크롤로 가져오기 (점진적으로)
        qdrant_ids = set()
        scroll_result = qdrant_client.scroll(
            collection_name=settings.QDRANT_COLLECTION,
            limit=1000,
            with_payload=False,
            with_vectors=False
        )
        
        batch_count = 0
        while scroll_result[0]:  # points가 있는 동안
            batch_count += 1
            batch_ids = [point.id for point in scroll_result[0]]
            qdrant_ids.update(batch_ids)
            print(f"배치 {batch_count}: {len(batch_ids)}개 points 수집, 총 {len(qdrant_ids)}개")
            
            # 다음 배치 가져오기
            if scroll_result[1]:  # next_page_offset이 있으면
                scroll_result = qdrant_client.scroll(
                    collection_name=settings.QDRANT_COLLECTION,
                    offset=scroll_result[1],
                    limit=1000,
                    with_payload=False,
                    with_vectors=False
                )
            else:
                break
        
        print(f"🎯 Qdrant 실제 points 개수: {len(qdrant_ids)}")
        
        # PostgreSQL product_ids를 set으로 변환
        pg_product_ids_set = set(pg_product_ids)
        
        # 차이점 분석
        missing_in_qdrant = pg_product_ids_set - qdrant_ids
        extra_in_qdrant = qdrant_ids - pg_product_ids_set
        
        print(f"\n🔍 분석 결과:")
        print(f"   PostgreSQL conversion_true: {len(pg_product_ids_set)}개")
        print(f"   Qdrant collection points: {len(qdrant_ids)}개")
        print(f"   PostgreSQL에는 있지만 Qdrant에는 없는 것: {len(missing_in_qdrant)}개")
        print(f"   Qdrant에는 있지만 PostgreSQL에는 없는 것: {len(extra_in_qdrant)}개")
        
        if missing_in_qdrant:
            print(f"\n❌ PostgreSQL에만 있는 product_ids (처음 10개):")
            for pid in list(missing_in_qdrant)[:10]:
                print(f"   - {pid}")
                
        if extra_in_qdrant:
            print(f"\n➕ Qdrant에만 있는 product_ids (처음 10개):")
            for pid in list(extra_in_qdrant)[:10]:
                print(f"   - {pid}")
        
    except Exception as e:
        print(f"❌ Qdrant 오류: {e}")

if __name__ == "__main__":
    asyncio.run(check_data_inconsistency()) 