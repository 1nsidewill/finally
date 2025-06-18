import asyncio
import asyncpg
from qdrant_client import QdrantClient
from src.config import get_settings
import json

async def check_data_inconsistency_fixed():
    """PostgreSQL의 uid와 Qdrant payload의 uid를 올바르게 비교"""
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
        # PostgreSQL에서 conversion_true인 uid들 가져오기
        pg_query = "SELECT COUNT(*) FROM product WHERE is_conversion = true"
        pg_count = await pg_conn.fetchval(pg_query)
        print(f"📊 PostgreSQL conversion_true 개수: {pg_count}")
        
        # conversion_true인 uid들 가져오기
        pg_uids_query = "SELECT uid FROM product WHERE is_conversion = true ORDER BY uid"
        pg_uids = await pg_conn.fetch(pg_uids_query)
        pg_uids_list = [row['uid'] for row in pg_uids]
        print(f"📋 PostgreSQL conversion_true UIDs: {len(pg_uids_list)}개")
        print(f"📋 첫 10개 PostgreSQL UIDs: {pg_uids_list[:10]}")
        
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
        
        # Qdrant에서 payload의 uid들 수집하기
        qdrant_uids = []
        scroll_result = qdrant_client.scroll(
            collection_name=settings.QDRANT_COLLECTION,
            limit=1000,
            with_payload=True,
            with_vectors=False
        )
        
        batch_count = 0
        while scroll_result[0]:  # points가 있는 동안
            batch_count += 1
            batch_uids = []
            for point in scroll_result[0]:
                if point.payload and 'uid' in point.payload:
                    batch_uids.append(point.payload['uid'])
            
            qdrant_uids.extend(batch_uids)
            print(f"배치 {batch_count}: {len(batch_uids)}개 UIDs 수집, 총 {len(qdrant_uids)}개")
            
            # 다음 배치 가져오기
            if scroll_result[1]:  # next_page_offset이 있으면
                scroll_result = qdrant_client.scroll(
                    collection_name=settings.QDRANT_COLLECTION,
                    offset=scroll_result[1],
                    limit=1000,
                    with_payload=True,
                    with_vectors=False
                )
            else:
                break
        
        print(f"🎯 Qdrant에서 추출한 UID 개수: {len(qdrant_uids)}")
        print(f"🎯 첫 10개 Qdrant UIDs: {qdrant_uids[:10]}")
        
        # PostgreSQL UIDs를 set으로 변환
        pg_uids_set = set(pg_uids_list)
        qdrant_uids_set = set(qdrant_uids)
        
        # 차이점 분석
        missing_in_qdrant = pg_uids_set - qdrant_uids_set
        extra_in_qdrant = qdrant_uids_set - pg_uids_set
        matching_uids = pg_uids_set & qdrant_uids_set
        
        print(f"\n🔍 정확한 분석 결과:")
        print(f"   PostgreSQL conversion_true UIDs: {len(pg_uids_set)}개")
        print(f"   Qdrant payload UIDs: {len(qdrant_uids_set)}개")
        print(f"   🎯 일치하는 UIDs: {len(matching_uids)}개")
        print(f"   ❌ PostgreSQL에만 있는 UIDs: {len(missing_in_qdrant)}개")
        print(f"   ➕ Qdrant에만 있는 UIDs: {len(extra_in_qdrant)}개")
        
        if missing_in_qdrant:
            print(f"\n❌ PostgreSQL에만 있는 UIDs (처음 10개):")
            for uid in list(missing_in_qdrant)[:10]:
                print(f"   - {uid}")
                
        if extra_in_qdrant:
            print(f"\n➕ Qdrant에만 있는 UIDs (처음 10개):")
            for uid in list(extra_in_qdrant)[:10]:
                print(f"   - {uid}")
        
        # 일치율 계산
        if len(pg_uids_set) > 0:
            match_rate = len(matching_uids) / len(pg_uids_set) * 100
            print(f"\n📈 데이터 일치율: {match_rate:.1f}%")
        
    except Exception as e:
        print(f"❌ Qdrant 오류: {e}")

if __name__ == "__main__":
    asyncio.run(check_data_inconsistency_fixed()) 