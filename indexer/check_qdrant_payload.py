import asyncio
from src.database.qdrant import QdrantManager

async def check_qdrant_payload():
    qdrant_manager = QdrantManager()
    
    print('=== Qdrant Payload 확인 ===')
    
    # 컬렉션 정보 확인
    collection_info = await qdrant_manager.get_collection_info()
    print(f'컬렉션: {qdrant_manager.collection_name}')
    print(f'벡터 수: {collection_info["points_count"]}')
    
    if collection_info["points_count"] > 0:
        # 몇 개 포인트의 payload 확인
        print('\n=== 최근 추가된 포인트들의 payload 확인 ===')
        
        # 스크롤로 포인트들 가져오기
        client = await qdrant_manager.get_async_client()
        scroll_result = await client.scroll(
            collection_name=qdrant_manager.collection_name,
            limit=3,
            with_payload=True,
            with_vectors=False
        )
        
        for i, point in enumerate(scroll_result[0]):
            print(f'\n--- Point {i+1} ---')
            print(f'ID: {point.id}')
            print(f'Provider UID: {point.payload.get("provider_uid")}')
            print(f'PID: {point.payload.get("pid")}')
            print(f'Title: {point.payload.get("title", "")[:50]}...')
            print(f'Image URLs: {point.payload.get("image_url", [])}')
            print(f'Image URL 개수: {len(point.payload.get("image_url", []))}')
    else:
        print('❌ 컬렉션에 포인트가 없습니다.')

if __name__ == "__main__":
    asyncio.run(check_qdrant_payload()) 