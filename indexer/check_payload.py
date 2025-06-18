#!/usr/bin/env python3

import asyncio
from src.database.qdrant import QdrantManager

async def check_current_payload():
    """현재 Qdrant payload 상태 확인"""
    
    print("🔍 === Qdrant Collection Payload 상태 확인 ===")
    
    qdrant = QdrantManager()
    await qdrant.initialize()
    
    try:
        # Collection 정보 확인
        collection_info = await qdrant.get_collection_info()
        print(f"✅ Collection: {collection_info.config.params.vectors.size}차원")
        print(f"📊 총 벡터 수: {collection_info.points_count}")
        
        # 샘플 포인트들 확인 (payload 구조 파악)
        print(f"\n📋 샘플 포인트들의 payload 구조:")
        
        points = await qdrant.client.scroll(
            collection_name=qdrant.collection_name,
            limit=5,
            with_payload=True,
            with_vectors=False
        )
        
        for i, point in enumerate(points[0], 1):
            print(f"\n{i}. Point ID: {point.id}")
            print(f"   Payload keys: {list(point.payload.keys())}")
            
            # 각 필드 값 확인
            for key, value in point.payload.items():
                if isinstance(value, str) and len(value) > 50:
                    print(f"   - {key}: '{value[:50]}...' (길이: {len(value)})")
                else:
                    print(f"   - {key}: {value}")
        
        # 특정 필드들의 통계
        print(f"\n📊 Payload 필드 통계:")
        
        # brand 필드 존재 여부 확인
        brand_filter = {
            "must": [
                {"has_id": list(range(1, 100))}  # 처음 100개 확인
            ]
        }
        
        sample_points = await qdrant.client.scroll(
            collection_name=qdrant.collection_name,
            scroll_filter=brand_filter,
            limit=20,
            with_payload=True,
            with_vectors=False
        )
        
        brand_count = 0
        url_count = 0
        img_url_count = 0
        odo_count = 0
        
        for point in sample_points[0]:
            if 'brand' in point.payload and point.payload['brand']:
                brand_count += 1
            if 'page_url' in point.payload and point.payload['page_url']:
                url_count += 1
            if 'images' in point.payload and point.payload['images']:
                img_url_count += 1
            if 'odo' in point.payload and point.payload['odo']:
                odo_count += 1
        
        total_checked = len(sample_points[0])
        print(f"   샘플 {total_checked}개 중:")
        print(f"   - brand 있음: {brand_count}개 ({brand_count/total_checked*100:.1f}%)")
        print(f"   - page_url 있음: {url_count}개 ({url_count/total_checked*100:.1f}%)")
        print(f"   - images 있음: {img_url_count}개 ({img_url_count/total_checked*100:.1f}%)")
        print(f"   - odo 있음: {odo_count}개 ({odo_count/total_checked*100:.1f}%)")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        
    finally:
        await qdrant.close()

if __name__ == "__main__":
    asyncio.run(check_current_payload()) 