"""
Qdrant에 삽입된 데이터 검증 스크립트
벡터 검색 및 데이터 일관성 확인
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.qdrant import QdrantManager
from src.services.embedding_service import EmbeddingService
from src.config import get_settings
from qdrant_client import QdrantClient
import json

settings = get_settings()

async def verify_qdrant_data():
    """Qdrant에 저장된 실제 데이터와 payload 확인"""
    
    # Qdrant 연결
    qdrant_client = QdrantClient(
        host=settings.QDRANT_HOST,
        port=settings.QDRANT_PORT,
        prefer_grpc=settings.QDRANT_PREFER_GRPC
    )
    
    try:
        # Collection 정보 확인
        collection_info = qdrant_client.get_collection(settings.QDRANT_COLLECTION)
        print(f"🎯 Collection: {settings.QDRANT_COLLECTION}")
        print(f"📊 Total points: {collection_info.points_count}")
        
        # 처음 몇 개 points 가져와서 payload 확인
        scroll_result = qdrant_client.scroll(
            collection_name=settings.QDRANT_COLLECTION,
            limit=10,
            with_payload=True,
            with_vectors=False
        )
        
        print(f"\n🔍 첫 10개 points 상세 정보:")
        for i, point in enumerate(scroll_result[0]):
            print(f"\n📌 Point {i+1}:")
            print(f"   • UUID: {point.id}")
            print(f"   • Payload keys: {list(point.payload.keys()) if point.payload else []}")
            
            if point.payload:
                for key, value in point.payload.items():
                    if len(str(value)) > 100:
                        print(f"   • {key}: {str(value)[:100]}...")
                    else:
                        print(f"   • {key}: {value}")
        
        # 전체 points에서 payload의 구조를 분석
        print(f"\n📋 전체 Points Payload 분석:")
        all_keys = set()
        product_ids = []
        uids = []
        
        scroll_result = qdrant_client.scroll(
            collection_name=settings.QDRANT_COLLECTION,
            limit=1000,
            with_payload=True,
            with_vectors=False
        )
        
        points_analyzed = 0
        while scroll_result[0]:
            for point in scroll_result[0]:
                points_analyzed += 1
                if point.payload:
                    all_keys.update(point.payload.keys())
                    # product_id나 pid가 있는지 확인
                    if 'product_id' in point.payload:
                        product_ids.append(point.payload['product_id'])
                    if 'pid' in point.payload:
                        product_ids.append(point.payload['pid'])
                    if 'uid' in point.payload:
                        uids.append(point.payload['uid'])
            
            # 다음 배치
            if scroll_result[1]:
                scroll_result = qdrant_client.scroll(
                    collection_name=settings.QDRANT_COLLECTION,
                    offset=scroll_result[1],
                    limit=1000,
                    with_payload=True,
                    with_vectors=False
                )
            else:
                break
        
        print(f"   • 분석된 points: {points_analyzed}개")
        print(f"   • 모든 payload keys: {sorted(all_keys)}")
        print(f"   • product_id/pid 개수: {len(product_ids)}")
        print(f"   • uid 개수: {len(uids)}")
        
        if product_ids:
            print(f"   • 첫 10개 product_ids: {product_ids[:10]}")
        if uids:
            print(f"   • 첫 10개 uids: {uids[:10]}")
            
    except Exception as e:
        print(f"❌ Qdrant 확인 오류: {e}")

async def main():
    print("🔧 Qdrant 데이터 검증 및 검색 테스트")
    print("=" * 50)
    
    await verify_qdrant_data()

if __name__ == "__main__":
    asyncio.run(main())