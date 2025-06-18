import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.config import get_settings
from src.database.qdrant import QdrantManager
from qdrant_client.http.models import Distance, VectorParams


async def clear_qdrant_collection():
    """Clear all data from Qdrant collection and recreate it"""
    config = get_settings()
    qdrant = QdrantManager()
    
    try:
        # Get async client
        client = await qdrant.get_async_client()
        
        # Collection name from config
        collection_name = config.QDRANT_COLLECTION
        
        print(f"🔍 기존 컬렉션 상태 확인: {collection_name}")
        
        # Check current collection info
        try:
            collection_info = await client.get_collection(collection_name)
            points_count = collection_info.points_count
            print(f"📊 현재 포인트 수: {points_count}")
        except Exception as e:
            print(f"⚠️ 컬렉션 정보 조회 실패: {e}")
            points_count = 0
        
        if points_count > 0:
            print(f"🗑️ {points_count}개 포인트 삭제 중...")
            
            # Delete the entire collection
            try:
                await client.delete_collection(collection_name)
                print(f"✅ 컬렉션 '{collection_name}' 삭제 완료")
            except Exception as e:
                print(f"❌ 컬렉션 삭제 실패: {e}")
                return False
        
        # Recreate the collection with same settings
        print(f"🔄 컬렉션 '{collection_name}' 재생성 중...")
        
        try:
            await client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=config.VECTOR_SIZE,  # 1536 for text-embedding-ada-002
                    distance=Distance.COSINE
                )
            )
            print(f"✅ 컬렉션 '{collection_name}' 재생성 완료")
        except Exception as e:
            print(f"❌ 컬렉션 재생성 실패: {e}")
            return False
        
        # Verify empty collection
        try:
            collection_info = await client.get_collection(collection_name)
            final_count = collection_info.points_count
            print(f"🎯 최종 포인트 수: {final_count}")
            
            if final_count == 0:
                print("✅ Qdrant 데이터 초기화 성공!")
                return True
            else:
                print(f"❌ 초기화 실패: {final_count}개 포인트가 남아있음")
                return False
                
        except Exception as e:
            print(f"❌ 검증 실패: {e}")
            return False
            
    except Exception as e:
        print(f"❌ 전체 프로세스 실패: {e}")
        return False
    
    finally:
        # Close connection
        await qdrant.close()


async def main():
    """Main execution function"""
    print("🚀 Qdrant 데이터 초기화 시작...")
    print("=" * 50)
    
    success = await clear_qdrant_collection()
    
    print("=" * 50)
    if success:
        print("🎉 Qdrant 데이터 초기화 완료!")
        print("💡 이제 대량 데이터 처리를 시작할 수 있습니다.")
    else:
        print("💥 Qdrant 데이터 초기화 실패!")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 