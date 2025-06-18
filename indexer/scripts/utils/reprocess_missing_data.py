#!/usr/bin/env python3

import asyncio
import os
from dotenv import load_dotenv
from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager
from src.config import get_settings

async def reprocess_missing_data():
    """누락된 데이터를 재처리하는 함수"""
    
    # 환경변수 로드
    load_dotenv('.env.dev')
    config = get_settings()
    
    print("🔄 누락된 데이터 재처리 시작...")
    
    # 데이터베이스 연결
    pg_manager = PostgreSQLManager()
    qdrant_manager = QdrantManager()
    
    try:
        # Qdrant collection 생성 확인
        await qdrant_manager.create_collection_if_not_exists()
        
        # PostgreSQL에서 변환 완료된 UID 가져오기
        pg_uids = await get_conversion_true_uids(pg_manager)
        print(f"📊 PostgreSQL conversion_true UIDs: {len(pg_uids)}개")
        
        # Qdrant에서 현재 저장된 UID 가져오기  
        try:
            qdrant_uids = await get_qdrant_uids(qdrant_manager)
            print(f"🎯 Qdrant UIDs: {len(qdrant_uids)}개")
        except Exception as e:
            print(f"⚠️ Qdrant에서 데이터 조회 실패: {e}")
            print("🆕 빈 collection으로 간주하고 전체 재처리 진행...")
            qdrant_uids = []
        
        # 누락된 UID 찾기
        missing_uids = set(pg_uids) - set(qdrant_uids)
        print(f"❌ 누락된 UIDs: {len(missing_uids)}개")
        print(f"❌ 누락된 UID 목록: {sorted(list(missing_uids))}")
        
        if not missing_uids:
            print("✅ 누락된 데이터가 없습니다!")
            return
            
        # 누락된 데이터 재처리
        success_count = 0
        failed_count = 0
        
        for uid in sorted(missing_uids):
            try:
                print(f"🔄 UID {uid} 재처리 중...")
                
                # PostgreSQL에서 해당 제품 데이터 가져오기
                product_data = await get_product_by_uid(pg_manager, uid)
                if not product_data:
                    print(f"⚠️  UID {uid}: PostgreSQL에서 데이터를 찾을 수 없음")
                    failed_count += 1
                    continue
                
                # 임베딩 생성
                content = f"{product_data.get('title', '')} {product_data.get('brand', '')} {product_data.get('content', '')}"
                embedding = await qdrant_manager.generate_embedding(content)
                
                if not embedding:
                    print(f"⚠️  UID {uid}: 임베딩 생성 실패")
                    failed_count += 1
                    continue
                
                # Qdrant에 저장
                await store_to_qdrant(qdrant_manager, product_data, embedding)
                print(f"✅ UID {uid}: 재처리 완료")
                success_count += 1
                
            except Exception as e:
                print(f"❌ UID {uid}: 재처리 실패 - {str(e)}")
                failed_count += 1
                
        print(f"\n🎯 재처리 완료:")
        print(f"   ✅ 성공: {success_count}개")
        print(f"   ❌ 실패: {failed_count}개")
        
    except Exception as e:
        print(f"❌ 재처리 중 오류: {str(e)}")
    finally:
        await pg_manager.close()
        await qdrant_manager.close()

async def get_conversion_true_uids(pg_manager):
    """PostgreSQL에서 변환 완료된 UID 목록 가져오기"""
    query = "SELECT uid FROM product WHERE is_conversion = true"
    rows = await pg_manager.execute_query(query)
    return [row['uid'] for row in rows]

async def get_qdrant_uids(qdrant_manager):
    """Qdrant에서 저장된 UID 목록 가져오기"""
    collection_name = "bike"
    
    # 모든 포인트의 payload에서 uid 추출
    uids = []
    offset = None
    batch_size = 100
    
    while True:
        client = await qdrant_manager.get_async_client()
        points, next_offset = await client.scroll(
            collection_name=collection_name,
            limit=batch_size,
            offset=offset,
            with_payload=True,
            with_vectors=False
        )
        
        if not points:
            break
            
        for point in points:
            if 'uid' in point.payload:
                uids.append(point.payload['uid'])
        
        offset = next_offset
        if offset is None:
            break
    
    return uids

async def get_product_by_uid(pg_manager, uid):
    """UID로 제품 데이터 가져오기"""
    query = """
    SELECT uid, pid, title, brand, content, price, status
    FROM product 
    WHERE uid = $1 AND is_conversion = true
    """
    rows = await pg_manager.execute_query(query, uid)
    return rows[0] if rows else None

async def store_to_qdrant(qdrant_manager, product_data, embedding):
    """Qdrant에 데이터 저장"""
    from src.database.qdrant import ensure_valid_uuid
    from qdrant_client.http.models import PointStruct
    
    collection_name = "bike"
    point_id = ensure_valid_uuid(str(product_data['pid']))
    
    # price를 문자열로 변환하여 JSON 호환성 확보
    price_value = str(product_data.get('price', '')) if product_data.get('price') is not None else ''
    
    payload = {
        'uid': product_data['uid'],
        'title': product_data.get('title', ''),
        'brand': product_data.get('brand', ''),
        'content': product_data.get('content', ''),
        'price': price_value,
        'status': product_data.get('status', '')
    }
    
    point = PointStruct(
        id=point_id,
        vector=embedding,
        payload=payload
    )
    
    await qdrant_manager.upsert_points([point])

if __name__ == "__main__":
    asyncio.run(reprocess_missing_data()) 