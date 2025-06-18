#!/usr/bin/env python3
"""
데이터 초기화 스크립트
1. Qdrant 컬렉션 완전 삭제 후 재생성
2. PostgreSQL is_conversion 필드 전체 false로 초기화  
"""

import asyncio
import asyncpg
import logging
from src.config import get_settings
from src.database.qdrant import QdrantManager
from src.database.postgresql import PostgreSQLManager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def clear_qdrant_collection():
    """Qdrant 컬렉션 완전 삭제 후 재생성"""
    try:
        logger.info("🗑️  Qdrant 컬렉션 초기화 시작...")
        
        qdrant_manager = QdrantManager()
        client = await qdrant_manager.get_async_client()
        
        # 기존 컬렉션 삭제
        try:
            await client.delete_collection(qdrant_manager.collection_name)
            logger.info(f"✅ 기존 컬렉션 삭제 완료: {qdrant_manager.collection_name}")
        except Exception as e:
            logger.warning(f"컬렉션 삭제 중 오류 (이미 없을 수 있음): {e}")
        
        # 새 컬렉션 생성 (최적화 설정 포함)
        await qdrant_manager.create_collection_if_not_exists()
        logger.info(f"✅ 새 컬렉션 생성 완료: {qdrant_manager.collection_name}")
        
        # 컬렉션 정보 확인
        collection_info = await client.get_collection(qdrant_manager.collection_name)
        logger.info(f"📊 컬렉션 상태: {collection_info.status}")
        logger.info(f"📊 벡터 개수: {collection_info.points_count}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Qdrant 컬렉션 초기화 실패: {e}")
        return False

async def reset_postgresql_flags():
    """PostgreSQL is_conversion 필드 전체 false로 초기화"""
    try:
        logger.info("🗑️  PostgreSQL is_conversion 플래그 초기화 시작...")
        
        settings = get_settings()
        postgresql_manager = PostgreSQLManager()
        
        async with postgresql_manager.get_connection() as conn:
            # 현재 상태 확인
            count_query = """
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE is_conversion = true) as converted,
                    COUNT(*) FILTER (WHERE is_conversion = false) as not_converted
                FROM product 
                WHERE status = 1
            """
            current_stats = await conn.fetchrow(count_query)
            
            logger.info(f"📊 현재 상태:")
            logger.info(f"   - 전체 제품: {current_stats['total']:,}개")
            logger.info(f"   - 변환 완료: {current_stats['converted']:,}개")
            logger.info(f"   - 변환 대기: {current_stats['not_converted']:,}개")
            
            # 모든 is_conversion을 false로 설정
            update_query = """
                UPDATE product 
                SET is_conversion = false
                WHERE status = 1 AND is_conversion = true
            """
            
            result = await conn.execute(update_query)
            updated_count = int(result.split()[-1])  # "UPDATE 1234" -> 1234
            
            logger.info(f"✅ {updated_count:,}개 제품의 is_conversion 플래그 초기화 완료")
            
            # 최종 상태 확인
            final_stats = await conn.fetchrow(count_query)
            logger.info(f"📊 최종 상태:")
            logger.info(f"   - 전체 제품: {final_stats['total']:,}개")
            logger.info(f"   - 변환 완료: {final_stats['converted']:,}개")
            logger.info(f"   - 변환 대기: {final_stats['not_converted']:,}개")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ PostgreSQL 플래그 초기화 실패: {e}")
        return False

async def main():
    """메인 초기화 함수"""
    try:
        logger.info("🚀 데이터 초기화 시작...")
        
        # 1. Qdrant 컬렉션 초기화
        qdrant_success = await clear_qdrant_collection()
        if not qdrant_success:
            logger.error("❌ Qdrant 초기화 실패로 중단")
            return
        
        # 2. PostgreSQL 플래그 초기화  
        postgres_success = await reset_postgresql_flags()
        if not postgres_success:
            logger.error("❌ PostgreSQL 초기화 실패로 중단")
            return
        
        logger.info("🎉 모든 데이터 초기화 완료!")
        logger.info("💫 이제 bulk_sync_with_checkpoints.py를 실행하여 동기화를 시작할 수 있습니다.")
        
    except Exception as e:
        logger.error(f"❌ 초기화 중 예상치 못한 오류: {e}")

if __name__ == "__main__":
    asyncio.run(main())