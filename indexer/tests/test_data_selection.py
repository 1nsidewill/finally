"""
첫 실제 데이터 처리를 위한 테스트 스크립트
is_conversion=false이고 status=1인 제품들을 몇십개만 선별
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.postgresql import PostgreSQLManager
from src.config import get_settings

settings = get_settings()

async def main():
    # PostgreSQL 매니저 초기화
    pg_manager = PostgreSQLManager()
    
    try:
        # 조건에 맞는 제품 수 확인
        count_query = """
        SELECT COUNT(*) as total_count
        FROM product 
        WHERE is_conversion = false 
        AND status = 1
        """
        
        count_result = await pg_manager.execute_single(count_query)
        print(f"📊 조건에 맞는 총 제품 수: {count_result['total_count']}개")
        
        # 테스트용으로 30개만 선별 (LIMIT)
        sample_query = """
        SELECT 
            uid as id,
            title as product_name,
            content,
            price,
            status,
            is_conversion,
            created_dt as created_at,
            updated_dt as updated_at
        FROM product 
        WHERE is_conversion = false 
        AND status = 1
        ORDER BY created_dt DESC
        LIMIT 30
        """
        
        sample_products = await pg_manager.execute_query(sample_query)
        print(f"\n🎯 테스트용 선별된 제품: {len(sample_products)}개")
        
        # 샘플 제품 정보 출력
        print("\n📋 샘플 제품 목록:")
        for i, product in enumerate(sample_products[:5], 1):  # 처음 5개만 출력
            print(f"{i}. ID: {product['id']}, 이름: {product['product_name'][:30]}...")
            print(f"   내용: {product['content'][:50] if product['content'] else 'N/A'}...")
            print(f"   가격: {product['price']:,}원" if product['price'] else "   가격: N/A")
            print(f"   변환상태: {product['is_conversion']}, 판매상태: {product['status']}")
            print()
        
        if len(sample_products) > 5:
            print(f"... 외 {len(sample_products) - 5}개 더")
        
        # 테이블 스키마 확인 (브랜드 컬럼이 있는지 확인)
        schema_query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'product'
        ORDER BY ordinal_position
        """
        
        schema_info = await pg_manager.execute_query(schema_query)
        print(f"\n🔍 Product 테이블 스키마:")
        for col in schema_info:
            print(f"   {col['column_name']}: {col['data_type']}")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        await pg_manager.close()

if __name__ == "__main__":
    asyncio.run(main())