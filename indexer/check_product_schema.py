import asyncio
import asyncpg
from src.config import get_settings

async def check_product_schema():
    config = get_settings()
    conn = await asyncpg.connect(
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        database=config.POSTGRES_DB,
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT
    )
    
    print('=== product 테이블 스키마 확인 ===')
    
    # 테이블 컬럼 정보
    columns = await conn.fetch("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = 'product'
        ORDER BY ordinal_position
    """)
    
    print('\n📋 컬럼 정보:')
    for col in columns:
        print(f"  - {col['column_name']}: {col['data_type']} ({'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'})")
    
    # 제약조건 확인
    constraints = await conn.fetch("""
        SELECT tc.constraint_name, tc.constraint_type, ccu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu 
            ON tc.constraint_name = ccu.constraint_name
        WHERE tc.table_name = 'product'
        ORDER BY tc.constraint_type, tc.constraint_name
    """)
    
    print('\n🔒 제약조건:')
    for const in constraints:
        print(f"  - {const['constraint_name']}: {const['constraint_type']} on {const['column_name']}")
    
    # 인덱스 확인
    indexes = await conn.fetch("""
        SELECT indexname, indexdef
        FROM pg_indexes 
        WHERE tablename = 'product'
        ORDER BY indexname
    """)
    
    print('\n📇 인덱스:')
    for idx in indexes:
        print(f"  - {idx['indexname']}: {idx['indexdef']}")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_product_schema()) 