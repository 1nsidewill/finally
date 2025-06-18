import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.database.postgresql import PostgreSQLManager

async def check_tables():
    pg = PostgreSQLManager()
    async with pg.get_connection() as conn:
        # Get all tables
        tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
        print('📋 사용 가능한 테이블들:')
        for table in tables:
            print(f'  • {table["tablename"]}')
        
        print()
        
        # Check for product-related tables
        product_tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '%product%';")
        if product_tables:
            print('🏍️ 제품 관련 테이블들:')
            for table in product_tables:
                print(f'  • {table["tablename"]}')
                
                # Get column info for product tables
                columns = await conn.fetch(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{table["tablename"]}'
                    ORDER BY ordinal_position;
                """)
                print(f'    컬럼들:')
                for col in columns:
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    print(f'      - {col["column_name"]}: {col["data_type"]} ({nullable})')
                print()

if __name__ == "__main__":
    asyncio.run(check_tables()) 