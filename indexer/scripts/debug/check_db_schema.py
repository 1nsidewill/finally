import asyncio
import asyncpg
from src.config import get_settings

async def check_db_schema():
    """PostgreSQL 데이터베이스의 테이블과 컬럼을 확인"""
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
        # 모든 테이블 목록 조회
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name
        """
        tables = await pg_conn.fetch(tables_query)
        print("📋 데이터베이스 테이블들:")
        for table in tables:
            print(f"   - {table['table_name']}")
        
        # 각 테이블의 컬럼 정보 확인
        for table in tables:
            table_name = table['table_name']
            columns_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
            """
            columns = await pg_conn.fetch(columns_query, table_name)
            print(f"\n🏗️ {table_name} 테이블 구조:")
            for col in columns:
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                print(f"   - {col['column_name']}: {col['data_type']} ({nullable})")
            
            # 각 테이블의 행 개수 확인
            try:
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                count = await pg_conn.fetchval(count_query)
                print(f"   📊 행 개수: {count}")
            except Exception as e:
                print(f"   ❌ 행 개수 조회 실패: {e}")
                
    except Exception as e:
        print(f"❌ 데이터베이스 오류: {e}")
    finally:
        await pg_conn.close()

if __name__ == "__main__":
    asyncio.run(check_db_schema()) 