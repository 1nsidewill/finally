import asyncio
import asyncpg
from src.config import get_settings

async def reset_postgres_conversion_state():
    config = get_settings()
    conn = await asyncpg.connect(
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        database=config.POSTGRES_DB,
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT
    )
    
    print('=== PostgreSQL 변환 상태 리셋 ===')
    
    # 변환 상태 리셋 전 현재 상태 확인
    print('\n리셋 전 상태:')
    converted_count = await conn.fetchval('SELECT COUNT(*) FROM product WHERE is_conversion = true')
    vector_count = await conn.fetchval('SELECT COUNT(*) FROM product WHERE vector_id IS NOT NULL')
    print(f'  - is_conversion=true: {converted_count:,}개')
    print(f'  - vector_id NOT NULL: {vector_count:,}개')
    
    # 변환 상태 리셋 실행
    print('\n🔄 변환 상태 리셋 중...')
    
    # is_conversion을 false로, vector_id를 null로 리셋
    result = await conn.execute('''
        UPDATE product 
        SET is_conversion = false, vector_id = null, updated_dt = NOW()
        WHERE is_conversion = true OR vector_id IS NOT NULL
    ''')
    
    updated_count = int(result.split()[-1])  # "UPDATE 100" -> 100
    print(f'✅ 업데이트 완료: {updated_count:,}개 레코드 리셋됨')
    
    # 리셋 후 상태 확인
    print('\n리셋 후 상태:')
    converted_count = await conn.fetchval('SELECT COUNT(*) FROM product WHERE is_conversion = true')
    vector_count = await conn.fetchval('SELECT COUNT(*) FROM product WHERE vector_id IS NOT NULL')
    insert_count = await conn.fetchval('''
        SELECT COUNT(*) FROM product 
        WHERE status = 1 AND is_conversion = false AND vector_id IS NULL
    ''')
    
    print(f'  - is_conversion=true: {converted_count:,}개')
    print(f'  - vector_id NOT NULL: {vector_count:,}개')
    print(f'  - INSERT 대상: {insert_count:,}개')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(reset_postgres_conversion_state()) 