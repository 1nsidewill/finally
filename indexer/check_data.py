import asyncio
import asyncpg
from src.config import get_settings

async def check_data():
    config = get_settings()
    conn = await asyncpg.connect(
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        database=config.POSTGRES_DB,
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT
    )
    
    # 전체 상태 확인
    print('=== PostgreSQL 상태 확인 ===')
    
    # 1. 총 상품 수
    total = await conn.fetchval('SELECT COUNT(*) FROM product')
    print(f'총 상품 수: {total:,}개')
    
    # 2. status별 분포
    status_counts = await conn.fetch('SELECT status, COUNT(*) as cnt FROM product GROUP BY status ORDER BY status')
    print('\nstatus별 분포:')
    for row in status_counts:
        print(f'  status {row["status"]}: {row["cnt"]:,}개')
    
    # 3. is_conversion별 분포
    conversion_counts = await conn.fetch('SELECT is_conversion, COUNT(*) as cnt FROM product GROUP BY is_conversion')
    print('\nis_conversion별 분포:')
    for row in conversion_counts:
        print(f'  is_conversion {row["is_conversion"]}: {row["cnt"]:,}개')
    
    # 4. vector_id 상태
    vector_counts = await conn.fetch('''
        SELECT 
            CASE WHEN vector_id IS NULL THEN 'NULL' ELSE 'NOT NULL' END as vector_status,
            COUNT(*) as cnt 
        FROM product 
        GROUP BY CASE WHEN vector_id IS NULL THEN 'NULL' ELSE 'NOT NULL' END
    ''')
    print('\nvector_id 상태:')
    for row in vector_counts:
        print(f'  vector_id {row["vector_status"]}: {row["cnt"]:,}개')
    
    # 5. INSERT 대상 (새 추가 대상)
    insert_count = await conn.fetchval('''
        SELECT COUNT(*) FROM product 
        WHERE status = 1 AND is_conversion = false AND vector_id IS NULL
    ''')
    print(f'\n📝 INSERT 대상: {insert_count:,}개')
    
    # 6. DELETE 대상
    delete_count = await conn.fetchval('''
        SELECT COUNT(*) FROM product 
        WHERE status != 1 AND is_conversion = true AND vector_id IS NOT NULL
    ''')
    print(f'🗑️ DELETE 대상: {delete_count:,}개')
    
    # 7. UPDATE 대상
    update_count = await conn.fetchval('''
        SELECT COUNT(*) FROM product 
        WHERE status = 1 AND is_conversion = false AND vector_id IS NOT NULL
    ''')
    print(f'🔄 UPDATE 대상: {update_count:,}개')
    
    # 8. 처음 100개 INSERT 대상 미리보기
    print('\n=== 처음 5개 INSERT 대상 미리보기 ===')
    samples = await conn.fetch('''
        SELECT provider_uid, pid, title, brand, price, odo, year
        FROM product 
        WHERE status = 1 AND is_conversion = false AND vector_id IS NULL
        ORDER BY created_dt ASC
        LIMIT 5
    ''')
    
    for i, row in enumerate(samples, 1):
        print(f'{i}. [{row["provider_uid"]}:{row["pid"]}] {row["title"][:50] if row["title"] else "제목없음"}...')
        print(f'   브랜드: {row["brand"] or "없음"}, 가격: {row["price"] or "없음"}원, 주행거리: {row["odo"] or "없음"}km, 연식: {row["year"] or "없음"}년')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_data()) 