import asyncio
import asyncpg
from src.config import get_settings

async def check_file_table():
    config = get_settings()
    conn = await asyncpg.connect(
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        database=config.POSTGRES_DB,
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT
    )
    
    print('=== file 테이블 구조 및 데이터 확인 ===')
    
    # 1. file 테이블 존재 여부 확인
    table_exists = await conn.fetchval('''
        SELECT EXISTS (
           SELECT FROM information_schema.tables 
           WHERE table_name = 'file'
        )
    ''')
    
    if not table_exists:
        print('❌ file 테이블이 존재하지 않습니다.')
        await conn.close()
        return
    
    # 2. file 테이블 스키마 확인
    print('\n=== file 테이블 컬럼 정보 ===')
    columns = await conn.fetch('''
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = 'file'
        ORDER BY ordinal_position
    ''')
    
    for col in columns:
        print(f"- {col['column_name']}: {col['data_type']} (null: {col['is_nullable']}) {col['column_default'] or ''}")
    
    # 3. file 테이블 전체 레코드 수
    total_files = await conn.fetchval('SELECT COUNT(*) FROM file')
    print(f'\n📁 총 파일 레코드 수: {total_files:,}개')
    
    # 4. product_uid가 있는 파일 수
    if 'product_uid' in [col['column_name'] for col in columns]:
        product_files = await conn.fetchval('SELECT COUNT(*) FROM file WHERE product_uid IS NOT NULL')
        print(f'📎 product_uid가 있는 파일: {product_files:,}개')
        
        # 5. count 필드 분포 확인  
        if 'count' in [col['column_name'] for col in columns]:
            count_stats = await conn.fetch('''
                SELECT count, COUNT(*) as cnt 
                FROM file 
                WHERE product_uid IS NOT NULL 
                GROUP BY count 
                ORDER BY count
            ''')
            print('\ncount 필드 분포:')
            for row in count_stats:
                print(f'  count {row["count"]}: {row["cnt"]:,}개')
        
        # 6. 샘플 데이터 확인
        print('\n=== 첫 5개 파일 샘플 ===')
        sample_files = await conn.fetch('''
            SELECT product_uid, url, count
            FROM file 
            WHERE product_uid IS NOT NULL
            ORDER BY product_uid ASC
            LIMIT 5
        ''')
        
        for i, row in enumerate(sample_files, 1):
            print(f'{i}. product_uid: {row["product_uid"]}')
            print(f'   url: {row["url"][:100] if row["url"] else "없음"}...')
            print(f'   count: {row["count"]}')
            print()
            
        # 7. 특정 product_uid의 파일들 (이미지 URL 생성 예시)
        if sample_files:
            test_product_uid = sample_files[0]['product_uid']
            print(f'=== product_uid {test_product_uid}의 이미지 URL 생성 예시 ===')
            
            product_files = await conn.fetch('''
                SELECT url, count
                FROM file
                WHERE product_uid = $1
            ''', test_product_uid)
            
            for file_record in product_files:
                url_template = file_record['url']
                count = file_record['count'] or 0
                
                print(f'URL 템플릿: {url_template}')
                print(f'count: {count}')
                
                if '{cnt}' in url_template and count > 0:
                    print('생성될 이미지 URLs:')
                    for i in range(1, count + 1):
                        image_url = url_template.replace('{cnt}', str(i))
                        print(f'  {i}: {image_url}')
                else:
                    print('이미지 URLs: [정적 URL 또는 count가 0]')
                print()
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_file_table()) 