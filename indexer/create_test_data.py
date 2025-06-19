import asyncio
import asyncpg
from src.config import get_settings
import uuid
import hashlib

async def create_test_data():
    config = get_settings()
    conn = await asyncpg.connect(
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        database=config.POSTGRES_DB,
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT
    )
    
    print('=== UPDATE/DELETE 테스트용 더미 데이터 생성 ===')
    
    try:
        # 1. UPDATE 테스트용 데이터 생성 (기존 벡터가 있는 상품의 내용을 수정)
        print('\n1️⃣ UPDATE 테스트용 데이터 준비 중...')
        
        # 이미 벡터가 있는 상품 중 일부를 가져와서 내용 수정
        existing_products = await conn.fetch("""
            SELECT provider_uid, pid, vector_id, title, content 
            FROM product 
            WHERE is_conversion = true AND vector_id IS NOT NULL 
            LIMIT 3
        """)
        
        if existing_products:
            print(f'  - 기존 상품 {len(existing_products)}개를 UPDATE 대상으로 설정')
            for i, product in enumerate(existing_products):
                # 제목과 내용을 수정하고 is_conversion을 false로 설정 (UPDATE 트리거)
                new_title = f"[수정됨] {product['title']}"
                new_content = f"[업데이트된 내용] {product['content'] or ''} - 테스트용 수정 데이터"
                
                await conn.execute("""
                    UPDATE product 
                    SET title = $1, content = $2, is_conversion = false, updated_dt = NOW()
                    WHERE provider_uid = $3 AND pid = $4
                """, new_title, new_content, product['provider_uid'], product['pid'])
                
                print(f'    ✅ [{product["provider_uid"]}:{product["pid"]}] 수정됨')
        
        # 2. DELETE 테스트용 데이터 생성 (기존 활성 상품을 비활성화)
        print('\n2️⃣ DELETE 테스트용 데이터 준비 중...')
        
        # 이미 벡터가 있는 상품 중 일부를 비활성화
        delete_candidates = await conn.fetch("""
            SELECT provider_uid, pid, vector_id, title 
            FROM product 
            WHERE is_conversion = true AND vector_id IS NOT NULL AND status = 1
            LIMIT 2
        """)
        
        if delete_candidates:
            print(f'  - 기존 상품 {len(delete_candidates)}개를 DELETE 대상으로 설정')
            for product in delete_candidates:
                # status를 2로 변경 (비활성화 - DELETE 트리거)
                await conn.execute("""
                    UPDATE product 
                    SET status = 2, updated_dt = NOW()
                    WHERE provider_uid = $1 AND pid = $2
                """, product['provider_uid'], product['pid'])
                
                print(f'    ✅ [{product["provider_uid"]}:{product["pid"]}] 비활성화됨')
        
        # 3. 새로운 INSERT 테스트용 더미 데이터 생성
        print('\n3️⃣ INSERT 테스트용 더미 데이터 생성 중...')
        
        # 새로운 더미 상품 5개 생성
        for i in range(5):
            # 고유한 provider_uid와 pid 생성
            provider_uid = 1  # 기존 provider 사용
            pid = 900000000 + i  # 테스트용 PID 범위
            
            # 더미 데이터
            title = f"테스트 상품 {i+1} - 벡터 검색용 더미 데이터"
            brand = "테스트브랜드" if i % 2 == 0 else None
            content = f"이것은 테스트용 상품 {i+1}번입니다. 임베딩과 벡터 검색 기능을 테스트하기 위한 더미 데이터입니다."
            price = 1000000 + (i * 100000)  # 100만원부터 50만원씩 증가
            location = f"테스트시 테스트구 {i+1}"
            odo = 10000 + (i * 5000)  # 1만km부터 5천km씩 증가
            year = 2020 + i
            
            # 고유한 uid 생성
            uid_str = f"{provider_uid}:{pid}"
            uid_hash = hashlib.md5(uid_str.encode()).hexdigest()
            uid = int(uid_hash[:8], 16)  # 8자리 hex를 int로 변환
            
            await conn.execute("""
                INSERT INTO product (
                    uid, provider_uid, pid, title, brand, content, price, location, odo, year,
                    status, is_conversion, vector_id, created_dt, updated_dt
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    1, false, NULL, NOW(), NOW()
                )
                ON CONFLICT (uid) DO UPDATE SET
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    status = 1,
                    is_conversion = false,
                    vector_id = NULL,
                    updated_dt = NOW()
            """, uid, provider_uid, str(pid), title, brand, content, price, location, odo, year)
            
            print(f'    ✅ 더미 상품 [{provider_uid}:{pid}] 생성됨')
        
        # 4. 더미 file 데이터도 생성 (이미지 URL 테스트용)
        print('\n4️⃣ 더미 file 데이터 생성 중...')
        
        for i in range(5):
            provider_uid = 1
            pid = 900000000 + i
            uid_str = f"{provider_uid}:{pid}"
            uid_hash = hashlib.md5(uid_str.encode()).hexdigest()
            uid = int(uid_hash[:8], 16)
            
            # 더미 이미지 URL 템플릿
            url_template = f"https://test.example.com/images/test_{pid}_{{cnt}}.jpg"
            count = 3 + i  # 3~7개 이미지
            
            await conn.execute("""
                INSERT INTO file (product_uid, url, count)
                VALUES ($1, $2, $3)
            """, uid, url_template, count)
            
            print(f'    ✅ 더미 파일 데이터 [UID:{uid}] 생성됨 ({count}개 이미지)')
        
        # 5. 현재 상태 확인
        print('\n5️⃣ 테스트 데이터 생성 결과 확인')
        
        insert_count = await conn.fetchval("""
            SELECT COUNT(*) FROM product 
            WHERE status = 1 AND is_conversion = false AND vector_id IS NULL
        """)
        
        update_count = await conn.fetchval("""
            SELECT COUNT(*) FROM product 
            WHERE status = 1 AND is_conversion = false AND vector_id IS NOT NULL
        """)
        
        delete_count = await conn.fetchval("""
            SELECT COUNT(*) FROM product 
            WHERE status != 1 AND is_conversion = true AND vector_id IS NOT NULL
        """)
        
        print(f'  📝 INSERT 대상: {insert_count:,}개')
        print(f'  🔄 UPDATE 대상: {update_count:,}개')
        print(f'  🗑️ DELETE 대상: {delete_count:,}개')
        
        # 6. 테스트 대상 상품들 미리보기
        print('\n6️⃣ 테스트 대상 상품 미리보기')
        
        if update_count > 0:
            print('\n🔄 UPDATE 대상:')
            update_samples = await conn.fetch("""
                SELECT provider_uid, pid, title, vector_id
                FROM product 
                WHERE status = 1 AND is_conversion = false AND vector_id IS NOT NULL
                LIMIT 5
            """)
            for product in update_samples:
                print(f'  - [{product["provider_uid"]}:{product["pid"]}] {product["title"][:50]}...')
        
        if delete_count > 0:
            print('\n🗑️ DELETE 대상:')
            delete_samples = await conn.fetch("""
                SELECT provider_uid, pid, title, vector_id
                FROM product 
                WHERE status != 1 AND is_conversion = true AND vector_id IS NOT NULL
                LIMIT 5
            """)
            for product in delete_samples:
                print(f'  - [{product["provider_uid"]}:{product["pid"]}] {product["title"][:50]}...')
        
        if insert_count > 0:
            print('\n📝 INSERT 대상 (새로운 더미 데이터):')
            insert_samples = await conn.fetch("""
                SELECT provider_uid, pid, title
                FROM product 
                WHERE provider_uid = 1 AND pid LIKE '90000000%' AND status = 1 AND is_conversion = false AND vector_id IS NULL
                LIMIT 5
            """)
            for product in insert_samples:
                print(f'  - [{product["provider_uid"]}:{product["pid"]}] {product["title"][:50]}...')
        
        print('\n✅ 테스트 데이터 생성 완료!')
        print('이제 /sync/poll 엔드포인트로 INSERT, UPDATE, DELETE를 모두 테스트할 수 있습니다.')
        
    except Exception as e:
        print(f'❌ 테스트 데이터 생성 실패: {e}')
        raise
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(create_test_data()) 