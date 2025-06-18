#!/usr/bin/env python3

import asyncio
from src.database.postgresql import PostgreSQLManager
from src.services.text_preprocessor import ProductTextPreprocessor

async def simple_test():
    """간단한 데이터 조회 및 전처리 테스트"""
    
    print("🧪 === 간단한 테스트 (ErrorHandler 제외) ===")
    
    # PostgreSQL 연결
    pg = PostgreSQLManager()
    
    try:
        print("\n1️⃣ PostgreSQL 연결 테스트")
        async with pg.get_connection() as conn:
            print("✅ PostgreSQL 연결 성공")
            
            # 테스트 PID (브랜드가 있는 제품)
            test_pid = "291563170"  # 야마하 R3
            
            print(f"\n2️⃣ 제품 데이터 조회 (PID: {test_pid})")
            
            # product 테이블에서 기본 정보 조회 (uid도 포함)
            product_query = """
                SELECT uid, pid, title, brand, price, content, year, odo
                FROM product 
                WHERE pid = $1
            """
            product_row = await conn.fetchrow(product_query, test_pid)
            
            if not product_row:
                print(f"❌ 제품을 찾을 수 없음: {test_pid}")
                return
                
            print(f"✅ 제품 데이터 조회 성공:")
            print(f"  - UID: {product_row['uid']}")
            print(f"  - PID: {product_row['pid']}")
            print(f"  - Title: {product_row['title']}")
            print(f"  - Brand: '{product_row['brand']}' (길이: {len(product_row['brand'] or '')})")
            print(f"  - Price: {product_row['price']}")
            print(f"  - Year: {product_row['year']}")
            print(f"  - ODO: {product_row['odo']}")
            
            # file 테이블에서 이미지 URL 조회 (uid 사용)
            print(f"\n3️⃣ 이미지 URL 조회")
            file_query = """
                SELECT url, count 
                FROM file 
                WHERE product_uid = $1 
                ORDER BY count
            """
            file_results = await conn.fetch(file_query, product_row['uid'])
            
            # 이미지 URL 리스트 구성
            images = []
            for file_row in file_results:
                url_template = file_row['url']
                count = file_row['count']
                # {cnt}를 실제 count 값으로 교체
                if '{cnt}' in url_template:
                    image_url = url_template.replace('{cnt}', str(count))
                    images.append(image_url)
                else:
                    images.append(url_template)
            
            print(f"✅ 이미지 URL 조회 성공: {len(images)}개")
            for i, img in enumerate(images[:3], 1):
                print(f"  {i}. {img}")
            if len(images) > 3:
                print(f"  ... 총 {len(images)}개")
            
            print(f"\n4️⃣ 텍스트 전처리 테스트")
            
            # 기존 방식 (brand 없이)
            old_text_data = {
                'title': product_row['title'] or '',
                'year': product_row['year'],
                'price': product_row['price'],
                'odo': product_row['odo'],
                'content': product_row['content'] or ''
            }
            
            # 새로운 방식 (brand 포함)
            new_text_data = {
                'title': product_row['title'] or '',
                'brand': product_row['brand'] or '',
                'year': product_row['year'],
                'price': product_row['price'],
                'odo': product_row['odo'],
                'content': product_row['content'] or ''
            }
            
            preprocessor = ProductTextPreprocessor()
            
            old_processed = preprocessor.preprocess_product_data(old_text_data)
            new_processed = preprocessor.preprocess_product_data(new_text_data)
            
            print(f"📝 기존 방식 (brand 없이, {len(old_processed)}자):")
            print(f"  {old_processed}")
            
            print(f"\n📝 새로운 방식 (brand 포함, {len(new_processed)}자):")
            print(f"  {new_processed}")
            
            print(f"\n📊 차이점:")
            print(f"  - 길이 차이: {len(new_processed) - len(old_processed)}자")
            if product_row['brand']:
                print(f"  - Brand 데이터 존재: '{product_row['brand']}'")
            else:
                print(f"  - Brand 데이터 없음 (NULL 또는 빈 문자열)")
            
            # Brand 추출 테스트
            extracted = preprocessor.extract_model_and_brand(product_row['title'] or '')
            print(f"  - 제목에서 추출된 Brand: '{extracted['brand']}'")
            print(f"  - 제목에서 추출된 Model: '{extracted['model']}'")
            
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await pg.close()
        print(f"\n✅ 테스트 완료")

if __name__ == "__main__":
    asyncio.run(simple_test()) 