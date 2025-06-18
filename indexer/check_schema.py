#!/usr/bin/env python3

import asyncio
from src.config import get_settings
from src.database.postgresql import PostgreSQLManager

async def check_schema():
    pg = PostgreSQLManager()
    
    async with pg.get_connection() as conn:
        # product 테이블 스키마 확인
        print("=== PRODUCT 테이블 스키마 ===")
        result = await conn.fetch("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'product' 
            ORDER BY ordinal_position
        """)
        
        for row in result:
            print(f"{row['column_name']:<15} {row['data_type']:<20} nullable={row['is_nullable']}")
        
        # file 테이블 스키마 확인  
        print("\n=== FILE 테이블 스키마 ===")
        result2 = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'file'
            ORDER BY ordinal_position
        """)
        
        for row in result2:
            print(f"{row['column_name']:<15} {row['data_type']:<20} nullable={row['is_nullable']}")
        
        # 실제 데이터 샘플 확인 (odo 사용)
        print("\n=== PRODUCT 샘플 데이터 (5개) ===")
        sample_products = await conn.fetch("""
            SELECT pid, title, brand, price, content, year, odo, created_dt, is_conversion
            FROM product 
            WHERE pid IS NOT NULL 
            LIMIT 5
        """)
        
        for product in sample_products:
            print(f"PID: {product['pid']}")
            print(f"  Title: {product['title']}")
            print(f"  Brand: {product['brand']}")
            print(f"  Price: {product['price']}")
            print(f"  Year: {product['year']}")
            print(f"  ODO: {product['odo']}")
            print(f"  Is_Conversion: {product['is_conversion']}")
            print(f"  Content: {product['content'][:100] if product['content'] else 'None'}...")
            print()
        
        # 컨버전 상태 통계
        print("=== CONVERSION 상태 통계 ===")
        conversion_stats = await conn.fetch("""
            SELECT 
                is_conversion,
                COUNT(*) as count,
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM product) as percentage
            FROM product 
            GROUP BY is_conversion
        """)
        
        for stat in conversion_stats:
            status = "변환됨" if stat['is_conversion'] else "미변환"
            print(f"{status}: {stat['count']:,}개 ({stat['percentage']:.1f}%)")
        
        # file 테이블 샘플
        print("\n=== FILE 샘플 데이터 (5개) ===")
        sample_files = await conn.fetch("""
            SELECT product_uid, url, count
            FROM file 
            LIMIT 5
        """)
        
        for file_row in sample_files:
            print(f"Product UID: {file_row['product_uid']}")
            print(f"  URL: {file_row['url']}")
            print(f"  Count: {file_row['count']}")
            
        # NULL 비율 체크
        print("\n=== NULL 데이터 비율 ===")
        null_stats = await conn.fetch("""
            SELECT 
                'title' as field,
                COUNT(*) FILTER (WHERE title IS NULL OR title = '') as null_count,
                COUNT(*) as total,
                (COUNT(*) FILTER (WHERE title IS NULL OR title = '') * 100.0 / COUNT(*)) as null_percentage
            FROM product
            UNION ALL
            SELECT 
                'brand' as field,
                COUNT(*) FILTER (WHERE brand IS NULL OR brand = '') as null_count,
                COUNT(*) as total,
                (COUNT(*) FILTER (WHERE brand IS NULL OR brand = '') * 100.0 / COUNT(*)) as null_percentage
            FROM product
            UNION ALL
            SELECT 
                'odo' as field,
                COUNT(*) FILTER (WHERE odo IS NULL) as null_count,
                COUNT(*) as total,
                (COUNT(*) FILTER (WHERE odo IS NULL) * 100.0 / COUNT(*)) as null_percentage
            FROM product
            UNION ALL
            SELECT 
                'content' as field,
                COUNT(*) FILTER (WHERE content IS NULL OR content = '') as null_count,
                COUNT(*) as total,
                (COUNT(*) FILTER (WHERE content IS NULL OR content = '') * 100.0 / COUNT(*)) as null_percentage
            FROM product
        """)
        
        for stat in null_stats:
            print(f"{stat['field']:<10}: NULL {stat['null_count']:,}/{stat['total']:,} ({stat['null_percentage']:.1f}%)")

if __name__ == "__main__":
    asyncio.run(check_schema()) 