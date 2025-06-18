#!/usr/bin/env python3

import asyncio
from src.database.postgresql import PostgreSQLManager

async def find_brand_product():
    pg = PostgreSQLManager()
    async with pg.get_connection() as conn:
        result = await conn.fetch("""
            SELECT pid, title, brand 
            FROM product 
            WHERE brand IS NOT NULL AND brand != '' 
            LIMIT 5
        """)
        print('=== 브랜드가 있는 제품들 ===')
        for row in result:
            print(f'PID: {row["pid"]}, Brand: "{row["brand"]}", Title: {row["title"]}')

if __name__ == "__main__":
    asyncio.run(find_brand_product()) 