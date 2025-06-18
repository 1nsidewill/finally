import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path('.').resolve()))
from src.database.qdrant import QdrantManager

async def check_qdrant():
    qm = QdrantManager()
    count = await qm.count_points()
    print(f'🔍 Qdrant 현재 포인트 수: {count}')
    
    # 방금 삽입한 데이터 검색
    points = await qm.get_points(['3743a65a-6869-528e-a7d9-aa502935b7f6'])
    if points:
        point = points[0]
        print(f'📦 방금 삽입한 제품 데이터:')
        print(f'  • ID: {point.id}')
        print(f'  • 제목: {point.payload.get("title", "")}')
        print(f'  • 브랜드: {point.payload.get("brand", "")}')
        print(f'  • UID: {point.payload.get("uid", "")}')
        print(f'  • 가격: {point.payload.get("price", "")}')
    else:
        print('❌ 데이터를 찾을 수 없습니다')

if __name__ == "__main__":
    asyncio.run(check_qdrant()) 