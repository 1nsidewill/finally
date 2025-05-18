from typing import Optional
from pathlib import Path
import httpx

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from core.database import get_db
from providers import bunjang

# 전역 키워드 큐
keyword_queue: list[str] = []

router = APIRouter(prefix="/sync", tags=["Sync"])

@router.post("/sync_bunjang_categories")
async def sync_bunjang_categories(db: AsyncSession = Depends(get_db)):
    categories = await bunjang.fetch_categories(db)
    await bunjang.save_categories(db, categories)
    return {"message": f"{len(categories)} categories synced."}

@router.post("/sync_bunjang_products")
async def sync_bunjang_products(keyword: Optional[str] = Query(default=None), db: AsyncSession = Depends(get_db)):
    page = 0
    total_processed = 0
    while True:
        products = await bunjang.fetch_products(keyword, page)
        if not products:
            break
        for product in products:
            pid = product.get("pid")
            if not pid:
                continue
            if product.get("status") != "0":
                await bunjang.delete_product_by_code(db, pid)
                continue
            detail = await bunjang.fetch_product_detail(pid)
            if detail:
                await bunjang.upsert_product(db, detail)
                total_processed += 1
        total_processed += len(products)
        page += 1
    return {"message": f"{len(products)} products synced."}

@router.post("/sync_bunjang_bike")
async def sync_bunjang_products(db: AsyncSession = Depends(get_db)):
    global keyword_queue

    text = Path("./list.txt").read_text(encoding="utf-8")
    keywords = set()
    for line in text.splitlines():
        for item in line.split(", "):
            kw = item.strip()
            if kw:
                keywords.add(kw)
    keyword_queue = sorted(keywords)

    batch_size = 5
    batch = [keyword_queue.pop(0) for _ in range(min(batch_size, len(keyword_queue)))]

    for kw in batch:
        await call_sync_bunjang(kw)

    return {"message": f"{len(batch)}개 키워드 처리 완료"}


async def call_sync_bunjang(keyword):
    global keyword_queue
    url = "http://localhost:8000/sync/sync_bunjang_products"
    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(url, params={"keyword": keyword})
            print(f"[{keyword}] ✅ {res.status_code}: {res.json()}")
    except Exception as e:
        print(f"[{keyword}] ❌ Error: {e}")
    if keyword_queue:
        return await call_sync_bunjang(keyword_queue.pop(0))
    return res.status_code, res.json()