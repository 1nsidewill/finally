import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from models import Provider, Code, Product
from datetime import datetime

from utils.time import safe_parse_datetime

async def fetch_categories(db: AsyncSession) -> list[dict]:
    result = await db.execute(
        select(Provider.provider_url).where(Provider.code == "BUNJANG")
    )
    provider_url = result.scalar_one_or_none()
    if not provider_url:
        raise ValueError("BUNJANG provider_url 없음")

    url = f"{provider_url.rstrip('/')}/1/categories/list.json"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json().get("categories", [])

async def save_categories(db: AsyncSession, categories: list[dict]):
    result = await db.execute(select(Code.uid).where(Code.code == "BUNJANG"))
    upper_uid = result.scalar_one_or_none()
    if not upper_uid:
        raise ValueError("code='BUNJANG' 항목 없음")

    result = await db.execute(
        select(Code).where(Code.code == "CATEGORY", Code.upper_uid == upper_uid)
    )
    existing = result.scalars().first()

    if existing:
        await db.execute(
            update(Code)
            .where(Code.uid == existing.uid)
            .values(
                rmk=categories,
                updated_dt=datetime.utcnow()  # ✅ 여기 추가
            )
        )
    else:
        new_code = Code(
            code="CATEGORY",
            upper_uid=upper_uid,
            depth=3,
            order=1,
            name="카테고리",
            rmk=categories
        )
        db.add(new_code)
    await db.commit()

async def fetch_products(keyword: str, page: int = 0):
    async with httpx.AsyncClient() as client:
        res = await client.get("https://api.bunjang.co.kr/api/1/find_v2.json", params={"q": keyword, "page": page, "n": 100 })
        res.raise_for_status()
        return res.json().get("list", [])

async def fetch_product_detail(pid: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 10; SM-G970N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.138 Mobile Safari/537.36"
    }
    url = f"https://api.bunjang.co.kr/api/pms/v3/products-detail/{pid}?viewerUid=-1"
    async with httpx.AsyncClient(headers=headers) as client:
        res = await client.get(url)
        res.raise_for_status()
        return res.json().get("data", {}).get("product")

async def upsert_product(db: AsyncSession, detail_data: dict):
    pid = str(detail_data["pid"])

    stmt = select(Product).where(Product.product_code == pid)
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing and existing.updated_dt != safe_parse_datetime(detail_data.get("updatedAt")):
        existing.title = detail_data.get("name")
        existing.content = detail_data.get("description")
        existing.desc = detail_data.get("description")
        existing.price = detail_data.get("price")
        existing.location = detail_data.get("geo", {}).get("address")
        existing.rmk = detail_data
        existing.updated_dt = safe_parse_datetime(detail_data.get("updatedAt"))

    elif existing is None:
        product = Product(
            provider_uid=1,
            product_code=pid,
            title=detail_data.get("name"),
            content=detail_data.get("description"),
            desc=detail_data.get("description"),
            price=detail_data.get("price"),
            location=detail_data.get("geo", {}).get("address"),
            rmk=detail_data,
            created_dt=safe_parse_datetime(detail_data.get("describedAt")),
            updated_dt=safe_parse_datetime(detail_data.get("updatedAt"))
        )
        db.add(product)

    await db.commit()

async def delete_product_by_code(db, product_code):
    await db.execute(delete(Product).where(Product.product_code == product_code))
    await db.commit()

