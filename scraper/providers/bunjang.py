import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from models import Provider, Code
from datetime import datetime

async def fetch_bunjang_categories(db: AsyncSession) -> list[dict]:
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
