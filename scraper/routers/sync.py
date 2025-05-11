from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from core.database import get_db
from providers.bunjang import fetch_bunjang_categories, save_categories

router = APIRouter(prefix="/sync", tags=["Sync"])

@router.post("/sync_bunjang_categories")
async def sync_bunjang_categories(db: AsyncSession = Depends(get_db)):
    categories = await fetch_bunjang_categories(db)
    await save_categories(db, categories)
    return {"message": f"{len(categories)} categories synced."}
