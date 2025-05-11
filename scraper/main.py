from fastapi import FastAPI
from contextlib import asynccontextmanager
from core.database import engine, Base
from routers import sync

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

app = FastAPI(lifespan=lifespan)
app.include_router(sync.router)
