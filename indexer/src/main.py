from fastapi import FastAPI
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
from src.config import get_settings
from src.api.router import api_router
from src.auth.router import router as auth_router
import os
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles
import uvloop
import asyncio
from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)  # 로그 레벨 설정

config = get_settings()

# Determine the environment
environment = os.getenv('ENVIRONMENT', 'dev')

# Initiate app with conditional documentation
if environment in ['dev']:
    app = FastAPI(title=config.app_name, root_path="/indexer")  # Enable docs in 'dev' and 'loc'
else:
    app = FastAPI(
        title=config.app_name,
        root_path="/indexer"
    )
# CORS 설정 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "http://localhost:8000", 
        "http://127.0.0.1:3000", 
        "http://127.0.0.1:8000",
        "http://10.178.0.2:3000",
        "http://10.178.0.2:8000",
        "http://10.178.0.2:80"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/indexer/docs")

# Serve static files
app.mount("/static", StaticFiles(directory="src/static"), name="static")

# Add favicon
@app.get("/favicon.ico", include_in_schema=False)
async def get_favicon():
    from fastapi.responses import FileResponse
    import os
    favicon_path = "src/static/favicon.ico"
    if os.path.exists(favicon_path):
        return FileResponse(favicon_path)
    # Return a simple 1x1 pixel transparent GIF if favicon doesn't exist
    from fastapi.responses import Response
    transparent_gif = b'\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff\x21\xf9\x04\x01\x00\x00\x00\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x04\x01\x00\x3b'
    return Response(content=transparent_gif, media_type="image/gif")

# Including Routers
app.include_router(api_router, prefix="/api", tags=["api"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])

@app.get("/health", include_in_schema=False)
async def health_check():
    """
    간단한 헬스체크 엔드포인트
    """
    return {"status": "Good"}

# 대시보드 라우트 추가
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """모니터링 대시보드 페이지를 제공합니다."""
    with open("src/templates/dashboard.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, proxy_headers=True, forwarded_allow_ips="*")