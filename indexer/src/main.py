from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
from src.config import get_settings
from src.api.router import api_router
from src.auth.router import router as auth_router
import os

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
# app.mount("/static", StaticFiles(directory="./static"), name="static")

# Including Routers
app.include_router(api_router, prefix="/api", tags=["api"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])

@app.get("/health", include_in_schema=False)
async def health_check():
    """
    간단한 헬스체크 엔드포인트
    """
    return {"status": "Good"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, proxy_headers=True, forwarded_allow_ips="*")