from fastapi import FastAPI
from fastapi.responses import RedirectResponse
import logging
from src.config import get_settings
from src.api.router import api_router
from src.auth.router import router as auth_router
from fastapi.middleware.cors import CORSMiddleware
import os

config = get_settings()

# Determine the environment
environment = os.getenv('ENVIRONMENT', 'dev')

# Initiate app with conditional documentation
if environment in ['dev']:
    app = FastAPI(title=config.app_name, root_path="/agent-service")  # Enable docs in 'dev' and 'loc'
else:
    app = FastAPI(
        title=config.app_name,
        root_path="/agent-service"
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # 프론트엔드 개발 서버 주소
    allow_credentials=True,
    allow_methods=["*"],  # 또는 ["GET", "POST", "OPTIONS"] 등 필요한 메소드만
    allow_headers=["*"],  # 또는 ["Content-Type", "Authorization"] 등 필요한 헤더만
)

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/agent-service/docs")

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
    logging.info("헬스체크 요청 받음")
    return {"status": "Good"}

if __name__ == "__main__":
    import uvicorn
    logging.info(f"서버 시작 - 환경: {environment}")
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, proxy_headers=True, forwarded_allow_ips="*")