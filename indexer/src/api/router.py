from fastapi import APIRouter
from typing import Dict, Any

# Indexer 서비스용 라우터
api_router = APIRouter(prefix="/api/v1", tags=["indexer"])

@api_router.get("/health")
async def health_check() -> Dict[str, Any]:
    """헬스체크 엔드포인트"""
    return {
        "status": "healthy",
        "service": "indexer",
        "version": "1.0.0"
    }

# TODO: Redis Queue 기반 sync 엔드포인트들 구현 예정
# - POST /sync - 동기화 요청
# - GET /sync/status - 동기화 상태 확인  
# - POST /retry - 실패 작업 재시도