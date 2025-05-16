from fastapi import APIRouter, HTTPException
from src.auth.jwt_utils import create_access_token
from src.config import get_settings

config = get_settings()

router = APIRouter()

@router.post("/token")
def issue_token(username: str, password: str):
    # 실제 서비스라면 DB에서 사용자 검증
    if not (username == config.test_username and password == config.test_password):
        raise HTTPException(status_code=401, detail="인증 실패")
    access_token = create_access_token({"sub": username})
    return {"access_token": access_token, "token_type": "bearer"} 