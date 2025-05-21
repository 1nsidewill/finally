from fastapi import APIRouter, HTTPException
from src.auth.jwt_utils import create_access_token
from src.config import get_settings
from pydantic import BaseModel

config = get_settings()

router = APIRouter()

class TokenRequest(BaseModel):
    username: str
    password: str

@router.post("/token")
def issue_token(request: TokenRequest):
    # 실제 서비스라면 DB에서 사용자 검증
    if not (request.username == config.test_username and request.password == config.test_password):
        raise HTTPException(status_code=401, detail="인증 실패")
    access_token = create_access_token({"sub": request.username})
    return {"access_token": access_token, "token_type": "bearer"} 