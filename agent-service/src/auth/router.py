from fastapi import APIRouter, HTTPException
from src.auth.jwt_utils import create_access_token

router = APIRouter()

@router.post("/token")
def issue_token(username: str, password: str):
    # 실제 서비스라면 DB에서 사용자 검증
    if not (username == "testuser" and password == "testpw"):
        raise HTTPException(status_code=401, detail="인증 실패")
    access_token = create_access_token({"sub": username})
    return {"access_token": access_token, "token_type": "bearer"} 