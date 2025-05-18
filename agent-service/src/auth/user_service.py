import os
from fastapi import HTTPException, status, Request
from src.auth.jwt_utils import verify_access_token

def get_current_user(request: Request):
    # dev 환경에서는 인증 우회
    if os.getenv("ENVIRONMENT", "dev") == "dev":
        return {"sub": "dev-user"}
    # 실제 인증
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="인증 토큰이 필요합니다.")
    token = auth_header.split(" ")[1]
    payload = verify_access_token(token)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="유효하지 않은 토큰입니다.")
    return payload 