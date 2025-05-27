FROM python:3.13-slim

# 1. 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    build-essential \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. uv 복사
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 3. 작업 디렉토리
WORKDIR /app
COPY . /app

# 4. .venv 생성 및 패키지 설치 (pyproject.toml 기반)
RUN uv sync --frozen --no-cache

RUN uv pip install fastapi sqlalchemy uvicorn httpx asyncpg python-dotenv

# 5. FastAPI 앱 실행 (uvicorn은 .venv 안에 설치됨)
CMD ["/app/.venv/bin/uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
