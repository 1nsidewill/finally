FROM python:3.13-slim

# 1. 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    build-essential \
    g++ \
    curl \
    cron \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 2. TZ 설정
ENV TZ=Asia/Seoul

# 3. uv 복사
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 4. 작업 디렉토리
WORKDIR /app
COPY . /app

# 5. .venv 생성 및 패키지 설치 (pyproject.toml 기반)
RUN uv sync --frozen --no-cache

# 6. CRON 복사
COPY cron /etc/cron.d/cron
RUN chmod 0644 /etc/cron.d/cron && crontab /etc/cron.d/cron
 
# 7. supervisor 복사
RUN uv pip install supervisor
COPY supervisord.conf /etc/supervisord.conf

# 8. FastAPI 앱 실행 (uvicorn은 .venv 안에 설치됨)
#CMD ["/app/.venv/bin/uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
CMD ["/app/.venv/bin/supervisord", "-c", "/etc/supervisord.conf"]

