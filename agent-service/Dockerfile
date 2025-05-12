FROM python:3.13-slim

# uv 설치 (pip 최신화 포함)
RUN pip install --upgrade pip && pip install uv

# 작업 디렉토리 생성 및 이동
WORKDIR /app

# 프로젝트 파일 복사
COPY . .

# 환경변수 예시 (필요시 수정)
ENV ENVIRONMENT=prod

# 의존성 설치 (가상환경 없이)
RUN uv sync

# 포트 오픈
EXPOSE 8000

# 실행 커맨드 (uvicorn)
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
