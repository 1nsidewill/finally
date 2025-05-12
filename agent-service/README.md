# 설치 및 실행 가이드 (uv 사용)

이 프로젝트는 [uv](https://github.com/astral-sh/uv)를 사용하여 Python 패키지 관리를 합니다.

## 1. 저장소 클론
```bash
git clone <레포지토리 주소>
cd agent-service
```

## 2. 의존성 설치
가상환경을 별도로 만들고 싶다면:
```bash
uv venv --python 3.13.3
uv sync
```

가상환경을 따로 만들지 않고, 현재 환경에 바로 설치하려면:
```bash
uv sync
```

> **참고:**
> - `uv venv`는 `.venv` 폴더에 가상환경을 만듭니다. (권장)
> - `uv sync`만 해도 의존성 설치가 되지만, 가상환경을 쓰는 것이 충돌 방지에 좋습니다.

## 3. 환경 변수 설정
`.env.dev` 등 환경 파일을 복사/수정하여 환경변수를 설정하세요.

## 4. FastAPI 서버 실행
```bash
uvicorn src.main:app --reload
```

---

## 요약
1. `git clone ... && cd agent-service`
2. `uv venv && source .venv/bin/activate` (권장)
3. `uv sync`
4. 환경파일 준비
5. `uvicorn src.main:app --reload`
