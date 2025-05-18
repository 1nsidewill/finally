---

## 📁 프로젝트 구조 (agent-service)

```
agent-service/
│
├── data/                        # 바이크 매물 등 텍스트 데이터 파일
│
├── src/
│   ├── api/
│   │   ├── router.py            # /api/query 등 주요 API 엔드포인트
│   │   ├── models.py            # Pydantic 데이터 모델 정의
│   │   ├── document_utils.py    # 문서 로딩/포맷팅 등 유틸 함수
│   │   └── schema.py            # (예비) 추가 스키마 정의용
│   │
│   ├── auth/
│   │   ├── router.py            # /auth/token 등 인증 관련 엔드포인트
│   │   ├── jwt_utils.py         # JWT 토큰 발급/검증 함수
│   │   └── user_service.py      # 인증 유저 검증 유틸 (get_current_user 등)
│   │
│   ├── config.py                # 환경변수 및 설정 관리
│   ├── main.py                  # FastAPI 앱 진입점
│   └── __init__.py
│
├── .env.dev                     # 개발 환경 변수 파일 예시
├── README.md                    # 프로젝트 설명서
└── requirements.txt             # Python 패키지 목록
```

---

### 📂 주요 폴더/파일 설명

- **src/api/**
  - `router.py` : 실제 서비스 API 엔드포인트(예: /api/query)
  - `models.py` : 요청/응답용 Pydantic 모델
  - `document_utils.py` : 문서 데이터 로딩, 포맷팅 등 유틸 함수

- **src/auth/**
  - `router.py` : 인증 관련 엔드포인트(예: /auth/token)
  - `jwt_utils.py` : JWT 토큰 생성/검증 함수
  - `user_service.py` : 인증 유저 검증(토큰 파싱 등) 유틸

- **src/config.py** : 환경변수, 설정값 관리
- **src/main.py** : FastAPI 앱 실행 및 라우터 등록

- **data/** : 바이크 매물 등 텍스트 데이터 파일 저장 폴더

- **.env.dev** : 개발 환경 변수 파일 예시
- **requirements.txt** : 의존성 패키지 목록

---

### 📝 참고

- API 엔드포인트는 `/api/`(서비스), `/auth/`(인증)로 명확히 분리되어 있습니다.
- 문서/인증/비즈니스 로직이 각각의 파일로 분리되어 유지보수와 확장성이 좋습니다.
- 환경변수(`.env.dev`)와 설정(`config.py`)을 통해 운영/개발 환경을 쉽게 전환할 수 있습니다.

---


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
