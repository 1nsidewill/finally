## 💡 프로젝트 최초 실행 방법

### 1. uv 설치

먼저, 시스템에 `uv`가 설치되어 있지 않다면 아래 명령어로 설치하세요.

```bash
pip install uv
```

### 2. 가상환경 구축 및 의존성 패키지 설치, 동기화

requirements.txt 또는 pyproject.toml 파일에 정의된 모든 의존성을
아래 명령어 한 줄로 설치 및 최신 상태로 동기화할 수 있습니다.

```bash
uv sync
```

### 3. 프로젝트 실행

아래 명령어로 Python 파일을 실행합니다.

```
uv run main.py run
```