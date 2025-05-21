# 🐦 finally

> **Finally, Find Early.**  
> With `nallybird.`  

AI 기반 중고 매물 검색 및 추천 서비스  
당신이 찾는 그 매물, **가장 먼저**, **가장 정확하게** 찾아드립니다.

---

## 🚀 소개

`finally`는 중고거래 시장의 **키워드 검색 한계**를 극복하기 위해 만들어졌습니다.  
AI가 직접 번개장터, 중고나라, 네이버 카페 등 다양한 플랫폼의 매물을 탐색하고,  
사용자에게 **후회 없는**, **빠른**, **정확한**, **합리적인** 선택을 제공합니다.

> “호구 잡히지 말고, 원하는 매물을 현명하게 찾자.”

---

## 🔍 주요 기능

- 📡 다양한 플랫폼의 매물 통합 크롤링  
- 🧠 AI 기반 자연어 검색 및 연관 매물 추천  
- 🛡️ 사기 피해 예방을 위한 가격 및 정보 검증 기능  
- 💬 대화형 검색 인터페이스 (추후 업데이트 예정)

---

## 🧩 프로젝트 구조

```bash
finally/
├── frontend/           # 사용자 웹 인터페이스 (React)
├── agent-service/      # AI 검색 및 추천 API
├── scraper/            # 매물 크롤링 및 수집 시스템
├── indexer/            # 데이터 전처리 및 벡터 인덱싱
└── user-service/       # 회원가입, 로그인, 인증, 결제 등 Core API
````

---

## 🛠 기술 스택

**Backend Microservices**

* ⚡️ [FastAPI](https://fastapi.tiangolo.com/) – 경량 비동기 Python 웹 프레임워크
* 🧠 [LangChain](https://www.langchain.com/) + [LangGraph](https://www.langchain.com/langgraph) – LLM 기반 AI 흐름 구성 및 제어
* 🧾 [PostgreSQL](https://www.postgresql.org/) – 관계형 데이터베이스
* 📦 [Qdrant](https://qdrant.tech/) – 고속 벡터 검색을 위한 벡터 DB

**Frontend**

* ⚛️ [React](https://reactjs.org/) – 사용자 인터페이스 구성

**Infrastructure & DevOps**

* 🐳 [Docker](https://www.docker.com/), [Docker Compose](https://docs.docker.com/compose/) – 멀티서비스 컨테이너 관리 및 배포
* 🌐 [Traefik](https://traefik.io/) – 컨테이너 기반 라우팅 및 SSL Reverse Proxy

---

## 🧑‍💻 팀원 소개

| 이름          | 역할                            | GitHub                                       |
| ----------- | ----------------------------- | -------------------------------------------- |
| Gyu Ha Yi   | Team Lead / Backend Developer | [@1nsidewill](https://github.com/1nsidewill) |
| Jaehong Kim | Backend Developer / DevOps    | [@SidNamo](https://github.com/SidNamo)       |
| Joohee Choo | Frontend Developer / Designer | [@choojoohee](https://github.com/choojoohee) |

---

## 🗓️ 로드맵

* [ ] **2025년 9월** – 1차 베타 테스트 런칭
* [ ] 중고거래 커뮤니티 확장 (예: 클리앙, 바이크매물 등)
* [ ] AI 기반 가격 적정성 분석 기능
* [ ] 사용자 맞춤 알림 및 푸시 추천 기능
* [ ] 중고 사기 이력 조회 연동 (예: 더치트)

---

## 📄 라이선스

본 프로젝트는 [MIT License](./LICENSE)를 따릅니다.
자유롭게 사용하고, 수정하고, 배포하되, 원작자의 표시를 남겨주세요.

---

## 🤝 기여 및 문의

기여는 언제든 환영입니다!
이슈 등록 또는 PR을 통해 함께해 주세요. 🙌
서비스 제휴, 협업 문의: [yiguha@gmail.com](mailto:yiguha@gmail.com)

---

> “빠르게 사고 싶다면,
> 똑똑하게 사고 싶다면,
> finally에서 시작하세요.”

