# 스크래퍼 팀을 위한 Job Format 명세서

**버전:** 1.0  
**작성일:** 2025-06-18  
**대상:** 스크래퍼 팀  

## 📋 개요

이 문서는 스크래퍼 팀이 **Indexer Redis Queue**에 작업을 제출할 때 사용해야 하는 **표준 Job Format**을 정의합니다.

모든 작업은 **JSON 형태**로 Redis Queue에 제출되며, 지정된 형식을 따라야 합니다.

---

## 🔄 지원하는 작업 타입 (Job Types)

### 1. **SYNC** - 새로운 제품 추가/동기화
- **목적**: 새로운 제품 데이터를 시스템에 추가하거나 전체 동기화
- **사용 시점**: 신규 제품 발견, 전체 데이터 리프레시

### 2. **UPDATE** - 기존 제품 정보 업데이트  
- **목적**: 기존 제품의 정보를 최신 상태로 업데이트
- **사용 시점**: 가격 변경, 상태 변경, 내용 수정

### 3. **DELETE** - 제품 삭제
- **목적**: 판매 완료되거나 삭제된 제품을 시스템에서 제거
- **사용 시점**: 제품 판매 완료, 게시물 삭제

---

## 📋 표준 Job Format

### 기본 구조

```json
{
  "id": "string (optional)",        // 작업 고유 ID (생략 시 자동 생성)
  "type": "string (required)",      // 작업 타입: "sync", "update", "delete"
  "product_id": "string (required)", // 제품 ID (번개장터 PID)
  "provider": "string (optional)",   // 플랫폼 명 (기본값: "bunjang")
  "product_data": {                 // 제품 데이터 (type이 "delete"가 아닌 경우 필수)
    // 제품 상세 정보 (아래 참조)
  },
  "timestamp": "string (optional)",  // 작업 생성 시간 (ISO 8601 형식)
  "metadata": {                     // 추가 메타데이터 (optional)
    // 작업 관련 추가 정보
  }
}
```

### 제품 데이터 구조 (product_data)

```json
{
  "pid": "string (required)",           // 제품 ID (번개장터 고유 ID)
  "title": "string (required)",         // 제품 제목
  "price": "integer (optional)",        // 가격 (원 단위, null 가능)
  "content": "string (optional)",       // 제품 설명/내용
  "year": "integer (optional)",         // 연식 (자동차/오토바이용)
  "mileage": "integer (optional)",      // 주행거리 (자동차/오토바이용)
  "page_url": "string (optional)",      // 제품 페이지 URL (생략 시 자동 생성)
  "images": ["string"] (optional)       // 이미지 URL 배열 (기본값: 빈 배열)
}
```

---

## 📝 Job Format 예시

### 1. SYNC 작업 (새 제품 추가)

```json
{
  "id": "sync_job_001",
  "type": "sync",
  "product_id": "bunmall_1234567",
  "provider": "bunjang",
  "product_data": {
    "pid": "bunmall_1234567",
    "title": "2020 야마하 YZF-R3 상태좋은 바이크 판매",
    "price": 4500000,
    "content": "2020년 야마하 YZF-R3입니다. 주행거리 15,000km로 상태 양호합니다. 정기점검 완료했으며, 사고이력 없습니다.",
    "year": 2020,
    "mileage": 15000,
    "page_url": "https://m.bunjang.co.kr/products/bunmall_1234567",
    "images": [
      "https://media.bunjang.co.kr/product/1234567_1.jpg",
      "https://media.bunjang.co.kr/product/1234567_2.jpg"
    ]
  },
  "timestamp": "2025-06-18T05:00:00Z",
  "metadata": {
    "source": "automated_scraper",
    "scraper_version": "2.1.0"
  }
}
```

### 2. UPDATE 작업 (가격 변경)

```json
{
  "id": "update_job_002",
  "type": "update", 
  "product_id": "bunmall_1234567",
  "provider": "bunjang",
  "product_data": {
    "pid": "bunmall_1234567",
    "title": "2020 야마하 YZF-R3 상태좋은 바이크 판매 [가격인하]",
    "price": 4200000,
    "content": "2020년 야마하 YZF-R3입니다. 주행거리 15,000km로 상태 양호합니다. 빠른 판매를 위해 가격을 인하했습니다!",
    "year": 2020,
    "mileage": 15000
  },
  "timestamp": "2025-06-18T08:30:00Z",
  "metadata": {
    "source": "price_monitor",
    "price_change": -300000
  }
}
```

### 3. DELETE 작업 (판매 완료)

```json
{
  "id": "delete_job_003",
  "type": "delete",
  "product_id": "bunmall_1234567",
  "provider": "bunjang",
  "timestamp": "2025-06-18T10:15:00Z",
  "metadata": {
    "source": "automated_scraper",
    "reason": "product_sold"
  }
}
```

---

## ⚠️ 중요 사항 및 제약 조건

### 필수 필드 검증
- **모든 작업**: `type`, `product_id` 필수
- **SYNC/UPDATE 작업**: `product_data.pid`, `product_data.title` 필수
- **DELETE 작업**: `product_data` 생략 가능

### 데이터 타입 제약
- `price`: 양의 정수 또는 null
- `year`: 4자리 연도 (예: 2020)
- `mileage`: 양의 정수 (km 단위)
- `images`: 유효한 URL 배열
- `timestamp`: ISO 8601 형식 (예: "2025-06-18T05:00:00Z")

### 크기 제한
- `title`: 최대 500자
- `content`: 최대 10,000자
- `images`: 최대 20개 URL
- 전체 job 크기: 최대 1MB

### Provider 정보
- **기본값**: "bunjang"
- **향후 확장**: "joongna", "carrot", etc.
- **UUID 생성**: `provider:product_id` 조합으로 고유성 보장

---

## 🚀 Redis Queue 사용법

### Queue 이름
```
기본 큐: indexer_jobs
```

### 작업 제출 (Python 예시)
```python
import redis
import json

# Redis 연결
r = redis.Redis(host='your-redis-host', port=6333, db=0)

# Job 생성
job = {
    "type": "sync",
    "product_id": "bunmall_1234567",
    "product_data": {
        "pid": "bunmall_1234567",
        "title": "상품 제목",
        "price": 1000000
    }
}

# Queue에 추가
r.lpush('indexer_jobs', json.dumps(job, ensure_ascii=False))
```

### 작업 제출 (Node.js 예시)
```javascript
const redis = require('redis');
const client = redis.createClient({
    host: 'your-redis-host',
    port: 6333
});

const job = {
    type: 'sync',
    product_id: 'bunmall_1234567',
    product_data: {
        pid: 'bunmall_1234567',
        title: '상품 제목',
        price: 1000000
    }
};

client.lpush('indexer_jobs', JSON.stringify(job));
```

---

## 🔍 작업 처리 결과 및 모니터링

### 처리 성공
- 작업이 성공적으로 처리되면 PostgreSQL 및 Qdrant에 데이터가 반영됩니다
- `is_conversion` 플래그가 `true`로 설정됩니다

### 처리 실패  
- 오류 발생 시 로그에 상세한 오류 정보가 기록됩니다
- 데이터 형식 오류, 필수 필드 누락 등의 경우 작업이 거부됩니다

### 중복 처리 방지
- `provider:product_id` 조합으로 중복 방지
- 같은 제품에 대한 UPDATE는 기존 데이터를 갱신합니다

---

## 📞 문의 및 지원

**문제 발생 시 연락처:**
- 개발팀: indexer-dev@team.com
- 시스템 모니터링: monitoring@team.com

**관련 문서:**
- [Redis Queue Interface 문서](./redis-queue-interface.md)
- [API 엔드포인트 문서](./api-endpoints.md)
- [에러 처리 가이드](./error-handling-guide.md)

---

**문서 버전 히스토리:**
- v1.0 (2025-06-18): 초기 버전 작성