DROP TABLE IF EXISTS file;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS category;
DROP TABLE IF EXISTS log;
DROP TABLE IF EXISTS code;
DROP TABLE IF EXISTS provider;

-- 1. provider 테이블
CREATE TABLE provider (
    uid           bigserial PRIMARY KEY,
    code          varchar(50) NOT NULL UNIQUE,
    url_main	  varchar(500),
    url_api		  varchar(500),
    url_logo	  text,
    name          varchar(100) NOT NULL,
    name_english  varchar(100) NOT NULL,
    "desc"        varchar(500),
    created_dt    timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_dt    timestamp DEFAULT CURRENT_TIMESTAMP
);

-- 2. code 테이블
CREATE TABLE code (
    uid        bigserial PRIMARY KEY,
    code       varchar(50),
    upper_uid  bigint,
    depth      smallint,
    "order"    smallint,
    name       varchar(100) NOT NULL,
    value      text,
    "desc"     text,
    rmk        jsonb,
    created_dt timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_dt timestamp DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (upper_uid) REFERENCES code(uid) ON DELETE SET NULL ON UPDATE CASCADE
);

-- 3. log 테이블 (code_uid 컬럼 추가)
CREATE TABLE log (
    uid         bigserial PRIMARY KEY,
    table_name  varchar(100) NOT NULL,
	table_uid	bigint NOT NULL,
    ip          inet,
    action      varchar(150) NOT NULL,
	status		varchar(50) NOT NULL,
    "desc"      text,
    created_dt  timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_dt  timestamp DEFAULT CURRENT_TIMESTAMP
);

-- 4. product 테이블
CREATE TABLE product (
    uid          bigserial PRIMARY KEY,
    provider_uid bigint NOT NULL,
    pid          varchar(60) NOT NULL,
    status       smallint NOT NULL,
    title        varchar(200) NOT NULL,
    brand        varchar(30),
    content      text,
    price        numeric(15, 2),
    location     varchar(200),
    category     varchar(100),
    color        varchar(30),
    odo          int,
    year         int,
    rmk          jsonb,
    "desc"       text,
    is_conversion boolean NOT NULL DEFAULT false,
    created_dt   timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_dt   timestamp DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (provider_uid) REFERENCES provider(uid) ON DELETE CASCADE ON UPDATE CASCADE
);

-- 5. category 테이블
CREATE TABLE category (
    uid          bigserial PRIMARY KEY,
    provider_uid bigint,
    title        varchar(200) NOT NULL,
    id           varchar(100) NOT NULL,
    depth        smallint,
    "order"      integer,
    created_dt   timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_dt   timestamp DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (provider_uid) REFERENCES provider(uid) ON DELETE SET NULL ON UPDATE CASCADE
);

-- 6. files 테이블
CREATE TABLE file (
    uid          bigserial PRIMARY KEY,
    provider_uid bigint,
    category_uid bigint,
    product_uid  bigint,
    url          text,
	path		 varchar(300),
    count        smallint DEFAULT 0,
    created_dt   timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_dt   timestamp DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (provider_uid) REFERENCES provider(uid) ON DELETE SET NULL ON UPDATE CASCADE,
    FOREIGN KEY (category_uid) REFERENCES category(uid) ON DELETE SET NULL ON UPDATE CASCADE,
    FOREIGN KEY (product_uid) REFERENCES product(uid) ON DELETE SET NULL ON UPDATE CASCADE
);

-- 7. failed_operations 테이블
CREATE TABLE IF NOT EXISTS failed_operations (
    id SERIAL PRIMARY KEY,
    operation_type VARCHAR(50) NOT NULL,          -- 'sync', 'update', 'delete', 'embedding'
    product_uid INTEGER NOT NULL,                 -- 관련 제품의 UID
    error_message TEXT NOT NULL,                  -- 에러 메시지
    error_details JSONB,                          -- 에러 상세 정보 (스택트레이스, 컨텍스트 등)
    retry_count INTEGER DEFAULT 0,                -- 현재 재시도 횟수
    max_retries INTEGER DEFAULT 3,                -- 최대 재시도 횟수
    next_retry_at TIMESTAMP WITH TIME ZONE,       -- 다음 재시도 예정 시간
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),     -- 최초 실패 시간
    last_attempted_at TIMESTAMP WITH TIME ZONE,            -- 마지막 시도 시간
    resolved_at TIMESTAMP WITH TIME ZONE                   -- 해결된 시간 (NULL이면 미해결)
);

-- COMMENT 구문 추가

-- provider
COMMENT ON COLUMN provider.uid IS '자동증가 기본키';
COMMENT ON COLUMN provider.code IS '프로바이더 코드 (unique)';
COMMENT ON COLUMN provider.url_main IS '메인 URL';
COMMENT ON COLUMN provider.url_api IS 'API 엔드포인트 URL';
COMMENT ON COLUMN provider.url_logo IS '로고 이미지 URL';
COMMENT ON COLUMN provider.name IS '프로바이더 한글명';
COMMENT ON COLUMN provider.name_english IS '프로바이더 영문명';
COMMENT ON COLUMN provider."desc" IS '설명';
COMMENT ON COLUMN provider.created_dt IS '생성일시';
COMMENT ON COLUMN provider.updated_dt IS '수정일시';

-- code
COMMENT ON COLUMN code.uid IS '자동증가 기본키';
COMMENT ON COLUMN code.code IS '코드값';
COMMENT ON COLUMN code.upper_uid IS '상위 코드 uid (계층 구조)';
COMMENT ON COLUMN code.depth IS '코드 깊이';
COMMENT ON COLUMN code."order" IS '정렬 순서';
COMMENT ON COLUMN code.name IS '코드 이름';
COMMENT ON COLUMN code.value IS '코드 값';
COMMENT ON COLUMN code."desc" IS '설명';
COMMENT ON COLUMN code.rmk IS '비고/추가정보(JSON)';
COMMENT ON COLUMN code.created_dt IS '생성일시';
COMMENT ON COLUMN code.updated_dt IS '수정일시';

-- log
COMMENT ON COLUMN log.uid IS '자동증가 기본키';
COMMENT ON COLUMN log.table_name IS '대상 테이블명';
COMMENT ON COLUMN log.table_uid IS '테이블 row 고유번호';
COMMENT ON COLUMN log.ip IS '요청 IP';
COMMENT ON COLUMN log.action IS '액션명(로그타입)';
COMMENT ON COLUMN log.status IS '상태값';
COMMENT ON COLUMN log."desc" IS '비고/상세내용';
COMMENT ON COLUMN log.created_dt IS '생성일시';
COMMENT ON COLUMN log.updated_dt IS '수정일시';

-- product
COMMENT ON COLUMN product.uid IS '자동증가 기본키';
COMMENT ON COLUMN product.provider_uid IS 'provider 테이블 FK';
COMMENT ON COLUMN product.pid IS '상품 고유 번호';
COMMENT ON COLUMN product.status IS '1: 판매중, 2: 예약중, 3: 판매완료, 9: 삭제';
COMMENT ON COLUMN product.title IS '제목';
COMMENT ON COLUMN product.brand IS '브랜드 명';
COMMENT ON COLUMN product.content IS '내용';
COMMENT ON COLUMN product.price IS '가격';
COMMENT ON COLUMN product.location IS '위치';
COMMENT ON COLUMN product.category IS '카테고리명';
COMMENT ON COLUMN product.color IS '색상';
COMMENT ON COLUMN product.odo IS '주행거리';
COMMENT ON COLUMN product.year IS '연식';
COMMENT ON COLUMN product.rmk IS '비고/추가정보(JSON)';
COMMENT ON COLUMN product."desc" IS '비고/상세내용';
COMMENT ON COLUMN product.is_conversion IS 'true: 변환 완료, false: 변환 전';
COMMENT ON COLUMN product.created_dt IS '생성일시';
COMMENT ON COLUMN product.updated_dt IS '수정일시';

-- files
COMMENT ON COLUMN file.uid IS '자동증가 기본키';
COMMENT ON COLUMN file.provider_uid IS 'provider 테이블 FK (nullable)';
COMMENT ON COLUMN file.category_uid IS 'category 테이블 FK (nullable)';
COMMENT ON COLUMN file.product_uid IS 'product 테이블 FK (nullable)';
COMMENT ON COLUMN file.url IS '파일의 전체 URL 또는 접근 주소';
COMMENT ON COLUMN file.path IS '서버 저장 상대경로(또는 절대경로)';
COMMENT ON COLUMN file.count IS '파일 수량(기본값 0)';
COMMENT ON COLUMN file.created_dt IS '생성일시';
COMMENT ON COLUMN file.updated_dt IS '수정일시';


-- category
COMMENT ON COLUMN category.uid IS '자동증가 기본키';
COMMENT ON COLUMN category.provider_uid IS 'provider 테이블 FK (nullable)';
COMMENT ON COLUMN category.title IS '카테고리명';
COMMENT ON COLUMN category.id IS '카테고리 ID(코드)';
COMMENT ON COLUMN category.depth IS '카테고리 깊이';
COMMENT ON COLUMN category."order" IS '정렬 순서';
COMMENT ON COLUMN category.created_dt IS '생성일시';
COMMENT ON COLUMN category.updated_dt IS '수정일시';

-- failed_operations
COMMENT ON TABLE failed_operations IS '실패한 작업들을 추적하고 재시도 메커니즘을 제공';

COMMENT ON COLUMN failed_operations.id IS '자동증가 기본키';
COMMENT ON COLUMN failed_operations.operation_type IS '작업 타입(sync, update, delete, embedding 등)';
COMMENT ON COLUMN failed_operations.product_uid IS 'product 테이블 FK';
COMMENT ON COLUMN failed_operations.error_message IS '에러 메시지';
COMMENT ON COLUMN failed_operations.error_details IS 'JSON 형태의 에러 상세 정보 (스택트레이스, 컨텍스트 등)';
COMMENT ON COLUMN failed_operations.retry_count IS '현재 재시도 횟수';
COMMENT ON COLUMN failed_operations.max_retries IS '최대 재시도 횟수';
COMMENT ON COLUMN failed_operations.next_retry_at IS '다음 재시도 예정 시간 (지수 백오프 적용)';
COMMENT ON COLUMN failed_operations.created_at IS '최초 실패 시간';
COMMENT ON COLUMN failed_operations.last_attempted_at IS '마지막 시도 시간';
COMMENT ON COLUMN failed_operations.resolved_at IS '성공적으로 재시도된 시간 (NULL이면 미해결)';


INSERT INTO provider
(code, url_main, url_api, url_logo, name, name_english, "desc", created_dt, updated_dt)
VALUES (
    'BUNJANG',
    'https://m.bunjang.co.kr/', -- 메인 URL
    'https://api.bunjang.co.kr/api/', -- API URL
    'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTM2IiBoZWlnaHQ9IjQwIiB2aWV3Qm94PSIwIDAgMTM2IDQwIiBmaWxsPSJub25lIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgPGcgaWQ9InouVEJEIC8gQlgtUmVmcmVzaCAvIGljX2xvZ290eXBlLXB3Ij4KICAgICAgICA8cmVjdCB3aWR0aD0iMTM2IiBoZWlnaHQ9IjQwIiBmaWxsPSJ3aGl0ZSIgLz4KICAgICAgICA8ZyBpZD0iTG9nb3R5cGUgLyBLUiAmIzIzNDsmIzE4MTsmIzE3MzsmIzIzNTsmIzE3MjsmIzE4NDsiPgogICAgICAgICAgICA8cGF0aCBpZD0iVmVjdG9yIgogICAgICAgICAgICAgICAgZD0iTTI1LjM5ODEgMTguNDQ0NUgxNi40MDMyTDE3Ljg2MiA1LjcyNThDMTcuODgxOSA1LjU1MDA1IDE3LjY3MTcgNS40NDU2NiAxNy41NDM2IDUuNTY3MjNMMS4wNTg4NCAyMS4zMjkxQzAuOTM0NjMgMjEuNDQ4MSAxLjAyMDUyIDIxLjY1NjggMS4xOTA5OSAyMS42NTU1TDkuOTYxMjYgMjEuNTYxN0w4LjcwNDU5IDM0LjU3NzdDOC42ODc0MSAzNC43NTM0IDguODk3NTEgMzQuODUzOSA5LjAyNDM3IDM0LjczMjNMMjUuNTI4OSAxOC43NzIyQzI1LjY1MTggMTguNjU0NiAyNS41NjcyIDE4LjQ0NzEgMjUuMzk4MSAxOC40NDcxVjE4LjQ0NDVaIgogICAgICAgICAgICAgICAgZmlsbD0iYmxhY2siIC8+CiAgICAgICAgICAgIDxwYXRoIGlkPSJWZWN0b3JfMiIKICAgICAgICAgICAgICAgIGQ9Ik00OC40MjA4IDI0Ljc4NzRWMTYuODExM0g0NS4xMzU3VjIxLjYxNzRDNDUuMTM1NyAyMS43MDQ2IDQ1LjA2NyAyMS43NzU5IDQ0Ljk3OTggMjEuNzgxMkwzMC43NjUzIDIyLjQ3MUMzMC42NzE1IDIyLjQ3NSAzMC41OTM1IDIyLjQwMSAzMC41OTM1IDIyLjMwNzFWNy4yMzEwNUMzMC41OTM1IDcuMTQxMTkgMzAuNjY2MiA3LjA2NzE5IDMwLjc1NzQgNy4wNjcxOUgzNS4zMDgzQzM1LjM5ODIgNy4wNjcxOSAzNS40NzIyIDcuMTM5ODcgMzUuNDcyMiA3LjIzMTA1VjEwLjg4ODdINDAuMjU3MVY3LjIzMTA1QzQwLjI1NzEgNy4xNDExOSA0MC4zMjk3IDcuMDY3MTkgNDAuNDIwOSA3LjA2NzE5SDQ0Ljk3MTlDNDUuMDYxNyA3LjA2NzE5IDQ1LjEzNTcgNy4xMzk4NyA0NS4xMzU3IDcuMjMxMDVWMTIuMjI3M0g0OC40MjA4VjYuOTU0ODdDNDguNDIwOCA2Ljg2NTAyIDQ4LjQ5MzUgNi43OTEwMiA0OC41ODQ3IDYuNzkxMDJINTMuMTM1NkM1My4yMjU1IDYuNzkxMDIgNTMuMjk5NSA2Ljg2MzY5IDUzLjI5OTUgNi45NTQ4N1YyNC43ODc0QzUzLjI5OTUgMjQuODc3MyA1My4yMjY4IDI0Ljk1MTMgNTMuMTM1NiAyNC45NTEzSDQ4LjU4NDdDNDguNDk0OCAyNC45NTEzIDQ4LjQyMDggMjQuODc4NiA0OC40MjA4IDI0Ljc4NzRaTTM1LjQ3MzUgMTcuNzk4NEw0MC4yNTg0IDE3LjY5NjdWMTQuNzcxMUgzNS40NzM1VjE3Ljc5ODRaIgogICAgICAgICAgICAgICAgZmlsbD0iYmxhY2siIC8+CiAgICAgICAgICAgIDxwYXRoIGlkPSJWZWN0b3JfMyIKICAgICAgICAgICAgICAgIGQ9Ik0zMi44NTQ4IDI0LjgwMDhIMzcuNDYyNkMzNy41NTI0IDI0LjgwMDggMzcuNjI2NCAyNC44NzM1IDM3LjYyNjQgMjQuOTY0NlYyNy45OTQ3TDUzLjEyOCAyNy4zMDQ5QzUzLjIyMDUgMjcuMzAwOSA1My4yOTg1IDI3LjM3NDkgNTMuMjk4NSAyNy40Njg3VjMxLjgzMzRDNTMuMjk4NSAzMS45MjE5IDUzLjIyODUgMzEuOTkzMyA1My4xNDEyIDMxLjk5NzJMMzIuODYxNCAzMi42ODU3QzMyLjc2ODkgMzIuNjg4MyAzMi42OTIzIDMyLjYxNDMgMzIuNjkyMyAzMi41MjE4VjI0Ljk2NDZDMzIuNjkyMyAyNC44NzQ4IDMyLjc2NDkgMjQuODAwOCAzMi44NTYxIDI0LjgwMDhIMzIuODU0OFoiCiAgICAgICAgICAgICAgICBmaWxsPSJibGFjayIgLz4KICAgICAgICAgICAgPHBhdGggaWQ9IlZlY3Rvcl80IgogICAgICAgICAgICAgICAgZD0iTTU1LjY3MzIgMjguNjg1OEw1NS45NTk5IDI4LjI3MjJDNTYuODA3IDI3LjA1NTIgNTcuNjE0MyAyNS43NDMgNTguMzYyMyAyNC4zNzI3QzU5LjExMjggMjIuOTk0NCA1OS43ODY4IDIxLjU3IDYwLjM2ODIgMjAuMTM3NUM2MC45NTc1IDE4LjY4NjYgNjEuNDcyOSAxNy4yMDEzIDYxLjg5NzEgMTUuNzIyN0M2Mi4yNDA2IDE0LjUzMjEgNjIuNTE5NSAxMy4zMzM1IDYyLjcyODIgMTIuMTUzNUw2Mi43Nzg1IDExLjg3Mkg1Ni43Mjc3QzU2LjYzNzggMTEuODcyIDU2LjU2MzggMTEuNzk5NCA1Ni41NjM4IDExLjcwODJWNy42MjIzNUM1Ni41NjM4IDcuNTMyNSA1Ni42MzY1IDcuNDU4NSA1Ni43Mjc3IDcuNDU4NUg2Ny44Nzc4QzY3Ljk3NDMgNy40NTg1IDY4LjA0OTYgNy41NDA0MiA2OC4wNDAzIDcuNjM1NTdMNjguMDEgNy45Nzc4MUM2Ny43OTA2IDEwLjQ3NzkgNjcuNDA4NyAxMi44NDMzIDY2Ljg4MDEgMTUuMDA2NUM2Ni4zNTQyIDE3LjE2NTcgNjUuNzE4NiAxOS4yMDU5IDY0Ljk5NzEgMjEuMDc1N0M2NC4yNzgzIDIyLjk0MDMgNjMuNDcwOSAyNC42OTkxIDYyLjU5NzQgMjYuMzA1OUM2MS43Mjc5IDI3LjkwMjIgNjAuODMwNyAyOS40MDg2IDU5LjkyNjggMzAuNzg2OUw1OS43NTc3IDMxLjA0NDVDNTkuNzA4OCAzMS4xMTg1IDU5LjYwOTcgMzEuMTQxIDU5LjUzNDQgMzEuMDkzNEw1NS42NzA1IDI4LjY4MThWMjguNjg0NUw1NS42NzMyIDI4LjY4NThaIgogICAgICAgICAgICAgICAgZmlsbD0iYmxhY2siIC8+CiAgICAgICAgICAgIDxwYXRoIGlkPSJWZWN0b3JfNSIKICAgICAgICAgICAgICAgIGQ9Ik03NS45NjgyIDMyLjYyODhWMjEuMDIxNEg3NC4xMDVWMzIuNjI4OEM3NC4xMDUgMzIuNzE4NiA3NC4wMzIzIDMyLjc5MjYgNzMuOTQxMSAzMi43OTI2SDY5LjU1NTNDNjkuNDY1NSAzMi43OTI2IDY5LjM5MTUgMzIuNzE5OSA2OS4zOTE1IDMyLjYyODhWNi45NTQ4N0M2OS4zOTE1IDYuODY1MDIgNjkuNDY0MiA2Ljc5MTAyIDY5LjU1NTMgNi43OTEwMkg3My45NDExQzc0LjAzMSA2Ljc5MTAyIDc0LjEwNSA2Ljg2MzY5IDc0LjEwNSA2Ljk1NDg3VjE2LjQzNzRINzUuOTY4MlY2Ljk1NDg3Qzc1Ljk2ODIgNi44NjUwMiA3Ni4wNDA5IDYuNzkxMDIgNzYuMTMyIDYuNzkxMDJIODAuNTcwN0M4MC42NjA2IDYuNzkxMDIgODAuNzM0NiA2Ljg2MzY5IDgwLjczNDYgNi45NTQ4N1YzMi42Mjg4QzgwLjczNDYgMzIuNzE4NiA4MC42NjE5IDMyLjc5MjYgODAuNTcwNyAzMi43OTI2SDc2LjEzMkM3Ni4wNDIyIDMyLjc5MjYgNzUuOTY4MiAzMi43MTk5IDc1Ljk2ODIgMzIuNjI4OFoiCiAgICAgICAgICAgICAgICBmaWxsPSJibGFjayIgLz4KICAgICAgICAgICAgPHBhdGggaWQ9IlZlY3Rvcl82IgogICAgICAgICAgICAgICAgZD0iTTgyLjkyNDMgMTguNTc3NkM4Ny4zMzY2IDE1LjAzNDkgODkuMTg5MiAxMS44ODMzIDg5LjIwNzcgMTEuODQ4OUw4OS40MzM2IDExLjQ4NDJIODQuMjk5OUM4NC4yMTAxIDExLjQ4NDIgODQuMTM2MSAxMS40MTE1IDg0LjEzNjEgMTEuMzIwNFY3LjI4NzM5Qzg0LjEzNjEgNy4xOTc1MyA4NC4yMDg4IDcuMTIzNTQgODQuMjk5OSA3LjEyMzU0SDk5Ljc0NDdDOTkuODM0NiA3LjEyMzU0IDk5LjkwODYgNy4xOTYyMSA5OS45MDg2IDcuMjg3MzlWMTEuMzIxN0M5OS45MDg2IDExLjQxMTUgOTkuODM1OSAxMS40ODU1IDk5Ljc0NDcgMTEuNDg1NUg5NC44OTkxTDk0LjgyOSAxMS42MDg0Qzk0LjU4NDUgMTIuMDQzMiA5NC4zMjQyIDEyLjQ4MzIgOTQuMDUzMyAxMi45MTRMOTMuOTI1MiAxMy4xMjAxTDk5LjkxNTIgMTYuNjU2M0M5OS45OTMxIDE2LjcwMjUgMTAwLjAyIDE2LjgwNDMgOTkuOTcyIDE2Ljg4MjJMOTcuNzE2MyAyMC41ODYyQzk3LjY2ODcgMjAuNjY1NSA5Ny41NjQ0IDIwLjY4OTIgOTcuNDg2NCAyMC42Mzc3TDkxLjQwMTIgMTYuNjFMOTEuMjYyNSAxNi43ODQ0Qzg5LjQ3OTkgMTkuMDMzNSA4Ni44NDUgMjEuMjY4IDg1Ljk1MTcgMjIuMDAyN0M4NS44ODE3IDIyLjA1OTYgODUuNzgxMiAyMi4wNDkgODUuNzIzMSAyMS45Nzg5TDgyLjkyMTcgMTguNTc3Nkg4Mi45MjQzWiIKICAgICAgICAgICAgICAgIGZpbGw9ImJsYWNrIiAvPgogICAgICAgICAgICA8cGF0aCBpZD0iVmVjdG9yXzciCiAgICAgICAgICAgICAgICBkPSJNMTAxLjc3NSAyMC45MTdWNi45NTQ4N0MxMDEuNzc1IDYuODY1MDIgMTAxLjg0NyA2Ljc5MTAyIDEwMS45MzkgNi43OTEwMkgxMDYuMjczQzEwNi4zNjMgNi43OTEwMiAxMDYuNDM3IDYuODYzNjkgMTA2LjQzNyA2Ljk1NDg3VjExLjQ3MDJIMTA5LjgyMUMxMDkuOTExIDExLjQ3MDIgMTA5Ljk4NSAxMS41NDI4IDEwOS45ODUgMTEuNjM0VjE1Ljg5MDNDMTA5Ljk4NSAxNS45ODAyIDEwOS45MTIgMTYuMDU0MiAxMDkuODIxIDE2LjA1NDJIMTA2LjQzN1YyMC45MTdDMTA2LjQzNyAyMS4wMDY5IDEwNi4zNjQgMjEuMDgwOSAxMDYuMjczIDIxLjA4MDlIMTAxLjkzOUMxMDEuODQ5IDIxLjA4MDkgMTAxLjc3NSAyMS4wMDgyIDEwMS43NzUgMjAuOTE3WiIKICAgICAgICAgICAgICAgIGZpbGw9ImJsYWNrIiAvPgogICAgICAgICAgICA8cGF0aCBpZD0iVmVjdG9yXzgiCiAgICAgICAgICAgICAgICBkPSJNMTMwLjEyMyAzMi42Mjg4VjIxLjAyMTRIMTI1LjkxMUMxMjUuODIyIDIxLjAyMTQgMTI1Ljc0OCAyMC45NDg3IDEyNS43NDggMjAuODU3NVYxNi42MDEyQzEyNS43NDggMTYuNTExNCAxMjUuODIgMTYuNDM3NCAxMjUuOTExIDE2LjQzNzRIMTMwLjEyM1Y2Ljk1NDg3QzEzMC4xMjMgNi44NjUwMiAxMzAuMTk2IDYuNzkxMDIgMTMwLjI4NyA2Ljc5MTAySDEzNC44MzhDMTM0LjkyOCA2Ljc5MTAyIDEzNS4wMDIgNi44NjM2OSAxMzUuMDAyIDYuOTU0ODdWMzIuNjI4OEMxMzUuMDAyIDMyLjcxODYgMTM0LjkyOSAzMi43OTI2IDEzNC44MzggMzIuNzkyNkgxMzAuMjg3QzEzMC4xOTcgMzIuNzkyNiAxMzAuMTIzIDMyLjcxOTkgMTMwLjEyMyAzMi42Mjg4WiIKICAgICAgICAgICAgICAgIGZpbGw9ImJsYWNrIiAvPgogICAgICAgICAgICA8cGF0aCBpZD0iVmVjdG9yXzkiCiAgICAgICAgICAgICAgICBkPSJNMTEyLjM0OCA3LjI2NDAySDEyNS42NzlDMTI1Ljc2OSA3LjI2NDAyIDEyNS44NDMgNy4zMzY2OSAxMjUuODQzIDcuNDI3ODdWMTEuNTE3N0MxMjUuODQzIDExLjYwNzUgMTI1Ljc3MSAxMS42ODE1IDEyNS42NzkgMTEuNjgxNUgxMTcuMDYyVjE2LjQzODZIMTIzLjcyNUMxMjMuODE1IDE2LjQzODYgMTIzLjg4OSAxNi41MTEzIDEyMy44ODkgMTYuNjAyNVYyMC44NTg4QzEyMy44ODkgMjAuOTQ4NyAxMjMuODE2IDIxLjAyMjcgMTIzLjcyNSAyMS4wMjI3SDExNy4wNjJWMjYuNjgzNkwxMjguMjQzIDI1LjY4OTlDMTI4LjMzOCAyNS42ODIgMTI4LjQyIDI1Ljc1NzMgMTI4LjQyIDI1Ljg1MjVWMjkuOTA1M0MxMjguNDIgMjkuOTg5OCAxMjguMzU1IDMwLjA1OTkgMTI4LjI3MiAzMC4wNjc4TDExMi4zNjIgMzEuNTc4MkMxMTIuMjY2IDMxLjU4NzQgMTEyLjE4NCAzMS41MTIxIDExMi4xODQgMzEuNDE1NlY3LjQyNjU1QzExMi4xODQgNy4zMzY3IDExMi4yNTYgNy4yNjI3IDExMi4zNDggNy4yNjI3VjcuMjY0MDJaIgogICAgICAgICAgICAgICAgZmlsbD0iYmxhY2siIC8+CiAgICAgICAgICAgIDxwYXRoIGlkPSJWZWN0b3JfMTAiCiAgICAgICAgICAgICAgICBkPSJNOTYuMjEzMSAzMy45NTkzQzg3LjAyNTIgMzMuOTU5MyA4NS42NTIzIDMwLjA3NTYgODUuNjUyMyAyNy43NTkyQzg1LjY1MjMgMjIuNjM0NyA5MS4zOTM5IDIxLjU1OTEgOTYuMjEzMSAyMS41NTkxQzEwMi45MjMgMjEuNTU5MSAxMDYuNzc0IDIzLjgyIDEwNi43NzQgMjcuNzU5MkMxMDYuNzc0IDMwLjA3MyAxMDUuNDAxIDMzLjk1OTMgOTYuMjEzMSAzMy45NTkzWk05Ni4yMTMxIDI1Ljg4NjdDOTMuNTI1MyAyNS44ODY3IDkwLjQ0MTEgMjYuMTAwOCA5MC40NDExIDI3Ljc2NzFDOTAuNDQxMSAyOS4wNDg5IDkyLjI3NTIgMjkuNjQ0OSA5Ni4yMTMxIDI5LjY0NDlDMTAwLjE1MSAyOS42NDQ5IDEwMS45ODUgMjkuMDQ2MyAxMDEuOTg1IDI3Ljc2NzFDMTAxLjk4NSAyNi40ODggMTAwLjA0MyAyNS44ODY3IDk2LjIxMzEgMjUuODg2N1oiCiAgICAgICAgICAgICAgICBmaWxsPSJibGFjayIgLz4KICAgICAgICA8L2c+CiAgICA8L2c+Cjwvc3ZnPg==', -- url_logo
    '번개장터', -- name (한글)
    'Bunjang',  -- name_english (영문)
    NULL, -- desc
    '2025-05-11 11:53:54.487',
    '2025-05-11 11:53:54.487'
);
