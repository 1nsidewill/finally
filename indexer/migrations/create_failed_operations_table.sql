-- migrations/create_failed_operations_table.sql
-- 실패한 작업들을 추적하고 재시도 메커니즘을 지원하는 테이블

CREATE TABLE IF NOT EXISTS failed_operations (
    id SERIAL PRIMARY KEY,
    operation_type VARCHAR(50) NOT NULL, -- 'sync', 'update', 'delete', 'embedding'
    product_uid INTEGER NOT NULL, -- 관련 제품의 UID
    error_message TEXT NOT NULL, -- 에러 메시지
    error_details JSONB, -- 에러 상세 정보 (스택트레이스, 컨텍스트 등)
    retry_count INTEGER DEFAULT 0, -- 현재 재시도 횟수
    max_retries INTEGER DEFAULT 3, -- 최대 재시도 횟수
    next_retry_at TIMESTAMP WITH TIME ZONE, -- 다음 재시도 예정 시간
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), -- 최초 실패 시간
    last_attempted_at TIMESTAMP WITH TIME ZONE, -- 마지막 시도 시간
    resolved_at TIMESTAMP WITH TIME ZONE -- 해결된 시간 (NULL이면 미해결)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_failed_operations_retry_schedule 
ON failed_operations (next_retry_at, retry_count) 
WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_failed_operations_product_uid 
ON failed_operations (product_uid);

CREATE INDEX IF NOT EXISTS idx_failed_operations_operation_type 
ON failed_operations (operation_type);

CREATE INDEX IF NOT EXISTS idx_failed_operations_status 
ON failed_operations (resolved_at, retry_count, max_retries);

-- 통계를 위한 뷰 생성
CREATE OR REPLACE VIEW failed_operations_stats AS
SELECT 
    operation_type,
    COUNT(*) as total_failures,
    COUNT(CASE WHEN resolved_at IS NOT NULL THEN 1 END) as resolved_count,
    COUNT(CASE WHEN retry_count >= max_retries AND resolved_at IS NULL THEN 1 END) as permanent_failures,
    COUNT(CASE WHEN retry_count < max_retries AND resolved_at IS NULL THEN 1 END) as pending_retries,
    AVG(retry_count) as avg_retry_count,
    MIN(created_at) as first_failure,
    MAX(created_at) as last_failure
FROM failed_operations
GROUP BY operation_type;

-- 실패 작업 정리를 위한 함수
CREATE OR REPLACE FUNCTION cleanup_resolved_failures(days_threshold INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM failed_operations
    WHERE resolved_at IS NOT NULL 
      AND resolved_at < NOW() - INTERVAL '1 day' * days_threshold;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE failed_operations IS '실패한 작업들을 추적하고 재시도 메커니즘을 제공';
COMMENT ON COLUMN failed_operations.operation_type IS 'sync, update, delete, embedding 등 작업 타입';
COMMENT ON COLUMN failed_operations.error_details IS 'JSON 형태의 에러 상세 정보 (스택트레이스, 컨텍스트 등)';
COMMENT ON COLUMN failed_operations.next_retry_at IS '다음 재시도 예정 시간 (지수 백오프 적용)';
COMMENT ON COLUMN failed_operations.resolved_at IS '성공적으로 재시도된 시간 (NULL이면 미해결)'; 