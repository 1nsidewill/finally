#!/usr/bin/env python3
# test_rate_limiting.py - Rate Limiting 및 Exponential Backoff 테스트

import time
from unittest.mock import patch, MagicMock
import openai
from src.services.embedding_service import RateLimiter, EmbeddingService, EmbeddingConfig

def test_rate_limiter():
    print("=== Rate Limiter 기능 테스트 ===")
    
    # 테스트용 낮은 제한으로 설정
    rate_limiter = RateLimiter(
        requests_per_minute=10,    # 분당 10개 요청
        tokens_per_minute=5000     # 분당 5000 토큰
    )
    
    print(f"Rate Limiter 설정:")
    print(f"  분당 요청 제한: 10")
    print(f"  분당 토큰 제한: 5000")
    
    # 1. 정상 요청 테스트
    print(f"\n1. 정상 요청 테스트")
    can_request = rate_limiter.can_make_request(100)
    print(f"   100 토큰 요청 가능: {can_request}")
    
    if can_request:
        rate_limiter.record_request(100)
        print(f"   100 토큰 요청 기록됨")
    
    # 2. 대량 토큰 요청 테스트
    print(f"\n2. 대량 토큰 요청 테스트")
    large_tokens = 6000  # 제한을 초과하는 토큰
    can_request_large = rate_limiter.can_make_request(large_tokens)
    print(f"   {large_tokens} 토큰 요청 가능: {can_request_large}")
    
    wait_time = rate_limiter.wait_time_needed(large_tokens)
    print(f"   필요한 대기 시간: {wait_time:.2f}초")

def test_exponential_backoff():
    print("\n\n=== Exponential Backoff 테스트 ===")
    
    config = EmbeddingConfig(
        base_delay=0.1,    # 테스트용 짧은 딜레이
        max_delay=2.0,     # 테스트용 짧은 최대 딜레이
        max_retries=5
    )
    
    print(f"Exponential Backoff 설정:")
    print(f"  기본 딜레이: {config.base_delay}초")
    print(f"  최대 딜레이: {config.max_delay}초")
    print(f"  최대 재시도: {config.max_retries}번")
    
    # Mock 서비스 생성
    with patch('src.services.embedding_service.OpenAI') as mock_openai:
        with patch('src.services.embedding_service.AsyncOpenAI') as mock_async_openai:
            mock_client = MagicMock()
            mock_openai.return_value = mock_client
            mock_async_openai.return_value = mock_client
            
            service = EmbeddingService(api_key="sk-test-dummy", config=config)
            
            print(f"\n시도별 백오프 딜레이:")
            for attempt in range(config.max_retries):
                delay = service._exponential_backoff(attempt)
                print(f"  시도 {attempt + 1}: {delay:.3f}초")

def test_error_handling():
    print("\n\n=== 에러 처리 테스트 ===")
    
    config = EmbeddingConfig(
        base_delay=0.1,
        max_delay=1.0,
        max_retries=3
    )
    
    with patch('src.services.embedding_service.OpenAI') as mock_openai:
        with patch('src.services.embedding_service.AsyncOpenAI') as mock_async_openai:
            mock_client = MagicMock()
            mock_openai.return_value = mock_client
            mock_async_openai.return_value = mock_client
            
            service = EmbeddingService(api_key="sk-test-dummy", config=config)
            
            # Rate Limit 에러 처리 테스트
            print("1. Rate Limit 에러 처리 테스트")
            mock_response = MagicMock()
            rate_limit_error = openai.RateLimitError(
                message="Rate limit exceeded",
                response=mock_response,
                body={}
            )
            
            should_retry = service._handle_api_error(rate_limit_error, 0)
            print(f"   Rate Limit 에러 재시도 여부: {should_retry}")

def main():
    print("🚀 Rate Limiting 및 Exponential Backoff 테스트 시작!")
    
    test_rate_limiter()
    test_exponential_backoff()
    test_error_handling()
    
    print("\n🎉 모든 Rate Limiting 테스트 완료!")

if __name__ == "__main__":
    main() 