# src/services/embedding_service.py
import os
import asyncio
import logging
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
import time
import openai
from openai import OpenAI, AsyncOpenAI
from openai.types import CreateEmbeddingResponse
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import threading

from .text_preprocessor import ProductTextPreprocessor
from ..config import get_settings

logger = logging.getLogger(__name__)

@dataclass
class EmbeddingConfig:
    """임베딩 서비스 설정"""
    model: str = "text-embedding-3-large"  # 3072 차원
    dimensions: int = 3072  # OpenAI text-embedding-3-large 기본 차원
    max_retries: int = 3
    base_delay: float = 1.0  # exponential backoff 기본 딜레이 (초)
    max_delay: float = 60.0  # 최대 딜레이
    batch_size: int = 100  # 배치당 최대 텍스트 수
    max_tokens_per_text: int = 8000  # 텍스트당 최대 토큰
    request_timeout: float = 30.0  # API 요청 타임아웃
    
    # Rate limiting 설정
    requests_per_minute: int = 5000  # OpenAI 기본 제한
    tokens_per_minute: int = 2000000  # OpenAI 기본 제한

class RateLimiter:
    """간단한 rate limiter 구현"""
    
    def __init__(self, requests_per_minute: int, tokens_per_minute: int):
        self.requests_per_minute = requests_per_minute
        self.tokens_per_minute = tokens_per_minute
        
        # 요청 횟수 추적
        self.request_times = []
        self.token_counts = []
        
        # 스레드 안전성을 위한 lock
        self._lock = threading.Lock()
    
    def can_make_request(self, estimated_tokens: int = 1000) -> bool:
        """요청 가능 여부 확인"""
        current_time = time.time()
        minute_ago = current_time - 60
        
        with self._lock:
            # 1분 이전 기록 제거
            self.request_times = [t for t in self.request_times if t > minute_ago]
            self.token_counts = [(t, c) for t, c in self.token_counts if t > minute_ago]
            
            # 현재 사용량 계산
            current_requests = len(self.request_times)
            current_tokens = sum(c for _, c in self.token_counts)
            
            # 제한 확인
            if current_requests >= self.requests_per_minute:
                return False
            if current_tokens + estimated_tokens > self.tokens_per_minute:
                return False
            
            return True
    
    def record_request(self, tokens_used: int):
        """요청 기록"""
        current_time = time.time()
        
        with self._lock:
            self.request_times.append(current_time)
            self.token_counts.append((current_time, tokens_used))
    
    def wait_time_needed(self, estimated_tokens: int = 1000) -> float:
        """요청 가능할 때까지 대기 시간 계산"""
        current_time = time.time()
        minute_ago = current_time - 60
        
        with self._lock:
            # 최근 요청들
            recent_requests = [t for t in self.request_times if t > minute_ago]
            recent_tokens = [(t, c) for t, c in self.token_counts if t > minute_ago]
            
            wait_times = []
            
            # 요청 수 제한으로 인한 대기
            if len(recent_requests) >= self.requests_per_minute:
                oldest_request = min(recent_requests)
                wait_times.append(oldest_request + 60 - current_time)
            
            # 토큰 제한으로 인한 대기
            current_tokens = sum(c for _, c in recent_tokens)
            if current_tokens + estimated_tokens > self.tokens_per_minute:
                # 토큰이 충분할 때까지 대기 시간 계산
                sorted_tokens = sorted(recent_tokens, key=lambda x: x[0])
                tokens_needed = current_tokens + estimated_tokens - self.tokens_per_minute
                
                for timestamp, token_count in sorted_tokens:
                    tokens_needed -= token_count
                    if tokens_needed <= 0:
                        wait_times.append(timestamp + 60 - current_time)
                        break
            
            return max(wait_times) if wait_times else 0.0

class EmbeddingService:
    """OpenAI 임베딩 서비스 클래스"""
    
    def __init__(self, 
                 api_key: Optional[str] = None,
                 config: Optional[EmbeddingConfig] = None):
        """
        Args:
            api_key: OpenAI API 키 (없으면 config.py에서)
            config: 임베딩 서비스 설정
        """
        self.config = config or EmbeddingConfig()
        
        # API 키 설정 - config.py에서 가져오기
        if api_key:
            self.api_key = api_key
        else:
            settings = get_settings()
            self.api_key = settings.OPENAI_API_KEY
            
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY가 필요합니다 (config.py 또는 매개변수)")
        
        # OpenAI 클라이언트 초기화
        self.client = OpenAI(
            api_key=self.api_key,
            max_retries=self.config.max_retries,
            timeout=self.config.request_timeout
        )
        
        self.async_client = AsyncOpenAI(
            api_key=self.api_key,
            max_retries=self.config.max_retries,
            timeout=self.config.request_timeout
        )
        
        # 텍스트 전처리기
        self.text_preprocessor = ProductTextPreprocessor()
        
        # Rate limiter
        self.rate_limiter = RateLimiter(
            self.config.requests_per_minute,
            self.config.tokens_per_minute
        )
        
        logger.info(f"임베딩 서비스 초기화: {self.config.model} ({self.config.dimensions}차원)")
    
    def estimate_tokens(self, text: str) -> int:
        """텍스트의 토큰 수 추정 (간단한 추정법)"""
        # 대략적인 추정: 영어는 4자당 1토큰, 한국어는 2자당 1토큰
        korean_chars = len([c for c in text if '가' <= c <= '힣'])
        other_chars = len(text) - korean_chars
        return korean_chars // 2 + other_chars // 4
    
    def _exponential_backoff(self, attempt: int) -> float:
        """exponential backoff 딜레이 계산"""
        delay = self.config.base_delay * (2 ** attempt)
        return min(delay, self.config.max_delay)
    
    def _handle_api_error(self, error: Exception, attempt: int) -> bool:
        """API 에러 처리 및 재시도 여부 결정"""
        if isinstance(error, openai.RateLimitError):
            delay = self._exponential_backoff(attempt)
            logger.warning(f"Rate limit 에러, {delay}초 대기 후 재시도 (시도 {attempt + 1})")
            time.sleep(delay)
            return True
        
        elif isinstance(error, openai.APIConnectionError):
            delay = self._exponential_backoff(attempt)
            logger.warning(f"연결 에러, {delay}초 대기 후 재시도 (시도 {attempt + 1})")
            time.sleep(delay)
            return True
        
        elif isinstance(error, openai.APIStatusError):
            if error.status_code >= 500:  # 서버 에러는 재시도
                delay = self._exponential_backoff(attempt)
                logger.warning(f"서버 에러 ({error.status_code}), {delay}초 대기 후 재시도 (시도 {attempt + 1})")
                time.sleep(delay)
                return True
            else:
                logger.error(f"클라이언트 에러 ({error.status_code}): {error}")
                return False
        
        logger.error(f"알 수 없는 에러: {error}")
        return False
    
    def create_embedding(self, text: str) -> Optional[np.ndarray]:
        """단일 텍스트의 임베딩 생성"""
        return self.create_embeddings([text])[0] if text.strip() else None
    
    def create_embeddings(self, texts: List[str]) -> List[Optional[np.ndarray]]:
        """여러 텍스트의 임베딩 배치 생성"""
        if not texts:
            return []
        
        # 빈 텍스트 필터링 및 인덱스 추적
        valid_texts = []
        valid_indices = []
        for i, text in enumerate(texts):
            if text and text.strip():
                valid_texts.append(text.strip())
                valid_indices.append(i)
        
        if not valid_texts:
            return [None] * len(texts)
        
        # 배치 크기로 분할
        results = [None] * len(texts)
        
        for i in range(0, len(valid_texts), self.config.batch_size):
            batch_texts = valid_texts[i:i + self.config.batch_size]
            batch_indices = valid_indices[i:i + self.config.batch_size]
            
            # 배치 임베딩 생성
            batch_embeddings = self._create_batch_embeddings(batch_texts)
            
            # 결과 배치에 할당
            for j, embedding in enumerate(batch_embeddings):
                original_index = batch_indices[j]
                results[original_index] = embedding
        
        return results
    
    def _create_batch_embeddings(self, texts: List[str]) -> List[Optional[np.ndarray]]:
        """배치 임베딩 생성 (내부 메서드)"""
        # 토큰 수 추정
        total_estimated_tokens = sum(self.estimate_tokens(text) for text in texts)
        
        # Rate limiting 체크
        wait_time = self.rate_limiter.wait_time_needed(total_estimated_tokens)
        if wait_time > 0:
            logger.info(f"Rate limit 대기: {wait_time:.1f}초")
            time.sleep(wait_time)
        
        for attempt in range(self.config.max_retries):
            try:
                logger.debug(f"임베딩 생성 요청: {len(texts)}개 텍스트 (시도 {attempt + 1})")
                
                response: CreateEmbeddingResponse = self.client.embeddings.create(
                    model=self.config.model,
                    input=texts,
                    dimensions=self.config.dimensions
                )
                
                # 토큰 사용량 기록
                tokens_used = response.usage.total_tokens
                self.rate_limiter.record_request(tokens_used)
                
                # 임베딩 추출
                embeddings = []
                for i, embedding_data in enumerate(response.data):
                    embeddings.append(np.array(embedding_data.embedding, dtype=np.float32))
                
                logger.debug(f"임베딩 생성 성공: {len(embeddings)}개, {tokens_used} 토큰 사용")
                return embeddings
                
            except Exception as e:
                if attempt < self.config.max_retries - 1:
                    if self._handle_api_error(e, attempt):
                        continue
                
                logger.error(f"임베딩 생성 실패: {e}")
                return [None] * len(texts)
        
        return [None] * len(texts)
    
    def embed_product_data(self, product_data: Dict[str, Any]) -> Optional[np.ndarray]:
        """매물 데이터를 전처리하고 임베딩 생성"""
        try:
            # 텍스트 전처리
            processed_text = self.text_preprocessor.preprocess_product_data(product_data)
            
            if not processed_text:
                logger.warning("전처리된 텍스트가 비어있음")
                return None
            
            # 임베딩 생성
            return self.create_embedding(processed_text)
            
        except Exception as e:
            logger.error(f"매물 데이터 임베딩 실패: {e}")
            return None
    
    def embed_product_batch(self, product_batch: List[Dict[str, Any]]) -> List[Optional[np.ndarray]]:
        """매물 데이터 배치 임베딩 생성"""
        try:
            # 배치 전처리
            processed_texts = []
            for product_data in product_batch:
                processed_text = self.text_preprocessor.preprocess_product_data(product_data)
                processed_texts.append(processed_text)
            
            # 배치 임베딩 생성
            return self.create_embeddings(processed_texts)
            
        except Exception as e:
            logger.error(f"배치 임베딩 실패: {e}")
            return [None] * len(product_batch)
    
    # 비동기 메서드들
    async def create_embeddings_async(self, texts: List[str]) -> List[Optional[np.ndarray]]:
        """비동기 배치 임베딩 생성"""
        if not texts:
            return []
        
        # 빈 텍스트 필터링
        valid_texts = []
        valid_indices = []
        for i, text in enumerate(texts):
            if text and text.strip():
                valid_texts.append(text.strip())
                valid_indices.append(i)
        
        if not valid_texts:
            return [None] * len(texts)
        
        results = [None] * len(texts)
        
        # 배치 처리
        for i in range(0, len(valid_texts), self.config.batch_size):
            batch_texts = valid_texts[i:i + self.config.batch_size]
            batch_indices = valid_indices[i:i + self.config.batch_size]
            
            batch_embeddings = await self._create_batch_embeddings_async(batch_texts)
            
            for j, embedding in enumerate(batch_embeddings):
                original_index = batch_indices[j]
                results[original_index] = embedding
        
        return results
    
    async def _create_batch_embeddings_async(self, texts: List[str]) -> List[Optional[np.ndarray]]:
        """비동기 배치 임베딩 생성 (내부 메서드)"""
        total_estimated_tokens = sum(self.estimate_tokens(text) for text in texts)
        
        # Rate limiting 체크
        wait_time = self.rate_limiter.wait_time_needed(total_estimated_tokens)
        if wait_time > 0:
            logger.info(f"Rate limit 대기: {wait_time:.1f}초")
            await asyncio.sleep(wait_time)
        
        for attempt in range(self.config.max_retries):
            try:
                logger.debug(f"비동기 임베딩 생성 요청: {len(texts)}개 텍스트 (시도 {attempt + 1})")
                
                response: CreateEmbeddingResponse = await self.async_client.embeddings.create(
                    model=self.config.model,
                    input=texts,
                    dimensions=self.config.dimensions
                )
                
                tokens_used = response.usage.total_tokens
                self.rate_limiter.record_request(tokens_used)
                
                embeddings = []
                for embedding_data in response.data:
                    embeddings.append(np.array(embedding_data.embedding, dtype=np.float32))
                
                logger.debug(f"비동기 임베딩 생성 성공: {len(embeddings)}개, {tokens_used} 토큰 사용")
                return embeddings
                
            except Exception as e:
                if attempt < self.config.max_retries - 1:
                    # 비동기 버전의 에러 처리
                    if isinstance(e, (openai.RateLimitError, openai.APIConnectionError)):
                        delay = self._exponential_backoff(attempt)
                        logger.warning(f"에러, {delay}초 대기 후 재시도 (시도 {attempt + 1})")
                        await asyncio.sleep(delay)
                        continue
                
                logger.error(f"비동기 임베딩 생성 실패: {e}")
                return [None] * len(texts)
        
        return [None] * len(texts)
    
    def get_config(self) -> EmbeddingConfig:
        """현재 설정 반환"""
        return self.config
    
    def get_stats(self) -> Dict[str, Any]:
        """서비스 통계 반환"""
        return {
            "model": self.config.model,
            "dimensions": self.config.dimensions,
            "batch_size": self.config.batch_size,
            "rate_limiter": {
                "requests_per_minute": self.config.requests_per_minute,
                "tokens_per_minute": self.config.tokens_per_minute,
            }
        }

# 전역 인스턴스 (싱글톤 패턴)
_embedding_service: Optional[EmbeddingService] = None

def get_embedding_service(api_key: Optional[str] = None, 
                         config: Optional[EmbeddingConfig] = None) -> EmbeddingService:
    """임베딩 서비스 싱글톤 인스턴스 반환"""
    global _embedding_service
    
    if _embedding_service is None:
        _embedding_service = EmbeddingService(api_key=api_key, config=config)
    
    return _embedding_service

# 편의 함수들
def embed_text(text: str) -> Optional[np.ndarray]:
    """텍스트 임베딩 편의 함수"""
    service = get_embedding_service()
    return service.create_embedding(text)

def embed_texts(texts: List[str]) -> List[Optional[np.ndarray]]:
    """텍스트 배치 임베딩 편의 함수"""
    service = get_embedding_service()
    return service.create_embeddings(texts)

def embed_product(product_data: Dict[str, Any]) -> Optional[np.ndarray]:
    """매물 데이터 임베딩 편의 함수"""
    service = get_embedding_service()
    return service.embed_product_data(product_data)