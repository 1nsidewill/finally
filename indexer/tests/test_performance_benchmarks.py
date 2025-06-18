#!/usr/bin/env python3
"""
단위 성능 벤치마크 테스트
각 컴포넌트의 성능을 정확히 측정하고 비교합니다.
"""

import pytest
import asyncio
import time
import json
import numpy as np
from typing import List, Dict, Any
import logging

# 테스트할 컴포넌트들
from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager
from src.database.redis import RedisManager
from src.services.embedding_service import EmbeddingService
from src.services.text_preprocessor import ProductTextPreprocessor

logging.basicConfig(level=logging.WARNING)  # 벤치마크 중 로그 최소화

class TestPerformanceBenchmarks:
    """각 컴포넌트의 단위 성능 벤치마크"""
    
    @pytest.fixture(scope="class")
    def postgres_manager(self):
        """PostgreSQL 매니저 픽스처"""
        return PostgreSQLManager()
    
    @pytest.fixture(scope="class")
    def qdrant_manager(self):
        """Qdrant 매니저 픽스처"""
        return QdrantManager()
    
    @pytest.fixture(scope="class")
    def redis_manager(self):
        """Redis 매니저 픽스처"""
        return RedisManager()
    
    @pytest.fixture(scope="class")
    def embedding_service(self):
        """임베딩 서비스 픽스처"""
        return EmbeddingService()
    
    @pytest.fixture(scope="class")
    def text_preprocessor(self):
        """텍스트 전처리기 픽스처"""
        return ProductTextPreprocessor()
    
    @pytest.fixture(scope="class")
    def sample_products(self):
        """테스트용 샘플 제품 데이터"""
        return [
            {
                "uid": i,
                "title": f"야마하 FZ-25 {2020 + (i % 5)}년식",
                "content": f"주행거리 {5000 + (i * 100)}km, 엔진 오일 교체됨, 타이어 새것",
                "price": f"{250 + (i % 50)}",
                "year": str(2020 + (i % 5)),
                "mileage": str(5000 + (i * 100))
            }
            for i in range(100)  # 100개 샘플
        ]
    
    # =============================================================================
    # Redis 성능 벤치마크
    # =============================================================================
    
    def test_redis_push_performance(self, benchmark, redis_manager):
        """Redis 작업 푸시 성능 측정"""
        test_job = {"id": 1, "type": "embedding", "data": "test data"}
        
        def push_job():
            return asyncio.run(redis_manager.push_job(test_job))
        
        result = benchmark(push_job)
        assert result is True
    
    def test_redis_pop_performance(self, benchmark, redis_manager):
        """Redis 작업 팝 성능 측정"""
        # 먼저 작업을 푸시
        test_job = {"id": 1, "type": "embedding", "data": "test data"}
        asyncio.run(redis_manager.push_job(test_job))
        
        def pop_job():
            return asyncio.run(redis_manager.pop_job())
        
        result = benchmark(pop_job)
        assert result is not None
    
    def test_redis_batch_push_performance(self, benchmark, redis_manager, sample_products):
        """Redis 배치 푸시 성능 측정"""
        jobs = [{"id": i, "type": "embedding", "product": product} 
                for i, product in enumerate(sample_products[:50])]
        
        def batch_push():
            return asyncio.run(redis_manager.push_jobs_batch(jobs))
        
        result = benchmark(batch_push)
        assert result is True
    
    # =============================================================================
    # 텍스트 전처리 성능 벤치마크
    # =============================================================================
    
    def test_text_preprocessing_performance(self, benchmark, text_preprocessor, sample_products):
        """텍스트 전처리 성능 측정"""
        product = sample_products[0]
        
        def preprocess_text():
            return text_preprocessor.preprocess_product_data(product)
        
        result = benchmark(preprocess_text)
        assert result is not None
        assert len(result) > 0
    
    def test_batch_text_preprocessing_performance(self, benchmark, text_preprocessor, sample_products):
        """배치 텍스트 전처리 성능 측정"""
        def batch_preprocess():
            results = []
            for product in sample_products[:20]:  # 20개 제품
                result = text_preprocessor.preprocess_product_data(product)
                results.append(result)
            return results
        
        results = benchmark(batch_preprocess)
        assert len(results) == 20
        assert all(result is not None for result in results)
    
    # =============================================================================
    # 임베딩 서비스 성능 벤치마크 (실제 API 호출)
    # =============================================================================
    
    @pytest.mark.slow
    def test_single_embedding_performance(self, benchmark, embedding_service):
        """단일 임베딩 생성 성능 측정"""
        test_text = "야마하 FZ-25 2023년식 주행거리 5000km 엔진 상태 양호"
        
        def create_embedding():
            return embedding_service.create_embedding(test_text)
        
        result = benchmark(create_embedding)
        assert result is not None
        assert len(result) == 3072  # OpenAI text-embedding-3-large 차원
    
    @pytest.mark.slow
    def test_batch_embedding_performance(self, benchmark, embedding_service, text_preprocessor, sample_products):
        """배치 임베딩 생성 성능 측정"""
        # 전처리된 텍스트 준비
        texts = [
            text_preprocessor.preprocess_product_data(product)
            for product in sample_products[:5]  # API 호출 비용 절약을 위해 5개만
        ]
        
        def create_batch_embeddings():
            return embedding_service.create_embeddings(texts)
        
        results = benchmark(create_batch_embeddings)
        assert len(results) == 5
        assert all(result is not None for result in results)
        assert all(len(result) == 3072 for result in results if result is not None)
    
    # =============================================================================
    # PostgreSQL 성능 벤치마크
    # =============================================================================
    
    def test_postgres_health_check_performance(self, benchmark, postgres_manager):
        """PostgreSQL 헬스체크 성능 측정"""
        def health_check():
            return asyncio.run(postgres_manager.health_check())
        
        result = benchmark(health_check)
        assert result is True
    
    def test_postgres_query_performance(self, benchmark, postgres_manager):
        """PostgreSQL 쿼리 성능 측정"""
        def query_products():
            return asyncio.run(postgres_manager.get_products_for_sync(batch_size=10))
        
        result = benchmark(query_products)
        assert isinstance(result, list)
    
    # =============================================================================
    # Qdrant 성능 벤치마크
    # =============================================================================
    
    def test_qdrant_health_check_performance(self, benchmark, qdrant_manager):
        """Qdrant 헬스체크 성능 측정"""
        def health_check():
            return asyncio.run(qdrant_manager.health_check())
        
        result = benchmark(health_check)
        assert result is True
    
    def test_qdrant_collection_info_performance(self, benchmark, qdrant_manager):
        """Qdrant 컬렉션 정보 조회 성능 측정"""
        def get_collection_info():
            return asyncio.run(qdrant_manager.get_collection_info())
        
        result = benchmark(get_collection_info)
        assert isinstance(result, dict)
        assert "status" in result
    
    # =============================================================================
    # 통합 워크플로우 성능 벤치마크
    # =============================================================================
    
    def test_complete_workflow_performance(self, benchmark, sample_products, text_preprocessor, redis_manager):
        """전체 워크플로우 성능 측정 (API 호출 제외)"""
        product = sample_products[0]
        
        def complete_workflow():
            # 1. 텍스트 전처리
            processed_text = text_preprocessor.preprocess_product_data(product)
            
            # 2. Redis 작업 생성
            job = {
                "id": product["uid"],
                "type": "embedding",
                "text": processed_text,
                "product": product
            }
            
            # 3. Redis에 푸시
            result = asyncio.run(redis_manager.push_job(job))
            
            return result
        
        result = benchmark(complete_workflow)
        assert result is True

# =============================================================================
# 메모리 사용량 테스트
# =============================================================================

@pytest.mark.memory
class TestMemoryUsage:
    """메모리 사용량 측정 테스트"""
    
    def test_redis_manager_memory_usage(self):
        """Redis 매니저 메모리 사용량 측정"""
        import tracemalloc
        
        tracemalloc.start()
        
        # Redis 매니저 초기화 및 작업
        redis_manager = RedisManager()
        jobs = [{"id": i, "data": f"test_data_{i}"} for i in range(1000)]
        
        # 배치 작업
        asyncio.run(redis_manager.push_jobs_batch(jobs))
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        print(f"Redis Manager - Current memory: {current / 1024 / 1024:.2f} MB")
        print(f"Redis Manager - Peak memory: {peak / 1024 / 1024:.2f} MB")
        
        # 메모리 사용량이 합리적인 범위에 있는지 확인 (50MB 이하)
        assert peak / 1024 / 1024 < 50
    
    def test_text_preprocessor_memory_usage(self, sample_products):
        """텍스트 전처리기 메모리 사용량 측정"""
        import tracemalloc
        
        tracemalloc.start()
        
        preprocessor = ProductTextPreprocessor()
        
        # 대량 텍스트 처리
        for _ in range(10):  # 1000개 제품 처리
            for product in sample_products:
                preprocessor.preprocess_product_data(product)
        
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        print(f"Text Preprocessor - Current memory: {current / 1024 / 1024:.2f} MB")
        print(f"Text Preprocessor - Peak memory: {peak / 1024 / 1024:.2f} MB")
        
        # 메모리 사용량이 합리적인 범위에 있는지 확인 (20MB 이하)
        assert peak / 1024 / 1024 < 20 