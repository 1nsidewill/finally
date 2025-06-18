#!/usr/bin/env python3
"""
메트릭 시스템 통합 테스트
모든 컴포넌트에서 메트릭이 제대로 수집되는지 확인합니다.
"""

import asyncio
import time
import requests
from typing import Dict, Any
import logging

# FastAPI 앱 import (서버가 실행 중이어야 함)
from src.monitoring.metrics import (
    get_metrics, 
    MetricsCollector,
    REDIS_JOBS_TOTAL,
    EMBEDDINGS_GENERATED_TOTAL,
    DB_QUERIES_TOTAL
)

# 테스트할 서비스들
from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager
from src.database.redis import RedisManager
from src.services.embedding_service import EmbeddingService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetricsIntegrationTest:
    """메트릭 통합 테스트 클래스"""
    
    def __init__(self):
        self.test_results = {
            "postgres_metrics": False,
            "qdrant_metrics": False,
            "redis_metrics": False,
            "embedding_metrics": False,
            "api_metrics": False,
            "prometheus_format": False
        }
    
    async def test_postgresql_metrics(self):
        """PostgreSQL 메트릭 테스트"""
        try:
            print("\n🔍 PostgreSQL 메트릭 테스트...")
            
            pg_manager = PostgreSQLManager()
            
            # 간단한 쿼리 실행 (메트릭 데코레이터가 적용됨)
            await pg_manager.health_check()
            
            # 메트릭 확인
            metrics_output = get_metrics()
            if 'db_queries_total{database="postgresql"' in metrics_output:
                print("✅ PostgreSQL 메트릭 수집 성공!")
                self.test_results["postgres_metrics"] = True
            else:
                print("❌ PostgreSQL 메트릭을 찾을 수 없습니다")
                
        except Exception as e:
            print(f"❌ PostgreSQL 메트릭 테스트 실패: {e}")
    
    async def test_redis_metrics(self):
        """Redis 메트릭 테스트"""
        try:
            print("\n🔍 Redis 메트릭 테스트...")
            
            redis_manager = RedisManager()
            
            # Redis 연결 테스트
            await redis_manager.ping()
            
            # 테스트 작업 추가
            test_job = {"test": "metrics", "timestamp": time.time()}
            await redis_manager.push_job(test_job, "test_metrics_queue")
            
            # 큐 크기 메트릭 업데이트
            queue_size = await redis_manager.get_queue_size("test_metrics_queue")
            await MetricsCollector.update_queue_size("test_metrics_queue", queue_size)
            
            # 메트릭 확인
            metrics_output = get_metrics()
            if 'redis_queue_size{queue_name="test_metrics_queue"}' in metrics_output:
                print("✅ Redis 메트릭 수집 성공!")
                self.test_results["redis_metrics"] = True
            else:
                print("❌ Redis 메트릭을 찾을 수 없습니다")
                
        except Exception as e:
            print(f"❌ Redis 메트릭 테스트 실패: {e}")
    
    async def test_embedding_metrics(self):
        """임베딩 서비스 메트릭 테스트"""
        try:
            print("\n🔍 임베딩 서비스 메트릭 테스트...")
            
            embedding_service = EmbeddingService()
            
            # 테스트 임베딩 생성 (메트릭 데코레이터가 적용됨)
            test_text = "테스트용 텍스트입니다"
            result = embedding_service.create_embedding(test_text)
            
            if result is not None:
                # 메트릭 확인
                metrics_output = get_metrics()
                if 'embeddings_generated_total{model="text-embedding-3-large"' in metrics_output:
                    print("✅ 임베딩 서비스 메트릭 수집 성공!")
                    self.test_results["embedding_metrics"] = True
                else:
                    print("❌ 임베딩 메트릭을 찾을 수 없습니다")
            else:
                print("❌ 임베딩 생성 실패")
                
        except Exception as e:
            print(f"❌ 임베딩 메트릭 테스트 실패: {e}")
    
    async def test_qdrant_metrics(self):
        """Qdrant 메트릭 테스트"""
        try:
            print("\n🔍 Qdrant 메트릭 테스트...")
            
            qdrant_manager = QdrantManager()
            
            # 컬렉션 생성 테스트
            await qdrant_manager.create_collection_if_not_exists()
            
            # 컬렉션 목록 조회 (메트릭이 있는지 확인)
            await qdrant_manager.list_collections()
            
            # 메트릭 확인
            metrics_output = get_metrics()
            if 'db_queries_total{database="qdrant"' in metrics_output:
                print("✅ Qdrant 메트릭 수집 성공!")
                self.test_results["qdrant_metrics"] = True
            else:
                print("❌ Qdrant 메트릭을 찾을 수 없습니다")
                
        except Exception as e:
            print(f"❌ Qdrant 메트릭 테스트 실패: {e}")
    
    def test_prometheus_format(self):
        """Prometheus 형식 테스트"""
        try:
            print("\n🔍 Prometheus 형식 테스트...")
            
            metrics_output = get_metrics()
            
            # 기본적인 Prometheus 형식 확인
            required_patterns = [
                "# HELP",
                "# TYPE",
                "_total",
                "_seconds",
                "_count"
            ]
            
            format_ok = True
            for pattern in required_patterns:
                if pattern not in metrics_output:
                    print(f"❌ Prometheus 패턴 누락: {pattern}")
                    format_ok = False
            
            if format_ok:
                print("✅ Prometheus 형식 검증 성공!")
                self.test_results["prometheus_format"] = True
            else:
                print("❌ Prometheus 형식 검증 실패")
                
        except Exception as e:
            print(f"❌ Prometheus 형식 테스트 실패: {e}")
    
    async def test_api_endpoints(self):
        """API 엔드포인트 메트릭 테스트"""
        try:
            print("\n🔍 API 엔드포인트 테스트...")
            
            # FastAPI 서버가 실행 중인지 확인
            test_urls = [
                "http://localhost:8000/health",
                "http://localhost:8000/metrics",
                "http://localhost:8000/metrics/status"
            ]
            
            endpoints_working = 0
            for url in test_urls:
                try:
                    response = requests.get(url, timeout=5)
                    if response.status_code == 200:
                        endpoints_working += 1
                        print(f"✅ {url} 응답 성공")
                    else:
                        print(f"❌ {url} 응답 실패: {response.status_code}")
                except requests.exceptions.RequestException as e:
                    print(f"❌ {url} 연결 실패: {e}")
            
            if endpoints_working == len(test_urls):
                print("✅ 모든 API 엔드포인트 정상 작동!")
                self.test_results["api_metrics"] = True
            else:
                print(f"❌ API 엔드포인트 일부 실패: {endpoints_working}/{len(test_urls)}")
                
        except Exception as e:
            print(f"❌ API 엔드포인트 테스트 실패: {e}")
    
    async def run_all_tests(self):
        """모든 테스트 실행"""
        print("🚀 메트릭 시스템 통합 테스트 시작")
        print("=" * 60)
        
        # 각 컴포넌트 테스트
        await self.test_postgresql_metrics()
        await self.test_redis_metrics()
        await self.test_embedding_metrics()
        await self.test_qdrant_metrics()
        await self.test_api_endpoints()
        self.test_prometheus_format()
        
        # 결과 요약
        print("\n" + "=" * 60)
        print("📊 테스트 결과 요약")
        print("=" * 60)
        
        passed = 0
        total = len(self.test_results)
        
        for test_name, result in self.test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{test_name:20} : {status}")
            if result:
                passed += 1
        
        print(f"\n총 테스트: {total}, 통과: {passed}, 실패: {total - passed}")
        
        if passed == total:
            print("🎉 모든 메트릭 테스트 통과!")
        else:
            print("⚠️  일부 메트릭 테스트 실패")
        
        print("\n📈 현재 수집된 메트릭 샘플:")
        print("-" * 40)
        metrics_sample = get_metrics()[:500]  # 처음 500자만 출력
        print(metrics_sample)
        if len(get_metrics()) > 500:
            print("... (더 많은 메트릭이 수집됨)")

async def main():
    """메인 테스트 실행"""
    test_runner = MetricsIntegrationTest()
    await test_runner.run_all_tests()

if __name__ == "__main__":
    asyncio.run(main()) 