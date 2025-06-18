#!/usr/bin/env python3
"""
Redis 연결 풀 최적화 벤치마크 테스트
다양한 연결 풀 설정으로 성능을 측정하여 최적의 설정을 찾습니다.
"""

import asyncio
import time
import json
import statistics
from datetime import datetime
from typing import List, Dict, Any, Tuple
import redis.asyncio as redis
from src.config import get_settings
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisPoolBenchmark:
    """Redis 연결 풀 성능 벤치마크"""
    
    def __init__(self):
        self.config = get_settings()
        self.test_queue = "test_pool_optimization"
        self.results: List[Dict[str, Any]] = []
        
    async def create_pool_config(self, max_connections: int, socket_timeout: float, 
                                connect_timeout: float, retry_on_timeout: bool = True) -> redis.ConnectionPool:
        """특정 설정으로 연결 풀 생성"""
        if self.config.REDIS_URL:
            pool = redis.ConnectionPool.from_url(
                self.config.REDIS_URL,
                max_connections=max_connections,
                socket_connect_timeout=connect_timeout,
                socket_timeout=socket_timeout,
                retry_on_timeout=retry_on_timeout,
                encoding='utf-8',
                decode_responses=True
            )
        else:
            pool = redis.ConnectionPool(
                host=self.config.REDIS_HOST,
                port=self.config.REDIS_PORT,
                db=self.config.REDIS_DB,
                password=self.config.REDIS_PASSWORD,
                max_connections=max_connections,
                socket_connect_timeout=connect_timeout,
                socket_timeout=socket_timeout,
                retry_on_timeout=retry_on_timeout,
                encoding='utf-8',
                decode_responses=True
            )
        
        return pool
    
    async def benchmark_pool_config(self, max_connections: int, socket_timeout: float,
                                  connect_timeout: float, test_jobs: int = 1000,
                                  batch_size: int = 30) -> Dict[str, Any]:
        """특정 연결 풀 설정으로 성능 벤치마크 실행"""
        
        config_name = f"max_conn_{max_connections}_sock_timeout_{socket_timeout}_conn_timeout_{connect_timeout}"
        logger.info(f"🧪 테스트 시작: {config_name}")
        
        pool = await self.create_pool_config(max_connections, socket_timeout, connect_timeout)
        redis_client = redis.Redis(connection_pool=pool)
        
        # 테스트 데이터 생성
        test_data = [
            {
                "id": i,
                "action": "sync",
                "product_uid": f"test-product-{i}",
                "data": {"index": i, "timestamp": time.time()}
            }
            for i in range(test_jobs)
        ]
        
        try:
            # 큐 초기화
            await redis_client.delete(self.test_queue)
            
            # === Push 성능 테스트 ===
            push_times = []
            push_start = time.time()
            
            # 배치로 작업 푸시
            for i in range(0, test_jobs, batch_size):
                batch = test_data[i:i + batch_size]
                batch_json = [json.dumps(job, ensure_ascii=False) for job in batch]
                
                batch_start = time.time()
                await redis_client.lpush(self.test_queue, *batch_json)
                batch_end = time.time()
                
                push_times.append(batch_end - batch_start)
            
            push_total_time = time.time() - push_start
            
            # === Pop 성능 테스트 ===
            pop_times = []
            pop_start = time.time()
            popped_jobs = 0
            
            while popped_jobs < test_jobs:
                # 배치로 작업 팝
                pipe = redis_client.pipeline()
                current_batch_size = min(batch_size, test_jobs - popped_jobs)
                
                for _ in range(current_batch_size):
                    pipe.rpop(self.test_queue)
                
                batch_start = time.time()
                results = await pipe.execute()
                batch_end = time.time()
                
                # 결과 처리
                valid_results = [r for r in results if r is not None]
                popped_jobs += len(valid_results)
                
                if valid_results:
                    pop_times.append(batch_end - batch_start)
                
                if not valid_results:  # 큐가 비었음
                    break
            
            pop_total_time = time.time() - pop_start
            
            # === 연결 풀 통계 ===
            pool_stats = {
                "created_connections": pool.connection_kwargs.get("max_connections", 0),
                "pool_size": max_connections
            }
            
            # === 결과 계산 ===
            result = {
                "config": {
                    "max_connections": max_connections,
                    "socket_timeout": socket_timeout,
                    "connect_timeout": connect_timeout,
                    "batch_size": batch_size
                },
                "test_params": {
                    "total_jobs": test_jobs,
                    "batches_count": len(push_times)
                },
                "push_performance": {
                    "total_time": push_total_time,
                    "jobs_per_second": test_jobs / push_total_time,
                    "avg_batch_time": statistics.mean(push_times) if push_times else 0,
                    "min_batch_time": min(push_times) if push_times else 0,
                    "max_batch_time": max(push_times) if push_times else 0
                },
                "pop_performance": {
                    "total_time": pop_total_time,
                    "jobs_per_second": popped_jobs / pop_total_time if pop_total_time > 0 else 0,
                    "avg_batch_time": statistics.mean(pop_times) if pop_times else 0,
                    "min_batch_time": min(pop_times) if pop_times else 0,
                    "max_batch_time": max(pop_times) if pop_times else 0,
                    "jobs_processed": popped_jobs
                },
                "overall_performance": {
                    "total_time": push_total_time + pop_total_time,
                    "overall_jobs_per_second": test_jobs / (push_total_time + pop_total_time),
                    "push_pop_ratio": push_total_time / pop_total_time if pop_total_time > 0 else 0
                },
                "pool_stats": pool_stats,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"✅ {config_name} 완료 - {result['overall_performance']['overall_jobs_per_second']:.1f} jobs/sec")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ {config_name} 실패: {e}")
            return {
                "config": {"max_connections": max_connections, "socket_timeout": socket_timeout, "connect_timeout": connect_timeout},
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            
        finally:
            # 정리
            try:
                await redis_client.delete(self.test_queue)
                await pool.disconnect()
            except:
                pass
    
    async def run_comprehensive_benchmark(self) -> Dict[str, Any]:
        """포괄적인 Redis 연결 풀 벤치마크 실행"""
        
        logger.info("🚀 Redis 연결 풀 최적화 벤치마크 시작")
        
        # 테스트할 설정들
        test_configurations = [
            # (max_connections, socket_timeout, connect_timeout)
            (10, 5.0, 5.0),    # 작은 풀
            (20, 5.0, 5.0),    # 현재 설정
            (30, 5.0, 5.0),    # 중간 풀
            (50, 5.0, 5.0),    # 권장 설정
            (75, 5.0, 5.0),    # 큰 풀
            (100, 5.0, 5.0),   # 최대 풀
            
            # 타임아웃 최적화 테스트 (50 연결로 고정)
            (50, 2.0, 2.0),    # 짧은 타임아웃
            (50, 3.0, 3.0),    # 중간 타임아웃 
            (50, 10.0, 10.0),  # 긴 타임아웃
        ]
        
        results = []
        
        for max_conn, sock_timeout, conn_timeout in test_configurations:
            try:
                result = await self.benchmark_pool_config(
                    max_connections=max_conn,
                    socket_timeout=sock_timeout,
                    connect_timeout=conn_timeout,
                    test_jobs=500,  # 빠른 테스트를 위해 줄임
                    batch_size=30   # 현재 배치 사이즈 사용
                )
                results.append(result)
                
                # 테스트 간 잠시 대기
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"설정 {max_conn}/{sock_timeout}/{conn_timeout} 테스트 실패: {e}")
        
        # 결과 분석
        analysis = self.analyze_results(results)
        
        # 결과 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"redis_pool_benchmark_{timestamp}.json"
        
        final_result = {
            "benchmark_info": {
                "timestamp": datetime.now().isoformat(),
                "test_count": len(results),
                "jobs_per_test": 500,
                "batch_size": 30
            },
            "results": results,
            "analysis": analysis
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_result, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 벤치마크 완료! 결과 저장: {output_file}")
        
        return final_result
    
    def analyze_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """벤치마크 결과 분석"""
        
        valid_results = [r for r in results if "error" not in r]
        
        if not valid_results:
            return {"error": "유효한 결과가 없습니다"}
        
        # 성능 지표별 분석
        performance_by_config = []
        
        for result in valid_results:
            config = result["config"]
            perf = result["overall_performance"]
            
            performance_by_config.append({
                "max_connections": config["max_connections"],
                "socket_timeout": config["socket_timeout"],
                "connect_timeout": config["connect_timeout"],
                "jobs_per_second": perf["overall_jobs_per_second"],
                "total_time": perf["total_time"],
                "push_jobs_per_second": result["push_performance"]["jobs_per_second"],
                "pop_jobs_per_second": result["pop_performance"]["jobs_per_second"]
            })
        
        # 최고 성능 찾기
        best_overall = max(performance_by_config, key=lambda x: x["jobs_per_second"])
        best_push = max(performance_by_config, key=lambda x: x["push_jobs_per_second"])
        best_pop = max(performance_by_config, key=lambda x: x["pop_jobs_per_second"])
        
        # 현재 설정 (20 연결) 성능
        current_config = next((p for p in performance_by_config 
                              if p["max_connections"] == 20 and p["socket_timeout"] == 5.0), None)
        
        analysis = {
            "best_overall_performance": best_overall,
            "best_push_performance": best_push,
            "best_pop_performance": best_pop,
            "current_config_performance": current_config,
            "performance_summary": performance_by_config,
            "recommendations": self.generate_recommendations(performance_by_config, current_config)
        }
        
        return analysis
    
    def generate_recommendations(self, performance_data: List[Dict], current_config: Dict) -> Dict[str, Any]:
        """성능 데이터를 기반으로 권장사항 생성"""
        
        # 연결 수별 성능 분석
        conn_performance = {}
        for perf in performance_data:
            conn_count = perf["max_connections"]
            if conn_count not in conn_performance:
                conn_performance[conn_count] = []
            conn_performance[conn_count].append(perf["jobs_per_second"])
        
        # 평균 성능 계산
        avg_performance = {
            conn: statistics.mean(perfs) 
            for conn, perfs in conn_performance.items()
        }
        
        # 최적 연결 수 찾기
        optimal_connections = max(avg_performance.items(), key=lambda x: x[1])
        
        recommendations = {
            "optimal_max_connections": optimal_connections[0],
            "expected_performance_improvement": f"{optimal_connections[1] / current_config['jobs_per_second']:.2f}x" if current_config else "N/A",
            "connection_analysis": avg_performance,
            "summary": f"연결 수 {optimal_connections[0]}로 설정 시 최고 성능 ({optimal_connections[1]:.1f} jobs/sec)"
        }
        
        return recommendations

async def main():
    """메인 실행 함수"""
    benchmark = RedisPoolBenchmark()
    
    try:
        # Redis 연결 확인
        config = get_settings()
        test_redis = redis.from_url(config.REDIS_URL) if config.REDIS_URL else redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)
        
        if not await test_redis.ping():
            raise Exception("Redis 서버에 연결할 수 없습니다")
        
        await test_redis.close()
        
        # 벤치마크 실행
        results = await benchmark.run_comprehensive_benchmark()
        
        # 결과 출력
        print("\n" + "="*80)
        print("🏆 REDIS 연결 풀 최적화 결과")
        print("="*80)
        
        if "analysis" in results:
            analysis = results["analysis"]
            
            print(f"\n📊 현재 설정 (연결 수 20):")
            if analysis["current_config_performance"]:
                current = analysis["current_config_performance"]
                print(f"   처리량: {current['jobs_per_second']:.1f} jobs/sec")
                print(f"   총 시간: {current['total_time']:.2f}s")
            
            print(f"\n🎯 최적 설정:")
            best = analysis["best_overall_performance"]
            print(f"   연결 수: {best['max_connections']}")
            print(f"   소켓 타임아웃: {best['socket_timeout']}s")
            print(f"   연결 타임아웃: {best['connect_timeout']}s")
            print(f"   처리량: {best['jobs_per_second']:.1f} jobs/sec")
            
            print(f"\n💡 권장사항:")
            recs = analysis["recommendations"]
            print(f"   {recs['summary']}")
            print(f"   성능 향상: {recs['expected_performance_improvement']}")
        
        print(f"\n📁 자세한 결과는 파일을 확인하세요")
        
    except Exception as e:
        logger.error(f"벤치마크 실행 실패: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 