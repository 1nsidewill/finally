# benchmark_batch_sizes.py

"""
Redis 큐 배치 크기 성능 벤치마킹 도구
다양한 배치 크기로 처리량과 지연 시간을 측정하여 최적값을 찾습니다.
"""

import asyncio
import logging
import time
import statistics
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import json

from src.config import get_settings
from src.database.redis import redis_manager

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class BenchmarkResult:
    """벤치마크 결과 데이터 클래스"""
    batch_size: int
    throughput_per_second: float
    avg_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    p95_latency_ms: float
    total_jobs_processed: int
    total_time_seconds: float
    cpu_efficiency: float

class BatchSizeBenchmark:
    """배치 크기 벤치마킹 클래스"""
    
    def __init__(self):
        self.settings = get_settings()
        self.test_queue = "benchmark_queue"
        
    async def setup(self):
        """벤치마크 환경 설정"""
        logger.info("🔧 벤치마크 환경 설정 중...")
        
        # Redis 연결 테스트
        health = await redis_manager.health_check()
        if not health:
            raise Exception("Redis 서버에 연결할 수 없습니다!")
        
        # 테스트 큐 초기화
        await redis_manager.clear_queue(self.test_queue)
        logger.info("✅ 벤치마크 환경 설정 완료")
    
    async def cleanup(self):
        """벤치마크 환경 정리"""
        logger.info("🧹 벤치마크 환경 정리 중...")
        await redis_manager.clear_queue(self.test_queue)
        logger.info("✅ 벤치마크 환경 정리 완료")
    
    def generate_test_jobs(self, count: int) -> List[Dict[str, Any]]:
        """테스트용 작업 데이터 생성"""
        jobs = []
        for i in range(count):
            job = {
                "type": "sync",
                "product_id": i + 1,
                "action": "index",
                "timestamp": datetime.now().isoformat(),
                "metadata": {
                    "source": "benchmark",
                    "batch_test": True,
                    "job_sequence": i
                }
            }
            jobs.append(job)
        return jobs
    
    async def populate_queue(self, job_count: int):
        """큐에 테스트 작업들 채우기"""
        logger.info(f"📥 큐에 {job_count}개 작업 추가 중...")
        
        jobs = self.generate_test_jobs(job_count)
        batch_size = 100  # 큐 채우기용 배치 크기
        
        total_added = 0
        for i in range(0, len(jobs), batch_size):
            batch = jobs[i:i + batch_size]
            added = await redis_manager.push_jobs_batch(batch, self.test_queue)
            total_added += added
        
        logger.info(f"✅ 총 {total_added}개 작업 추가 완료")
        return total_added
    
    async def process_jobs_with_batch_size(self, batch_size: int, max_jobs: int = 1000) -> BenchmarkResult:
        """특정 배치 크기로 작업 처리 및 성능 측정"""
        logger.info(f"⚡ 배치 크기 {batch_size}로 성능 테스트 시작...")
        
        latencies = []
        total_processed = 0
        start_time = time.time()
        
        while total_processed < max_jobs:
            batch_start = time.time()
            
            # 배치로 작업 가져오기
            if batch_size == 1:
                # 단일 작업 처리 (블로킹)
                job = await redis_manager.pop_job(self.test_queue, timeout=1)
                jobs = [job] if job else []
            else:
                # 배치 작업 처리 (논블로킹)
                jobs = await redis_manager.pop_jobs_batch(batch_size, self.test_queue)
            
            batch_end = time.time()
            
            if not jobs:
                logger.debug("큐가 비었습니다. 테스트 종료.")
                break
            
            # 배치 처리 시간 기록
            batch_latency = (batch_end - batch_start) * 1000  # ms 단위
            latencies.append(batch_latency)
            total_processed += len(jobs)
            
            # 실제 작업 처리 시뮬레이션 (임베딩, Qdrant 저장 등)
            await self.simulate_job_processing(jobs)
            
            # 진행 상황 로깅
            if total_processed % 100 == 0:
                logger.debug(f"처리 진행: {total_processed}/{max_jobs}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # 성능 메트릭 계산
        if latencies:
            avg_latency = statistics.mean(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p95_latency = statistics.quantiles(latencies, n=20)[18]  # 95th percentile
        else:
            avg_latency = min_latency = max_latency = p95_latency = 0
        
        throughput = total_processed / total_time if total_time > 0 else 0
        cpu_efficiency = total_processed / (total_time * batch_size) if total_time > 0 else 0
        
        result = BenchmarkResult(
            batch_size=batch_size,
            throughput_per_second=throughput,
            avg_latency_ms=avg_latency,
            min_latency_ms=min_latency,
            max_latency_ms=max_latency,
            p95_latency_ms=p95_latency,
            total_jobs_processed=total_processed,
            total_time_seconds=total_time,
            cpu_efficiency=cpu_efficiency
        )
        
        logger.info(f"✅ 배치 크기 {batch_size} 테스트 완료: {throughput:.2f} jobs/sec")
        return result
    
    async def simulate_job_processing(self, jobs: List[Dict[str, Any]]):
        """실제 작업 처리 시뮬레이션"""
        # Redis 큐에서 작업을 가져온 후의 실제 처리를 시뮬레이션
        # (텍스트 전처리, 임베딩 생성, Qdrant 저장 등)
        
        # 실제 환경에서의 처리 시간을 시뮬레이션
        # - 텍스트 전처리: ~1ms per job
        # - 임베딩 API 호출: ~50ms per batch (OpenAI)
        # - Qdrant 저장: ~10ms per batch
        
        processing_time = 0.001 * len(jobs)  # 텍스트 전처리 시간
        processing_time += 0.05  # 임베딩 API 호출 시간 (배치당)
        processing_time += 0.01  # Qdrant 저장 시간 (배치당)
        
        await asyncio.sleep(processing_time)
    
    async def run_benchmark_suite(self, 
                                 batch_sizes: List[int] = None,
                                 jobs_per_test: int = 1000) -> List[BenchmarkResult]:
        """전체 배치 크기 벤치마크 스위트 실행"""
        
        if batch_sizes is None:
            # 기본 테스트 배치 크기들
            batch_sizes = [1, 5, 10, 20, 30, 50, 75, 100, 150, 200]
        
        logger.info(f"🚀 배치 크기 벤치마크 시작!")
        logger.info(f"테스트할 배치 크기들: {batch_sizes}")
        logger.info(f"각 테스트당 작업 수: {jobs_per_test}")
        
        results = []
        
        for batch_size in batch_sizes:
            try:
                # 큐 초기화 및 작업 추가
                await redis_manager.clear_queue(self.test_queue)
                await self.populate_queue(jobs_per_test + 100)  # 여유분 추가
                
                # 벤치마크 실행
                result = await self.process_jobs_with_batch_size(batch_size, jobs_per_test)
                results.append(result)
                
                # 테스트 간 잠시 대기
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ 배치 크기 {batch_size} 테스트 실패: {e}")
                continue
        
        return results
    
    def analyze_results(self, results: List[BenchmarkResult]) -> Dict[str, Any]:
        """벤치마크 결과 분석"""
        if not results:
            return {"error": "분석할 결과가 없습니다"}
        
        # 최고 처리량 찾기
        best_throughput = max(results, key=lambda r: r.throughput_per_second)
        
        # 최저 지연시간 찾기
        best_latency = min(results, key=lambda r: r.avg_latency_ms)
        
        # 최고 CPU 효율성 찾기
        best_efficiency = max(results, key=lambda r: r.cpu_efficiency)
        
        # 균형점 찾기 (처리량과 지연시간의 균형)
        # 정규화된 점수 계산: (처리량 점수 - 지연시간 패널티)
        max_throughput = max(r.throughput_per_second for r in results)
        min_latency = min(r.avg_latency_ms for r in results)
        max_latency = max(r.avg_latency_ms for r in results)
        
        for result in results:
            throughput_score = result.throughput_per_second / max_throughput
            latency_penalty = (result.avg_latency_ms - min_latency) / (max_latency - min_latency) if max_latency > min_latency else 0
            result.balance_score = throughput_score - (latency_penalty * 0.3)
        
        best_balance = max(results, key=lambda r: r.balance_score)
        
        analysis = {
            "total_tests": len(results),
            "best_throughput": {
                "batch_size": best_throughput.batch_size,
                "throughput": best_throughput.throughput_per_second,
                "latency": best_throughput.avg_latency_ms
            },
            "best_latency": {
                "batch_size": best_latency.batch_size,
                "throughput": best_latency.throughput_per_second,
                "latency": best_latency.avg_latency_ms
            },
            "best_efficiency": {
                "batch_size": best_efficiency.batch_size,
                "throughput": best_efficiency.throughput_per_second,
                "efficiency": best_efficiency.cpu_efficiency
            },
            "recommended_balance": {
                "batch_size": best_balance.batch_size,
                "throughput": best_balance.throughput_per_second,
                "latency": best_balance.avg_latency_ms,
                "balance_score": best_balance.balance_score
            }
        }
        
        return analysis
    
    def print_results(self, results: List[BenchmarkResult], analysis: Dict[str, Any]):
        """벤치마크 결과 출력"""
        print(f"\n{'='*80}")
        print("📊 배치 크기 벤치마크 결과")
        print(f"{'='*80}")
        
        # 결과 테이블
        print(f"{'배치크기':<8} {'처리량(jobs/s)':<15} {'평균지연(ms)':<12} {'P95지연(ms)':<11} {'효율성':<8}")
        print("-" * 80)
        
        for result in results:
            print(f"{result.batch_size:<8} "
                  f"{result.throughput_per_second:<15.2f} "
                  f"{result.avg_latency_ms:<12.2f} "
                  f"{result.p95_latency_ms:<11.2f} "
                  f"{result.cpu_efficiency:<8.3f}")
        
        # 분석 결과
        print(f"\n{'='*80}")
        print("🎯 분석 결과 및 권장사항")
        print(f"{'='*80}")
        
        print(f"🏆 최고 처리량: 배치크기 {analysis['best_throughput']['batch_size']} "
              f"({analysis['best_throughput']['throughput']:.2f} jobs/s)")
        
        print(f"⚡ 최저 지연시간: 배치크기 {analysis['best_latency']['batch_size']} "
              f"({analysis['best_latency']['latency']:.2f} ms)")
        
        print(f"⚙️ 최고 효율성: 배치크기 {analysis['best_efficiency']['batch_size']} "
              f"(효율성: {analysis['best_efficiency']['efficiency']:.3f})")
        
        print(f"\n🎯 **권장 배치 크기: {analysis['recommended_balance']['batch_size']}**")
        print(f"   - 처리량: {analysis['recommended_balance']['throughput']:.2f} jobs/s")
        print(f"   - 평균 지연시간: {analysis['recommended_balance']['latency']:.2f} ms")
        print(f"   - 균형 점수: {analysis['recommended_balance']['balance_score']:.3f}")
        
        print(f"\n💡 권장사항:")
        recommended_size = analysis['recommended_balance']['batch_size']
        print(f"   - REDIS_BATCH_SIZE를 {recommended_size}로 설정하세요")
        print(f"   - 이 값은 처리량과 지연시간의 최적 균형점입니다")
        
        if recommended_size <= 10:
            print("   - 작은 배치 크기: 실시간성 중시, 낮은 지연시간")
        elif recommended_size <= 50:
            print("   - 중간 배치 크기: 처리량과 지연시간의 균형")
        else:
            print("   - 큰 배치 크기: 높은 처리량, 배치 처리 효율성")

async def main():
    """메인 벤치마크 실행 함수"""
    benchmark = BatchSizeBenchmark()
    
    try:
        # 환경 설정
        await benchmark.setup()
        
        # 벤치마크 실행
        # 사용자 정의 배치 크기들 (현재 설정값 포함)
        current_batch_size = get_settings().REDIS_BATCH_SIZE
        test_sizes = [1, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100]
        
        # 현재 설정값이 테스트 목록에 없으면 추가
        if current_batch_size not in test_sizes:
            test_sizes.append(current_batch_size)
            test_sizes.sort()
        
        print(f"현재 설정된 REDIS_BATCH_SIZE: {current_batch_size}")
        print(f"테스트할 배치 크기들: {test_sizes}")
        
        results = await benchmark.run_benchmark_suite(
            batch_sizes=test_sizes,
            jobs_per_test=500  # 테스트 규모 (프로덕션에서는 더 크게)
        )
        
        # 결과 분석
        analysis = benchmark.analyze_results(results)
        
        # 결과 출력
        benchmark.print_results(results, analysis)
        
        # 결과를 JSON 파일로 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"batch_size_benchmark_{timestamp}.json"
        
        results_data = {
            "timestamp": datetime.now().isoformat(),
            "current_config": current_batch_size,
            "test_sizes": test_sizes,
            "results": [
                {
                    "batch_size": r.batch_size,
                    "throughput_per_second": r.throughput_per_second,
                    "avg_latency_ms": r.avg_latency_ms,
                    "p95_latency_ms": r.p95_latency_ms,
                    "cpu_efficiency": r.cpu_efficiency,
                    "total_jobs_processed": r.total_jobs_processed,
                    "total_time_seconds": r.total_time_seconds
                }
                for r in results
            ],
            "analysis": analysis
        }
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 결과가 {results_file}에 저장되었습니다.")
        
    except Exception as e:
        logger.error(f"❌ 벤치마크 실행 실패: {e}")
        raise
    
    finally:
        # 환경 정리
        await benchmark.cleanup()
        await redis_manager.close()

if __name__ == "__main__":
    asyncio.run(main())