#!/usr/bin/env python3
"""
30k 매물 처리 통합 부하 테스트
실제 운영 환경을 시뮬레이션하여 전체 시스템 성능을 측정합니다.
"""

import asyncio
import time
import json
import psutil
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
import logging
import signal
import sys

# 프로젝트 모듈들
from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager  
from src.database.redis import RedisManager
from src.services.embedding_service import EmbeddingService
from src.services.text_preprocessor import ProductTextPreprocessor
from src.monitoring.metrics import get_metrics
from src.config import get_settings

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class LoadTestMetrics:
    """부하 테스트 메트릭 데이터 클래스"""
    start_time: float
    end_time: Optional[float] = None
    total_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    jobs_per_second: float = 0.0
    avg_processing_time: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    redis_connections_used: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class MockProductGenerator:
    """실제 매물 데이터와 유사한 모의 데이터 생성기"""
    
    def __init__(self):
        self.brands = ["YAMAHA", "HONDA", "KAWASAKI", "SUZUKI", "DUCATI", "BMW", "KTM"]
        self.models = ["FZ-25", "CB300R", "NINJA 400", "GSX-R125", "MONSTER", "G310R", "RC200"]
        self.conditions = ["매우양호", "양호", "보통", "수리필요"]
        
    def generate_product(self, uid: int) -> Dict[str, Any]:
        """단일 매물 데이터 생성"""
        import random
        
        brand = random.choice(self.brands)
        model = random.choice(self.models)
        year = random.randint(2015, 2024)
        price = random.randint(150, 800)  # 150~800만원
        mileage = random.randint(0, 50000)  # 0~50,000km
        condition = random.choice(self.conditions)
        
        return {
            "uid": uid,
            "title": f"{brand} {model} {year}년식",
            "content": f"주행거리 {mileage:,}km, 상태 {condition}, 정기점검 완료",
            "price": str(price),
            "year": str(year),
            "mileage": str(mileage),
            "brand": brand,
            "model": model,
            "is_conversion": False,  # 새로운 데이터
            "created_at": datetime.now().isoformat()
        }
    
    def generate_batch(self, start_uid: int, batch_size: int) -> List[Dict[str, Any]]:
        """배치 매물 데이터 생성"""
        return [
            self.generate_product(start_uid + i) 
            for i in range(batch_size)
        ]

class LoadTestRunner:
    """부하 테스트 실행기"""
    
    def __init__(self):
        self.config = get_settings()
        self.product_generator = MockProductGenerator()
        self.text_preprocessor = ProductTextPreprocessor()
        self.embedding_service = EmbeddingService()
        self.redis_manager = RedisManager()
        
        # 메트릭 수집
        self.metrics = LoadTestMetrics(start_time=time.time())
        self.is_running = True
        self.processed_jobs = []
        
        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Graceful shutdown"""
        logger.info(f"신호 {signum} 수신, 테스트를 종료합니다...")
        self.is_running = False
        
    async def monitor_system_resources(self):
        """시스템 리소스 모니터링"""
        while self.is_running:
            try:
                # CPU 및 메모리 사용률
                cpu_percent = psutil.cpu_percent(interval=1)
                memory_info = psutil.virtual_memory()
                
                self.metrics.cpu_usage_percent = cpu_percent
                self.metrics.memory_usage_mb = memory_info.used / 1024 / 1024
                
                # Redis 연결 수 추정 (실제로는 모니터링 메트릭에서 가져와야 함)
                self.metrics.redis_connections_used = len(self.redis_manager._connection_pool._available_connections) if hasattr(self.redis_manager, '_connection_pool') else 0
                
                # 5초마다 로그
                if int(time.time()) % 5 == 0:
                    logger.info(f"시스템 리소스 - CPU: {cpu_percent:.1f}%, 메모리: {self.metrics.memory_usage_mb:.1f}MB")
                
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"리소스 모니터링 오류: {e}")
                await asyncio.sleep(5)
    
    async def simulate_job_processing(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """단일 작업 처리 시뮬레이션"""
        job_start_time = time.time()
        
        try:
            # 1. 텍스트 전처리
            processed_text = self.text_preprocessor.preprocess_product_data(job_data)
            
            # 2. 임베딩 생성 (실제 API 호출은 비용 때문에 모의)
            # embedding = self.embedding_service.create_embedding(processed_text)
            # 대신 랜덤 벡터 생성 (3072 차원)
            import numpy as np
            mock_embedding = np.random.random(3072).tolist()
            
            # 3. 처리 시간 시뮬레이션 (실제 임베딩 API 호출 시간 근사)
            await asyncio.sleep(0.1)  # 100ms 시뮬레이션
            
            job_end_time = time.time()
            processing_time = job_end_time - job_start_time
            
            result = {
                "job_id": job_data["uid"],
                "status": "completed",
                "processing_time": processing_time,
                "processed_text": processed_text,
                "embedding_length": len(mock_embedding),
                "timestamp": datetime.now().isoformat()
            }
            
            self.metrics.completed_jobs += 1
            return result
            
        except Exception as e:
            logger.error(f"작업 {job_data['uid']} 처리 실패: {e}")
            self.metrics.failed_jobs += 1
            self.metrics.errors.append(f"Job {job_data['uid']}: {str(e)}")
            
            return {
                "job_id": job_data["uid"],
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def worker_simulation(self, worker_id: int, job_queue: asyncio.Queue):
        """개별 워커 시뮬레이션"""
        logger.info(f"워커 {worker_id} 시작")
        
        while self.is_running:
            try:
                # 큐에서 작업 가져오기 (타임아웃 설정)
                try:
                    job = await asyncio.wait_for(job_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # 작업 처리
                result = await self.simulate_job_processing(job)
                self.processed_jobs.append(result)
                
                # 큐 작업 완료 표시
                job_queue.task_done()
                
                # 진행 상황 로그 (100개마다)
                if self.metrics.completed_jobs % 100 == 0:
                    elapsed = time.time() - self.metrics.start_time
                    rate = self.metrics.completed_jobs / elapsed if elapsed > 0 else 0
                    logger.info(f"처리 완료: {self.metrics.completed_jobs}/{self.metrics.total_jobs} ({rate:.1f} jobs/sec)")
                
            except Exception as e:
                logger.error(f"워커 {worker_id} 오류: {e}")
                await asyncio.sleep(1)
        
        logger.info(f"워커 {worker_id} 종료")
    
    async def run_load_test(self, total_products: int = 30000, batch_size: int = 30, num_workers: int = 10):
        """메인 부하 테스트 실행"""
        logger.info(f"🚀 30k 매물 처리 부하 테스트 시작")
        logger.info(f"📊 설정: 총 {total_products:,}개 매물, 배치 크기 {batch_size}, 워커 {num_workers}개")
        
        self.metrics.total_jobs = total_products
        
        # 작업 큐 생성 
        job_queue = asyncio.Queue(maxsize=batch_size * 5)  # 큐 크기 제한
        
        # 시스템 모니터링 태스크 시작
        monitor_task = asyncio.create_task(self.monitor_system_resources())
        
        # 워커 태스크들 시작
        worker_tasks = [
            asyncio.create_task(self.worker_simulation(i, job_queue))
            for i in range(num_workers)
        ]
        
        # 데이터 생성 및 큐에 추가
        logger.info("📦 매물 데이터 생성 및 큐에 추가 중...")
        
        total_batches = (total_products + batch_size - 1) // batch_size
        
        try:
            for batch_num in range(total_batches):
                if not self.is_running:
                    break
                    
                start_uid = batch_num * batch_size
                current_batch_size = min(batch_size, total_products - start_uid)
                
                # 배치 데이터 생성
                batch_products = self.product_generator.generate_batch(start_uid, current_batch_size)
                
                # 큐에 추가
                for product in batch_products:
                    await job_queue.put(product)
                
                # 진행 상황 로그
                if batch_num % 100 == 0:
                    logger.info(f"데이터 생성: {batch_num}/{total_batches} 배치 완료")
                
                # 큐가 너무 차지 않도록 잠시 대기
                while job_queue.qsize() > batch_size * 3:
                    await asyncio.sleep(0.1)
            
            logger.info("🏁 모든 데이터 생성 완료, 처리 완료 대기 중...")
            
            # 모든 작업이 완료될 때까지 대기
            await job_queue.join()
            
        except Exception as e:
            logger.error(f"부하 테스트 중 오류: {e}")
            self.metrics.errors.append(f"Main test error: {str(e)}")
        
        finally:
            # 종료 처리
            self.is_running = False
            self.metrics.end_time = time.time()
            
            # 태스크 정리
            monitor_task.cancel()
            for task in worker_tasks:
                task.cancel()
            
            # 태스크 완료 대기
            await asyncio.gather(monitor_task, *worker_tasks, return_exceptions=True)
            
            # 최종 메트릭 계산
            self._calculate_final_metrics()
            
            # 결과 출력
            self._print_results()
            
            # 결과 저장
            await self._save_results()
    
    def _calculate_final_metrics(self):
        """최종 메트릭 계산"""
        total_time = self.metrics.end_time - self.metrics.start_time
        
        if total_time > 0:
            self.metrics.jobs_per_second = self.metrics.completed_jobs / total_time
        
        if self.processed_jobs:
            processing_times = [
                job["processing_time"] 
                for job in self.processed_jobs 
                if job.get("processing_time")
            ]
            if processing_times:
                self.metrics.avg_processing_time = sum(processing_times) / len(processing_times)
    
    def _print_results(self):
        """결과 출력"""
        total_time = self.metrics.end_time - self.metrics.start_time
        success_rate = (self.metrics.completed_jobs / self.metrics.total_jobs * 100) if self.metrics.total_jobs > 0 else 0
        
        print("\n" + "="*60)
        print("🏆 30K 매물 처리 부하 테스트 결과")
        print("="*60)
        print(f"📊 전체 통계:")
        print(f"   • 총 작업 수: {self.metrics.total_jobs:,}")
        print(f"   • 완료된 작업: {self.metrics.completed_jobs:,}")
        print(f"   • 실패한 작업: {self.metrics.failed_jobs:,}")
        print(f"   • 성공률: {success_rate:.1f}%")
        print(f"   • 총 소요 시간: {total_time:.2f}초")
        print(f"   • 처리 속도: {self.metrics.jobs_per_second:.1f} jobs/sec")
        print(f"   • 평균 처리 시간: {self.metrics.avg_processing_time:.3f}초")
        print()
        print(f"🖥️ 시스템 리소스:")
        print(f"   • CPU 사용률: {self.metrics.cpu_usage_percent:.1f}%")
        print(f"   • 메모리 사용량: {self.metrics.memory_usage_mb:.1f}MB")
        print(f"   • Redis 연결 수: {self.metrics.redis_connections_used}")
        print()
        
        if self.metrics.errors:
            print(f"❌ 오류 ({len(self.metrics.errors)}개):")
            for error in self.metrics.errors[:5]:  # 처음 5개만 표시
                print(f"   • {error}")
            if len(self.metrics.errors) > 5:
                print(f"   • ... 그 외 {len(self.metrics.errors) - 5}개")
        else:
            print("✅ 오류 없음")
        
        print("="*60)
    
    async def _save_results(self):
        """결과를 JSON 파일로 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"load_test_results_{timestamp}.json"
        
        results = {
            "test_info": {
                "timestamp": datetime.now().isoformat(),
                "total_products": self.metrics.total_jobs,
                "batch_size": self.config.REDIS_BATCH_SIZE,
                "redis_max_connections": self.config.REDIS_MAX_CONNECTIONS,
                "redis_connection_timeout": self.config.REDIS_CONNECTION_TIMEOUT
            },
            "metrics": asdict(self.metrics),
            "sample_processed_jobs": self.processed_jobs[:10]  # 샘플 10개만
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 결과가 {filename}에 저장되었습니다")
        except Exception as e:
            logger.error(f"결과 저장 실패: {e}")

async def main():
    """메인 함수"""
    print("🏍️ 30K 오토바이 매물 처리 부하 테스트")
    print("=" * 50)
    
    # 테스트 설정
    total_products = 30000  # 실제 30k 매물
    batch_size = 30        # 최적화된 배치 크기
    num_workers = 10       # 동시 워커 수
    
    # 사용자 확인
    print(f"설정:")
    print(f"  • 총 매물 수: {total_products:,}개")
    print(f"  • 배치 크기: {batch_size}")
    print(f"  • 워커 수: {num_workers}")
    print(f"  • 예상 소요 시간: 약 {total_products / (batch_size * 2):.0f}분")
    print()
    
    confirm = input("테스트를 시작하시겠습니까? (y/N): ")
    if confirm.lower() != 'y':
        print("테스트가 취소되었습니다.")
        return
    
    # 테스트 실행
    runner = LoadTestRunner()
    
    try:
        await runner.run_load_test(
            total_products=total_products,
            batch_size=batch_size, 
            num_workers=num_workers
        )
    except KeyboardInterrupt:
        print("\n사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        print(f"\n테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 