#!/usr/bin/env python3
"""
Redis Queue Worker Daemon

스크래퍼 팀의 Job을 자동으로 처리하는 백그라운드 워커 데몬
"""

import asyncio
import logging
import signal
import sys
from typing import List, Dict, Any
from src.config import get_settings
from src.database.redis import RedisManager
from src.workers.job_poller import JobPoller, PollingConfig, PollingStrategy
from src.workers.job_processor import JobProcessor

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class QueueWorkerDaemon:
    """Redis Queue Worker 데몬"""
    
    def __init__(self):
        self.config = get_settings()
        self.redis_manager = RedisManager()
        self.job_processor = JobProcessor()
        self.poller = None
        self.is_running = False
        
        # 폴링 설정
        self.polling_config = PollingConfig(
            batch_size=10,
            poll_interval=1.0,
            blocking_timeout=5,
            strategy=PollingStrategy.ADAPTIVE
        )
        
    async def initialize(self):
        """워커 초기화"""
        try:
            logger.info("🚀 Redis Queue Worker 데몬 초기화 중...")
            
            # Redis 연결 확인
            if not await self.redis_manager.health_check():
                raise Exception("Redis 연결 실패")
            
            # Job Processor 초기화
            await self.job_processor.initialize()
            
            # Job Poller 생성
            self.poller = JobPoller(
                redis_manager=self.redis_manager,
                config=self.polling_config
            )
            
            logger.info("✅ 워커 데몬 초기화 완료")
            
        except Exception as e:
            logger.error(f"❌ 워커 데몬 초기화 실패: {e}")
            raise
    
    async def process_jobs(self, jobs: List[Dict[str, Any]]):
        """Job 배치 처리"""
        logger.info(f"📦 {len(jobs)}개 Job 처리 시작")
        
        for job in jobs:
            try:
                # Job 타입별 처리
                job_type = job.get('type', '').lower()
                
                if job_type == 'sync':
                    await self._process_sync_job(job)
                elif job_type == 'update':
                    await self._process_update_job(job)
                elif job_type == 'delete':
                    await self._process_delete_job(job)
                else:
                    logger.warning(f"⚠️ 알 수 없는 Job 타입: {job_type}")
                    
            except Exception as e:
                logger.error(f"❌ Job 처리 실패 {job.get('id', 'unknown')}: {e}")
        
        logger.info(f"✅ {len(jobs)}개 Job 처리 완료")
    
    async def _process_sync_job(self, job: Dict[str, Any]):
        """SYNC Job 처리"""
        product_id = job.get('product_id')
        provider = job.get('provider', 'bunjang')
        product_data = job.get('product_data', {})
        
        logger.info(f"🔄 SYNC Job 처리: {product_id} ({provider})")
        
        # JobProcessor를 통해 처리
        result = await self.job_processor.process_job(job)
        
        if result.success:
            logger.info(f"✅ SYNC 성공: {product_id}")
        else:
            logger.error(f"❌ SYNC 실패: {product_id} - {result.message}")
    
    async def _process_update_job(self, job: Dict[str, Any]):
        """UPDATE Job 처리"""
        product_id = job.get('product_id')
        provider = job.get('provider', 'bunjang')
        
        logger.info(f"🔄 UPDATE Job 처리: {product_id} ({provider})")
        
        # JobProcessor를 통해 처리
        result = await self.job_processor.process_job(job)
        
        if result.success:
            logger.info(f"✅ UPDATE 성공: {product_id}")
        else:
            logger.error(f"❌ UPDATE 실패: {product_id} - {result.message}")
    
    async def _process_delete_job(self, job: Dict[str, Any]):
        """DELETE Job 처리"""
        product_id = job.get('product_id')
        provider = job.get('provider', 'bunjang')
        
        logger.info(f"🔄 DELETE Job 처리: {product_id} ({provider})")
        
        # JobProcessor를 통해 처리
        result = await self.job_processor.process_job(job)
        
        if result.success:
            logger.info(f"✅ DELETE 성공: {product_id}")
        else:
            logger.error(f"❌ DELETE 실패: {product_id} - {result.message}")
    
    async def start(self):
        """워커 데몬 시작"""
        if self.is_running:
            logger.warning("워커가 이미 실행 중입니다")
            return
        
        self.is_running = True
        logger.info("🚀 Redis Queue Worker 데몬 시작")
        
        try:
            # 폴링 시작 (무한 루프)
            await self.poller.start_polling(
                job_handler=self.process_jobs,
                queue_name=self.config.REDIS_QUEUE_NAME
            )
        except asyncio.CancelledError:
            logger.info("워커 데몬이 취소되었습니다")
        except Exception as e:
            logger.error(f"워커 데몬 실행 중 오류: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """워커 데몬 중지"""
        if not self.is_running:
            return
        
        logger.info("🛑 Redis Queue Worker 데몬 중지 중...")
        self.is_running = False
        
        if self.poller:
            await self.poller.stop_polling()
        
        if self.job_processor:
            await self.job_processor.close()
        
        logger.info("✅ 워커 데몬 중지 완료")
    
    def get_stats(self) -> Dict[str, Any]:
        """워커 통계 반환"""
        base_stats = {
            "daemon_running": self.is_running,
            "queue_name": self.config.REDIS_QUEUE_NAME,
            "polling_config": {
                "batch_size": self.polling_config.batch_size,
                "poll_interval": self.polling_config.poll_interval,
                "strategy": self.polling_config.strategy.value
            }
        }
        
        if self.poller:
            base_stats.update(self.poller.get_stats())
        
        return base_stats

# 전역 워커 인스턴스
worker_daemon = QueueWorkerDaemon()

async def signal_handler():
    """시그널 핸들러"""
    logger.info("종료 시그널 수신, 워커 중지 중...")
    await worker_daemon.stop()

async def main():
    """메인 함수"""
    # 시그널 핸들러 설정
    def signal_callback(signum, frame):
        logger.info(f"시그널 {signum} 수신")
        asyncio.create_task(signal_handler())
    
    signal.signal(signal.SIGINT, signal_callback)
    signal.signal(signal.SIGTERM, signal_callback)
    
    try:
        # 워커 초기화 및 시작
        await worker_daemon.initialize()
        await worker_daemon.start()
        
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"워커 데몬 실행 실패: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 