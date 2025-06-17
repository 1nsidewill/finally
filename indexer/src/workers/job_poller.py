"""
Job Polling Mechanism for Redis Queue Worker

비동기 폴링 시스템으로 Redis 큐에서 작업을 배치로 가져오는 모듈
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Callable, Awaitable
from dataclasses import dataclass
from enum import Enum
import time
import json

from src.config import get_settings
from src.database.redis import RedisManager

logger = logging.getLogger(__name__)

class PollingStrategy(Enum):
    """폴링 전략 옵션"""
    BLOCKING = "blocking"      # 블로킹 폴링 (새 작업이 올 때까지 대기)
    NON_BLOCKING = "non_blocking"  # 논블로킹 폴링 (즉시 반환)
    ADAPTIVE = "adaptive"      # 적응형 폴링 (큐 상태에 따라 조정)

@dataclass
class PollingConfig:
    """폴링 설정"""
    batch_size: int = 10
    poll_interval: float = 1.0  # 폴링 간격(초)
    blocking_timeout: int = 5   # 블로킹 타임아웃(초)
    strategy: PollingStrategy = PollingStrategy.ADAPTIVE
    max_idle_time: float = 30.0  # 최대 유휴 시간(초)
    adaptive_min_interval: float = 0.1  # 적응형 최소 간격
    adaptive_max_interval: float = 5.0   # 적응형 최대 간격

@dataclass
class PollingStats:
    """폴링 통계"""
    total_polls: int = 0
    successful_polls: int = 0
    jobs_retrieved: int = 0
    empty_polls: int = 0
    errors: int = 0
    start_time: float = 0
    last_success_time: float = 0

class JobPoller:
    """
    Redis Queue Job Polling Mechanism
    
    비동기 폴링 시스템으로 Redis 큐에서 작업을 배치로 가져오는 클래스
    """
    
    def __init__(self, redis_manager: RedisManager, config: Optional[PollingConfig] = None):
        self.redis_manager = redis_manager
        self.settings = get_settings()
        self.config = config or PollingConfig(
            batch_size=self.settings.REDIS_BATCH_SIZE,
            poll_interval=self.settings.REDIS_POLL_INTERVAL,
            blocking_timeout=self.settings.REDIS_BLOCKING_TIMEOUT,
        )
        
        self.stats = PollingStats()
        self.is_running = False
        self._stop_event = asyncio.Event()
        
        # 적응형 폴링을 위한 상태
        self._current_interval = self.config.poll_interval
        self._consecutive_empty_polls = 0
        
        logger.info(f"JobPoller 초기화 완료: {self.config}")
    
    async def start_polling(
        self, 
        job_handler: Callable[[List[Dict[Any, Any]]], Awaitable[None]],
        queue_name: Optional[str] = None
    ) -> None:
        """
        폴링 시작
        
        Args:
            job_handler: 작업 처리 콜백 함수
            queue_name: 큐 이름 (None이면 기본 큐 사용)
        """
        if self.is_running:
            logger.warning("폴링이 이미 실행 중입니다")
            return
            
        self.is_running = True
        self.stats.start_time = time.time()
        self._stop_event.clear()
        
        queue_name = queue_name or self.settings.REDIS_QUEUE_NAME
        logger.info(f"폴링 시작: 큐={queue_name}, 전략={self.config.strategy.value}")
        
        try:
            while self.is_running and not self._stop_event.is_set():
                poll_start = time.time()
                
                try:
                    # 작업 폴링
                    jobs = await self._poll_jobs(queue_name)
                    self.stats.total_polls += 1
                    
                    if jobs:
                        # 작업 처리
                        self.stats.successful_polls += 1
                        self.stats.jobs_retrieved += len(jobs)
                        self.stats.last_success_time = time.time()
                        self._consecutive_empty_polls = 0
                        
                        logger.debug(f"작업 {len(jobs)}개 가져옴, 처리 시작")
                        await job_handler(jobs)
                        
                        # 적응형 폴링: 작업이 있으면 간격 단축
                        if self.config.strategy == PollingStrategy.ADAPTIVE:
                            self._current_interval = max(
                                self.config.adaptive_min_interval,
                                self._current_interval * 0.8
                            )
                    else:
                        # 빈 폴링
                        self.stats.empty_polls += 1
                        self._consecutive_empty_polls += 1
                        
                        # 적응형 폴링: 연속 빈 폴링이면 간격 증가
                        if self.config.strategy == PollingStrategy.ADAPTIVE:
                            if self._consecutive_empty_polls >= 3:
                                self._current_interval = min(
                                    self.config.adaptive_max_interval,
                                    self._current_interval * 1.5
                                )
                        
                        logger.debug(f"빈 폴링 (연속 {self._consecutive_empty_polls}회)")
                    
                    # 폴링 간격 조정
                    await self._wait_for_next_poll(poll_start)
                    
                except Exception as e:
                    self.stats.errors += 1
                    logger.error(f"폴링 중 오류 발생: {e}")
                    
                    # 오류 시 잠시 대기
                    await asyncio.sleep(min(5.0, self._current_interval * 2))
                
        except asyncio.CancelledError:
            logger.info("폴링이 취소되었습니다")
        finally:
            self.is_running = False
            logger.info("폴링 종료")
    
    async def _poll_jobs(self, queue_name: str) -> List[Dict[Any, Any]]:
        """
        큐에서 작업 폴링
        
        Args:
            queue_name: 큐 이름
            
        Returns:
            작업 리스트
        """
        if self.config.strategy == PollingStrategy.BLOCKING:
            # 블로킹 폴링: 작업이 있을 때까지 대기
            job = await self.redis_manager.pop_job(
                queue_name=queue_name, 
                blocking_timeout=self.config.blocking_timeout
            )
            return [job] if job else []
            
        elif self.config.strategy == PollingStrategy.NON_BLOCKING:
            # 논블로킹 폴링: 즉시 반환
            return await self.redis_manager.pop_jobs_batch(
                queue_name=queue_name, 
                count=self.config.batch_size
            )
            
        else:  # ADAPTIVE
            # 적응형 폴링: 큐 길이에 따라 조정
            queue_length = await self.redis_manager.get_queue_length(queue_name)
            
            if queue_length == 0:
                return []
            elif queue_length >= self.config.batch_size:
                # 큐에 충분한 작업이 있으면 배치로 가져오기
                return await self.redis_manager.pop_jobs_batch(
                    queue_name=queue_name, 
                    count=self.config.batch_size
                )
            else:
                # 큐에 적은 작업만 있으면 모두 가져오기
                return await self.redis_manager.pop_jobs_batch(
                    queue_name=queue_name, 
                    count=queue_length
                )
    
    async def _wait_for_next_poll(self, poll_start_time: float) -> None:
        """
        다음 폴링까지 대기
        
        Args:
            poll_start_time: 폴링 시작 시간
        """
        poll_duration = time.time() - poll_start_time
        wait_time = max(0, self._current_interval - poll_duration)
        
        if wait_time > 0:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait_time)
            except asyncio.TimeoutError:
                pass  # 정상적인 타임아웃
    
    async def stop_polling(self) -> None:
        """폴링 중지"""
        if not self.is_running:
            logger.warning("폴링이 실행 중이 아닙니다")
            return
            
        logger.info("폴링 중지 요청")
        self._stop_event.set()
        self.is_running = False
    
    def get_stats(self) -> Dict[str, Any]:
        """폴링 통계 반환"""
        runtime = time.time() - self.stats.start_time if self.stats.start_time > 0 else 0
        idle_time = time.time() - self.stats.last_success_time if self.stats.last_success_time > 0 else 0
        
        return {
            "is_running": self.is_running,
            "runtime_seconds": runtime,
            "idle_time_seconds": idle_time,
            "total_polls": self.stats.total_polls,
            "successful_polls": self.stats.successful_polls,
            "empty_polls": self.stats.empty_polls,
            "errors": self.stats.errors,
            "jobs_retrieved": self.stats.jobs_retrieved,
            "success_rate": (
                self.stats.successful_polls / self.stats.total_polls 
                if self.stats.total_polls > 0 else 0
            ),
            "avg_jobs_per_poll": (
                self.stats.jobs_retrieved / self.stats.successful_polls 
                if self.stats.successful_polls > 0 else 0
            ),
            "current_interval": self._current_interval,
            "consecutive_empty_polls": self._consecutive_empty_polls,
        }
    
    def reset_stats(self) -> None:
        """통계 초기화"""
        self.stats = PollingStats()
        self._current_interval = self.config.poll_interval
        self._consecutive_empty_polls = 0
        logger.info("폴링 통계 초기화")

class BatchJobPoller(JobPoller):
    """
    배치 최적화된 Job Poller
    
    대량의 작업을 효율적으로 처리하기 위한 확장된 폴링 메커니즘
    """
    
    def __init__(self, redis_manager: RedisManager, config: Optional[PollingConfig] = None):
        super().__init__(redis_manager, config)
        self._batch_buffer: List[Dict[Any, Any]] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        
    async def start_batch_polling(
        self,
        job_handler: Callable[[List[Dict[Any, Any]]], Awaitable[None]],
        queue_name: Optional[str] = None,
        buffer_size: int = 50,
        flush_interval: float = 2.0
    ) -> None:
        """
        배치 폴링 시작 (버퍼링 포함)
        
        Args:
            job_handler: 작업 처리 콜백 함수
            queue_name: 큐 이름
            buffer_size: 버퍼 크기
            flush_interval: 플러시 간격(초)
        """
        self._batch_buffer = []
        
        # 주기적 플러시 태스크 시작
        self._flush_task = asyncio.create_task(
            self._periodic_flush(job_handler, flush_interval, buffer_size)
        )
        
        # 기본 폴링 시작 (버퍼링 핸들러 사용)
        await self.start_polling(
            job_handler=lambda jobs: self._buffer_jobs(jobs, job_handler, buffer_size),
            queue_name=queue_name
        )
    
    async def _buffer_jobs(
        self,
        jobs: List[Dict[Any, Any]],
        job_handler: Callable[[List[Dict[Any, Any]]], Awaitable[None]],
        buffer_size: int
    ) -> None:
        """작업을 버퍼에 추가하고 필요시 플러시"""
        async with self._buffer_lock:
            self._batch_buffer.extend(jobs)
            
            if len(self._batch_buffer) >= buffer_size:
                await self._flush_buffer(job_handler)
    
    async def _periodic_flush(
        self,
        job_handler: Callable[[List[Dict[Any, Any]]], Awaitable[None]],
        interval: float,
        min_batch_size: int
    ) -> None:
        """주기적으로 버퍼 플러시"""
        while self.is_running:
            await asyncio.sleep(interval)
            
            async with self._buffer_lock:
                if len(self._batch_buffer) >= min_batch_size:
                    await self._flush_buffer(job_handler)
    
    async def _flush_buffer(
        self,
        job_handler: Callable[[List[Dict[Any, Any]]], Awaitable[None]]
    ) -> None:
        """버퍼 내용을 처리하고 비우기"""
        if not self._batch_buffer:
            return
            
        jobs_to_process = self._batch_buffer.copy()
        self._batch_buffer.clear()
        
        logger.debug(f"버퍼 플러시: {len(jobs_to_process)}개 작업 처리")
        await job_handler(jobs_to_process)
    
    async def stop_polling(self) -> None:
        """폴링 중지 및 남은 버퍼 처리"""
        await super().stop_polling()
        
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # 남은 버퍼 내용 처리 (선택적)
        if self._batch_buffer:
            logger.info(f"폴링 종료 시 남은 버퍼: {len(self._batch_buffer)}개 작업") 