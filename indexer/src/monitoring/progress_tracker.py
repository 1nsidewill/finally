#!/usr/bin/env python3
"""
📊 Progress Tracking System
대용량 데이터 처리를 위한 상세한 진행률 추적 및 모니터링 시스템
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from pathlib import Path
import logging
from contextlib import asynccontextmanager
import threading
import queue
import psutil

logger = logging.getLogger(__name__)

@dataclass
class BatchProgress:
    """배치 처리 진행 상황"""
    batch_id: int
    start_time: datetime
    end_time: Optional[datetime] = None
    total_items: int = 0
    processed_items: int = 0
    successful_items: int = 0
    failed_items: int = 0
    error_details: List[Dict[str, Any]] = None
    processing_rate: float = 0.0  # items per second
    memory_usage_mb: float = 0.0
    
    def __post_init__(self):
        if self.error_details is None:
            self.error_details = []
    
    @property
    def duration_seconds(self) -> float:
        """처리 시간 (초)"""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now() - self.start_time).total_seconds()
    
    @property
    def success_rate(self) -> float:
        """성공률 (%)"""
        if self.processed_items == 0:
            return 0.0
        return (self.successful_items / self.processed_items) * 100
    
    @property
    def is_completed(self) -> bool:
        """배치 완료 여부"""
        return self.end_time is not None
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환 (JSON 직렬화용)"""
        data = asdict(self)
        data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        return data

@dataclass
class ProcessingSession:
    """전체 처리 세션 정보"""
    session_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_batches: int = 0
    completed_batches: int = 0
    total_items: int = 0
    processed_items: int = 0
    successful_items: int = 0
    failed_items: int = 0
    average_processing_rate: float = 0.0
    peak_memory_usage_mb: float = 0.0
    
    @property
    def completion_percentage(self) -> float:
        """완료율 (%)"""
        if self.total_items == 0:
            return 0.0
        return (self.processed_items / self.total_items) * 100
    
    @property
    def estimated_time_remaining(self) -> Optional[timedelta]:
        """예상 남은 시간"""
        if self.average_processing_rate == 0 or self.processed_items == 0:
            return None
        remaining_items = self.total_items - self.processed_items
        remaining_seconds = remaining_items / self.average_processing_rate
        return timedelta(seconds=remaining_seconds)
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        data = asdict(self)
        data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        if self.estimated_time_remaining:
            data['estimated_time_remaining'] = str(self.estimated_time_remaining)
        return data

class ProgressTracker:
    """📊 진행률 추적 및 모니터링 시스템"""
    
    def __init__(self, session_id: str, log_dir: str = "./.taskmaster/logs"):
        self.session_id = session_id
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # 세션 정보
        self.session = ProcessingSession(
            session_id=session_id,
            start_time=datetime.now()
        )
        
        # 배치 진행 상황
        self.batches: Dict[int, BatchProgress] = {}
        self.current_batch: Optional[BatchProgress] = None
        
        # 로깅 설정
        self.progress_log_file = self.log_dir / f"progress_{session_id}.json"
        self.detailed_log_file = self.log_dir / f"detailed_{session_id}.log"
        
        # 실시간 모니터링
        self._monitoring_active = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._stats_queue = queue.Queue()
        
        # 콜백 함수들
        self.progress_callbacks: List[Callable[[ProcessingSession], None]] = []
        self.batch_callbacks: List[Callable[[BatchProgress], None]] = []
        
        # 로거 설정
        self._setup_detailed_logger()
        
        logger.info(f"📊 Progress Tracker 초기화: {session_id}")
    
    def _setup_detailed_logger(self):
        """상세 로깅 설정"""
        self.detailed_logger = logging.getLogger(f"progress_tracker_{self.session_id}")
        self.detailed_logger.setLevel(logging.DEBUG)
        
        # 파일 핸들러
        file_handler = logging.FileHandler(self.detailed_log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # 포맷터
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        self.detailed_logger.addHandler(file_handler)
        self.detailed_logger.propagate = False
    
    def set_total_items(self, total_items: int, total_batches: int):
        """전체 처리할 아이템 수와 배치 수 설정"""
        self.session.total_items = total_items
        self.session.total_batches = total_batches
        
        self.detailed_logger.info(f"🎯 처리 목표 설정: {total_items}개 아이템, {total_batches}개 배치")
        self._save_progress()
    
    def start_batch(self, batch_id: int, batch_size: int) -> BatchProgress:
        """배치 처리 시작"""
        batch = BatchProgress(
            batch_id=batch_id,
            start_time=datetime.now(),
            total_items=batch_size,
            memory_usage_mb=self._get_memory_usage()
        )
        
        self.batches[batch_id] = batch
        self.current_batch = batch
        
        self.detailed_logger.info(f"🚀 배치 {batch_id} 시작: {batch_size}개 아이템")
        
        # 콜백 실행
        for callback in self.batch_callbacks:
            try:
                callback(batch)
            except Exception as e:
                logger.warning(f"배치 콜백 실행 실패: {e}")
        
        return batch
    
    def update_batch_progress(
        self, 
        batch_id: int, 
        processed: int, 
        successful: int, 
        failed: int,
        error_details: Optional[List[Dict[str, Any]]] = None
    ):
        """배치 진행 상황 업데이트"""
        if batch_id not in self.batches:
            logger.warning(f"알 수 없는 배치 ID: {batch_id}")
            return
        
        batch = self.batches[batch_id]
        batch.processed_items = processed
        batch.successful_items = successful
        batch.failed_items = failed
        
        if error_details:
            batch.error_details.extend(error_details)
        
        # 처리 속도 계산
        if batch.duration_seconds > 0:
            batch.processing_rate = processed / batch.duration_seconds
        
        # 메모리 사용량 업데이트
        current_memory = self._get_memory_usage()
        batch.memory_usage_mb = current_memory
        if current_memory > self.session.peak_memory_usage_mb:
            self.session.peak_memory_usage_mb = current_memory
        
        self.detailed_logger.debug(
            f"📈 배치 {batch_id} 진행: {processed}/{batch.total_items} "
            f"(성공: {successful}, 실패: {failed}, 속도: {batch.processing_rate:.2f}/s)"
        )
        
        # 콜백 실행
        for callback in self.batch_callbacks:
            try:
                callback(batch)
            except Exception as e:
                logger.warning(f"배치 진행 콜백 실행 실패: {e}")
    
    def complete_batch(self, batch_id: int):
        """배치 처리 완료"""
        if batch_id not in self.batches:
            logger.warning(f"알 수 없는 배치 ID: {batch_id}")
            return
        
        batch = self.batches[batch_id]
        batch.end_time = datetime.now()
        
        # 세션 통계 업데이트
        self.session.completed_batches += 1
        self.session.processed_items += batch.processed_items
        self.session.successful_items += batch.successful_items
        self.session.failed_items += batch.failed_items
        
        # 평균 처리 속도 계산
        completed_batches = [b for b in self.batches.values() if b.is_completed]
        if completed_batches:
            total_rate = sum(b.processing_rate for b in completed_batches)
            self.session.average_processing_rate = total_rate / len(completed_batches)
        
        self.detailed_logger.info(
            f"✅ 배치 {batch_id} 완료: {batch.duration_seconds:.1f}초, "
            f"성공률: {batch.success_rate:.1f}%, 속도: {batch.processing_rate:.2f}/s"
        )
        
        if batch.failed_items > 0:
            self.detailed_logger.warning(f"⚠️ 배치 {batch_id}에서 {batch.failed_items}개 실패")
            for error in batch.error_details:
                self.detailed_logger.error(f"   - {error}")
        
        self.current_batch = None
        self._save_progress()
        
        # 콜백 실행
        for callback in self.batch_callbacks:
            try:
                callback(batch)
            except Exception as e:
                logger.warning(f"배치 완료 콜백 실행 실패: {e}")
        
        # 세션 콜백 실행
        for callback in self.progress_callbacks:
            try:
                callback(self.session)
            except Exception as e:
                logger.warning(f"진행률 콜백 실행 실패: {e}")
    
    def complete_session(self):
        """전체 세션 완료"""
        self.session.end_time = datetime.now()
        
        total_duration = (self.session.end_time - self.session.start_time).total_seconds()
        
        self.detailed_logger.info("🎉 전체 처리 세션 완료!")
        self.detailed_logger.info(f"   - 총 처리 시간: {total_duration:.1f}초")
        self.detailed_logger.info(f"   - 총 처리 아이템: {self.session.processed_items}/{self.session.total_items}")
        self.detailed_logger.info(f"   - 성공률: {(self.session.successful_items/self.session.processed_items*100):.1f}%")
        self.detailed_logger.info(f"   - 평균 처리 속도: {self.session.average_processing_rate:.2f}/s")
        self.detailed_logger.info(f"   - 최대 메모리 사용량: {self.session.peak_memory_usage_mb:.1f}MB")
        
        self._save_progress()
        self.stop_monitoring()
        
        # 콜백 실행
        for callback in self.progress_callbacks:
            try:
                callback(self.session)
            except Exception as e:
                logger.warning(f"세션 완료 콜백 실행 실패: {e}")
    
    def add_progress_callback(self, callback: Callable[[ProcessingSession], None]):
        """진행률 콜백 추가"""
        self.progress_callbacks.append(callback)
    
    def add_batch_callback(self, callback: Callable[[BatchProgress], None]):
        """배치 콜백 추가"""
        self.batch_callbacks.append(callback)
    
    def start_monitoring(self, interval_seconds: int = 10):
        """실시간 모니터링 시작"""
        if self._monitoring_active:
            return
        
        self._monitoring_active = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(interval_seconds,),
            daemon=True
        )
        self._monitor_thread.start()
        
        logger.info(f"🔍 실시간 모니터링 시작 (간격: {interval_seconds}초)")
    
    def stop_monitoring(self):
        """실시간 모니터링 중지"""
        self._monitoring_active = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=1)
        
        logger.info("🔍 실시간 모니터링 중지")
    
    def _monitor_loop(self, interval_seconds: int):
        """모니터링 루프 (별도 스레드에서 실행)"""
        while self._monitoring_active:
            try:
                # 현재 상태 로깅
                memory_usage = self._get_memory_usage()
                cpu_percent = psutil.cpu_percent()
                
                self.detailed_logger.debug(
                    f"📊 시스템 상태 - 메모리: {memory_usage:.1f}MB, CPU: {cpu_percent:.1f}%"
                )
                
                if self.current_batch:
                    self.detailed_logger.debug(
                        f"📈 현재 배치 {self.current_batch.batch_id}: "
                        f"{self.current_batch.processed_items}/{self.current_batch.total_items} "
                        f"({self.current_batch.success_rate:.1f}% 성공률)"
                    )
                
                # 전체 진행률 로깅
                self.detailed_logger.debug(
                    f"🎯 전체 진행률: {self.session.completion_percentage:.1f}% "
                    f"({self.session.processed_items}/{self.session.total_items})"
                )
                
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"모니터링 루프 오류: {e}")
                time.sleep(interval_seconds)
    
    def _get_memory_usage(self) -> float:
        """현재 메모리 사용량 (MB)"""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0.0
    
    def _save_progress(self):
        """진행 상황을 파일에 저장"""
        try:
            progress_data = {
                "session": self.session.to_dict(),
                "batches": {
                    str(batch_id): batch.to_dict() 
                    for batch_id, batch in self.batches.items()
                },
                "last_updated": datetime.now().isoformat()
            }
            
            with open(self.progress_log_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"진행 상황 저장 실패: {e}")
    
    def load_progress(self) -> bool:
        """이전 진행 상황 로드"""
        try:
            if not self.progress_log_file.exists():
                return False
            
            with open(self.progress_log_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 세션 정보 복원
            session_data = data["session"]
            self.session.total_batches = session_data.get("total_batches", 0)
            self.session.completed_batches = session_data.get("completed_batches", 0)
            self.session.total_items = session_data.get("total_items", 0)
            self.session.processed_items = session_data.get("processed_items", 0)
            self.session.successful_items = session_data.get("successful_items", 0)
            self.session.failed_items = session_data.get("failed_items", 0)
            
            # 배치 정보 복원
            for batch_id_str, batch_data in data["batches"].items():
                batch_id = int(batch_id_str)
                batch = BatchProgress(
                    batch_id=batch_id,
                    start_time=datetime.fromisoformat(batch_data["start_time"]),
                    total_items=batch_data["total_items"],
                    processed_items=batch_data["processed_items"],
                    successful_items=batch_data["successful_items"],
                    failed_items=batch_data["failed_items"],
                    error_details=batch_data.get("error_details", []),
                    processing_rate=batch_data.get("processing_rate", 0.0),
                    memory_usage_mb=batch_data.get("memory_usage_mb", 0.0)
                )
                
                if batch_data.get("end_time"):
                    batch.end_time = datetime.fromisoformat(batch_data["end_time"])
                
                self.batches[batch_id] = batch
            
            logger.info(f"📂 이전 진행 상황 로드 완료: {len(self.batches)}개 배치")
            return True
            
        except Exception as e:
            logger.error(f"진행 상황 로드 실패: {e}")
            return False
    
    def get_summary(self) -> Dict[str, Any]:
        """현재 진행 상황 요약"""
        return {
            "session": self.session.to_dict(),
            "current_batch": self.current_batch.to_dict() if self.current_batch else None,
            "completed_batches": len([b for b in self.batches.values() if b.is_completed]),
            "total_batches": len(self.batches),
            "system_memory_mb": self._get_memory_usage(),
            "system_cpu_percent": psutil.cpu_percent()
        }

@asynccontextmanager
async def track_progress(session_id: str, total_items: int, total_batches: int):
    """진행률 추적 컨텍스트 매니저"""
    tracker = ProgressTracker(session_id)
    tracker.set_total_items(total_items, total_batches)
    tracker.start_monitoring()
    
    try:
        yield tracker
    finally:
        tracker.complete_session()

# 사용 예시 및 테스트 함수
async def demo_progress_tracking():
    """진행률 추적 시스템 데모"""
    session_id = f"demo_{int(time.time())}"
    total_items = 100
    total_batches = 10
    
    async with track_progress(session_id, total_items, total_batches) as tracker:
        # 진행률 콜백 추가
        def on_progress(session: ProcessingSession):
            print(f"📊 전체 진행률: {session.completion_percentage:.1f}%")
        
        tracker.add_progress_callback(on_progress)
        
        # 배치 처리 시뮬레이션
        for batch_id in range(total_batches):
            batch_size = 10
            batch = tracker.start_batch(batch_id, batch_size)
            
            # 배치 내 아이템 처리 시뮬레이션
            for i in range(batch_size):
                await asyncio.sleep(0.1)  # 처리 시뮬레이션
                
                # 가끔 실패 시뮬레이션
                success = i % 7 != 0  # 7번째마다 실패
                
                if success:
                    tracker.update_batch_progress(
                        batch_id, i + 1, i + 1, 0
                    )
                else:
                    tracker.update_batch_progress(
                        batch_id, i + 1, i, 1,
                        [{"item": i, "error": "시뮬레이션 실패"}]
                    )
            
            tracker.complete_batch(batch_id)
            print(f"✅ 배치 {batch_id} 완료")

if __name__ == "__main__":
    # 데모 실행
    asyncio.run(demo_progress_tracking()) 