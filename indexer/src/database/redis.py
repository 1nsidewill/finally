"""
Redis 연결 풀 및 큐 관리
redis.asyncio를 사용한 비동기 Redis 클라이언트 관리
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Union
import json
from contextlib import asynccontextmanager
import redis.asyncio as redis
from src.config import get_settings

logger = logging.getLogger(__name__)

class RedisManager:
    """Redis 연결 풀 및 큐 관리자"""
    
    def __init__(self):
        self.config = get_settings()
        self._pool: Optional[redis.ConnectionPool] = None
        self._redis: Optional[redis.Redis] = None
        
        # 큐 관련 설정
        self.queue_name = self.config.REDIS_QUEUE_NAME
        self.batch_size = self.config.REDIS_BATCH_SIZE
        self.poll_interval = self.config.REDIS_POLL_INTERVAL
        self.blocking_timeout = self.config.REDIS_BLOCKING_TIMEOUT
        
        logger.info(f"Redis 매니저 초기화 - Queue: {self.queue_name}")
    
    async def get_connection_pool(self) -> redis.ConnectionPool:
        """Redis 연결 풀 생성 및 반환"""
        if self._pool is None:
            try:
                # Redis URL이 설정된 경우 우선 사용
                if self.config.REDIS_URL:
                    self._pool = redis.ConnectionPool.from_url(
                        self.config.REDIS_URL,
                        max_connections=self.config.REDIS_MAX_CONNECTIONS,
                        socket_connect_timeout=self.config.REDIS_CONNECTION_TIMEOUT,
                        retry_on_timeout=self.config.REDIS_RETRY_ON_TIMEOUT,
                        encoding='utf-8',
                        decode_responses=True
                    )
                else:
                    # 개별 설정으로 연결 풀 생성
                    self._pool = redis.ConnectionPool(
                        host=self.config.REDIS_HOST,
                        port=self.config.REDIS_PORT,
                        db=self.config.REDIS_DB,
                        password=self.config.REDIS_PASSWORD,
                        max_connections=self.config.REDIS_MAX_CONNECTIONS,
                        socket_connect_timeout=self.config.REDIS_CONNECTION_TIMEOUT,
                        retry_on_timeout=self.config.REDIS_RETRY_ON_TIMEOUT,
                        encoding='utf-8',
                        decode_responses=True
                    )
                
                logger.info(f"Redis 연결 풀 생성 완료: {self.config.REDIS_HOST}:{self.config.REDIS_PORT}")
                
            except Exception as e:
                logger.error(f"Redis 연결 풀 생성 실패: {e}")
                raise
        
        return self._pool
    
    async def get_redis_client(self) -> redis.Redis:
        """Redis 클라이언트 반환"""
        if self._redis is None:
            pool = await self.get_connection_pool()
            self._redis = redis.Redis(connection_pool=pool)
        
        return self._redis
    
    @asynccontextmanager
    async def get_connection(self):
        """Redis 연결 컨텍스트 매니저"""
        redis_client = await self.get_redis_client()
        try:
            yield redis_client
        except Exception as e:
            logger.error(f"Redis 연결 사용 중 오류: {e}")
            raise
    
    async def health_check(self) -> bool:
        """Redis 연결 상태 확인"""
        try:
            async with self.get_connection() as redis_client:
                result = await redis_client.ping()
                logger.debug(f"Redis 헬스체크 성공: {result}")
                return result
        except Exception as e:
            logger.error(f"Redis 헬스체크 실패: {e}")
            return False
    
    # ======= 큐 관련 메서드들 =======
    
    async def push_job(self, job_data: Dict[str, Any], queue_name: Optional[str] = None) -> bool:
        """작업을 큐에 추가"""
        try:
            queue = queue_name or self.queue_name
            job_json = json.dumps(job_data, ensure_ascii=False)
            
            async with self.get_connection() as redis_client:
                result = await redis_client.lpush(queue, job_json)
                logger.debug(f"작업 추가 완료: {queue} -> {result}")
                return True
                
        except Exception as e:
            logger.error(f"작업 추가 실패: {e}")
            return False
    
    async def push_jobs_batch(self, jobs: List[Dict[str, Any]], queue_name: Optional[str] = None) -> int:
        """여러 작업을 배치로 큐에 추가"""
        try:
            queue = queue_name or self.queue_name
            job_jsons = [json.dumps(job, ensure_ascii=False) for job in jobs]
            
            async with self.get_connection() as redis_client:
                result = await redis_client.lpush(queue, *job_jsons)
                logger.debug(f"배치 작업 추가 완료: {queue} -> {len(job_jsons)}개")
                return len(job_jsons)
                
        except Exception as e:
            logger.error(f"배치 작업 추가 실패: {e}")
            return 0
    
    async def pop_job(self, queue_name: Optional[str] = None, timeout: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """큐에서 작업을 하나 가져오기 (블로킹)"""
        try:
            queue = queue_name or self.queue_name
            timeout = timeout or self.blocking_timeout
            
            async with self.get_connection() as redis_client:
                result = await redis_client.brpop(queue, timeout=timeout)
                
                if result:
                    _, job_json = result
                    job_data = json.loads(job_json)
                    logger.debug(f"작업 팝 완료: {queue}")
                    return job_data
                else:
                    logger.debug(f"큐에서 대기 시간 초과: {queue}")
                    return None
                    
        except Exception as e:
            logger.error(f"작업 팝 실패: {e}")
            return None
    
    async def pop_jobs_batch(self, batch_size: Optional[int] = None, queue_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """큐에서 여러 작업을 배치로 가져오기 (논블로킹)"""
        try:
            queue = queue_name or self.queue_name
            batch_size = batch_size or self.batch_size
            
            jobs = []
            async with self.get_connection() as redis_client:
                # 파이프라인을 사용해서 한 번에 여러 개 가져오기
                pipe = redis_client.pipeline()
                
                for _ in range(batch_size):
                    pipe.rpop(queue)
                
                results = await pipe.execute()
                
                for result in results:
                    if result:
                        job_data = json.loads(result)
                        jobs.append(job_data)
                
                if jobs:
                    logger.debug(f"배치 작업 팝 완료: {queue} -> {len(jobs)}개")
                
                return jobs
                
        except Exception as e:
            logger.error(f"배치 작업 팝 실패: {e}")
            return []
    
    async def get_queue_length(self, queue_name: Optional[str] = None) -> int:
        """큐의 작업 개수 반환"""
        try:
            queue = queue_name or self.queue_name
            
            async with self.get_connection() as redis_client:
                length = await redis_client.llen(queue)
                logger.debug(f"큐 길이: {queue} -> {length}")
                return length
                
        except Exception as e:
            logger.error(f"큐 길이 조회 실패: {e}")
            return 0
    
    async def clear_queue(self, queue_name: Optional[str] = None) -> bool:
        """큐 비우기"""
        try:
            queue = queue_name or self.queue_name
            
            async with self.get_connection() as redis_client:
                await redis_client.delete(queue)
                logger.info(f"큐 클리어 완료: {queue}")
                return True
                
        except Exception as e:
            logger.error(f"큐 클리어 실패: {e}")
            return False
    
    async def peek_jobs(self, count: int = 10, queue_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """큐의 작업들을 제거하지 않고 미리보기"""
        try:
            queue = queue_name or self.queue_name
            
            async with self.get_connection() as redis_client:
                job_jsons = await redis_client.lrange(queue, -count, -1)
                
                jobs = []
                for job_json in job_jsons:
                    job_data = json.loads(job_json)
                    jobs.append(job_data)
                
                logger.debug(f"큐 미리보기: {queue} -> {len(jobs)}개")
                return jobs
                
        except Exception as e:
            logger.error(f"큐 미리보기 실패: {e}")
            return []
    
    # ======= 일반 Redis 연산 =======
    
    async def set_value(self, key: str, value: Any, expire: Optional[int] = None) -> bool:
        """키-값 저장"""
        try:
            async with self.get_connection() as redis_client:
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False)
                
                await redis_client.set(key, value, ex=expire)
                logger.debug(f"값 저장 완료: {key}")
                return True
                
        except Exception as e:
            logger.error(f"값 저장 실패: {key} -> {e}")
            return False
    
    async def get_value(self, key: str, default: Any = None) -> Any:
        """키로 값 조회"""
        try:
            async with self.get_connection() as redis_client:
                value = await redis_client.get(key)
                
                if value is None:
                    return default
                
                # JSON 파싱 시도
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    return value
                    
        except Exception as e:
            logger.error(f"값 조회 실패: {key} -> {e}")
            return default
    
    async def delete_key(self, key: str) -> bool:
        """키 삭제"""
        try:
            async with self.get_connection() as redis_client:
                result = await redis_client.delete(key)
                logger.debug(f"키 삭제 완료: {key} -> {result}")
                return result > 0
                
        except Exception as e:
            logger.error(f"키 삭제 실패: {key} -> {e}")
            return False
    
    async def close(self):
        """연결 풀 종료"""
        try:
            if self._redis:
                await self._redis.aclose()
                logger.info("Redis 클라이언트 연결 종료")
            
            if self._pool:
                await self._pool.aclose()
                logger.info("Redis 연결 풀 종료")
                
        except Exception as e:
            logger.error(f"Redis 연결 종료 중 오류: {e}")

# 전역 인스턴스 (싱글톤 패턴)
redis_manager = RedisManager()

# 편의 함수들
async def get_redis_client() -> redis.Redis:
    """Redis 클라이언트 가져오기"""
    return await redis_manager.get_redis_client()

async def push_job(job_data: Dict[str, Any], queue_name: Optional[str] = None) -> bool:
    """작업을 큐에 추가하는 편의 함수"""
    return await redis_manager.push_job(job_data, queue_name)

async def pop_job(queue_name: Optional[str] = None, timeout: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """큐에서 작업을 가져오는 편의 함수"""
    return await redis_manager.pop_job(queue_name, timeout) 