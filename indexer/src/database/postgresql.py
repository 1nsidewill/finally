# src/database/postgresql.py
import asyncio
import asyncpg
from typing import AsyncGenerator, List, Dict, Any, Optional
import logging
from contextlib import asynccontextmanager
from src.config import get_settings

logger = logging.getLogger(__name__)

class PostgreSQLManager:
    """PostgreSQL 연결 및 세션 관리자"""
    
    def __init__(self):
        self.config = get_settings()
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_lock = asyncio.Lock()
    
    async def get_pool(self) -> asyncpg.Pool:
        """연결 풀 가져오기 (Lazy Loading)"""
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:
                    try:
                        self._pool = await asyncpg.create_pool(
                            user=self.config.POSTGRES_USER,
                            password=self.config.POSTGRES_PASSWORD,
                            database=self.config.POSTGRES_DB,
                            host=self.config.POSTGRES_HOST,
                            port=self.config.POSTGRES_PORT,
                            min_size=5,  # 최소 연결 수
                            max_size=20,  # 최대 연결 수
                            command_timeout=60,  # 명령 타임아웃 (초)
                            server_settings={
                                'jit': 'off'  # JIT 비활성화로 성능 안정화
                            }
                        )
                        logger.info("PostgreSQL 연결 풀이 성공적으로 생성되었습니다")
                    except Exception as e:
                        logger.error(f"PostgreSQL 연결 풀 생성 실패: {e}")
                        raise
        return self._pool
    
    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """연결 풀에서 연결 가져오기 (Context Manager)"""
        pool = await self.get_pool()
        connection = None
        try:
            connection = await pool.acquire()
            logger.debug("PostgreSQL 연결을 풀에서 가져왔습니다")
            yield connection
        except Exception as e:
            logger.error(f"PostgreSQL 연결 오류: {e}")
            raise
        finally:
            if connection:
                await pool.release(connection)
                logger.debug("PostgreSQL 연결을 풀에 반환했습니다")
    
    async def execute_query(self, query: str, *args) -> List[asyncpg.Record]:
        """SELECT 쿼리 실행"""
        async with self.get_connection() as conn:
            try:
                result = await conn.fetch(query, *args)
                logger.debug(f"쿼리 실행 성공: {len(result)}개 행 반환")
                return result
            except Exception as e:
                logger.error(f"쿼리 실행 실패: {query[:100]}... - {e}")
                raise
    
    async def execute_single(self, query: str, *args) -> Optional[asyncpg.Record]:
        """단일 행 SELECT 쿼리 실행"""
        async with self.get_connection() as conn:
            try:
                result = await conn.fetchrow(query, *args)
                logger.debug("단일 행 쿼리 실행 성공")
                return result
            except Exception as e:
                logger.error(f"단일 행 쿼리 실행 실패: {query[:100]}... - {e}")
                raise
    
    async def execute_command(self, query: str, *args) -> str:
        """INSERT/UPDATE/DELETE 쿼리 실행"""
        async with self.get_connection() as conn:
            try:
                result = await conn.execute(query, *args)
                logger.debug(f"명령 실행 성공: {result}")
                return result
            except Exception as e:
                logger.error(f"명령 실행 실패: {query[:100]}... - {e}")
                raise
    
    async def execute_batch(self, query: str, args_list: List[tuple]) -> None:
        """배치 INSERT/UPDATE 실행"""
        async with self.get_connection() as conn:
            try:
                await conn.executemany(query, args_list)
                logger.debug(f"배치 실행 성공: {len(args_list)}개 행 처리")
            except Exception as e:
                logger.error(f"배치 실행 실패: {query[:100]}... - {e}")
                raise
    
    async def get_products_by_conversion_status(
        self, 
        is_conversion: bool = False, 
        limit: int = 1000
    ) -> List[asyncpg.Record]:
        """is_conversion 상태별 제품 조회"""
        query = """
            SELECT uid, title, content, price, created_dt, updated_dt, is_conversion
            FROM product 
            WHERE is_conversion = $1
            ORDER BY updated_dt DESC
            LIMIT $2
        """
        return await self.execute_query(query, is_conversion, limit)
    
    async def update_conversion_status(self, product_ids: List[int], status: bool) -> str:
        """제품들의 is_conversion 상태 업데이트"""
        query = """
            UPDATE product 
            SET is_conversion = $1, updated_dt = NOW()
            WHERE uid = ANY($2)
        """
        return await self.execute_command(query, status, product_ids)
    
    async def get_products_for_sync(self, batch_size: int = 100) -> List[asyncpg.Record]:
        """동기화가 필요한 제품들 조회 (is_conversion=false)"""
        query = """
            SELECT uid, title, content, price, created_dt, updated_dt
            FROM product 
            WHERE is_conversion = false
            ORDER BY updated_dt ASC
            LIMIT $1
        """
        return await self.execute_query(query, batch_size)
    
    async def health_check(self) -> bool:
        """PostgreSQL 연결 상태 확인"""
        try:
            result = await self.execute_single("SELECT 1 as health")
            return result['health'] == 1 if result else False
        except Exception as e:
            logger.error(f"PostgreSQL 헬스체크 실패: {e}")
            return False
    
    async def close(self):
        """연결 풀 종료"""
        if self._pool:
            await self._pool.close()
            logger.info("PostgreSQL 연결 풀이 종료되었습니다")

# 전역 인스턴스 (싱글톤 패턴)
postgres_manager = PostgreSQLManager()

# 편의 함수들
async def get_connection():
    """PostgreSQL 연결 가져오기"""
    async with postgres_manager.get_connection() as conn:
        yield conn

async def execute_query(query: str, *args) -> List[asyncpg.Record]:
    """SELECT 쿼리 실행"""
    return await postgres_manager.execute_query(query, *args)

async def execute_command(query: str, *args) -> str:
    """INSERT/UPDATE/DELETE 쿼리 실행"""
    return await postgres_manager.execute_command(query, *args)