"""
Job Processor for Redis Queue Worker

PostgreSQL과 Qdrant 간 데이터 동기화를 위한 작업 처리 모듈
- SYNC: 새로운 제품 추가/전체 동기화
- UPDATE: 기존 제품 정보 변경  
- DELETE: 제품 삭제
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import json
import uuid

from src.config import get_settings
from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager
from src.services.text_preprocessor import ProductTextPreprocessor
from src.services.embedding_service import EmbeddingService
from src.services.error_handler import ErrorHandler, ErrorContext

logger = logging.getLogger(__name__)

class JobType(Enum):
    """작업 타입"""
    SYNC = "sync"       # 새로운 제품 추가/동기화
    UPDATE = "update"   # 기존 제품 정보 업데이트
    DELETE = "delete"   # 제품 삭제

@dataclass
class JobResult:
    """작업 처리 결과"""
    job_id: str
    job_type: JobType
    product_id: str
    success: bool
    message: str
    processing_time: float
    error: Optional[str] = None
    vector_id: Optional[str] = None

@dataclass
class ProductData:
    """제품 데이터 구조"""
    pid: str
    title: str
    price: Optional[int] = None
    content: Optional[str] = None
    year: Optional[int] = None
    mileage: Optional[int] = None
    page_url: Optional[str] = None
    images: List[str] = None
    
    def __post_init__(self):
        # 페이지 URL 생성
        if not self.page_url and self.pid:
            self.page_url = f"https://m.bunjang.co.kr/products/{self.pid}"
        
        if self.images is None:
            self.images = []

class JobProcessor:
    """Redis Queue Job Processor"""
    
    def __init__(self):
        self.settings = get_settings()
        self.postgresql_manager = None
        self.qdrant_manager = None
        self.text_preprocessor = ProductTextPreprocessor()
        self.embedding_service = EmbeddingService()
        self.error_handler = ErrorHandler()
        
        # 통계
        self.stats = {
            'total_processed': 0,
            'sync_count': 0,
            'update_count': 0,
            'delete_count': 0,
            'success_count': 0,
            'error_count': 0,
        }
    
    async def initialize(self):
        """데이터베이스 매니저 초기화"""
        try:
            # PostgreSQL 매니저 초기화
            self.postgresql_manager = PostgreSQLManager()
            # PostgreSQL은 lazy loading이므로 pool 생성 테스트
            await self.postgresql_manager.get_pool()
            
            # Qdrant 매니저 초기화
            self.qdrant_manager = QdrantManager()
            # Qdrant는 lazy loading이므로 클라이언트 생성 테스트 및 컬렉션 확인
            await self.qdrant_manager.get_async_client()
            await self.qdrant_manager.create_collection_if_not_exists()
            
            # ErrorHandler 초기화
            await self.error_handler.initialize()
            
            logger.info("✅ JobProcessor 초기화 완료")
            
        except Exception as e:
            logger.error(f"❌ JobProcessor 초기화 실패: {e}")
            raise
    
    async def close(self):
        """리소스 정리"""
        if self.postgresql_manager:
            await self.postgresql_manager.close()
        if self.qdrant_manager:
            await self.qdrant_manager.close()
        # EmbeddingService는 close 메서드가 없음 (stateless)
        if self.error_handler:
            await self.error_handler.close()
        
        logger.info("🔹 JobProcessor 리소스 정리 완료")
    
    async def process_job(self, job_data: Dict[str, Any]) -> JobResult:
        """단일 작업 처리"""
        import time
        start_time = time.time()
        
        try:
            # 작업 데이터 파싱
            job_id = job_data.get('id', str(uuid.uuid4()))
            job_type_str = job_data.get('type', '').lower()
            product_id = str(job_data.get('product_id', ''))
            
            # 작업 타입 검증
            try:
                job_type = JobType(job_type_str)
            except ValueError:
                raise ValueError(f"지원하지 않는 작업 타입: {job_type_str}")
            
            if not product_id:
                raise ValueError("product_id가 필요합니다")
            
            # 작업 타입별 처리
            result = None
            if job_type == JobType.SYNC:
                result = await self._process_sync(job_id, product_id)
            elif job_type == JobType.UPDATE:
                result = await self._process_update(job_id, product_id)
            elif job_type == JobType.DELETE:
                result = await self._process_delete(job_id, product_id)
            
            # 통계 업데이트
            self.stats['total_processed'] += 1
            self.stats[f'{job_type.value}_count'] += 1
            if result.success:
                self.stats['success_count'] += 1
            else:
                self.stats['error_count'] += 1
            
            processing_time = time.time() - start_time
            result.processing_time = processing_time
            
            logger.info(f"✅ 작업 처리 완료: {job_type.value} - {product_id} ({processing_time:.2f}s)")
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = str(e)
            
            # ErrorHandler로 오류 처리
            try:
                context = ErrorContext(
                    job_id=job_id if 'job_id' in locals() else str(uuid.uuid4()),
                    job_type=job_type.value if 'job_type' in locals() else 'unknown',
                    product_id=product_id if 'product_id' in locals() else '',
                    operation_step='job_processing',
                    additional_data={
                        'processing_time': processing_time,
                        'job_data': job_data
                    }
                )
                await self.error_handler.handle_error(e, context)
            except Exception as error_handling_exception:
                logger.error(f"🚨 오류 처리 중 예외 발생: {error_handling_exception}")
            
            self.stats['total_processed'] += 1
            self.stats['error_count'] += 1
            
            logger.error(f"❌ 작업 처리 실패: {job_id} - {error_msg}")
            
            return JobResult(
                job_id=job_id if 'job_id' in locals() else str(uuid.uuid4()),
                job_type=job_type if 'job_type' in locals() else JobType.SYNC,
                product_id=product_id if 'product_id' in locals() else '',
                success=False,
                message=f"작업 처리 실패: {error_msg}",
                processing_time=processing_time,
                error=error_msg
            )
    
    async def process_jobs_batch(self, jobs: List[Dict[str, Any]]) -> List[JobResult]:
        """배치 작업 처리"""
        if not jobs:
            return []
        
        logger.info(f"🔄 배치 작업 처리 시작: {len(jobs)}개")
        
        # 병렬 처리
        tasks = [self.process_job(job) for job in jobs]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 예외 처리
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                job_data = jobs[i] if i < len(jobs) else {}
                error_result = JobResult(
                    job_id=job_data.get('id', str(uuid.uuid4())),
                    job_type=JobType.SYNC,
                    product_id=str(job_data.get('product_id', '')),
                    success=False,
                    message=f"배치 처리 중 예외 발생: {result}",
                    processing_time=0.0,
                    error=str(result)
                )
                processed_results.append(error_result)
            else:
                processed_results.append(result)
        
        success_count = sum(1 for r in processed_results if r.success)
        logger.info(f"✅ 배치 작업 완료: {success_count}/{len(jobs)} 성공")
        
        return processed_results
    
    async def _fetch_product_data(self, product_id: str) -> ProductData:
        """PostgreSQL에서 제품 데이터 조회"""
        try:
            async with self.postgresql_manager.get_connection() as conn:
                # product 테이블에서 기본 정보 조회
                product_query = """
                    SELECT pid, title, price, content, year, mileage
                    FROM product 
                    WHERE pid = $1
                """
                product_row = await conn.fetchrow(product_query, product_id)
                
                if not product_row:
                    raise ValueError(f"제품을 찾을 수 없습니다: {product_id}")
                
                # file 테이블에서 이미지 URL 조회 (product_uid로 조인)
                file_query = """
                    SELECT url, count 
                    FROM file 
                    WHERE product_uid = $1 
                    ORDER BY count
                """
                file_results = await conn.fetch(file_query, product_id)
                
                # 이미지 URL 리스트 구성
                images = []
                for file_row in file_results:
                    url_template = file_row['url']
                    count = file_row['count']
                    # {cnt}를 실제 count 값으로 교체
                    if '{cnt}' in url_template:
                        image_url = url_template.replace('{cnt}', str(count))
                        images.append(image_url)
                    else:
                        images.append(url_template)
                
                # ProductData 객체 생성
                product_data = ProductData(
                    pid=product_row['pid'],
                    title=product_row['title'] or '',
                    price=product_row['price'],
                    content=product_row['content'] or '',
                    year=product_row['year'],
                    mileage=product_row['mileage'],
                    images=images
                )
                
                logger.debug(f"🔍 제품 데이터 조회 완료: {product_id} (이미지 {len(images)}개)")
                return product_data
            
        except Exception as e:
            logger.error(f"❌ 제품 데이터 조회 실패: {product_id} - {e}")
            raise
    
    async def _process_sync(self, job_id: str, product_id: str) -> JobResult:
        """SYNC 작업 처리: 새로운 제품 추가/동기화"""
        operation_step = "sync_init"
        
        try:
            # 1. PostgreSQL에서 제품 데이터 조회
            operation_step = "data_fetch"
            product_data = await self._fetch_product_data(product_id)
            
            # 2. 텍스트 전처리
            operation_step = "text_preprocessing"
            processed_text = self.text_preprocessor.preprocess_product_data({
                'title': product_data.title,
                'year': product_data.year,
                'price': product_data.price,
                'mileage': product_data.mileage,
                'content': product_data.content
            })
            
            # 3. 임베딩 생성
            operation_step = "embedding_generation"
            embedding = await self.embedding_service.get_embedding_async(processed_text)
            
            # 4. Qdrant에 벡터 삽입
            operation_step = "vector_insertion"
            vector_id = str(uuid.uuid4())
            metadata = {
                'product_id': product_data.pid,
                'title': product_data.title,
                'price': product_data.price,
                'year': product_data.year,
                'mileage': product_data.mileage,
                'page_url': product_data.page_url,
                'images': product_data.images,
                'processed_text': processed_text,
                'synced_at': asyncio.get_event_loop().time()
            }
            
            await self.qdrant_manager.upsert_vector_async(
                vector_id=vector_id,
                vector=embedding,
                metadata=metadata
            )
            
            return JobResult(
                job_id=job_id,
                job_type=JobType.SYNC,
                product_id=product_id,
                success=True,
                message=f"제품 동기화 완료: {product_data.title}",
                processing_time=0.0,  # 나중에 계산됨
                vector_id=vector_id
            )
            
        except Exception as e:
            # 단계별 오류 처리
            try:
                context = ErrorContext(
                    job_id=job_id,
                    job_type='sync',
                    product_id=product_id,
                    operation_step=operation_step,
                    additional_data={
                        'processed_text': processed_text if 'processed_text' in locals() else None,
                        'product_data': product_data.__dict__ if 'product_data' in locals() else None
                    }
                )
                await self.error_handler.handle_error(e, context)
            except Exception as error_handling_exception:
                logger.error(f"🚨 SYNC 오류 처리 중 예외: {error_handling_exception}")
            
            return JobResult(
                job_id=job_id,
                job_type=JobType.SYNC,
                product_id=product_id,
                success=False,
                message=f"SYNC 실패 ({operation_step}): {str(e)}",
                processing_time=0.0,
                error=str(e)
            )
    
    async def _process_update(self, job_id: str, product_id: str) -> JobResult:
        """UPDATE 작업 처리: 기존 제품 정보 업데이트"""
        try:
            # 1. Qdrant에서 기존 벡터 찾기
            existing_vectors = await self.qdrant_manager.search_vectors(
                query_text="",  # 빈 쿼리로 검색
                filter_conditions={"product_id": product_id},
                limit=1
            )
            
            if not existing_vectors:
                # 기존 벡터가 없으면 SYNC 작업으로 처리
                logger.info(f"🔄 기존 벡터 없음, SYNC로 전환: {product_id}")
                return await self._process_sync(job_id, product_id)
            
            # 2. 새로운 제품 데이터 조회
            product_data = await self._fetch_product_data(product_id)
            
            # 3. 텍스트 전처리
            processed_text = self.text_preprocessor.preprocess_product_data({
                'title': product_data.title,
                'year': product_data.year,
                'price': product_data.price,
                'mileage': product_data.mileage,
                'content': product_data.content
            })
            
            # 4. 새로운 임베딩 생성
            embedding = await self.embedding_service.get_embedding_async(processed_text)
            
            # 5. 기존 벡터 업데이트
            vector_id = existing_vectors[0]['id']
            metadata = {
                'product_id': product_data.pid,
                'title': product_data.title,
                'price': product_data.price,
                'year': product_data.year,
                'mileage': product_data.mileage,
                'page_url': product_data.page_url,
                'images': product_data.images,
                'processed_text': processed_text,
                'updated_at': asyncio.get_event_loop().time()
            }
            
            await self.qdrant_manager.upsert_vector_async(
                vector_id=vector_id,
                vector=embedding,
                metadata=metadata
            )
            
            return JobResult(
                job_id=job_id,
                job_type=JobType.UPDATE,
                product_id=product_id,
                success=True,
                message=f"제품 업데이트 완료: {product_data.title}",
                processing_time=0.0,
                vector_id=vector_id
            )
            
        except Exception as e:
            return JobResult(
                job_id=job_id,
                job_type=JobType.UPDATE,
                product_id=product_id,
                success=False,
                message=f"UPDATE 실패: {str(e)}",
                processing_time=0.0,
                error=str(e)
            )
    
    async def _process_delete(self, job_id: str, product_id: str) -> JobResult:
        """DELETE 작업 처리: 제품 삭제"""
        try:
            # 1. Qdrant에서 해당 제품의 모든 벡터 찾기
            existing_vectors = await self.qdrant_manager.search_vectors(
                query_text="",  # 빈 쿼리로 검색
                filter_conditions={"product_id": product_id},
                limit=100  # 혹시 모를 중복 벡터들까지 모두 찾기
            )
            
            if not existing_vectors:
                return JobResult(
                    job_id=job_id,
                    job_type=JobType.DELETE,
                    product_id=product_id,
                    success=True,
                    message=f"삭제할 벡터가 없음: {product_id}",
                    processing_time=0.0
                )
            
            # 2. 모든 관련 벡터 삭제
            deleted_count = 0
            for vector in existing_vectors:
                try:
                    await self.qdrant_manager.delete_vector(vector['id'])
                    deleted_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ 벡터 삭제 실패: {vector['id']} - {e}")
            
            return JobResult(
                job_id=job_id,
                job_type=JobType.DELETE,
                product_id=product_id,
                success=True,
                message=f"제품 삭제 완료: {deleted_count}개 벡터 삭제",
                processing_time=0.0
            )
            
        except Exception as e:
            return JobResult(
                job_id=job_id,
                job_type=JobType.DELETE,
                product_id=product_id,
                success=False,
                message=f"DELETE 실패: {str(e)}",
                processing_time=0.0,
                error=str(e)
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """처리 통계 반환"""
        return {
            **self.stats,
            'success_rate': (
                self.stats['success_count'] / self.stats['total_processed'] * 100
                if self.stats['total_processed'] > 0 else 0
            )
        }
    
    def reset_stats(self):
        """통계 초기화"""
        for key in self.stats:
            self.stats[key] = 0 