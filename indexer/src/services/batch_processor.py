# src/services/batch_processor.py
import asyncio
import logging
from typing import List, Dict, Any, Optional, Callable, Tuple
import time
from datetime import datetime
from dataclasses import dataclass, field
import json
from pathlib import Path

from .text_preprocessor import ProductTextPreprocessor
from ..database.postgresql import PostgreSQLManager
from ..database.qdrant import QdrantManager

logger = logging.getLogger(__name__)

@dataclass
class BatchConfig:
    """배치 처리 설정"""
    batch_size: int = 100               # 한 번에 처리할 매물 수
    max_concurrent_batches: int = 3     # 동시 배치 수
    delay_between_batches: float = 1.0  # 배치 간 딜레이(초)
    
    # 재시도 설정
    max_retries: int = 3
    retry_delay: float = 5.0
    
    # 진행상황 저장
    save_progress_every: int = 10       # N개 배치마다 진행상황 저장
    progress_file: str = "batch_progress.json"
    
    # 로깅
    log_every: int = 5                  # N개 배치마다 로그 출력

@dataclass
class BatchProgress:
    """배치 처리 진행상황"""
    total_items: int = 0
    processed_items: int = 0
    successful_items: int = 0
    failed_items: int = 0
    current_batch: int = 0
    start_time: Optional[datetime] = None
    last_update: Optional[datetime] = None
    failed_item_ids: List[int] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환 (JSON 직렬화용)"""
        return {
            'total_items': self.total_items,
            'processed_items': self.processed_items,
            'successful_items': self.successful_items,
            'failed_items': self.failed_items,
            'current_batch': self.current_batch,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'failed_item_ids': self.failed_item_ids,
            'completion_percentage': (self.processed_items / self.total_items * 100) if self.total_items > 0 else 0,
            'success_rate': (self.successful_items / self.processed_items * 100) if self.processed_items > 0 else 0
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BatchProgress':
        """딕셔너리에서 생성"""
        progress = cls()
        progress.total_items = data.get('total_items', 0)
        progress.processed_items = data.get('processed_items', 0)
        progress.successful_items = data.get('successful_items', 0)
        progress.failed_items = data.get('failed_items', 0)
        progress.current_batch = data.get('current_batch', 0)
        progress.failed_item_ids = data.get('failed_item_ids', [])
        
        if data.get('start_time'):
            progress.start_time = datetime.fromisoformat(data['start_time'])
        if data.get('last_update'):
            progress.last_update = datetime.fromisoformat(data['last_update'])
            
        return progress

class BatchProcessor:
    """대량 매물 데이터 배치 처리 클래스"""
    
    def __init__(self, 
                 postgres_manager: PostgreSQLManager,
                 qdrant_manager: QdrantManager,
                 config: Optional[BatchConfig] = None):
        """
        Args:
            postgres_manager: PostgreSQL 매니저
            qdrant_manager: Qdrant 매니저
            config: 배치 처리 설정
        """
        self.postgres_manager = postgres_manager
        self.qdrant_manager = qdrant_manager
        
        # config.py에서 기본값 가져오기
        from ..config import get_settings
        settings = get_settings()
        
        # config가 없으면 settings에서 생성
        if config is None:
            config = BatchConfig(
                batch_size=settings.BATCH_SIZE,
                max_retries=settings.MAX_RETRIES,
                retry_delay=settings.RETRY_DELAY,
                save_progress_every=settings.SAVE_PROGRESS_EVERY,
                log_every=settings.LOG_EVERY
            )
        
        self.config = config
        
        # 텍스트 전처리기 초기화
        self.text_preprocessor = ProductTextPreprocessor()
        
        # 진행상황 추적
        self.progress = BatchProgress()
        
        # 진행상황 파일 경로
        self.progress_file_path = Path(self.config.progress_file)
        
        logger.info(f"배치 프로세서 초기화 - 배치 크기: {self.config.batch_size}")
    
    def load_progress(self) -> bool:
        """저장된 진행상황 로드"""
        try:
            if self.progress_file_path.exists():
                with open(self.progress_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.progress = BatchProgress.from_dict(data)
                    logger.info(f"진행상황 로드 완료: {self.progress.processed_items}/{self.progress.total_items} 처리됨")
                    return True
        except Exception as e:
            logger.error(f"진행상황 로드 실패: {e}")
        return False
    
    def save_progress(self):
        """현재 진행상황 저장"""
        try:
            self.progress.last_update = datetime.now()
            with open(self.progress_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.progress.to_dict(), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"진행상황 저장 실패: {e}")
    
    async def get_unprocessed_products(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """미처리 매물 조회 (is_conversion=False인 제품들)"""
        try:
            # PostgreSQL에서 변환되지 않은 제품들 조회
            products = await self.postgres_manager.get_products_by_conversion_status(
                is_conversion=False, 
                limit=limit or 10000
            )
            
            # 딕셔너리 형태로 변환
            product_list = []
            for product in products:
                product_dict = {
                    'id': product['uid'],
                    'title': product['title'] or '',
                    'content': product['content'] or '',
                    'price': product['price'] or 0,
                    'created_dt': product['created_dt'],
                    'updated_dt': product['updated_dt']
                }
                product_list.append(product_dict)
            
            logger.info(f"미처리 제품 {len(product_list)}개 조회 완료")
            return product_list
            
        except Exception as e:
            logger.error(f"미처리 제품 조회 실패: {e}")
            raise

    async def process_products_batch(self, products: List[Dict[str, Any]]) -> Tuple[int, int]:
        """제품 배치 처리
        
        Returns:
            Tuple[성공 개수, 실패 개수]
        """
        successful = 0
        failed = 0
        
        try:
            # 1. 텍스트 전처리 및 임베딩 생성 준비
            texts_to_embed = []
            for product in products:
                # 텍스트 전처리기를 사용해서 텍스트 생성
                processed_text = self.text_preprocessor.preprocess_product_data({
                    'title': product.get('title', ''),
                    'price': product.get('price', 0),
                    'content': product.get('content', '')
                })
                texts_to_embed.append(processed_text)
            
            # 2. 배치로 임베딩 생성 (Qdrant 매니저의 기존 메서드 사용)
            embeddings = await self.qdrant_manager.generate_embeddings_batch(texts_to_embed)
            
            # 3. Qdrant에 벡터 저장 및 PostgreSQL 플래그 업데이트
            for i, (product, embedding) in enumerate(zip(products, embeddings)):
                product_id = product['id']  # 정수 그대로 사용 (UUID 문제 해결)
                
                try:
                    if embedding is not None:
                        # Qdrant에 벡터 저장
                        metadata = {
                            'uid': product['id'],
                            'title': product.get('title', ''),
                            'price': product.get('price', 0),
                            'content': product.get('content', ''),
                            'created_dt': product.get('created_dt').isoformat() if product.get('created_dt') else None,
                            'updated_dt': product.get('updated_dt').isoformat() if product.get('updated_dt') else None,
                            'processed_text': texts_to_embed[i]
                        }
                        
                        await self.qdrant_manager.upsert_vector_async(
                            vector_id=str(product_id),  # 정수를 문자열로 변환
                            vector=embedding,
                            metadata=metadata
                        )
                        
                        # PostgreSQL에서 is_conversion 플래그 업데이트
                        await self._update_conversion_flag(product['id'], True)
                        
                        successful += 1
                    else:
                        # 임베딩 생성 실패
                        await self._log_failed_operation(product['id'], "임베딩 생성 실패")
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"제품 {product['id']} 처리 실패: {e}")
                    await self._log_failed_operation(product['id'], str(e))
                    failed += 1
                    
        except Exception as e:
            logger.error(f"배치 처리 중 전체 실패: {e}")
            failed = len(products)
        
        return successful, failed
    
    async def _update_conversion_flag(self, product_id: int, is_conversion: bool):
        """제품의 변환 플래그 업데이트"""
        try:
            await self.postgres_manager.update_conversion_status([product_id], is_conversion)
        except Exception as e:
            logger.error(f"변환 플래그 업데이트 실패 (ID: {product_id}): {e}")
    
    async def _log_failed_operation(self, product_id: int, error_message: str):
        """실패한 작업 로깅"""
        try:
            # 간단한 로깅 (failed_operations 테이블이 있다면 사용)
            async with self.postgres_manager.get_connection() as conn:
                # 테이블이 존재하는지 확인하고 있으면 사용
                try:
                    await conn.execute("""
                        INSERT INTO failed_operations (product_id, operation_type, error_message, created_at)
                        VALUES ($1, $2, $3, NOW())
                    """, product_id, "embedding_conversion", error_message)
                except Exception:
                    # failed_operations 테이블이 없으면 로그만 남김
                    logger.warning(f"제품 {product_id} 처리 실패: {error_message}")
        except Exception as e:
            logger.error(f"실패 로그 저장 실패: {e}")
    
    async def process_all_products(self, 
                                 resume: bool = True,
                                 progress_callback: Optional[Callable[[BatchProgress], None]] = None) -> BatchProgress:
        """모든 미처리 제품 배치 처리
        
        Args:
            resume: 이전 진행상황에서 재개할지 여부
            progress_callback: 진행상황 콜백 함수
            
        Returns:
            최종 진행상황
        """
        # 진행상황 로드 (resume=True인 경우)
        if resume:
            self.load_progress()
        
        # 미처리 제품 조회
        if self.progress.total_items == 0:
            all_products = await self.get_unprocessed_products()
            self.progress.total_items = len(all_products)
            self.progress.start_time = datetime.now()
            
            if self.progress.total_items == 0:
                logger.info("처리할 제품이 없습니다.")
                return self.progress
        else:
            # 재개 시 남은 제품만 조회
            remaining = self.progress.total_items - self.progress.processed_items
            all_products = await self.get_unprocessed_products(limit=remaining)
        
        logger.info(f"총 {self.progress.total_items}개 제품 배치 처리 시작 (재개: {resume})")
        
        # 배치 단위로 처리
        for i in range(0, len(all_products), self.config.batch_size):
            batch_products = all_products[i:i + self.config.batch_size]
            self.progress.current_batch += 1
            
            # 배치 처리 (재시도 포함)
            batch_successful, batch_failed = await self._process_batch_with_retry(batch_products)
            
            # 진행상황 업데이트
            self.progress.processed_items += len(batch_products)
            self.progress.successful_items += batch_successful
            self.progress.failed_items += batch_failed
            
            # 실패한 제품 ID 기록
            for j, product in enumerate(batch_products):
                if j >= batch_successful:  # 실패한 항목들
                    self.progress.failed_item_ids.append(product['id'])
            
            # 진행상황 콜백 호출
            if progress_callback:
                progress_callback(self.progress)
            
            # 주기적 저장
            if self.progress.current_batch % self.config.save_progress_every == 0:
                self.save_progress()
            
            # 주기적 로깅
            if self.progress.current_batch % self.config.log_every == 0:
                completion = (self.progress.processed_items / self.progress.total_items) * 100
                success_rate = (self.progress.successful_items / self.progress.processed_items) * 100 if self.progress.processed_items > 0 else 0
                logger.info(f"진행률: {completion:.1f}% ({self.progress.processed_items}/{self.progress.total_items}), 성공률: {success_rate:.1f}%")
            
            # 배치 간 딜레이
            if i + self.config.batch_size < len(all_products):
                await asyncio.sleep(self.config.delay_between_batches)
        
        # 최종 저장
        self.save_progress()
        
        # 완료 로그
        elapsed_time = datetime.now() - self.progress.start_time if self.progress.start_time else None
        completion = (self.progress.processed_items / self.progress.total_items) * 100
        success_rate = (self.progress.successful_items / self.progress.processed_items) * 100 if self.progress.processed_items > 0 else 0
        
        logger.info(f"배치 처리 완료!")
        logger.info(f"  총 처리: {self.progress.processed_items}/{self.progress.total_items} ({completion:.1f}%)")
        logger.info(f"  성공: {self.progress.successful_items} ({success_rate:.1f}%)")
        logger.info(f"  실패: {self.progress.failed_items}")
        if elapsed_time:
            logger.info(f"  소요 시간: {elapsed_time}")
        
        return self.progress
    
    async def _process_batch_with_retry(self, batch_products: List[Dict[str, Any]]) -> Tuple[int, int]:
        """재시도가 포함된 배치 처리"""
        for attempt in range(self.config.max_retries):
            try:
                successful, failed = await self.process_products_batch(batch_products)
                return successful, failed
                
            except Exception as e:
                if attempt < self.config.max_retries - 1:
                    logger.warning(f"배치 처리 실패 (시도 {attempt + 1}/{self.config.max_retries}): {e}")
                    await asyncio.sleep(self.config.retry_delay)
                else:
                    logger.error(f"배치 처리 최종 실패: {e}")
                    return 0, len(batch_products)
        
        return 0, len(batch_products)

# 편의 함수들
async def create_batch_processor(config: Optional[BatchConfig] = None) -> BatchProcessor:
    """배치 프로세서 생성 (의존성 자동 주입)"""
    from ..config import get_settings
    from ..database import postgres_manager, qdrant_manager
    
    settings = get_settings()
    
    return BatchProcessor(
        postgres_manager=postgres_manager,
        qdrant_manager=qdrant_manager,
        config=config or BatchConfig()
    ) 