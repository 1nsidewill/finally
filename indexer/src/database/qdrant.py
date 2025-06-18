# src/database/qdrant.py
import os
import asyncio
from typing import List, Dict, Any, Optional, AsyncGenerator
import logging
from contextlib import asynccontextmanager
import uuid
from qdrant_client import QdrantClient, AsyncQdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import Distance, VectorParams, PointStruct, Filter
from qdrant_client.models import Record, ScoredPoint, UpdateResult
from langchain_openai import OpenAIEmbeddings
from langchain.embeddings import CacheBackedEmbeddings
from langchain.storage import LocalFileStore
from src.config import get_settings
from ..monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)

def ensure_valid_uuid(id_value: Any) -> str:
    """정수나 문자열 ID를 유효한 UUID 형식으로 변환 (Deprecated)"""
    # logger.warning("ensure_valid_uuid는 deprecated. generate_product_vector_id 사용 권장")  # 임시로 주석 처리
    try:
        # 이미 유효한 UUID인지 확인
        if isinstance(id_value, str):
            try:
                uuid.UUID(id_value)
                return id_value
            except ValueError:
                pass
        
        # 정수나 문자열을 UUID로 변환
        if isinstance(id_value, (int, str)):
            # 정수를 문자열로 변환하고 UUID 네임스페이스 사용
            id_str = str(id_value)
            namespace = uuid.NAMESPACE_DNS
            generated_uuid = uuid.uuid5(namespace, id_str)
            return str(generated_uuid)
        
        # 기본값 반환
        return str(uuid.uuid4())
        
    except Exception as e:
        logger.warning(f"UUID 변환 실패: {id_value} -> {e}, 랜덤 UUID 생성")
        return str(uuid.uuid4())

def generate_product_vector_id(uid: str, provider: str = "bunjang") -> str:
    """product.uid + provider로 고유한 벡터 ID 생성
    
    Args:
        uid: 제품의 고유 식별자 (예: 'bunmall_1234567')
        provider: 플랫폼/제공자 정보 (예: 'bunjang', 'joongonara')
    
    Returns:
        str: UUID v5 기반 고유 벡터 ID
    
    Examples:
        >>> generate_product_vector_id("bunmall_1234567", "bunjang")
        "550e8400-e29b-41d4-a716-446655440001"
    """
    try:
        # provider:uid 형태로 조합
        combined_id = f"{provider}:{uid}"
        
        # UUID v5로 결정론적 생성 (같은 입력 = 같은 출력)
        namespace = uuid.NAMESPACE_DNS
        generated_uuid = uuid.uuid5(namespace, combined_id)
        
        logger.debug(f"벡터 ID 생성: {combined_id} -> {generated_uuid}")
        return str(generated_uuid)
        
    except Exception as e:
        logger.error(f"벡터 ID 생성 실패: uid={uid}, provider={provider} -> {e}")
        # 실패 시 랜덤 UUID 생성
        fallback_uuid = str(uuid.uuid4())
        logger.warning(f"Fallback UUID 사용: {fallback_uuid}")
        return fallback_uuid

class QdrantManager:
    """Qdrant 벡터 데이터베이스 연결 및 관리자"""
    
    def __init__(self):
        self.config = get_settings()
        self._client: Optional[QdrantClient] = None
        self._async_client: Optional[AsyncQdrantClient] = None
        self._embeddings: Optional[CacheBackedEmbeddings] = None
        self._client_lock = asyncio.Lock()
        
        # Qdrant 설정
        self.host = self.config.QDRANT_HOST
        self.port = self.config.QDRANT_PORT
        self.grpc_port = self.config.QDRANT_GRPC_PORT
        self.prefer_grpc = self.config.QDRANT_PREFER_GRPC
        self.collection_name = self.config.QDRANT_COLLECTION
        self.vector_size = self.config.VECTOR_SIZE
    
    def get_sync_client(self) -> QdrantClient:
        """동기 Qdrant 클라이언트 가져오기 (Lazy Loading)"""
        if self._client is None:
            try:
                self._client = QdrantClient(
                    host=self.host,
                    port=self.port,
                    grpc_port=self.grpc_port if self.prefer_grpc else None,
                    prefer_grpc=self.prefer_grpc
                )
                logger.info(f"Qdrant 동기 클라이언트 연결 성공: {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Qdrant 동기 클라이언트 연결 실패: {e}")
                raise
        return self._client
    
    async def get_async_client(self) -> AsyncQdrantClient:
        """비동기 Qdrant 클라이언트 가져오기 (Lazy Loading)"""
        if self._async_client is None:
            async with self._client_lock:
                if self._async_client is None:
                    try:
                        self._async_client = AsyncQdrantClient(
                            host=self.host,
                            port=self.port,
                            grpc_port=self.grpc_port if self.prefer_grpc else None,
                            prefer_grpc=self.prefer_grpc
                        )
                        logger.info(f"Qdrant 비동기 클라이언트 연결 성공: {self.host}:{self.port}")
                    except Exception as e:
                        logger.error(f"Qdrant 비동기 클라이언트 연결 실패: {e}")
                        raise
        return self._async_client
    
    def get_embeddings(self) -> CacheBackedEmbeddings:
        """OpenAI 임베딩 모델 가져오기 (캐싱 포함)"""
        if self._embeddings is None:
            try:
                # 기본 OpenAI 임베딩 모델 초기화
                base_embeddings = OpenAIEmbeddings(
                    model="text-embedding-3-large",
                    openai_api_key=self.config.OPENAI_API_KEY,
                    dimensions=self.vector_size,
                )
                
                # 캐싱 설정
                cache_dir = "./cache/embeddings"
                os.makedirs(cache_dir, exist_ok=True)
                store = LocalFileStore(cache_dir)
                
                # 캐시 백업 임베딩 초기화
                self._embeddings = CacheBackedEmbeddings.from_bytes_store(
                    base_embeddings,
                    store,
                    namespace=f"{base_embeddings.model}-{self.vector_size}d"
                )
                
                logger.info(f"임베딩 캐시 설정 완료: {cache_dir}")
            except Exception as e:
                logger.error(f"임베딩 모델 초기화 실패: {e}")
                raise
        return self._embeddings
    
    async def create_collection_if_not_exists(self) -> bool:
        """컬렉션이 존재하지 않으면 생성 (최적화 설정 포함)"""
        try:
            client = self.get_sync_client()
            collections = client.get_collections().collections
            collection_names = [c.name for c in collections]
            
            if self.collection_name not in collection_names:
                client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.vector_size,
                        distance=Distance.COSINE
                    ),
                    # 🚀 Storage Optimization 설정
                    optimizers_config=models.OptimizersConfigDiff(
                        # 인덱싱 임계값: 20K 포인트부터 인덱싱 시작
                        indexing_threshold=20000,
                        # 메모리 매핑 임계값: 50K 포인트부터 메모리 매핑 사용
                        memmap_threshold=50000,
                        # 최대 세그먼트 크기: 200K 포인트
                        max_segment_size=200000,
                        # 최대 최적화 스레드 수
                        max_optimization_threads=2,
                        # 삭제된 벡터 정리 임계값 (70%)
                        deleted_threshold=0.7,
                        # 벡터 압축 활성화
                        vacuum_min_vector_number=1000,
                        # 기본 세그먼트 수
                        default_segment_number=2
                    ),
                    # 🗄️ 디스크 저장 최적화
                    on_disk_payload=True,  # payload를 디스크에 저장
                    # 🔧 HNSW 인덱스 최적화 설정
                    hnsw_config=models.HnswConfigDiff(
                        m=16,  # 연결 수 (기본값, 메모리 vs 정확도 균형)
                        ef_construct=100,  # 인덱스 구축 시 탐색 깊이
                        full_scan_threshold=10000,  # 전체 스캔 임계값
                        max_indexing_threads=2,  # 인덱싱 스레드 수
                        on_disk=False,  # 인덱스는 메모리에 유지 (성능)
                        payload_m=16  # payload와 연결된 링크 수
                    ),
                    # 🔄 Quantization 설정 (메모리 절약)
                    quantization_config=models.ScalarQuantization(
                        scalar=models.ScalarQuantizationConfig(
                            type=models.ScalarType.INT8,  # 8비트 양자화
                            quantile=0.99,  # 99% 분위수 사용
                            always_ram=True  # 양자화된 벡터는 RAM에 유지
                        )
                    ),
                    # 🧹 WAL (Write-Ahead Log) 설정
                    wal_config=models.WalConfigDiff(
                        wal_capacity_mb=32,  # WAL 용량 32MB
                        wal_segments_ahead=0  # 미리 생성할 WAL 세그먼트 수
                    )
                )
                logger.info(f"컬렉션 '{self.collection_name}' 최적화 설정과 함께 생성 완료")
                logger.info("🚀 적용된 최적화 설정:")
                logger.info("  - 인덱싱 임계값: 20K 포인트")
                logger.info("  - 메모리 매핑: 50K 포인트부터")
                logger.info("  - 최대 세그먼트 크기: 200K 포인트")
                logger.info("  - HNSW 인덱스: m=16, ef_construct=100")
                logger.info("  - INT8 양자화 활성화 (메모리 절약)")
                logger.info("  - 디스크 payload 저장 활성화")
                return True
            else:
                logger.debug(f"컬렉션 '{self.collection_name}' 이미 존재")
                return False
        except Exception as e:
            logger.error(f"컬렉션 생성 실패: {e}")
            raise
    
    async def generate_embedding(self, text: str) -> List[float]:
        """텍스트를 벡터로 변환"""
        try:
            embeddings = self.get_embeddings()
            vector = await embeddings.aembed_query(text)
            logger.debug(f"임베딩 생성 완료: {len(vector)} 차원")
            return vector
        except Exception as e:
            logger.error(f"임베딩 생성 실패: {e}")
            raise
    
    async def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """배치로 여러 텍스트를 벡터로 변환"""
        try:
            embeddings = self.get_embeddings()
            vectors = await embeddings.aembed_documents(texts)
            logger.debug(f"배치 임베딩 생성 완료: {len(vectors)}개 벡터")
            return vectors
        except Exception as e:
            logger.error(f"배치 임베딩 생성 실패: {e}")
            raise
    
    @MetricsCollector.track_db_query("qdrant", "upsert")
    async def upsert_points(
        self, 
        points: List[PointStruct], 
        wait: bool = True
    ) -> Dict[str, Any]:
        """포인트들을 Qdrant에 삽입/업데이트"""
        try:
            client = await self.get_async_client()
            result = await client.upsert(
                collection_name=self.collection_name,
                wait=wait,
                points=points
            )
            logger.debug(f"{len(points)}개 포인트 upsert 완료")
            return {"status": "success", "operation_id": result.operation_id}
        except Exception as e:
            logger.error(f"포인트 upsert 실패: {e}")
            raise

    @MetricsCollector.track_db_query("qdrant", "batch_upsert_optimized")
    async def upsert_points_batch_optimized(
        self, 
        points: List[PointStruct], 
        batch_size: int = 100,
        wait: bool = False,
        parallel_batches: int = 3
    ) -> Dict[str, Any]:
        """🚀 대용량 데이터를 위한 최적화된 배치 업로드"""
        try:
            if not points:
                return {"status": "success", "processed": 0}
            
            client = await self.get_async_client()
            total_points = len(points)
            
            # 포인트들을 배치로 분할
            batches = [
                points[i:i + batch_size] 
                for i in range(0, total_points, batch_size)
            ]
            
            logger.info(f"🚀 배치 최적화 업로드 시작: {total_points}개 포인트, {len(batches)}개 배치")
            
            # 병렬 배치 처리를 위한 세마포어
            semaphore = asyncio.Semaphore(parallel_batches)
            operation_ids = []
            
            async def process_batch(batch_points: List[PointStruct], batch_idx: int):
                async with semaphore:
                    try:
                        result = await client.upsert(
                            collection_name=self.collection_name,
                            wait=wait,
                            points=batch_points
                        )
                        logger.debug(f"배치 {batch_idx + 1}/{len(batches)} 완료: {len(batch_points)}개 포인트")
                        return result.operation_id
                    except Exception as e:
                        logger.error(f"배치 {batch_idx + 1} 업로드 실패: {e}")
                        raise
            
            # 모든 배치를 병렬로 처리
            tasks = [
                process_batch(batch, idx) 
                for idx, batch in enumerate(batches)
            ]
            
            operation_ids = await asyncio.gather(*tasks)
            
            logger.info(f"✅ 배치 최적화 업로드 완료: {total_points}개 포인트 처리됨")
            return {
                "status": "success", 
                "processed": total_points,
                "batches": len(batches),
                "operation_ids": operation_ids
            }
            
        except Exception as e:
            logger.error(f"배치 최적화 업로드 실패: {e}")
            raise

    async def optimize_collection(self) -> Dict[str, Any]:
        """🧹 컬렉션 최적화 실행 (Vacuum, Merge 등)"""
        try:
            client = await self.get_async_client()
            
            logger.info("🧹 컬렉션 최적화 시작...")
            
            # 1. 삭제된 벡터 정리 (Vacuum)
            logger.info("1️⃣ 삭제된 벡터 정리 중...")
            vacuum_result = await client.update_collection(
                collection_name=self.collection_name,
                optimizer_config=models.OptimizersConfigDiff(
                    deleted_threshold=0.1,  # 임시로 낮춰서 강제 정리
                    vacuum_min_vector_number=1
                )
            )
            
            # 2. 인덱스 재구축
            logger.info("2️⃣ 인덱스 최적화 중...")
            # 잠시 후 다시 원래 설정으로 복원
            await asyncio.sleep(1)
            restore_result = await client.update_collection(
                collection_name=self.collection_name,
                optimizer_config=models.OptimizersConfigDiff(
                    deleted_threshold=0.7,  # 원래 설정으로 복원
                    vacuum_min_vector_number=1000
                )
            )
            
            # 3. 컬렉션 정보 조회로 최적화 결과 확인
            collection_info = await client.get_collection(self.collection_name)
            
            logger.info("✅ 컬렉션 최적화 완료")
            logger.info(f"  - 총 포인트 수: {collection_info.points_count}")
            logger.info(f"  - 인덱스 상태: {collection_info.status}")
            
            return {
                "status": "success",
                "points_count": collection_info.points_count,
                "collection_status": collection_info.status,
                "vacuum_operation_id": vacuum_result.operation_id if vacuum_result else None,
                "restore_operation_id": restore_result.operation_id if restore_result else None
            }
            
        except Exception as e:
            logger.error(f"컬렉션 최적화 실패: {e}")
            raise

    async def get_storage_stats(self) -> Dict[str, Any]:
        """📊 스토리지 사용량 및 성능 통계 조회"""
        try:
            client = await self.get_async_client()
            
            # 컬렉션 정보 조회
            collection_info = await client.get_collection(self.collection_name)
            
            # 클러스터 정보 조회 (가능한 경우)
            try:
                cluster_info = await client.cluster_info()
            except:
                cluster_info = None
            
            stats = {
                "collection_name": self.collection_name,
                "points_count": collection_info.points_count,
                "segments_count": len(collection_info.segments) if collection_info.segments else 0,
                "status": collection_info.status,
                "optimizer_status": collection_info.optimizer_status,
                "vectors_count": collection_info.vectors_count if hasattr(collection_info, 'vectors_count') else None,
                "indexed_vectors_count": collection_info.indexed_vectors_count if hasattr(collection_info, 'indexed_vectors_count') else None,
            }
            
            if cluster_info:
                stats["cluster_status"] = cluster_info
            
            logger.debug(f"스토리지 통계 조회 완료: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"스토리지 통계 조회 실패: {e}")
            raise
    
    async def upsert_vector_async(
        self, 
        vector_id: str, 
        vector: List[float], 
        metadata: Optional[Dict[str, Any]] = None,
        wait: bool = True
    ) -> Dict[str, Any]:
        """단일 벡터를 Qdrant에 삽입/업데이트 (batch_processor 호환성을 위한 메서드)"""
        try:
            # 이미 유효한 UUID인지 확인
            try:
                uuid.UUID(vector_id)
                valid_uuid = vector_id  # 이미 유효한 UUID
            except ValueError:
                # 유효하지 않으면 변환 (fallback to old logic)
                valid_uuid = ensure_valid_uuid(vector_id)
            
            point = PointStruct(
                id=valid_uuid,
                vector=vector,
                payload=metadata or {}
            )
            
            client = await self.get_async_client()
            result = await client.upsert(
                collection_name=self.collection_name,
                wait=wait,
                points=[point]
            )
            logger.debug(f"벡터 upsert 완료: ID={vector_id} -> UUID={valid_uuid}")
            return {"status": "success", "operation_id": result.operation_id, "uuid": valid_uuid}
        except Exception as e:
            logger.error(f"벡터 upsert 실패 (ID: {vector_id}): {e}")
            raise
    
    async def delete_points(
        self, 
        point_ids: List[str], 
        wait: bool = True
    ) -> Dict[str, Any]:
        """포인트들을 Qdrant에서 삭제"""
        try:
            client = await self.get_async_client()
            result = await client.delete(
                collection_name=self.collection_name,
                wait=wait,
                points_selector=models.PointIdsList(
                    points=point_ids
                )
            )
            logger.debug(f"{len(point_ids)}개 포인트 삭제 완료")
            return {"status": "success", "operation_id": result.operation_id}
        except Exception as e:
            logger.error(f"포인트 삭제 실패: {e}")
            raise
    
    @MetricsCollector.track_db_query("qdrant", "search")
    async def search_points(
        self,
        query_vector: List[float],
        limit: int = 10,
        filter_conditions: Optional[Filter] = None,
        with_payload: bool = True,
        with_vectors: bool = False
    ) -> List[ScoredPoint]:
        """벡터 유사도 검색"""
        try:
            client = await self.get_async_client()
            results = await client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=limit,
                query_filter=filter_conditions,
                with_payload=with_payload,
                with_vectors=with_vectors
            )
            logger.debug(f"검색 완료: {len(results)}개 결과")
            return results
        except Exception as e:
            logger.error(f"벡터 검색 실패: {e}")
            raise

    async def search_similar_vectors(
        self,
        query_vector: List[float],
        limit: int = 10,
        filter_conditions: Optional[Filter] = None,
        with_payload: bool = True,
        with_vectors: bool = False
    ) -> List[ScoredPoint]:
        """벡터 유사도 검색 (호환성을 위한 별칭)"""
        return await self.search_points(
            query_vector=query_vector,
            limit=limit,
            filter_conditions=filter_conditions,
            with_payload=with_payload,
            with_vectors=with_vectors
        )
    
    async def get_points(
        self, 
        point_ids: List[str], 
        with_payload: bool = True, 
        with_vectors: bool = False
    ) -> List[Record]:
        """특정 포인트들 조회"""
        try:
            client = await self.get_async_client()
            results = await client.retrieve(
                collection_name=self.collection_name,
                ids=point_ids,
                with_payload=with_payload,
                with_vectors=with_vectors
            )
            logger.debug(f"{len(results)}개 포인트 조회 완료")
            return results
        except Exception as e:
            logger.error(f"포인트 조회 실패: {e}")
            raise
    
    async def count_points(self) -> int:
        """컬렉션의 총 포인트 수 반환"""
        try:
            client = await self.get_async_client()
            result = await client.count(collection_name=self.collection_name)
            count = result.count
            logger.debug(f"총 포인트 수: {count}")
            return count
        except Exception as e:
            logger.error(f"포인트 카운트 실패: {e}")
            raise
    
    @MetricsCollector.track_db_query("qdrant", "list_collections")
    async def list_collections(self) -> List[str]:
        """모든 컬렉션 목록 반환"""
        try:
            client = await self.get_async_client()
            collections = await client.get_collections()
            collection_names = [c.name for c in collections.collections]
            logger.debug(f"컬렉션 목록: {collection_names}")
            return collection_names
        except Exception as e:
            logger.error(f"컬렉션 목록 조회 실패: {e}")
            raise

    async def get_collection_info(self) -> Dict[str, Any]:
        """컬렉션 정보 조회"""
        try:
            client = await self.get_async_client()
            info = await client.get_collection(self.collection_name)
            logger.debug("컬렉션 정보 조회 완료")
            return {
                "status": info.status,
                "optimizer_status": info.optimizer_status,
                "vectors_count": info.vectors_count,
                "points_count": info.points_count,
                "config": info.config
            }
        except Exception as e:
            logger.error(f"컬렉션 정보 조회 실패: {e}")
            raise
    
    async def health_check(self) -> bool:
        """Qdrant 연결 상태 확인"""
        try:
            client = await self.get_async_client()
            collections = await client.get_collections()
            return len(collections.collections) >= 0
        except Exception as e:
            logger.error(f"Qdrant 헬스체크 실패: {e}")
            return False
    
    async def close(self):
        """클라이언트 연결 종료"""
        if self._async_client:
            await self._async_client.close()
            logger.info("Qdrant 비동기 클라이언트 연결 종료")
        if self._client:
            self._client.close()
            logger.info("Qdrant 동기 클라이언트 연결 종료")

# 전역 인스턴스 (싱글톤 패턴)
qdrant_manager = QdrantManager()

# 편의 함수들
async def get_async_client() -> AsyncQdrantClient:
    """비동기 Qdrant 클라이언트 가져오기"""
    return await qdrant_manager.get_async_client()

def get_sync_client() -> QdrantClient:
    """동기 Qdrant 클라이언트 가져오기"""
    return qdrant_manager.get_sync_client()

async def generate_embedding(text: str) -> List[float]:
    """텍스트 임베딩 생성"""
    return await qdrant_manager.generate_embedding(text)