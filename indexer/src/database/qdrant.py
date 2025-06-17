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

logger = logging.getLogger(__name__)

def ensure_valid_uuid(id_value: Any) -> str:
    """정수나 문자열 ID를 유효한 UUID 형식으로 변환"""
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
        """컬렉션이 존재하지 않으면 생성"""
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
                    optimizers_config=models.OptimizersConfigDiff(
                        indexing_threshold=20000,
                    ),
                    on_disk_payload=True
                )
                logger.info(f"컬렉션 '{self.collection_name}' 생성 완료")
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
    
    async def upsert_vector_async(
        self, 
        vector_id: str, 
        vector: List[float], 
        metadata: Optional[Dict[str, Any]] = None,
        wait: bool = True
    ) -> Dict[str, Any]:
        """단일 벡터를 Qdrant에 삽입/업데이트 (batch_processor 호환성을 위한 메서드)"""
        try:
            # UUID 형식으로 변환
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