import os
import uuid
from typing import List, Dict, Any, Optional, Union, Tuple
import numpy as np
from qdrant_client import QdrantClient, AsyncQdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, Range, MatchValue
from qdrant_client.models import Record, ScoredPoint, UpdateResult, UpdateStatus

from src.api.models import QdrantDocument
from src.api.schema import DocumentCreate, DocumentUpdate, DocumentMetadata
from src.config import get_settings
from langchain_openai import OpenAIEmbeddings
from langchain_core.embeddings import Embeddings

# 캐싱 관련 import 추가
from langchain.embeddings import CacheBackedEmbeddings
from langchain.storage import LocalFileStore
import hashlib
import re

config = get_settings()


class QdrantService:
    """Qdrant 벡터 데이터베이스와 상호작용하는 서비스 클래스"""
    
    def __init__(self):
        # Qdrant 연결 설정
        # host: Qdrant 서버 호스트 주소
        # port: Qdrant HTTP API 포트
        # grpc_port: Qdrant gRPC API 포트
        # prefer_grpc: gRPC 프로토콜 사용 여부 (성능이 더 좋음)
        # collection_name: 벡터를 저장할 컬렉션 이름
        # vector_size: 임베딩 벡터의 차원 수 (사용하는 임베딩 모델에 따라 결정)
        self.host = config.QDRANT_HOST
        self.port = config.QDRANT_PORT
        self.grpc_port = config.QDRANT_GRPC_PORT
        self.prefer_grpc = config.QDRANT_PREFER_GRPC
        self.collection_name = config.QDRANT_COLLECTION
        self.vector_size = config.VECTOR_SIZE
        
        # Qdrant 클라이언트 초기화 - gRPC 사용 (더 빠른 성능)
        self.client = QdrantClient(
            host=self.host, 
            port=self.port,
            grpc_port=self.grpc_port if self.prefer_grpc else None,
            prefer_grpc=self.prefer_grpc
        )
        
        # 비동기 클라이언트도 함께 초기화
        self.async_client = AsyncQdrantClient(
            host=self.host, 
            port=self.port,
            grpc_port=self.grpc_port if self.prefer_grpc else None,
            prefer_grpc=self.prefer_grpc
        )
        
        # 기본 OpenAI 임베딩 모델 초기화
        base_embeddings = OpenAIEmbeddings(
            model="text-embedding-3-large",
            openai_api_key=config.OPENAI_API_KEY,
            dimensions=self.vector_size,
        )
        
        # 캐싱 설정 - 로컬 파일 시스템 사용 (테스트용)
        cache_dir = "./cache/embeddings"
        os.makedirs(cache_dir, exist_ok=True)
        
        store = LocalFileStore(cache_dir)
        
        # 캐시 백업 임베딩 초기화
        self.embeddings = CacheBackedEmbeddings.from_bytes_store(
            base_embeddings,
            store,
            namespace=f"{base_embeddings.model}-{self.vector_size}d"  # 모델명 + 차원수로 네임스페이스 구분
        )
        
        print(f"임베딩 캐시 설정 완료: {cache_dir}")
        
        # 컬렉션이 없으면 생성
        self._create_collection_if_not_exists()
    
    def _create_collection_if_not_exists(self) -> None:
        """컬렉션이 존재하지 않으면 생성"""
        collections = self.client.get_collections().collections
        collection_names = [c.name for c in collections]
        
        if self.collection_name not in collection_names:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=self.vector_size, distance=Distance.COSINE),
                optimizers_config=models.OptimizersConfigDiff(
                    indexing_threshold=20000,  # 색인화 임계값 (이 수 이상의 벡터가 추가되면 자동으로 인덱싱)
                ),
                on_disk_payload=True  # 메모리 사용량 감소를 위해 페이로드를 디스크에 저장
            )
            print(f"컬렉션 '{self.collection_name}'이 생성되었습니다.")
    
    async def generate_embedding_text(self, document: DocumentCreate) -> str:
        """문서 정보를 기반으로 구조화된 프롬프트 형식의 임베딩용 텍스트 생성"""
        # 이미 임베딩 텍스트가 제공된 경우
        if document.embedding_text:
            return document.embedding_text
        
        metadata = document.metadata or DocumentMetadata()
        
        # 구조화된 프롬프트 형식으로 임베딩 텍스트 생성
        embedding_parts = []
        
        # 1. 제목 (가장 중요한 정보)
        embedding_parts.append(f"## 제목: {document.title}")
        
        # 2. 핵심 매물 정보 (고가치 정보들)
        vehicle_info = []
        if metadata.model_name:
            vehicle_info.append(f"모델명: {metadata.model_name}")
        if metadata.year:
            vehicle_info.append(f"연식: {metadata.year}년")
        if metadata.price:
            vehicle_info.append(f"가격: {metadata.price:,}원")
        if metadata.odo:
            vehicle_info.append(f"주행거리: {metadata.odo:,}km")
        if metadata.color:
            vehicle_info.append(f"색상: {metadata.color}")

        if vehicle_info:
            embedding_parts.append(f"### 매물 정보:\n{' | '.join(vehicle_info)}")
        
        # 3. 상세 내용 (설명 텍스트)
        if metadata.content:
            embedding_parts.append(f"### 매물 상세 설명:\n{metadata.content}")
        
        # 4. 추가 정보 (있는 경우)
        additional_info = []
        if metadata.image_url:
            additional_info.append("이미지 첨부됨")
        if metadata.last_modified_at:
            additional_info.append(f"최종 수정: {metadata.last_modified_at}")
        
        if additional_info:
            embedding_parts.append(f"### 추가 정보: {' | '.join(additional_info)}")
        
        # 구조화된 텍스트 결합
        embedding_text = "\n\n".join(embedding_parts)
        
        return embedding_text

    async def insert_document(self, document: DocumentCreate, wait: bool = True) -> str:
        """문서를 Qdrant에 삽입"""
        # Qdrant point ID 결정 (DB UID 우선 사용)
        if document.metadata and document.metadata.id:
            # DB UID가 있으면 그걸 Qdrant point ID로 사용
            point_id = str(document.metadata.id)
        elif document.id:
            # Document ID가 있으면 사용
            point_id = document.id
        else:
            # 둘 다 없으면 새로 생성
            point_id = str(uuid.uuid4())
            # 생성된 ID를 metadata에도 저장
            if document.metadata:
                document.metadata.id = point_id
            else:
                document.metadata = DocumentMetadata(id=point_id)
        
        # 임베딩용 텍스트 생성
        embedding_text = await self.generate_embedding_text(document)
        
        # 임베딩 생성
        vector = await self.generate_embedding(embedding_text)
        
        # 메타데이터 준비
        metadata_dict = document.metadata.dict(exclude_none=True) if document.metadata else {}
        
        # Qdrant에 점 추가
        await self.async_client.upsert(
            collection_name=self.collection_name,
            wait=wait,
            points=[
                PointStruct(
                    id=point_id,  # DB UID = Qdrant point ID
                    vector=vector,
                    payload={
                        "title": document.title,
                        "embedding_text": embedding_text,
                        **metadata_dict
                    }
                )
            ]
        )
        
        return point_id
    
    async def batch_insert_documents(
        self, 
        documents: List[DocumentCreate], 
        ids: Optional[List[str]] = None,
        batch_size: int = 100,
        wait: bool = True
    ) -> List[str]:
        """여러 문서를 일괄적으로 Qdrant에 삽입 (대용량 처리를 위한 배치 기능 포함)"""
        doc_ids = []
        points = []
        
        # 모든 문서의 임베딩 텍스트를 먼저 생성
        embedding_texts = []
        for document in documents:
            embedding_text = await self.generate_embedding_text(document)
            embedding_texts.append(embedding_text)
        
        # 배치로 임베딩 생성 (성능 향상)
        vectors = await self.generate_embeddings_batch(embedding_texts)
        
        for idx, document in enumerate(documents):
            # UUID 형식 보장
            if document.metadata and document.metadata.id:
                point_id = convert_to_uuid_if_needed(str(document.metadata.id))
            elif document.id:
                point_id = convert_to_uuid_if_needed(document.id)
            else:
                point_id = str(uuid.uuid4())
            
            # UUID 형식 검증
            if not is_valid_uuid(point_id):
                raise ValueError(f"유효하지 않은 UUID 형식: {point_id}")
            
            doc_ids.append(point_id)
            
            # 메타데이터 준비
            metadata_dict = document.metadata.dict(exclude_none=True) if document.metadata else {}
            
            # Qdrant 포인트 준비
            points.append(
                PointStruct(
                    id=point_id,  # DB UID = Qdrant point ID
                    vector=vectors[idx],
                    payload={
                        "title": document.title,
                        "embedding_text": embedding_texts[idx],
                        **metadata_dict
                    }
                )
            )
        
        # 일괄 삽입 (배치 크기로 분할)
        if points:
            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]
                await self.async_client.upsert(
                    collection_name=self.collection_name,
                    wait=wait,
                    points=batch
                )
        
        return doc_ids
    
    async def delete_document(self, doc_id: str, wait: bool = True) -> Dict[str, Any]:
        """문서 ID로 문서 삭제"""
        try:
            operation_result = await self.async_client.delete(
                collection_name=self.collection_name,
                points_selector=models.PointIdsList(
                    points=[doc_id]
                ),
                wait=wait
            )
            
            return {
                "success": True,
                "operation_id": str(operation_result.operation_id) if operation_result.operation_id else None
            }
        except Exception as e:
            print(f"문서 삭제 중 오류 발생: {e}")
            return {"success": False}
    
    async def batch_delete_documents(self, doc_ids: List[str], wait: bool = True) -> Dict[str, Any]:
        """여러 문서를 일괄적으로 삭제"""
        try:
            operation_result = await self.async_client.delete(
                collection_name=self.collection_name,
                points_selector=models.PointIdsList(
                    points=doc_ids
                ),
                wait=wait
            )
            
            return {
                "success": True,
                "operation_id": str(operation_result.operation_id) if operation_result.operation_id else None
            }
        except Exception as e:
            print(f"문서 일괄 삭제 중 오류 발생: {e}")
            return {"success": False}
    
    async def get_document(self, doc_id: str, with_vectors: bool = False) -> Optional[Dict[str, Any]]:
        """문서 ID로 문서 조회"""
        try:
            result = await self.async_client.retrieve(
                collection_name=self.collection_name,
                ids=[doc_id],
                with_vectors=with_vectors,
                with_payload=True
            )
            
            if result and len(result) > 0:
                point = result[0]
                response = {
                    "id": point.id,
                    "content": point.payload.get("content"),
                    "metadata": {k: v for k, v in point.payload.items() if k != "content"}
                }
                
                if with_vectors:
                    response["vector"] = point.vector
                
                return response
            
            return None
        except Exception as e:
            print(f"문서 조회 중 오류 발생: {e}")
            return None
    
    async def list_documents(
        self, 
        offset: int = 0, 
        limit: int = 10,
        with_vectors: bool = False
    ) -> Tuple[List[Dict[str, Any]], int]:
        """문서 목록 조회 (페이징 지원)"""
        try:
            # 전체 문서 수 먼저 조회
            total_count = await self.count_documents()
            
            # 문서 목록 조회
            scroll_results = await self.async_client.scroll(
                collection_name=self.collection_name,
                limit=limit,
                offset=offset,
                with_vectors=with_vectors,
                with_payload=True
            )
            
            points = scroll_results.points
            documents = []
            
            for point in points:
                doc = {
                    "id": point.id,
                    "content": point.payload.get("content"),
                    "metadata": {k: v for k, v in point.payload.items() if k != "content"}
                }
                
                if with_vectors:
                    doc["vector"] = point.vector
                
                documents.append(doc)
            
            return documents, total_count
        except Exception as e:
            print(f"문서 목록 조회 중 오류 발생: {e}")
            return [], 0
    
    async def update_document(self, doc_id: str, update_data: DocumentUpdate, wait: bool = True) -> Dict[str, Any]:
        """문서 업데이트 (내용 또는 메타데이터)"""
        try:
            # 기존 문서 조회
            document = await self.get_document(doc_id)
            if not document:
                return {"success": False}
            
            update_operations = []
            
            # 내용 업데이트가 있다면 임베딩도 재생성
            if update_data.content is not None:
                # 새 임베딩 생성
                vector = await self.generate_embedding(update_data.content)
                
                # 벡터 업데이트
                await self.async_client.update_vectors(
                    collection_name=self.collection_name,
                    points=[
                        models.PointVectors(
                            id=doc_id,
                            vector=vector
                        )
                    ],
                    wait=wait
                )
                
                # 내용 업데이트 작업 추가
                update_operations.append(
                    models.SetPayload(
                        payload={"content": update_data.content},
                        points=[doc_id]
                    )
                )
            
            # 메타데이터 업데이트가 있다면 적용
            if update_data.metadata:
                metadata_dict = update_data.metadata.dict(exclude_none=True)
                
                if metadata_dict:
                    # 메타데이터 업데이트 작업 추가
                    update_operations.append(
                        models.SetPayload(
                            payload=metadata_dict,
                            points=[doc_id]
                        )
                    )
            
            # 업데이트 시간 추가
            from datetime import datetime
            update_operations.append(
                models.SetPayload(
                    payload={"updated_at": datetime.now().isoformat()},
                    points=[doc_id]
                )
            )
            
            # 모든 페이로드 업데이트 작업 수행
            if update_operations:
                last_operation = None
                for operation in update_operations:
                    operation_result = await self.async_client.set_payload(
                        collection_name=self.collection_name,
                        payload=operation.payload,
                        points=operation.points,
                        wait=wait
                    )
                    last_operation = operation_result
                
                return {
                    "success": True,
                    "operation_id": str(last_operation.operation_id) if last_operation and last_operation.operation_id else None
                }
            
            return {"success": True}
        except Exception as e:
            print(f"문서 업데이트 중 오류 발생: {e}")
            return {"success": False}
    
    async def search_documents_by_text(
        self, 
        query: str, 
        limit: int = 5,
        filter: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """텍스트 쿼리를 기반으로 유사한 문서 검색"""
        try:
            # 쿼리 텍스트에서 임베딩 생성
            query_vector = await self.generate_embedding(query)
            
            # 필터 변환 (제공된 경우)
            query_filter = self._convert_filter_dict_to_model(filter) if filter else None
            
            # 벡터 검색 수행
            search_results = await self.async_client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=limit,
                query_filter=query_filter,
                with_payload=True,
                score_threshold=0.5  # 유사도 점수 임계값 (이보다 낮은 결과는 제외)
            )
            
            # 결과 포맷팅
            results = []
            for point in search_results:
                results.append({
                    "id": point.id,
                    "content": point.payload.get("content"),
                    "score": point.score,
                    "metadata": {k: v for k, v in point.payload.items() if k != "content"}
                })
            
            return results
        except Exception as e:
            print(f"텍스트 검색 중 오류 발생: {e}")
            return []
    
    async def search_documents_by_vector(
        self, 
        query_vector: List[float], 
        limit: int = 5,
        filter: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """벡터를 기반으로 유사한 문서 검색"""
        try:
            # 필터 변환 (제공된 경우)
            query_filter = self._convert_filter_dict_to_model(filter) if filter else None
            
            # 벡터 검색 수행
            search_results = await self.async_client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=limit,
                query_filter=query_filter,
                with_payload=True,
                score_threshold=0.5  # 유사도 점수 임계값 (이보다 낮은 결과는 제외)
            )
            
            # 결과 포맷팅
            results = []
            for point in search_results:
                results.append({
                    "id": point.id,
                    "content": point.payload.get("content"),
                    "score": point.score,
                    "metadata": {k: v for k, v in point.payload.items() if k != "content"}
                })
            
            return results
        except Exception as e:
            print(f"벡터 검색 중 오류 발생: {e}")
            return []
    
    def _convert_filter_dict_to_model(self, filter_dict: Dict[str, Any]) -> Filter:
        """사전 형식의 필터를 Qdrant Filter 모델로 변환"""
        # 여기서는 간단한 필터 변환 구현
        # 실제 구현에서는 더 복잡한 조건 및 중첩 필터를 지원할 수 있음
        must_conditions = []
        should_conditions = []
        must_not_conditions = []
        
        # 필터가 더 복잡한 구조인 경우 더 정교한 변환 로직이 필요합니다.
        # 현재 구현은 기본적인 필드 조건만 지원합니다.
        
        if "must" in filter_dict:
            for condition in filter_dict["must"]:
                field_condition = self._create_field_condition(condition)
                if field_condition:
                    must_conditions.append(field_condition)
        
        if "should" in filter_dict:
            for condition in filter_dict["should"]:
                field_condition = self._create_field_condition(condition)
                if field_condition:
                    should_conditions.append(field_condition)
        
        if "must_not" in filter_dict:
            for condition in filter_dict["must_not"]:
                field_condition = self._create_field_condition(condition)
                if field_condition:
                    must_not_conditions.append(field_condition)
        
        return Filter(
            must=must_conditions if must_conditions else None,
            should=should_conditions if should_conditions else None,
            must_not=must_not_conditions if must_not_conditions else None
        )
    
    def _create_field_condition(self, condition: Dict[str, Any]) -> Optional[FieldCondition]:
        """필드 조건 생성"""
        if not isinstance(condition, dict) or "key" not in condition:
            return None
        
        key = condition["key"]
        
        if "match" in condition:
            return FieldCondition(
                key=key,
                match=MatchValue(value=condition["match"])
            )
        elif "range" in condition:
            range_dict = condition["range"]
            range_params = {}
            
            if "gte" in range_dict:
                range_params["gte"] = range_dict["gte"]
            if "gt" in range_dict:
                range_params["gt"] = range_dict["gt"]
            if "lte" in range_dict:
                range_params["lte"] = range_dict["lte"]
            if "lt" in range_dict:
                range_params["lt"] = range_dict["lt"]
            
            if range_params:
                return FieldCondition(
                    key=key,
                    range=Range(**range_params)
                )
        
        return None
    
    async def count_documents(self) -> int:
        """컬렉션 내 문서 수 조회"""
        try:
            collection_info = await self.async_client.get_collection(self.collection_name)
            return collection_info.vectors_count
        except Exception as e:
            print(f"문서 수 조회 중 오류 발생: {e}")
            return 0
    
    async def get_collection_info(self) -> Dict[str, Any]:
        """컬렉션 정보 조회"""
        try:
            collection_info = await self.async_client.get_collection(self.collection_name)
            return {
                "name": collection_info.name,
                "status": collection_info.status,
                "vectors_count": collection_info.vectors_count,
                "segments_count": collection_info.segments_count,
                "config": {
                    "vector_size": collection_info.config.params.vectors.size,
                    "distance": collection_info.config.params.vectors.distance,
                    "on_disk": collection_info.config.params.on_disk_payload
                }
            }
        except Exception as e:
            print(f"컬렉션 정보 조회 중 오류 발생: {e}")
            return {}

    async def generate_embedding(self, text: str) -> List[float]:
        """텍스트에서 임베딩 벡터 생성 (캐싱 지원)"""
        try:
            # LangChain CacheBackedEmbeddings를 사용하여 자동 캐싱
            embedding_vector = await self.embeddings.aembed_query(text)
            
            # 벡터 크기 검증
            if len(embedding_vector) != self.vector_size:
                raise ValueError(f"임베딩 벡터 크기가 예상과 다릅니다. 예상: {self.vector_size}, 실제: {len(embedding_vector)}")
            
            return embedding_vector
            
        except Exception as e:
            print(f"임베딩 생성 중 오류 발생: {e}")
            return [0.0] * self.vector_size
    
    async def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """여러 텍스트에서 임베딩 벡터 일괄 생성 (캐싱 지원)"""
        try:
            # LangChain CacheBackedEmbeddings의 배치 메서드 사용
            embedding_vectors = await self.embeddings.aembed_documents(texts)
            
            # 각 벡터 크기 검증
            for i, vector in enumerate(embedding_vectors):
                if len(vector) != self.vector_size:
                    print(f"경고: {i}번째 임베딩 벡터 크기가 예상과 다릅니다. 예상: {self.vector_size}, 실제: {len(vector)}")
                    embedding_vectors[i] = [0.0] * self.vector_size
            
            return embedding_vectors
            
        except Exception as e:
            print(f"배치 임베딩 생성 중 오류 발생: {e}")
            return [[0.0] * self.vector_size for _ in texts]

    def get_cache_stats(self) -> Dict[str, Any]:
        """캐시 통계 정보 조회"""
        try:
            cache_dir = "./cache/embeddings"
            if not os.path.exists(cache_dir):
                return {"cache_files": 0, "total_size_mb": 0}
            
            files = os.listdir(cache_dir)
            total_size = sum(os.path.getsize(os.path.join(cache_dir, f)) for f in files)
            
            return {
                "cache_files": len(files),
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "cache_directory": cache_dir
            }
        except Exception as e:
            print(f"캐시 통계 조회 중 오류: {e}")
            return {"error": str(e)}

    def clear_cache(self) -> Dict[str, Any]:
        """캐시 삭제 (테스트 후 정리용)"""
        try:
            cache_dir = "./cache/embeddings"
            if os.path.exists(cache_dir):
                import shutil
                shutil.rmtree(cache_dir)
                os.makedirs(cache_dir, exist_ok=True)
                return {"success": True, "message": "캐시가 성공적으로 삭제되었습니다."}
            else:
                return {"success": True, "message": "삭제할 캐시가 없습니다."}
        except Exception as e:
            return {"success": False, "error": str(e)}


def is_valid_uuid(uuid_string: str) -> bool:
    """UUID 형식이 유효한지 확인"""
    uuid_pattern = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        re.IGNORECASE
    )
    return bool(uuid_pattern.match(uuid_string))

def convert_to_uuid_if_needed(point_id: str) -> str:
    """필요시 point ID를 UUID 형식으로 변환"""
    if is_valid_uuid(point_id):
        return point_id
    
    # 숫자인 경우 UUID 형식으로 변환
    if point_id.isdigit():
        int_id = int(point_id)
        hex_str = f"{int_id:032x}"
        return f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:32]}"
    
    # 그 외의 경우 해시를 사용하여 UUID 형식 생성
    import hashlib
    hash_bytes = hashlib.md5(point_id.encode()).hexdigest()
    return f"{hash_bytes[:8]}-{hash_bytes[8:12]}-{hash_bytes[12:16]}-{hash_bytes[16:20]}-{hash_bytes[20:32]}"


# 싱글톤으로 서비스 인스턴스 제공
qdrant_service = QdrantService() 