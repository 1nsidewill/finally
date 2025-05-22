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
    
    async def generate_embedding(self, text: str) -> List[float]:
        """텍스트에서 임베딩 벡터 생성 (임베딩 모델을 사용하여 구현 필요)"""
        # 여기서는 임시로 랜덤 벡터 생성 (실제 구현시 임베딩 모델로 대체해야 함)
        # 예: sentence-transformers, OpenAI 임베딩 API 등 사용
        return list(np.random.rand(self.vector_size).astype(float))
    
    async def insert_document(self, document: DocumentCreate, wait: bool = True) -> str:
        """문서를 Qdrant에 삽입"""
        # 문서 ID 처리 (제공된 경우 사용, 아니면 생성)
        doc_id = document.id if document.id else str(uuid.uuid4())
        
        # 문서 내용에서 임베딩 생성
        vector = await self.generate_embedding(document.content)
        
        # 메타데이터 준비
        metadata_dict = document.metadata.dict(exclude_none=True) if document.metadata else {}
        
        # Qdrant에 점 추가
        await self.async_client.upsert(
            collection_name=self.collection_name,
            wait=wait,
            points=[
                PointStruct(
                    id=doc_id,
                    vector=vector,
                    payload={
                        "content": document.content,
                        **metadata_dict
                    }
                )
            ]
        )
        
        return doc_id
    
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
        
        # ID 리스트가 제공되었는지 확인
        use_provided_ids = ids is not None and len(ids) == len(documents)
        
        for idx, document in enumerate(documents):
            # 문서 ID 처리 (제공된 경우 사용, 아니면 생성)
            if use_provided_ids:
                doc_id = ids[idx]
            else:
                doc_id = document.id if document.id else str(uuid.uuid4())
            
            doc_ids.append(doc_id)
            
            # 문서 내용에서 임베딩 생성
            vector = await self.generate_embedding(document.content)
            
            # 메타데이터 준비
            metadata_dict = document.metadata.dict(exclude_none=True) if document.metadata else {}
            
            # Qdrant 포인트 준비
            points.append(
                PointStruct(
                    id=doc_id,
                    vector=vector,
                    payload={
                        "content": document.content,
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


# 싱글톤으로 서비스 인스턴스 제공
qdrant_service = QdrantService() 