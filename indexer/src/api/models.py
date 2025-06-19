"""
API 요청/응답 모델들
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    result: str

class QdrantDocument:
    """Qdrant에 저장될 문서 구조를 정의하는 클래스"""
    
    def __init__(
        self,
        id: str,
        content: str,
        vector: List[float],
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.id = id
        self.content = content
        self.vector = vector
        self.metadata = metadata or {}
        
        # 메타데이터에 생성 시간이 없으면 현재 시간을 추가
        if "created_at" not in self.metadata:
            self.metadata["created_at"] = datetime.now().isoformat()
            
        if "updated_at" not in self.metadata:
            self.metadata["updated_at"] = self.metadata["created_at"]
    
    def to_dict(self) -> Dict[str, Any]:
        """Qdrant 포맷으로 변환"""
        return {
            "id": self.id,
            "content": self.content,
            "vector": self.vector,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QdrantDocument':
        """딕셔너리로부터 QdrantDocument 객체 생성"""
        return cls(
            id=data["id"],
            content=data["content"],
            vector=data["vector"],
            metadata=data.get("metadata", {})
        )
    
    def update(self, content: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """문서 내용과 메타데이터 업데이트"""
        if content is not None:
            self.content = content
        
        if metadata is not None:
            self.metadata.update(metadata)
        
        # 업데이트 시간 갱신
        self.metadata["updated_at"] = datetime.now().isoformat()

class SyncRequest(BaseModel):
    """수동 동기화 요청"""
    product_uid: str = Field(..., description="재처리할 매물의 UID")
    force: bool = Field(False, description="이미 처리된 매물도 강제로 재처리할지 여부")
    priority: str = Field("normal", description="작업 우선순위: high, normal, low")

class SyncResponse(BaseModel):
    """수동 동기화 응답"""
    message: str
    job_id: str
    product_uid: str
    timestamp: float

class RetryRequest(BaseModel):
    """실패 작업 재시도 요청"""
    operation_ids: Optional[List[int]] = Field(None, description="재시도할 특정 작업 ID들 (없으면 모든 재시도 가능한 작업)")
    max_operations: int = Field(100, description="한 번에 재시도할 최대 작업 수")
    operation_type: Optional[str] = Field(None, description="특정 작업 타입만 재시도 (sync, update, delete)")

class RetryResponse(BaseModel):
    """실패 작업 재시도 응답"""
    message: str
    retried_count: int
    failed_retry_count: int
    job_ids: List[str]
    timestamp: float

class QueueStatusResponse(BaseModel):
    """큐 상태 응답"""
    total_pending: int
    total_processing: int
    total_failed: int
    queue_details: Dict[str, Any]
    worker_status: Dict[str, Any]
    timestamp: float

class FailedOperation(BaseModel):
    """실패한 작업 정보"""
    id: int
    product_uid: str
    operation_type: str
    error_message: str
    retry_count: int
    max_retries: int
    next_retry_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    context: Optional[Dict[str, Any]]

class FailuresResponse(BaseModel):
    """실패 작업 목록 응답"""
    failed_operations: List[FailedOperation]
    total_count: int
    page: int
    page_size: int
    has_more: bool
    timestamp: float


