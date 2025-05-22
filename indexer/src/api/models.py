from pydantic import BaseModel
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
