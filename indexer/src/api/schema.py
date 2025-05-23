from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, model_validator


class DocumentMetadata(BaseModel):
    """문서의 메타데이터를 정의하는 모델"""
    id: Optional[str] = Field(None, description="DB의 고유 식별자(UID)")
    content: Optional[str] = Field(None, description="매물 전체 설명 텍스트")
    image_url: Optional[str] = Field(None, description="이미지 주소")
    last_modified_at: Optional[str] = Field(None, description="마지막 수정 시간 (ISO8601 형식)")
    model_name: Optional[str] = Field(None, description="모델명 (예: R3, R6 등)")
    brand: Optional[str] = Field(None, description="브랜드 (예: Yamaha, Honda 등)")
    category: Optional[str] = Field(None, description="장르 (예: sports, naked 등)")
    price: Optional[int] = Field(None, description="가격 (단위: 원)")
    km: Optional[int] = Field(None, description="주행거리 (단위: km)")
    year: Optional[int] = Field(None, description="연식 (연도)")
    color: Optional[str] = Field(None, description="색상")
    extra: Optional[Dict[str, Any]] = Field(None, description="추가 메타데이터")


class DocumentBase(BaseModel):
    """문서의 기본 정보를 정의하는 모델"""
    title: str = Field(..., description="매물 제목")
    metadata: Optional[DocumentMetadata] = Field(default_factory=DocumentMetadata, description="문서의 메타데이터")


class DocumentCreate(DocumentBase):
    """문서 생성 요청 모델"""
    id: Optional[str] = Field(None, description="문서 ID (지정하지 않으면 자동 생성)")
    embedding_text: Optional[str] = Field(None, description="임베딩 생성에 사용될 텍스트 (지정하지 않으면 자동 생성)")


class DocumentCreateBatch(BaseModel):
    """여러 문서 일괄 생성 요청 모델"""
    documents: List[DocumentCreate] = Field(..., description="생성할 문서 리스트")
    ids: Optional[List[str]] = Field(None, description="문서 ID 리스트 (지정하지 않으면 자동 생성)")
    
    @model_validator(mode='after')
    def validate_ids_length(self):
        """ids가 제공되면 documents와 길이가 일치하는지 확인"""
        values = self
        documents = values.documents
        ids = values.ids
        
        if ids is not None and len(documents) != len(ids):
            raise ValueError("ids 리스트의 길이는 documents 리스트의 길이와 같아야 합니다.")
        
        return values


class DocumentResponse(DocumentBase):
    """문서 응답 모델"""
    id: str = Field(..., description="문서의 고유 ID")
    vector_id: str = Field(..., description="벡터 데이터베이스 내 ID")
    score: Optional[float] = Field(None, description="검색 결과의 유사도 점수")


class DocumentUpdate(BaseModel):
    """문서 업데이트 요청 모델"""
    content: Optional[str] = Field(None, description="업데이트할 문서 내용")
    metadata: Optional[DocumentMetadata] = Field(None, description="업데이트할 메타데이터")

    class Config:
        validate_assignment = True


class DocumentDelete(BaseModel):
    """문서 삭제 요청 모델"""
    id: str = Field(..., description="삭제할 문서의 ID")


class BatchDocumentDelete(BaseModel):
    """여러 문서 일괄 삭제 요청 모델"""
    ids: List[str] = Field(..., description="삭제할 문서 ID 리스트")


class SearchQuery(BaseModel):
    """텍스트 검색 쿼리 모델"""
    query_text: str = Field(..., description="검색할 텍스트")
    limit: int = Field(5, description="반환할 최대 결과 수")
    filter: Optional[Dict[str, Any]] = Field(None, description="검색 필터")


class SearchVector(BaseModel):
    """벡터 검색 쿼리 모델"""
    query_vector: List[float] = Field(..., description="검색할 벡터")
    limit: int = Field(5, description="반환할 최대 결과 수")
    filter: Optional[Dict[str, Any]] = Field(None, description="검색 필터")


class APIResponse(BaseModel):
    """API 응답의 기본 구조"""
    success: bool = Field(..., description="요청 성공 여부")
    message: str = Field(..., description="응답 메시지")
    data: Optional[Any] = Field(None, description="응답 데이터")


class DocumentListResponse(APIResponse):
    """문서 목록 응답 모델"""
    data: Optional[List[DocumentResponse]] = None
    total: Optional[int] = Field(None, description="전체 문서 수")


class DocumentSingleResponse(APIResponse):
    """단일 문서 응답 모델"""
    data: Optional[DocumentResponse] = None


class OperationResponse(APIResponse):
    """작업 결과 응답 모델"""
    operation_id: Optional[str] = Field(None, description="작업 ID")
    affected_count: Optional[int] = Field(None, description="영향받은 문서 수")
