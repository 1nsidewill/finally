import os

from fastapi import APIRouter, HTTPException, status, Request, Query
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional

from src.api.schema import (
    DocumentCreate, 
    DocumentCreateBatch,
    DocumentUpdate, 
    DocumentDelete, 
    BatchDocumentDelete, 
    DocumentListResponse, 
    DocumentSingleResponse,
    APIResponse,
    OperationResponse,
    SearchQuery,
    SearchVector
)
from src.config import get_settings
from src.services.qdrant_service import qdrant_service

config = get_settings()
api_router = APIRouter()

@api_router.post("/documents", response_model=DocumentSingleResponse)
async def create_document(
    document: DocumentCreate, 
    wait: bool = Query(True, description="작업 완료 후 응답할지 여부")
):
    """문서 생성 및 벡터 저장 엔드포인트"""
    try:
        doc_id = await qdrant_service.insert_document(document, wait=wait)
        
        # 문서 조회하여 응답 생성
        created_doc = await qdrant_service.get_document(doc_id)
        
        if created_doc:
            return DocumentSingleResponse(
                success=True,
                message="문서가 성공적으로 생성되었습니다.",
                data={
                    "id": doc_id,
                    "vector_id": doc_id,  # Qdrant에서는 동일한 ID 사용
                    "content": created_doc["content"],
                    "metadata": created_doc["metadata"]
                }
            )
        
        return DocumentSingleResponse(
            success=False,
            message="문서 생성 후 조회 중 오류가 발생했습니다.",
            data=None
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 생성 중 오류가 발생했습니다: {str(e)}"
        )

@api_router.post("/documents/batch", response_model=DocumentListResponse)
async def batch_create_documents(
    batch_request: DocumentCreateBatch, 
    wait: bool = Query(True, description="작업 완료 후 응답할지 여부")
):
    """여러 문서 일괄 생성 및 벡터 저장 엔드포인트"""
    try:
        doc_ids = await qdrant_service.batch_insert_documents(
            batch_request.documents, 
            ids=batch_request.ids,
            wait=wait
        )
        
        # 생성된 문서들 조회
        created_docs = []
        for doc_id in doc_ids:
            doc_data = await qdrant_service.get_document(doc_id)
            if doc_data:
                created_docs.append({
                    "id": doc_id,
                    "vector_id": doc_id,
                    "content": doc_data["content"],
                    "metadata": doc_data["metadata"]
                })
        
        return DocumentListResponse(
            success=True,
            message=f"{len(doc_ids)}개의 문서가 성공적으로 생성되었습니다.",
            data=created_docs if created_docs else None,
            total=len(doc_ids)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 일괄 생성 중 오류가 발생했습니다: {str(e)}"
        )

@api_router.get("/documents/{doc_id}", response_model=DocumentSingleResponse)
async def get_document(
    doc_id: str, 
    with_vectors: bool = Query(False, description="벡터 데이터 포함 여부")
):
    """문서 ID로 단일 문서 조회"""
    try:
        document = await qdrant_service.get_document(doc_id, with_vectors=with_vectors)
        
        if not document:
            return DocumentSingleResponse(
                success=False,
                message=f"문서 ID '{doc_id}'를 찾을 수 없습니다.",
                data=None
            )
        
        response_data = {
            "id": doc_id,
            "vector_id": doc_id,
            "content": document["content"],
            "metadata": document["metadata"]
        }
        
        # 벡터 데이터 포함 요청 시
        if with_vectors and "vector" in document:
            response_data["vector"] = document["vector"]
        
        return DocumentSingleResponse(
            success=True,
            message="문서를 성공적으로 조회했습니다.",
            data=response_data
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 조회 중 오류가 발생했습니다: {str(e)}"
        )

@api_router.get("/documents", response_model=DocumentListResponse)
async def list_documents(
    offset: int = Query(0, description="시작 인덱스", ge=0),
    limit: int = Query(10, description="문서 수", ge=1, le=100),
    with_vectors: bool = Query(False, description="벡터 데이터 포함 여부")
):
    """문서 목록 조회"""
    try:
        documents, total = await qdrant_service.list_documents(
            offset=offset,
            limit=limit,
            with_vectors=with_vectors
        )
        
        # 응답 데이터 구성
        response_data = []
        for doc in documents:
            item = {
                "id": doc["id"],
                "vector_id": doc["id"],
                "content": doc["content"],
                "metadata": doc["metadata"]
            }
            
            # 벡터 데이터 포함 요청 시
            if with_vectors and "vector" in doc:
                item["vector"] = doc["vector"]
            
            response_data.append(item)
        
        return DocumentListResponse(
            success=True,
            message=f"{len(documents)}개의 문서를 성공적으로 조회했습니다.",
            data=response_data,
            total=total
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 목록 조회 중 오류가 발생했습니다: {str(e)}"
        )

@api_router.put("/documents/{doc_id}", response_model=OperationResponse)
async def update_document(
    doc_id: str, 
    update_data: DocumentUpdate, 
    wait: bool = Query(True, description="작업 완료 후 응답할지 여부")
):
    """문서 업데이트 (내용 또는 메타데이터)"""
    try:
        # 문서 존재 여부 확인
        document = await qdrant_service.get_document(doc_id)
        if not document:
            return OperationResponse(
                success=False,
                message=f"업데이트할 문서 ID '{doc_id}'를 찾을 수 없습니다.",
                data=None
            )
        
        # 문서 업데이트
        operation_result = await qdrant_service.update_document(doc_id, update_data, wait=wait)
        
        if operation_result.get("success", False):
            return OperationResponse(
                success=True,
                message="문서가 성공적으로 업데이트되었습니다.",
                data={"id": doc_id},
                operation_id=operation_result.get("operation_id"),
                affected_count=1
            )
        
        return OperationResponse(
            success=False,
            message="문서 업데이트 중 오류가 발생했습니다.",
            data=None
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 업데이트 중 오류가 발생했습니다: {str(e)}"
        )

@api_router.delete("/documents/{doc_id}", response_model=OperationResponse)
async def delete_document(
    doc_id: str, 
    wait: bool = Query(True, description="작업 완료 후 응답할지 여부")
):
    """문서 삭제"""
    try:
        # 문서 존재 여부 확인
        document = await qdrant_service.get_document(doc_id)
        if not document:
            return OperationResponse(
                success=False,
                message=f"삭제할 문서 ID '{doc_id}'를 찾을 수 없습니다.",
                data=None
            )
        
        # 문서 삭제
        operation_result = await qdrant_service.delete_document(doc_id, wait=wait)
        
        if operation_result.get("success", False):
            return OperationResponse(
                success=True,
                message=f"문서 ID '{doc_id}'가 성공적으로 삭제되었습니다.",
                data=None,
                operation_id=operation_result.get("operation_id"),
                affected_count=1
            )
        
        return OperationResponse(
            success=False,
            message="문서 삭제 중 오류가 발생했습니다.",
            data=None
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 삭제 중 오류가 발생했습니다: {str(e)}"
        )

@api_router.post("/documents/batch-delete", response_model=OperationResponse)
async def batch_delete_documents(
    delete_request: BatchDocumentDelete, 
    wait: bool = Query(True, description="작업 완료 후 응답할지 여부")
):
    """여러 문서 일괄 삭제"""
    try:
        operation_result = await qdrant_service.batch_delete_documents(delete_request.ids, wait=wait)
        
        if operation_result.get("success", False):
            return OperationResponse(
                success=True,
                message=f"{len(delete_request.ids)}개의 문서가 성공적으로 삭제되었습니다.",
                data=None,
                operation_id=operation_result.get("operation_id"),
                affected_count=len(delete_request.ids)
            )
        
        return OperationResponse(
            success=False,
            message="문서 일괄 삭제 중 오류가 발생했습니다.",
            data=None
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 일괄 삭제 중 오류가 발생했습니다: {str(e)}"
        )

@api_router.post("/search/text", response_model=DocumentListResponse)
async def search_by_text(
    query: SearchQuery
):
    """텍스트 기반 벡터 유사도 검색"""
    try:
        results = await qdrant_service.search_documents_by_text(
            query.query_text, 
            limit=query.limit,
            filter=query.filter
        )
        
        # 결과 포맷팅
        formatted_results = []
        for doc in results:
            formatted_results.append({
                "id": doc["id"],
                "vector_id": doc["id"],
                "content": doc["content"],
                "metadata": doc["metadata"],
                "score": doc.get("score")  # 유사도 점수
            })
        
        return DocumentListResponse(
            success=True,
            message=f"'{query.query_text}'에 대한 검색 결과 {len(formatted_results)}건을 찾았습니다.",
            data=formatted_results,
            total=len(formatted_results)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"텍스트 검색 중 오류가 발생했습니다: {str(e)}"
        )

@api_router.post("/search/vector", response_model=DocumentListResponse)
async def search_by_vector(
    query: SearchVector
):
    """벡터 기반 유사도 검색"""
    try:
        results = await qdrant_service.search_documents_by_vector(
            query.query_vector, 
            limit=query.limit,
            filter=query.filter
        )
        
        # 결과 포맷팅
        formatted_results = []
        for doc in results:
            formatted_results.append({
                "id": doc["id"],
                "vector_id": doc["id"],
                "content": doc["content"],
                "metadata": doc["metadata"],
                "score": doc.get("score")  # 유사도 점수
            })
        
        return DocumentListResponse(
            success=True,
            message=f"벡터 검색 결과 {len(formatted_results)}건을 찾았습니다.",
            data=formatted_results,
            total=len(formatted_results)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"벡터 검색 중 오류가 발생했습니다: {str(e)}"
        )

@api_router.get("/documents/count", response_model=APIResponse)
async def count_documents():
    """컬렉션 내 문서 수 조회"""
    try:
        count = await qdrant_service.count_documents()
        
        return APIResponse(
            success=True,
            message=f"현재 문서 수: {count}",
            data={"count": count}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 수 조회 중 오류가 발생했습니다: {str(e)}"
        )
    

# src/api/router.py에 추가
from src.services.test_data_loader import run_test_data_loading

# 테스트 데이터 로드 엔드포인트
@api_router.post("/test/load-data", response_model=APIResponse)
async def load_test_data():
    """PostgreSQL에서 테스트 데이터를 가져와 Qdrant에 로드하는 테스트 엔드포인트"""
    try:
        count = await run_test_data_loading()
        return APIResponse(
            success=True,
            message=f"테스트 데이터 로드 완료: {count}개 문서 삽입됨",
            data={"count": count}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"테스트 데이터 로드 중 오류 발생: {str(e)}"
        )

@api_router.get("/cache/stats", response_model=APIResponse)
async def get_cache_stats():
    """임베딩 캐시 통계 조회"""
    try:
        stats = qdrant_service.get_cache_stats()
        return APIResponse(
            success=True,
            message="캐시 통계 조회 성공",
            data=stats
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"캐시 통계 조회 실패: {str(e)}")

@api_router.delete("/cache/clear", response_model=APIResponse)
async def clear_cache():
    """임베딩 캐시 삭제 (테스트 후 정리용)"""
    try:
        result = qdrant_service.clear_cache()
        return APIResponse(
            success=result["success"],
            message=result.get("message", "캐시 삭제 완료"),
            data=result
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"캐시 삭제 실패: {str(e)}")