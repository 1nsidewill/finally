"""
FastAPI 라우터 설정
각 엔드포인트에 대한 메트릭 수집 포함
"""

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse
from typing import List, Dict, Any, Optional
import logging
import time
import json
from datetime import datetime

from ..database.postgresql import PostgreSQLManager
from ..database.qdrant import QdrantManager
from qdrant_client.models import PointStruct

from ..services.embedding_service import EmbeddingService, get_embedding_service
from ..monitoring.metrics import MetricsCollector, get_metrics_bytes
from ..config import get_settings
from .models import (
    SyncRequest, SyncResponse
)

logger = logging.getLogger(__name__)
router = APIRouter()

# 설정 로드
config = get_settings()

# 데이터베이스 매니저 인스턴스
postgres_manager = PostgreSQLManager()
qdrant_manager = QdrantManager()

# =============================================================================
# 상태 확인 엔드포인트
# =============================================================================

@router.get("/health", tags=["health"])
@MetricsCollector.track_redis_job("api", "health_check")
async def health_check():
    """서비스 상태 확인"""
    try:
        # 각 컴포넌트 상태 확인
        pg_manager = PostgreSQLManager()
        qdrant_manager = QdrantManager()
        
        status = {
            "status": "healthy",
            "timestamp": time.time(),
            "components": {
                "postgresql": "unknown",
                "qdrant": "unknown"
            }
        }
        
        # PostgreSQL 연결 확인
        try:
            async with pg_manager.get_connection() as conn:
                await conn.execute("SELECT 1")
                status["components"]["postgresql"] = "healthy"
        except Exception as e:
            status["components"]["postgresql"] = f"unhealthy: {str(e)}"
            status["status"] = "degraded"
        
        # Qdrant 연결 확인
        try:
            collections = await qdrant_manager.list_collections()
            status["components"]["qdrant"] = "healthy"
        except Exception as e:
            status["components"]["qdrant"] = f"unhealthy: {str(e)}"
            status["status"] = "degraded"
        

        
        return status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

# =============================================================================
# 메트릭 엔드포인트
# =============================================================================

@router.get("/metrics", tags=["monitoring"])
async def get_prometheus_metrics():
    """Prometheus 메트릭 반환"""
    try:
        metrics_data = get_metrics_bytes()
        return Response(
            content=metrics_data,
            media_type="text/plain; version=0.0.4; charset=utf-8"
        )
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")

@router.get("/metrics/status", tags=["monitoring"])
async def get_metrics_status():
    """메트릭 수집 상태 확인"""
    try:
        # 시스템 메트릭 업데이트
        await MetricsCollector.update_system_metrics()
        
        return {
            "status": "collecting",
            "timestamp": time.time(),
            "metrics_endpoint": "/metrics"
        }
        
    except Exception as e:
        logger.error(f"Failed to get metrics status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get metrics status: {str(e)}")

# =============================================================================
# 동기화 관련 엔드포인트
# =============================================================================



@router.get("/sync/status", tags=["sync"])
async def get_sync_status():
    """동기화 상태 확인"""
    try:
        pg_manager = PostgreSQLManager()
        qdrant_manager = QdrantManager()
        
        # PostgreSQL에서 총 매물 수 확인
        async with pg_manager.get_connection() as conn:
            pg_count_result = await conn.fetchrow(
                "SELECT COUNT(*) as total FROM products"
            )
            pg_total = pg_count_result['total'] if pg_count_result else 0
        
        # Qdrant에서 총 벡터 수 확인
        try:
            collection_info = await qdrant_manager.get_collection_info()
            qdrant_total = collection_info.points_count if collection_info else 0
        except Exception as e:
            logger.warning(f"Failed to get Qdrant count: {e}")
            qdrant_total = 0
        
        # 동기화 상태 계산
        sync_percentage = (qdrant_total / pg_total * 100) if pg_total > 0 else 0
        
        return {
            "postgresql_count": pg_total,
            "qdrant_count": qdrant_total,
            "sync_percentage": round(sync_percentage, 2),
            "needs_sync": pg_total != qdrant_total,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to get sync status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get sync status: {str(e)}")

# =============================================================================
# 검색 엔드포인트 (추후 구현)
# =============================================================================

@router.post("/search", tags=["search"])
async def vector_search(query: Dict[str, Any]):
    """벡터 검색 (추후 구현)"""
    try:
        # 현재는 플레이스홀더
        return {
            "message": "Vector search endpoint - coming soon",
            "query": query,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Vector search failed: {e}")
        raise HTTPException(status_code=500, detail=f"Vector search failed: {str(e)}")

# =============================================================================
# 🔄 데이터 리셋 및 개별 처리 엔드포인트
# =============================================================================

@router.post("/reset/qdrant", tags=["reset"])
async def reset_qdrant_collection():
    """🗑️ Qdrant bike 컬렉션 완전 삭제 및 재생성"""
    try:
        qdrant_manager = QdrantManager()
        
        # 컬렉션 존재 확인
        collections = await qdrant_manager.list_collections()
        collection_name = config.QDRANT_COLLECTION
        
        if collection_name in collections:
            # 기존 컬렉션 삭제
            await qdrant_manager.delete_collection()
            logger.info(f"컬렉션 '{collection_name}' 삭제 완료")
        
        # 새 컬렉션 생성
        await qdrant_manager.create_collection()
        logger.info(f"컬렉션 '{collection_name}' 생성 완료")
        
        return {
            "success": True,
            "message": f"Qdrant 컬렉션 '{collection_name}' 리셋 완료",
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Qdrant 컬렉션 리셋 실패: {e}")
        raise HTTPException(status_code=500, detail=f"Qdrant 컬렉션 리셋 실패: {str(e)}")

@router.post("/reset/postgresql", tags=["reset"])
async def reset_postgresql_flags():
    """🔄 PostgreSQL product 테이블의 is_conversion, vector_id 리셋"""
    try:
        pg_manager = PostgreSQLManager()
        
        async with pg_manager.get_connection() as conn:
            # is_conversion을 false로, vector_id를 null로 리셋
            result = await conn.execute(
                "UPDATE product SET is_conversion = false, vector_id = null"
            )
            
            # 확인용 통계
            stats = await conn.fetchrow(
                """
                SELECT 
                    COUNT(*) as total_products,
                    COUNT(CASE WHEN is_conversion = true THEN 1 END) as converted_count,
                    COUNT(CASE WHEN vector_id IS NOT NULL THEN 1 END) as vector_id_count
                FROM product
                """
            )
            
        return {
            "success": True,
            "message": "PostgreSQL 플래그 리셋 완료",
            "stats": dict(stats),
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"PostgreSQL 플래그 리셋 실패: {e}")
        raise HTTPException(status_code=500, detail=f"PostgreSQL 플래그 리셋 실패: {str(e)}")

@router.post("/process/single/{uid}", tags=["process"])
async def process_single_product(uid: str):
    """🔧 단일 제품 벡터화 처리 (테스트용)"""
    try:
        pg_manager = PostgreSQLManager()
        qdrant_manager = QdrantManager()
        embedding_service = EmbeddingService()
        
        # 1. PostgreSQL에서 제품 정보 조회
        async with pg_manager.get_connection() as conn:
            # uid가 숫자인지 확인 후 적절한 타입으로 변환
            try:
                if uid.isdigit():
                    uid_param = int(uid)
                else:
                    uid_param = uid
            except:
                uid_param = uid
            
            product = await conn.fetchrow(
                "SELECT uid, pid, provider_uid, title, content, price FROM product WHERE uid = $1",
                uid_param
            )
            
            if not product:
                raise HTTPException(status_code=404, detail=f"제품을 찾을 수 없습니다: {uid}")
        
        # 2. 벡터 ID 생성 (provider_uid:pid 기반)
        from ..database.qdrant import generate_product_vector_id
        vector_id = generate_product_vector_id(
            str(product['provider_uid'] or product['uid']), 
            str(product['pid'])
        )
        
        # 3. 임베딩 생성
        text_to_embed = f"{product['title']} {product['content'] or ''}"
        embedding = await embedding_service.generate_embedding(text_to_embed)
        
        # 4. Qdrant에 업로드
        payload = {
            "uid": str(product['uid']),
            "pid": str(product['pid']),
            "provider_uid": product['provider_uid'],
            "title": product['title'],
            "price": product['price']
        }
        
        await qdrant_manager.upsert_points([{
            "id": vector_id,
            "vector": embedding,
            "payload": payload
        }])
        
        # 5. PostgreSQL 업데이트
        async with pg_manager.get_connection() as conn:
            await conn.execute(
                "UPDATE product SET vector_id = $1, is_conversion = true WHERE uid = $2",
                vector_id, uid_param
            )
        
        return {
            "success": True,
            "message": f"제품 {uid} 처리 완료",
            "data": {
                "uid": str(product['uid']),
                "pid": str(product['pid']),
                "vector_id": vector_id,
                "title": product['title'][:50] + "..." if len(product['title']) > 50 else product['title']
            },
            "timestamp": time.time()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"단일 제품 처리 실패 {uid}: {e}")
        raise HTTPException(status_code=500, detail=f"단일 제품 처리 실패: {str(e)}")

@router.get("/test/sample-products", tags=["test"])
async def get_sample_products(limit: int = 5):
    """🧪 테스트용 샘플 제품 목록 조회"""
    try:
        pg_manager = PostgreSQLManager()
        
        async with pg_manager.get_connection() as conn:
            products = await conn.fetch(
                """
                SELECT uid, pid, provider_uid, title, is_conversion, vector_id
                FROM product 
                WHERE title IS NOT NULL AND title != ''
                ORDER BY uid 
                LIMIT $1
                """,
                limit
            )
        
        return {
            "success": True,
            "message": f"샘플 제품 {len(products)}개 조회",
            "products": [
                {
                    "uid": str(p['uid']),
                    "pid": str(p['pid']),
                    "provider_uid": p['provider_uid'],
                    "title": p['title'][:50] + "..." if len(p['title']) > 50 else p['title'],
                    "is_conversion": p['is_conversion'],
                    "vector_id": str(p['vector_id']) if p['vector_id'] else None
                }
                for p in products
            ],
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"샘플 제품 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"샘플 제품 조회 실패: {str(e)}")

@router.get("/test/qdrant-status", tags=["test"])
async def get_qdrant_test_status():
    """🔍 Qdrant 컬렉션 상태 확인"""
    try:
        qdrant_manager = QdrantManager()
        
        # 컬렉션 정보 조회
        collections = await qdrant_manager.list_collections()
        collection_name = config.QDRANT_COLLECTION
        
        collection_exists = collection_name in collections
        
        if collection_exists:
            collection_info = await qdrant_manager.get_collection_info()
            points_count = collection_info.get('points_count', 0) if collection_info else 0
        else:
            points_count = 0
        
        return {
            "success": True,
            "collection_name": collection_name,
            "collection_exists": collection_exists,
            "points_count": points_count,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Qdrant 상태 확인 실패: {e}")
        raise HTTPException(status_code=500, detail=f"Qdrant 상태 확인 실패: {str(e)}")

@router.post("/process/batch-small", tags=["process"])
async def process_small_batch(start_uid: int, count: int = 10):
    """🔧 소량 배치 처리 (테스트 완료 후 사용)"""
    try:
        if count > 50:
            raise HTTPException(status_code=400, detail="count는 50 이하여야 합니다")
        
        pg_manager = PostgreSQLManager()
        
        # 처리할 제품들 조회
        async with pg_manager.get_connection() as conn:
            products = await conn.fetch(
                """
                SELECT uid FROM product 
                WHERE uid >= $1 AND title IS NOT NULL AND title != ''
                AND is_conversion = false
                ORDER BY uid 
                LIMIT $2
                """,
                start_uid, count
            )
        
        results = {
            "success": 0,
            "failed": 0,
            "errors": []
        }
        
        # 각 제품을 개별 처리
        for product in products:
            try:
                # 단일 처리 엔드포인트 로직 재사용
                await process_single_product(str(product['uid']))
                results["success"] += 1
            except Exception as e:
                results["failed"] += 1
                results["errors"].append(f"UID {product['uid']}: {str(e)}")
                logger.error(f"배치 처리 중 실패 {product['uid']}: {e}")
        
        return {
            "success": True,
            "message": f"소량 배치 처리 완료: 성공 {results['success']}, 실패 {results['failed']}",
            "results": results,
            "timestamp": time.time()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"소량 배치 처리 실패: {e}")
        raise HTTPException(status_code=500, detail=f"소량 배치 처리 실패: {str(e)}")

# =============================================================================
# 디버깅 엔드포인트
# =============================================================================

@router.get("/debug/info", tags=["debug"])
async def get_debug_info():
    """디버깅 정보 반환"""
    try:
        return {
            "config": {
                "embedding_model": config.OPENAI_EMBEDDING_MODEL,
                "qdrant_collection": config.QDRANT_COLLECTION
            },
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to get debug info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get debug info: {str(e)}")

# =============================================================================
# 폴링 기반 작업 관리 API
# =============================================================================

@router.post("/sync/direct", tags=["operations"], response_model=SyncResponse)
async def direct_sync(request: SyncRequest):
    """직접 동기화 실행 (큐 없이)"""
    try:
        # 직접 동기화 로직 (큐 없이)
        embedding_service = EmbeddingService()
        pg_manager = PostgreSQLManager()
        qdrant_manager = QdrantManager()
        
        processed_count = 0
        failed_count = 0
        
        async with pg_manager.get_connection() as conn:
            if request.product_uid:
                # 특정 UID 동기화
                try:
                    result = await conn.fetchrow(
                        "SELECT * FROM products WHERE uid = $1", request.product_uid
                    )
                    if result:
                        # 임베딩 생성 및 Qdrant 업로드
                        await embedding_service.process_product(dict(result))
                        processed_count += 1
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    logger.error(f"Failed to sync product {request.product_uid}: {e}")
                    failed_count += 1
                    
        return SyncResponse(
            status="completed",
            message=f"직접 동기화 완료. 처리: {processed_count}, 실패: {failed_count}",
            processed_count=processed_count,
            failed_count=failed_count
        )
        
    except Exception as e:
        logger.error(f"Direct sync failed: {e}")
        raise HTTPException(status_code=500, detail=f"Direct sync failed: {str(e)}")

@router.get("/status/processing", tags=["monitoring"])
async def get_processing_status():
    """현재 처리 상태 확인 (폴링용)"""
    try:
        pg_manager = PostgreSQLManager()
        qdrant_manager = QdrantManager()
        
        # 데이터베이스 상태 확인
        async with pg_manager.get_connection() as conn:
            # 총 제품 수
            total_products = await conn.fetchval("SELECT COUNT(*) FROM products")
            
            # 최근 업데이트된 제품 수 (1시간 내)
            recent_updates = await conn.fetchval(
                "SELECT COUNT(*) FROM products WHERE updated_at > NOW() - INTERVAL '1 hour'"
            )
        
        # Qdrant 상태 확인
        try:
            collection_info = await qdrant_manager.get_collection_info()
            vector_count = collection_info.points_count if collection_info else 0
        except Exception as e:
            logger.warning(f"Failed to get Qdrant count: {e}")
            vector_count = 0
        
        # 동기화 백분율
        sync_percentage = (vector_count / total_products * 100) if total_products > 0 else 0
        
        return {
            "status": "active",
            "total_products": total_products,
            "vector_count": vector_count,
            "sync_percentage": round(sync_percentage, 2),
            "recent_updates": recent_updates,
            "needs_sync": total_products != vector_count,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to get processing status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get processing status: {str(e)}")

# Qdrant Storage Optimization 엔드포인트들 추가
@router.post("/qdrant/optimize", response_model=Dict[str, Any])
async def optimize_qdrant_collection():
    """🧹 Qdrant 컬렉션 최적화 실행"""
    try:
        qdrant_manager = QdrantManager()
        result = await qdrant_manager.optimize_collection()
        
        return {
            "success": True,
            "message": "컬렉션 최적화 완료",
            "data": result
        }
    except Exception as e:
        logger.error(f"컬렉션 최적화 실패: {e}")
        raise HTTPException(status_code=500, detail=f"컬렉션 최적화 실패: {str(e)}")

@router.get("/qdrant/storage-stats", response_model=Dict[str, Any])
async def get_qdrant_storage_stats():
    """📊 Qdrant 스토리지 사용량 및 성능 통계"""
    try:
        qdrant_manager = QdrantManager()
        stats = await qdrant_manager.get_storage_stats()
        
        return {
            "success": True,
            "message": "스토리지 통계 조회 완료",
            "data": stats
        }
    except Exception as e:
        logger.error(f"스토리지 통계 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"스토리지 통계 조회 실패: {str(e)}")

@router.post("/qdrant/batch-upload-optimized", response_model=Dict[str, Any])
async def batch_upload_optimized(
    batch_size: int = 100,
    parallel_batches: int = 3,
    wait: bool = False
):
    """🚀 최적화된 배치 업로드 (테스트용 - 실제 데이터는 sync 엔드포인트 사용)"""
    try:
        # 이 엔드포인트는 테스트/데모용입니다
        # 실제 데이터 업로드는 /sync 엔드포인트를 통해 수행됩니다
        
        return {
            "success": True,
            "message": "배치 업로드 최적화 설정 확인됨",
            "config": {
                "batch_size": batch_size,
                "parallel_batches": parallel_batches,
                "wait": wait
            },
            "note": "실제 데이터 업로드는 /sync 엔드포인트를 사용하세요"
        }
    except Exception as e:
        logger.error(f"배치 업로드 설정 확인 실패: {e}")
        raise HTTPException(status_code=500, detail=f"배치 업로드 설정 확인 실패: {str(e)}")



# =============================================================================
# 제품 관리 엔드포인트 (폴링 기반)
# =============================================================================

@router.get("/products/count", tags=["products"])
async def get_products_count():
    """제품 수 조회"""
    try:
        pg_manager = PostgreSQLManager()
        
        async with pg_manager.get_connection() as conn:
            total_count = await conn.fetchval("SELECT COUNT(*) FROM products")
            
            # 제공업체별 수
            provider_counts = await conn.fetch(
                "SELECT provider, COUNT(*) as count FROM products GROUP BY provider"
            )
            
            return {
                "total_count": total_count,
                "provider_counts": {row['provider']: row['count'] for row in provider_counts},
                "timestamp": time.time()
            }
            
    except Exception as e:
        logger.error(f"Failed to get products count: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get products count: {str(e)}")

@router.get("/products/recent", tags=["products"])
async def get_recent_products(limit: int = 10):
    """최근 제품 목록 조회"""
    try:
        pg_manager = PostgreSQLManager()
        
        async with pg_manager.get_connection() as conn:
            rows = await conn.fetch(
                "SELECT uid, title, provider, price, created_at "
                "FROM products ORDER BY created_at DESC LIMIT $1",
                limit
            )
            
            products = []
            for row in rows:
                products.append({
                    "uid": row['uid'],
                    "title": row['title'],
                    "provider": row['provider'],
                    "price": row['price'],
                    "created_at": row['created_at'].isoformat() if row['created_at'] else None
                })
            
            return {
                "products": products,
                "count": len(products),
                "timestamp": time.time()
            }
            
    except Exception as e:
        logger.error(f"Failed to get recent products: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get recent products: {str(e)}")

@router.get("/products/{product_uid}/sync-status", tags=["products"])
async def get_product_sync_status(product_uid: str):
    """특정 제품의 동기화 상태 확인"""
    try:
        pg_manager = PostgreSQLManager()
        qdrant_manager = QdrantManager()
        
        # PostgreSQL에서 제품 정보 조회
        async with pg_manager.get_connection() as conn:
            product = await conn.fetchrow(
                "SELECT uid, title, provider, updated_at FROM products WHERE uid = $1",
                product_uid
            )
            
            if not product:
                raise HTTPException(status_code=404, detail="Product not found")
        
        # Qdrant에서 벡터 존재 확인
        try:
            point = await qdrant_manager.get_point(product_uid)
            vector_exists = point is not None
        except Exception as e:
            logger.warning(f"Failed to check vector for {product_uid}: {e}")
            vector_exists = False
        
        return {
            "product_uid": product_uid,
            "title": product['title'],
            "provider": product['provider'],
            "updated_at": product['updated_at'].isoformat() if product['updated_at'] else None,
            "vector_exists": vector_exists,
            "sync_needed": not vector_exists,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Failed to get product sync status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get product sync status: {str(e)}")

@router.post("/sync/poll")
async def poll_and_sync():
    """
    메인 폴링 엔드포인트: PostgreSQL에서 변경사항을 감지하고 Qdrant와 동기화
    
    작업 분류:
    - INSERT: status=1, is_conversion=false, vector_id=null
    - DELETE: status!=1, is_conversion=true  
    - UPDATE: status=1, is_conversion=false, vector_id not null
    """
    try:
        logger.info("🚀 동기화 폴링 시작")
        
        # PostgreSQL에서 동기화 대상 조회
        async with postgres_manager.get_connection() as conn:
            # INSERT 대상: 새로 추가된 활성 상품들 (file 테이블과 조인하여 이미지 URL 포함)
            insert_query = """
                SELECT p.provider_uid, p.pid, p.title, p.brand, p.content, p.price, p.location, p.odo, p.year, p.uid,
                       f.url as file_url, f.count as file_count
                FROM product p
                LEFT JOIN file f ON p.uid = f.product_uid
                WHERE p.status = 1 AND p.is_conversion = false AND p.vector_id IS NULL
                ORDER BY p.created_dt ASC
                LIMIT 5000
            """
            insert_products = await conn.fetch(insert_query)
            
            # DELETE 대상: 비활성화된 상품들
            delete_query = """
                SELECT provider_uid, pid, vector_id
                FROM product 
                WHERE status != 1 AND is_conversion = true AND vector_id IS NOT NULL
                ORDER BY updated_dt ASC
                LIMIT 5000
            """
            delete_products = await conn.fetch(delete_query)
            
            # UPDATE 대상: 수정된 활성 상품들 (file 테이블과 조인하여 이미지 URL 포함)
            update_query = """
                SELECT p.provider_uid, p.pid, p.title, p.brand, p.content, p.price, p.location, p.odo, p.year, p.vector_id, p.uid,
                       f.url as file_url, f.count as file_count
                FROM product p
                LEFT JOIN file f ON p.uid = f.product_uid
                WHERE p.status = 1 AND p.is_conversion = false AND p.vector_id IS NOT NULL
                ORDER BY p.updated_dt ASC
                LIMIT 5000
            """
            update_products = await conn.fetch(update_query)
        
        results = {
            "insert_count": 0,
            "update_count": 0, 
            "delete_count": 0,
            "errors": []
        }
        
        # 1. INSERT 처리
        if insert_products:
            logger.info(f"📝 INSERT 처리: {len(insert_products)}개 상품")
            
            # 임베딩 생성을 위한 텍스트 준비
            embedding_service = get_embedding_service()
            texts_to_embed = []
            
            for product in insert_products:
                # provider_uid:pid 형식으로 vector_id 생성
                vector_id_str = f"{product['provider_uid']}:{product['pid']}"
                
                # 임베딩할 텍스트 조합 (brand + title + content + price + location + odo + year)
                text_parts = []
                if product['brand']:
                    text_parts.append(str(product['brand']))
                if product['title']:
                    text_parts.append(str(product['title']))
                if product['content']:
                    text_parts.append(str(product['content']))
                if product['price']:
                    text_parts.append(f"가격 {product['price']}원")
                if product['location']:
                    text_parts.append(f"위치 {product['location']}")
                if product['odo']:
                    text_parts.append(f"주행거리 {product['odo']}km")
                if product['year']:
                    text_parts.append(f"연식 {product['year']}년")
                
                combined_text = " ".join(text_parts)
                texts_to_embed.append(combined_text)
            
            # 배치로 임베딩 생성
            embeddings = await embedding_service.create_embeddings_async(texts_to_embed)
            
            # Qdrant에 벡터 삽입
            points_to_insert = []
            successful_inserts = []
            
            for i, (product, embedding) in enumerate(zip(insert_products, embeddings)):
                if embedding is not None:
                    vector_id_str = f"{product['provider_uid']}:{product['pid']}"
                    # UUID 생성
                    import uuid
                    import hashlib
                    
                    # provider_uid:pid를 hash하여 UUID 생성
                    hash_string = hashlib.md5(vector_id_str.encode()).hexdigest()
                    vector_uuid = str(uuid.UUID(hash_string))
                    
                    # 이미지 URL 리스트 생성
                    image_urls = []
                    if product['file_url'] and product['file_count'] and '{cnt}' in product['file_url']:
                        file_count = product['file_count']
                        url_template = product['file_url']
                        for i in range(1, file_count + 1):
                            image_url = url_template.replace('{cnt}', str(i))
                            image_urls.append(image_url)
                    
                    # payload 구성
                    payload = {
                        "provider_uid": product['provider_uid'],
                        "pid": str(product['pid']),
                        "title": product['title'] or "",
                        "brand": product['brand'] or "",
                        "content": product['content'] or "",
                        "price": float(product['price']) if product['price'] else 0.0,
                        "location": product['location'] or "",
                        "odo": float(product['odo']) if product['odo'] else 0.0,
                        "year": int(product['year']) if product['year'] else 0,
                        "image_url": image_urls
                    }
                    
                    point = PointStruct(
                        id=vector_uuid,
                        vector=embedding.tolist(),
                        payload=payload
                    )
                    points_to_insert.append(point)
                    successful_inserts.append((product['provider_uid'], product['pid'], vector_uuid))
            
            # Qdrant에 배치 삽입
            if points_to_insert:
                await qdrant_manager.upsert_points_batch_optimized(points_to_insert)
                
                # PostgreSQL 업데이트: is_conversion=true, vector_id 설정
                async with postgres_manager.get_connection() as conn:
                    for provider_uid, pid, vector_uuid in successful_inserts:
                        await conn.execute(
                            "UPDATE product SET is_conversion = true, vector_id = $1, updated_dt = NOW() WHERE provider_uid = $2 AND pid = $3",
                            vector_uuid, provider_uid, pid
                        )
                
                results["insert_count"] = len(successful_inserts)
                logger.info(f"✅ INSERT 완료: {len(successful_inserts)}개 상품")
        
        # 2. DELETE 처리
        if delete_products:
            logger.info(f"🗑️ DELETE 처리: {len(delete_products)}개 상품")
            
            vector_ids_to_delete = [str(product['vector_id']) for product in delete_products if product['vector_id']]
            
            if vector_ids_to_delete:
                # Qdrant에서 벡터 삭제
                await qdrant_manager.delete_points(vector_ids_to_delete)
                
                # PostgreSQL 업데이트: vector_id를 null로 설정
                async with postgres_manager.get_connection() as conn:
                    for product in delete_products:
                        if product['vector_id']:
                            await conn.execute(
                                "UPDATE product SET vector_id = NULL, updated_dt = NOW() WHERE provider_uid = $1 AND pid = $2",
                                product['provider_uid'], product['pid']
                            )
                
                results["delete_count"] = len(vector_ids_to_delete)
                logger.info(f"✅ DELETE 완료: {len(vector_ids_to_delete)}개 상품")
        
        # 3. UPDATE 처리 (기존 벡터 삭제 후 새 임베딩으로 재삽입)
        if update_products:
            logger.info(f"🔄 UPDATE 처리: {len(update_products)}개 상품")
            
            # 먼저 기존 벡터들을 삭제
            vector_ids_to_delete = [str(product['vector_id']) for product in update_products if product['vector_id']]
            if vector_ids_to_delete:
                logger.info(f"🗑️ 기존 벡터 삭제: {len(vector_ids_to_delete)}개")
                await qdrant_manager.delete_points(vector_ids_to_delete)
            
            # 임베딩 생성을 위한 텍스트 준비
            texts_to_embed = []
            
            for product in update_products:
                # 임베딩할 텍스트 조합 (새로운 데이터로 다시 생성)
                text_parts = []
                if product['brand']:
                    text_parts.append(str(product['brand']))
                if product['title']:
                    text_parts.append(str(product['title']))
                if product['content']:
                    text_parts.append(str(product['content']))
                if product['price']:
                    text_parts.append(f"가격 {product['price']}원")
                if product['location']:
                    text_parts.append(f"위치 {product['location']}")
                if product['odo']:
                    text_parts.append(f"주행거리 {product['odo']}km")
                if product['year']:
                    text_parts.append(f"연식 {product['year']}년")
                
                combined_text = " ".join(text_parts)
                texts_to_embed.append(combined_text)
            
            # 배치로 새 임베딩 생성
            embeddings = await embedding_service.create_embeddings_async(texts_to_embed)
            
            # 새 임베딩으로 Qdrant에 재삽입
            points_to_insert = []
            successful_updates = []
            
            for i, (product, embedding) in enumerate(zip(update_products, embeddings)):
                if embedding is not None and product['vector_id']:
                    # 이미지 URL 리스트 생성
                    image_urls = []
                    if product['file_url'] and product['file_count'] and '{cnt}' in product['file_url']:
                        file_count = product['file_count']
                        url_template = product['file_url']
                        for j in range(1, file_count + 1):
                            image_url = url_template.replace('{cnt}', str(j))
                            image_urls.append(image_url)
                    
                    # 동일한 vector_id를 사용하여 재삽입
                    # payload 구성
                    payload = {
                        "provider_uid": product['provider_uid'],
                        "pid": str(product['pid']),
                        "title": product['title'] or "",
                        "brand": product['brand'] or "",
                        "content": product['content'] or "",
                        "price": float(product['price']) if product['price'] else 0.0,
                        "location": product['location'] or "",
                        "odo": float(product['odo']) if product['odo'] else 0.0,
                        "year": int(product['year']) if product['year'] else 0,
                        "image_url": image_urls
                    }
                    
                    point = PointStruct(
                        id=str(product['vector_id']),  # 기존과 동일한 vector_id 사용 (문자열 변환)
                        vector=embedding.tolist(),  # 새로 생성된 임베딩
                        payload=payload
                    )
                    points_to_insert.append(point)
                    successful_updates.append((product['provider_uid'], product['pid']))
            
            # Qdrant에 새 벡터들 배치 삽입
            if points_to_insert:
                logger.info(f"📝 새 임베딩으로 재삽입: {len(points_to_insert)}개")
                await qdrant_manager.upsert_points_batch_optimized(points_to_insert)
                
                # PostgreSQL 업데이트: is_conversion=true
                async with postgres_manager.get_connection() as conn:
                    for provider_uid, pid in successful_updates:
                        await conn.execute(
                            "UPDATE product SET is_conversion = true, updated_dt = NOW() WHERE provider_uid = $1 AND pid = $2",
                            provider_uid, pid
                        )
                
                results["update_count"] = len(successful_updates)
                logger.info(f"✅ UPDATE 완료: {len(successful_updates)}개 상품 (삭제 후 재삽입)")
        
        logger.info("🎉 동기화 폴링 완료")
        return {
            "status": "success",
            "message": "동기화 폴링이 성공적으로 완료되었습니다",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"동기화 폴링 실패: {e}")
        return {
            "status": "error",
            "message": f"동기화 폴링 중 오류 발생: {str(e)}"
        }

@router.post("/sync/poll-test")
async def poll_and_sync_test(limit: int = 100):
    """
    테스트용 폴링 엔드포인트: 제한된 수량만 동기화 (기본 100개)
    
    작업 분류:
    - INSERT: status=1, is_conversion=false, vector_id=null
    - DELETE: status!=1, is_conversion=true  
    - UPDATE: status=1, is_conversion=false, vector_id not null
    """
    try:
        logger.info(f"🚀 테스트 동기화 폴링 시작 (limit: {limit})")
        
        # PostgreSQL에서 동기화 대상 조회 (제한된 수량)
        async with postgres_manager.get_connection() as conn:
            # INSERT 대상: 새로 추가된 활성 상품들 (file 테이블과 조인하여 이미지 URL 포함)
            insert_query = """
                SELECT p.provider_uid, p.pid, p.title, p.brand, p.content, p.price, p.location, p.odo, p.year, p.uid,
                       f.url as file_url, f.count as file_count
                FROM product p
                LEFT JOIN file f ON p.uid = f.product_uid
                WHERE p.status = 1 AND p.is_conversion = false AND p.vector_id IS NULL
                ORDER BY p.created_dt ASC
                LIMIT $1
            """
            insert_products = await conn.fetch(insert_query, limit)
            
            # DELETE 대상: 비활성화된 상품들
            delete_query = """
                SELECT provider_uid, pid, vector_id
                FROM product 
                WHERE status != 1 AND is_conversion = true AND vector_id IS NOT NULL
                ORDER BY updated_dt ASC
                LIMIT $1
            """
            delete_products = await conn.fetch(delete_query, limit)
            
            # UPDATE 대상: 수정된 활성 상품들 (file 테이블과 조인하여 이미지 URL 포함)
            update_query = """
                SELECT p.provider_uid, p.pid, p.title, p.brand, p.content, p.price, p.location, p.odo, p.year, p.vector_id, p.uid,
                       f.url as file_url, f.count as file_count
                FROM product p
                LEFT JOIN file f ON p.uid = f.product_uid
                WHERE p.status = 1 AND p.is_conversion = false AND p.vector_id IS NOT NULL
                ORDER BY p.updated_dt ASC
                LIMIT $1
            """
            update_products = await conn.fetch(update_query, limit)
        
        results = {
            "insert_count": 0,
            "update_count": 0, 
            "delete_count": 0,
            "errors": []
        }
        
        logger.info(f"📊 대상 수량 - INSERT: {len(insert_products)}, DELETE: {len(delete_products)}, UPDATE: {len(update_products)}")
        
        # 1. INSERT 처리
        if insert_products:
            logger.info(f"📝 INSERT 처리: {len(insert_products)}개 상품")
            
            # 임베딩 생성을 위한 텍스트 준비
            embedding_service = get_embedding_service()
            texts_to_embed = []
            
            for product in insert_products:
                # provider_uid:pid 형식으로 vector_id 생성
                vector_id_str = f"{product['provider_uid']}:{product['pid']}"
                
                # 임베딩할 텍스트 조합 (brand + title + content + price + location + odo + year)
                text_parts = []
                if product['brand']:
                    text_parts.append(str(product['brand']))
                if product['title']:
                    text_parts.append(str(product['title']))
                if product['content']:
                    text_parts.append(str(product['content']))
                if product['price']:
                    text_parts.append(f"가격 {product['price']}원")
                if product['location']:
                    text_parts.append(f"위치 {product['location']}")
                if product['odo']:
                    text_parts.append(f"주행거리 {product['odo']}km")
                if product['year']:
                    text_parts.append(f"연식 {product['year']}년")
                
                combined_text = " ".join(text_parts)
                texts_to_embed.append(combined_text)
                logger.debug(f"임베딩 텍스트 [{vector_id_str}]: {combined_text[:100]}...")
            
            # 배치로 임베딩 생성
            logger.info(f"🤖 임베딩 생성 중... ({len(texts_to_embed)}개)")
            embeddings = await embedding_service.create_embeddings_async(texts_to_embed)
            logger.info(f"✅ 임베딩 생성 완료: {sum(1 for e in embeddings if e is not None)}/{len(embeddings)}개 성공")
            
            # Qdrant에 벡터 삽입
            points_to_insert = []
            successful_inserts = []
            
            for i, (product, embedding) in enumerate(zip(insert_products, embeddings)):
                if embedding is not None:
                    vector_id_str = f"{product['provider_uid']}:{product['pid']}"
                    # UUID 생성
                    import uuid
                    import hashlib
                    
                    # provider_uid:pid를 hash하여 UUID 생성
                    hash_string = hashlib.md5(vector_id_str.encode()).hexdigest()
                    vector_uuid = str(uuid.UUID(hash_string))
                    
                    # 이미지 URL 리스트 생성
                    image_urls = []
                    if product['file_url'] and product['file_count'] and '{cnt}' in product['file_url']:
                        file_count = product['file_count']
                        url_template = product['file_url']
                        for j in range(1, file_count + 1):
                            image_url = url_template.replace('{cnt}', str(j))
                            image_urls.append(image_url)
                    
                    # payload 구성
                    payload = {
                         "provider_uid": product['provider_uid'],
                         "pid": str(product['pid']),
                         "title": product['title'] or "",
                         "brand": product['brand'] or "",
                         "content": product['content'] or "",
                         "price": float(product['price']) if product['price'] else 0.0,
                         "location": product['location'] or "",
                         "odo": float(product['odo']) if product['odo'] else 0.0,
                         "year": int(product['year']) if product['year'] else 0,
                         "image_url": image_urls
                     }
                     
                    point = PointStruct(
                        id=vector_uuid,
                        vector=embedding.tolist(),
                        payload=payload
                    )
                    points_to_insert.append(point)
                    successful_inserts.append((product['provider_uid'], product['pid'], vector_uuid))
                    logger.debug(f"Point 준비: {vector_id_str} -> {vector_uuid}")
                else:
                    logger.warning(f"임베딩 실패: {product['provider_uid']}:{product['pid']}")
            
            # Qdrant에 배치 삽입
            if points_to_insert:
                logger.info(f"📤 Qdrant에 벡터 업로드 중... ({len(points_to_insert)}개)")
                await qdrant_manager.upsert_points_batch_optimized(points_to_insert)
                logger.info(f"✅ Qdrant 업로드 완료")
                
                # PostgreSQL 업데이트: is_conversion=true, vector_id 설정
                logger.info(f"📊 PostgreSQL 상태 업데이트 중...")
                async with postgres_manager.get_connection() as conn:
                    for provider_uid, pid, vector_uuid in successful_inserts:
                        await conn.execute(
                            "UPDATE product SET is_conversion = true, vector_id = $1, updated_dt = NOW() WHERE provider_uid = $2 AND pid = $3",
                            vector_uuid, provider_uid, pid
                        )
                
                results["insert_count"] = len(successful_inserts)
                logger.info(f"✅ INSERT 완료: {len(successful_inserts)}개 상품")
        
        # 2. DELETE 처리
        if delete_products:
            logger.info(f"🗑️ DELETE 처리: {len(delete_products)}개 상품")
            
            vector_ids_to_delete = [str(product['vector_id']) for product in delete_products if product['vector_id']]
            
            if vector_ids_to_delete:
                # Qdrant에서 벡터 삭제
                await qdrant_manager.delete_points(vector_ids_to_delete)
                
                # PostgreSQL 업데이트: vector_id를 null로 설정
                async with postgres_manager.get_connection() as conn:
                    for product in delete_products:
                        if product['vector_id']:
                            await conn.execute(
                                "UPDATE product SET vector_id = NULL, updated_dt = NOW() WHERE provider_uid = $1 AND pid = $2",
                                product['provider_uid'], product['pid']
                            )
                
                results["delete_count"] = len(vector_ids_to_delete)
                logger.info(f"✅ DELETE 완료: {len(vector_ids_to_delete)}개 상품")
        
        # 3. UPDATE 처리 (기존 벡터 삭제 후 새 임베딩으로 재삽입)
        if update_products:
            logger.info(f"🔄 UPDATE 처리: {len(update_products)}개 상품")
            
            # 먼저 기존 벡터들을 삭제
            vector_ids_to_delete = [str(product['vector_id']) for product in update_products if product['vector_id']]
            if vector_ids_to_delete:
                logger.info(f"🗑️ 기존 벡터 삭제: {len(vector_ids_to_delete)}개")
                await qdrant_manager.delete_points(vector_ids_to_delete)
            
            # 임베딩 생성을 위한 텍스트 준비
            texts_to_embed = []
            
            for product in update_products:
                # 임베딩할 텍스트 조합 (새로운 데이터로 다시 생성)
                text_parts = []
                if product['brand']:
                    text_parts.append(str(product['brand']))
                if product['title']:
                    text_parts.append(str(product['title']))
                if product['content']:
                    text_parts.append(str(product['content']))
                if product['price']:
                    text_parts.append(f"가격 {product['price']}원")
                if product['location']:
                    text_parts.append(f"위치 {product['location']}")
                if product['odo']:
                    text_parts.append(f"주행거리 {product['odo']}km")
                if product['year']:
                    text_parts.append(f"연식 {product['year']}년")
                
                combined_text = " ".join(text_parts)
                texts_to_embed.append(combined_text)
            
            # 배치로 새 임베딩 생성
            embeddings = await embedding_service.create_embeddings_async(texts_to_embed)
            
            # 새 임베딩으로 Qdrant에 재삽입
            points_to_insert = []
            successful_updates = []
            
            for i, (product, embedding) in enumerate(zip(update_products, embeddings)):
                if embedding is not None and product['vector_id']:
                    # 이미지 URL 리스트 생성
                    image_urls = []
                    if product['file_url'] and product['file_count'] and '{cnt}' in product['file_url']:
                        file_count = product['file_count']
                        url_template = product['file_url']
                        for j in range(1, file_count + 1):
                            image_url = url_template.replace('{cnt}', str(j))
                            image_urls.append(image_url)
                    
                    # 동일한 vector_id를 사용하여 재삽입
                    # payload 구성
                    payload = {
                        "provider_uid": product['provider_uid'],
                        "pid": str(product['pid']),
                        "title": product['title'] or "",
                        "brand": product['brand'] or "",
                        "content": product['content'] or "",
                        "price": float(product['price']) if product['price'] else 0.0,
                        "location": product['location'] or "",
                        "odo": float(product['odo']) if product['odo'] else 0.0,
                        "year": int(product['year']) if product['year'] else 0,
                        "image_url": image_urls
                    }
                    
                    point = PointStruct(
                        id=str(product['vector_id']),  # 기존과 동일한 vector_id 사용 (문자열 변환)
                        vector=embedding.tolist(),  # 새로 생성된 임베딩
                        payload=payload
                    )
                    points_to_insert.append(point)
                    successful_updates.append((product['provider_uid'], product['pid']))
            
            # Qdrant에 새 벡터들 배치 삽입
            if points_to_insert:
                logger.info(f"📝 새 임베딩으로 재삽입: {len(points_to_insert)}개")
                await qdrant_manager.upsert_points_batch_optimized(points_to_insert)
                
                # PostgreSQL 업데이트: is_conversion=true
                async with postgres_manager.get_connection() as conn:
                    for provider_uid, pid in successful_updates:
                        await conn.execute(
                            "UPDATE product SET is_conversion = true, updated_dt = NOW() WHERE provider_uid = $1 AND pid = $2",
                            provider_uid, pid
                        )
                
                results["update_count"] = len(successful_updates)
                logger.info(f"✅ UPDATE 완료: {len(successful_updates)}개 상품 (삭제 후 재삽입)")
        
        logger.info("🎉 테스트 동기화 폴링 완료")
        return {
            "status": "success",
            "message": f"테스트 동기화 폴링이 성공적으로 완료되었습니다 (limit: {limit})",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"테스트 동기화 폴링 실패: {e}")
        return {
            "status": "error",
            "message": f"테스트 동기화 폴링 중 오류 발생: {str(e)}"
        }

# Export alias for compatibility
api_router = router

