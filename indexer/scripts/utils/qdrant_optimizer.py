import asyncio
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.config import get_settings
from src.database.qdrant import QdrantManager
from qdrant_client import models
from qdrant_client.models import Distance, VectorParams, PointStruct

@dataclass
class QdrantOptimizationConfig:
    """Qdrant 최적화 설정"""
    # 인덱싱 임계값 - 대량 데이터를 위해 증가
    indexing_threshold: int = 50000  # 12,873개보다 충분히 큰 값
    
    # 벡터 압축 설정
    compression_ratio: Optional[float] = None  # None = 압축 안 함
    
    # 배치 크기 설정
    batch_size: int = 100  # 한 번에 업로드할 포인트 수
    
    # 메모리 최적화
    on_disk_payload: bool = True
    memmap_threshold: Optional[int] = 100000  # 메모리 맵 임계값
    
    # 옵티마이저 설정
    vacuum_min_vector_number: int = 1000
    default_segment_number: int = 4  # CPU 코어 수에 맞춤
    
    # 성능 설정
    max_optimization_threads: int = 1  # 동시 최적화 스레드
    max_indexing_threads: int = 0  # 0 = 자동 (CPU 코어 수에 맞춤)

class OptimizedQdrantManager(QdrantManager):
    """대량 데이터 처리를 위한 최적화된 Qdrant 관리자"""
    
    def __init__(self, optimization_config: Optional[QdrantOptimizationConfig] = None):
        super().__init__()
        self.opt_config = optimization_config or QdrantOptimizationConfig()
        
    async def recreate_optimized_collection(self) -> bool:
        """최적화된 설정으로 컬렉션 재생성"""
        try:
            client = self.get_sync_client()
            
            # 기존 컬렉션 삭제 (존재하는 경우)
            collections = client.get_collections().collections
            collection_names = [c.name for c in collections]
            
            if self.collection_name in collection_names:
                print(f"🗑️ 기존 컬렉션 '{self.collection_name}' 삭제 중...")
                client.delete_collection(collection_name=self.collection_name)
                print(f"✅ 기존 컬렉션 삭제 완료")
            
            # 최적화된 설정으로 새 컬렉션 생성
            print(f"🚀 최적화된 컬렉션 '{self.collection_name}' 생성 중...")
            
            # 압축 설정
            quantization_config = None
            if self.opt_config.compression_ratio:
                quantization_config = models.ScalarQuantization(
                    scalar=models.ScalarQuantizationConfig(
                        type=models.ScalarType.INT8,
                        quantile=0.99,
                        always_ram=False
                    )
                )
            
            # 옵티마이저 설정
            optimizers_config = models.OptimizersConfigDiff(
                deleted_threshold=0.2,
                vacuum_min_vector_number=self.opt_config.vacuum_min_vector_number,
                default_segment_number=self.opt_config.default_segment_number,
                max_segment_size=None,  # 자동 설정
                memmap_threshold=self.opt_config.memmap_threshold,
                indexing_threshold=self.opt_config.indexing_threshold,
                flush_interval_sec=5,
                max_optimization_threads=self.opt_config.max_optimization_threads,
            )
            
            # HNSW 설정 (벡터 검색 최적화)
            hnsw_config = models.HnswConfigDiff(
                m=16,  # 연결 수 (기본값)
                ef_construct=200,  # 구축 시 탐색 폭 (높을수록 정확하지만 느림)
                full_scan_threshold=10000,  # 전체 스캔 임계값
                max_indexing_threads=self.opt_config.max_indexing_threads,
                on_disk=False,  # 인덱스를 메모리에 유지 (빠른 검색)
            )
            
            client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=self.vector_size,
                    distance=Distance.COSINE,
                    hnsw_config=hnsw_config,
                    quantization_config=quantization_config,
                    on_disk=False,  # 벡터는 메모리에 (검색 성능)
                ),
                optimizers_config=optimizers_config,
                on_disk_payload=self.opt_config.on_disk_payload,  # 페이로드만 디스크에
            )
            
            print(f"✅ 최적화된 컬렉션 '{self.collection_name}' 생성 완료")
            print(f"🔧 적용된 최적화:")
            print(f"  • 인덱싱 임계값: {self.opt_config.indexing_threshold:,}")
            print(f"  • 배치 크기: {self.opt_config.batch_size}")
            print(f"  • 디스크 페이로드: {self.opt_config.on_disk_payload}")
            print(f"  • 세그먼트 수: {self.opt_config.default_segment_number}")
            print(f"  • 메모리 맵 임계값: {self.opt_config.memmap_threshold:,}")
            
            return True
            
        except Exception as e:
            print(f"❌ 최적화된 컬렉션 생성 실패: {e}")
            raise
    
    async def upsert_points_batch(
        self, 
        points: List[PointStruct], 
        batch_size: Optional[int] = None,
        show_progress: bool = True
    ) -> Dict[str, Any]:
        """대량 포인트를 배치로 업로드"""
        batch_size = batch_size or self.opt_config.batch_size
        total_points = len(points)
        
        if total_points == 0:
            return {"status": "success", "total_points": 0, "batches": 0}
        
        client = await self.get_async_client()
        operation_ids = []
        
        if show_progress:
            print(f"📦 {total_points:,}개 포인트를 {batch_size}개씩 배치 업로드 시작...")
        
        start_time = time.time()
        
        for i in range(0, total_points, batch_size):
            batch = points[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_points + batch_size - 1) // batch_size
            
            try:
                result = await client.upsert(
                    collection_name=self.collection_name,
                    wait=True,  # 각 배치 완료 대기
                    points=batch
                )
                operation_ids.append(result.operation_id)
                
                if show_progress:
                    progress = (i + len(batch)) / total_points * 100
                    print(f"  📦 배치 {batch_num}/{total_batches} 완료 ({len(batch)}개) - {progress:.1f}%")
                
            except Exception as e:
                print(f"❌ 배치 {batch_num} 업로드 실패: {e}")
                raise
        
        elapsed_time = time.time() - start_time
        speed = total_points / elapsed_time if elapsed_time > 0 else 0
        
        if show_progress:
            print(f"✅ 배치 업로드 완료:")
            print(f"  • 총 포인트: {total_points:,}개")
            print(f"  • 총 배치: {len(operation_ids)}개")
            print(f"  • 처리 시간: {elapsed_time:.2f}초")
            print(f"  • 처리 속도: {speed:.2f} 포인트/초")
        
        return {
            "status": "success",
            "total_points": total_points,
            "batches": len(operation_ids),
            "operation_ids": operation_ids,
            "processing_time": elapsed_time,
            "points_per_second": speed
        }
    
    async def get_collection_stats(self) -> Dict[str, Any]:
        """컬렉션 통계 및 최적화 상태 조회"""
        try:
            client = await self.get_async_client()
            
            # 기본 정보
            info = await client.get_collection(collection_name=self.collection_name)
            count = await client.count(collection_name=self.collection_name)
            
            stats = {
                "collection_name": self.collection_name,
                "total_points": count.count,
                "vector_size": info.config.params.vectors.size,
                "distance": info.config.params.vectors.distance.value,
                "status": info.status.value,
                "optimizer_status": info.optimizer_status,
                "indexing_threshold": info.config.optimizer_config.indexing_threshold,
                "segments_count": len(info.result.segments) if info.result and info.result.segments else 0,
                "disk_usage_bytes": sum(seg.disk_usage_bytes for seg in info.result.segments) if info.result and info.result.segments else 0,
                "ram_usage_bytes": sum(seg.ram_usage_bytes for seg in info.result.segments) if info.result and info.result.segments else 0,
            }
            
            # 세그먼트 정보
            if info.result and info.result.segments:
                segments_info = []
                for seg in info.result.segments:
                    segments_info.append({
                        "segment_type": seg.segment_type.value if seg.segment_type else "unknown",
                        "num_vectors": seg.num_vectors,
                        "num_points": seg.num_points,
                        "disk_usage_mb": seg.disk_usage_bytes / (1024 * 1024) if seg.disk_usage_bytes else 0,
                        "ram_usage_mb": seg.ram_usage_bytes / (1024 * 1024) if seg.ram_usage_bytes else 0,
                    })
                stats["segments"] = segments_info
            
            return stats
            
        except Exception as e:
            print(f"❌ 컬렉션 통계 조회 실패: {e}")
            raise
    
    async def optimize_collection(self) -> Dict[str, Any]:
        """컬렉션 수동 최적화 실행"""
        try:
            print(f"🔧 컬렉션 '{self.collection_name}' 최적화 시작...")
            
            client = await self.get_async_client()
            
            # 인덱스 최적화 실행
            result = await client.create_field_index(
                collection_name=self.collection_name,
                field_name="uid",  # uid 필드에 인덱스 생성
                field_schema=models.PayloadSchemaType.INTEGER
            )
            
            print(f"✅ 컬렉션 최적화 완료")
            return {"status": "success", "result": result}
            
        except Exception as e:
            print(f"❌ 컬렉션 최적화 실패: {e}")
            raise

async def test_optimization():
    """최적화 테스트"""
    print("🧪 Qdrant 최적화 테스트 시작")
    print("=" * 50)
    
    # 최적화된 관리자 생성
    config = QdrantOptimizationConfig(
        indexing_threshold=50000,  # 12,873개보다 충분히 큰 값
        batch_size=100,
        default_segment_number=4,
    )
    
    manager = OptimizedQdrantManager(config)
    
    # 최적화된 컬렉션 생성
    await manager.recreate_optimized_collection()
    
    # 컬렉션 통계 확인
    stats = await manager.get_collection_stats()
    print(f"\n📊 컬렉션 통계:")
    print(f"  • 상태: {stats['status']}")
    print(f"  • 포인트 수: {stats['total_points']:,}")
    print(f"  • 벡터 크기: {stats['vector_size']}")
    print(f"  • 인덱싱 임계값: {stats['indexing_threshold']:,}")
    print(f"  • 세그먼트 수: {stats['segments_count']}")
    print(f"  • 디스크 사용량: {stats['disk_usage_bytes'] / (1024*1024):.1f}MB")
    print(f"  • RAM 사용량: {stats['ram_usage_bytes'] / (1024*1024):.1f}MB")
    
    print(f"\n✅ 최적화 완료! 대량 데이터 처리 준비됨")

if __name__ == "__main__":
    asyncio.run(test_optimization()) 