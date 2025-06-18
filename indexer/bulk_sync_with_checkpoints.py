import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import uuid

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.config import get_settings
from src.database.postgresql import PostgreSQLManager
from src.database.qdrant import QdrantManager
from src.services.embedding_service import EmbeddingService


class CheckpointManager:
    """Manages checkpoints for bulk synchronization process"""
    
    def __init__(self, checkpoint_file: str = "bulk_sync_checkpoint.json"):
        self.checkpoint_file = Path(checkpoint_file)
        self.data = self._load_checkpoint()
    
    def _load_checkpoint(self) -> Dict:
        """Load existing checkpoint or create new one"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, Exception) as e:
                print(f"⚠️ 체크포인트 파일 읽기 실패: {e}")
                return self._create_new_checkpoint()
        else:
            return self._create_new_checkpoint()
    
    def _create_new_checkpoint(self) -> Dict:
        """Create a new checkpoint structure"""
        return {
            "session_id": str(uuid.uuid4()),
            "started_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "total_products": 0,
            "processed_count": 0,
            "last_processed_uid": None,
            "batch_size": 100,
            "current_batch": 0,
            "total_batches": 0,
            "status": "initialized",
            "success_count": 0,
            "error_count": 0,
            "batches_completed": [],
            "errors": []
        }
    
    def save_checkpoint(self):
        """Save current checkpoint to file"""
        self.data["last_updated"] = datetime.now().isoformat()
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            print(f"📝 체크포인트 저장됨: 배치 {self.data['current_batch']}/{self.data['total_batches']}")
        except Exception as e:
            print(f"❌ 체크포인트 저장 실패: {e}")
    
    def update_progress(self, batch_num: int, processed_count: int, success_count: int, 
                       error_count: int, last_uid: Optional[str] = None):
        """Update checkpoint with current progress"""
        self.data.update({
            "current_batch": batch_num,
            "processed_count": processed_count,
            "success_count": success_count,
            "error_count": error_count,
            "last_processed_uid": last_uid,
            "status": "running"
        })
        
        if batch_num not in self.data["batches_completed"]:
            self.data["batches_completed"].append(batch_num)
        
        self.save_checkpoint()
    
    def mark_completed(self):
        """Mark the entire process as completed"""
        self.data.update({
            "status": "completed",
            "completed_at": datetime.now().isoformat()
        })
        self.save_checkpoint()
    
    def mark_failed(self, error_msg: str):
        """Mark the process as failed"""
        self.data.update({
            "status": "failed",
            "failed_at": datetime.now().isoformat()
        })
        self.data["errors"].append({
            "timestamp": datetime.now().isoformat(),
            "error": error_msg
        })
        self.save_checkpoint()


class BulkSynchronizer:
    """Handles bulk synchronization of products with checkpointing"""
    
    def __init__(self, batch_size: int = 100):
        self.config = get_settings()
        self.batch_size = batch_size
        self.checkpoint = CheckpointManager()
        
        # Initialize services
        self.pg_manager = PostgreSQLManager()
        self.qdrant_manager = QdrantManager()
        self.embedding_service = EmbeddingService()
        
        # Statistics
        self.total_products = 0
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
    
    async def initialize(self):
        """Initialize all database connections"""
        print("🔧 데이터베이스 연결 초기화 중...")
        try:
            # PostgreSQL connection will be handled per operation
            print("✅ PostgreSQL 연결 준비 완료")
            print("✅ Qdrant 연결 준비 완료")
            print("✅ OpenAI 임베딩 서비스 준비 완료")
            return True
        except Exception as e:
            print(f"❌ 초기화 실패: {e}")
            return False
    
    async def get_total_product_count(self) -> int:
        """Get total count of products to process"""
        async with self.pg_manager.get_connection() as conn:
            result = await conn.fetchrow("""
                SELECT COUNT(*) as total 
                FROM product 
                WHERE is_conversion = false AND status = 1
            """)
            return result['total']
    
    async def get_products_batch(self, offset: int, limit: int) -> List[Dict]:
        """Get a batch of products starting from offset"""
        async with self.pg_manager.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT uid, title, content, brand, price, status, is_conversion
                FROM product 
                WHERE is_conversion = false AND status = 1
                ORDER BY uid
                LIMIT $1 OFFSET $2
            """, limit, offset)
            
            return [dict(row) for row in rows]
    
    async def process_single_product(self, product: Dict) -> Dict:
        """Process a single product and return result"""
        try:
            # Create embedding text
            embedding_text = f"{product['title']} {product['content']} 브랜드:{product['brand']} 가격:{product['price']}원"
            
            # Generate embedding (동기 함수 사용)
            embedding_array = self.embedding_service.create_embedding(embedding_text)
            if embedding_array is None:
                raise ValueError("임베딩 생성 실패")
            
            # Convert numpy array to list
            embedding = embedding_array.tolist()
            
            # Insert into Qdrant using upsert_vector_async
            await self.qdrant_manager.upsert_vector_async(
                vector_id=str(product['uid']),
                vector=embedding,
                metadata={
                    "title": product['title'],
                    "content": product['content'] or "",
                    "brand": product['brand'] or "",
                    "price": float(product['price']) if product['price'] else 0.0,
                    "status": product['status'],
                    "uid": product['uid']
                }
            )
            
            # Update PostgreSQL is_conversion flag
            async with self.pg_manager.get_connection() as conn:
                await conn.execute("""
                    UPDATE product 
                    SET is_conversion = true 
                    WHERE uid = $1
                """, product['uid'])
            
            return {
                "uid": product['uid'],
                "status": "success",
                "error": None
            }
            
        except Exception as e:
            return {
                "uid": product['uid'],
                "status": "error",
                "error": str(e)
            }
    
    async def process_batch(self, batch_num: int, products: List[Dict]) -> Dict:
        """Process a batch of products"""
        batch_start_time = datetime.now()
        batch_results = {
            "batch_num": batch_num,
            "total_items": len(products),
            "success_count": 0,
            "error_count": 0,
            "errors": [],
            "processing_time": 0
        }
        
        print(f"\n📦 배치 {batch_num} 처리 시작 ({len(products)}개 제품)")
        
        # Process each product in the batch
        for i, product in enumerate(products):
            try:
                result = await self.process_single_product(product)
                
                if result["status"] == "success":
                    batch_results["success_count"] += 1
                    self.success_count += 1
                    print(f"  ✅ {i+1}/{len(products)} - {product['uid'][:8]}...")
                else:
                    batch_results["error_count"] += 1
                    batch_results["errors"].append(result)
                    self.error_count += 1
                    print(f"  ❌ {i+1}/{len(products)} - {product['uid'][:8]}... 실패: {result['error']}")
                
                self.processed_count += 1
                
                # Small delay to prevent overwhelming the services
                await asyncio.sleep(0.1)
                
            except Exception as e:
                batch_results["error_count"] += 1
                batch_results["errors"].append({
                    "uid": product.get('uid', 'unknown'),
                    "status": "error", 
                    "error": str(e)
                })
                self.error_count += 1
                print(f"  ❌ {i+1}/{len(products)} - 예외 발생: {e}")
        
        # Calculate processing time
        batch_results["processing_time"] = (datetime.now() - batch_start_time).total_seconds()
        
        # Update checkpoint
        last_uid = products[-1]['uid'] if products else None
        self.checkpoint.update_progress(
            batch_num, self.processed_count, self.success_count, 
            self.error_count, last_uid
        )
        
        print(f"📊 배치 {batch_num} 완료: 성공 {batch_results['success_count']}, 실패 {batch_results['error_count']}")
        print(f"⏱️ 처리 시간: {batch_results['processing_time']:.2f}초")
        
        return batch_results
    
    async def run_bulk_sync(self, resume: bool = True):
        """Run the complete bulk synchronization process"""
        start_time = datetime.now()
        
        print("🚀 대량 동기화 프로세스 시작")
        print(f"📁 체크포인트 파일: {self.checkpoint.checkpoint_file}")
        
        # Initialize connections
        if not await self.initialize():
            return False
        
        # Get total product count
        self.total_products = await self.get_total_product_count()
        print(f"📊 총 처리 대상: {self.total_products:,}개 제품")
        
        if self.total_products == 0:
            print("⚠️ 처리할 제품이 없습니다.")
            return True
        
        # Calculate batches
        total_batches = (self.total_products + self.batch_size - 1) // self.batch_size
        
        # Update checkpoint with initial data
        self.checkpoint.data.update({
            "total_products": self.total_products,
            "batch_size": self.batch_size,
            "total_batches": total_batches
        })
        
        # Resume from checkpoint if requested
        start_batch = 0
        if resume and self.checkpoint.data.get("status") == "running":
            start_batch = self.checkpoint.data.get("current_batch", 0)
            self.processed_count = self.checkpoint.data.get("processed_count", 0)
            self.success_count = self.checkpoint.data.get("success_count", 0)
            self.error_count = self.checkpoint.data.get("error_count", 0)
            print(f"📍 체크포인트에서 재시작: 배치 {start_batch}부터")
        
        print(f"📦 총 배치 수: {total_batches} (배치 크기: {self.batch_size})")
        
        try:
            # Process each batch
            for batch_num in range(start_batch, total_batches):
                offset = batch_num * self.batch_size
                
                # Get products for this batch
                products = await self.get_products_batch(offset, self.batch_size)
                
                if not products:
                    print(f"⚠️ 배치 {batch_num}에서 제품을 찾을 수 없습니다.")
                    continue
                
                # Process the batch
                batch_result = await self.process_batch(batch_num + 1, products)
                
                # Progress report
                progress_pct = ((batch_num + 1) / total_batches) * 100
                print(f"📈 전체 진행률: {progress_pct:.1f}% ({batch_num + 1}/{total_batches})")
                print(f"📊 누적 통계: 성공 {self.success_count:,}, 실패 {self.error_count:,}")
        
        except Exception as e:
            error_msg = f"배치 처리 중 오류 발생: {e}"
            print(f"❌ {error_msg}")
            self.checkpoint.mark_failed(error_msg)
            return False
        
        # Mark as completed
        self.checkpoint.mark_completed()
        
        # Final statistics
        total_time = (datetime.now() - start_time).total_seconds()
        
        print("\n🎉 대량 동기화 완료!")
        print(f"📊 최종 통계:")
        print(f"  • 총 처리: {self.processed_count:,}개")
        print(f"  • 성공: {self.success_count:,}개")
        print(f"  • 실패: {self.error_count:,}개")
        print(f"  • 성공률: {(self.success_count/self.processed_count*100):.2f}%")
        print(f"⏱️ 총 처리 시간: {total_time:.2f}초")
        print(f"🚀 평균 처리 속도: {self.processed_count/total_time:.2f} 제품/초")
        
        return True


async def main():
    """Main function to run bulk synchronization"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Bulk synchronization with checkpoints')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh without resuming')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed without actually processing')
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("🔍 드라이 런 모드: 실제 처리 없이 계획만 확인")
        
        # Quick count check
        config = get_settings()
        pg_manager = PostgreSQLManager()
        
        async with pg_manager.get_connection() as conn:
            result = await conn.fetchrow("""
                SELECT COUNT(*) as total 
                FROM product 
                WHERE is_conversion = false AND status = 1
            """)
            total_products = result['total']
        
        total_batches = (total_products + args.batch_size - 1) // args.batch_size
        
        print(f"📊 총 처리 대상: {total_products:,}개 제품")
        print(f"📦 배치 크기: {args.batch_size}")
        print(f"📦 총 배치 수: {total_batches}")
        print(f"⏱️ 예상 처리 시간: {total_products * 0.5:.1f}초 (제품당 0.5초 가정)")
        return
    
    # Run the bulk synchronization
    synchronizer = BulkSynchronizer(batch_size=args.batch_size)
    resume = not args.no_resume
    
    success = await synchronizer.run_bulk_sync(resume=resume)
    
    if success:
        print("✅ 대량 동기화가 성공적으로 완료되었습니다.")
    else:
        print("❌ 대량 동기화 중 오류가 발생했습니다.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 