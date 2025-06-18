import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from bulk_sync_with_checkpoints import BulkSynchronizer

async def test_small_batch():
    """Test processing a small batch of 50 products"""
    print("🧪 소규모 배치 테스트 시작 (50개 제품)")
    print("=" * 50)
    
    # Create synchronizer with small batch size
    synchronizer = BulkSynchronizer(batch_size=50)
    
    # Override total products to limit to just first batch
    synchronizer.total_products = 50  # Limit to first 50 products only
    
    # Initialize
    if not await synchronizer.initialize():
        print("❌ 초기화 실패")
        return False
    
    # Get first batch of products
    products = await synchronizer.get_products_batch(0, 50)
    print(f"📦 첫 번째 배치 로드됨: {len(products)}개 제품")
    
    if not products:
        print("⚠️ 처리할 제품이 없습니다.")
        return True
    
    # Show first few products
    print("\n🏍️ 처리될 제품 샘플:")
    for i, product in enumerate(products[:5]):
        print(f"  {i+1}. {product['uid']}: {product['title'][:50]}...")
    if len(products) > 5:
        print(f"  ... 그리고 {len(products)-5}개 더")
    
    print(f"\n🚀 배치 1 처리 시작...")
    
    # Process the batch
    batch_result = await synchronizer.process_batch(1, products)
    
    print("\n📊 처리 결과:")
    print(f"  • 총 처리: {batch_result['total_items']}개")
    print(f"  • 성공: {batch_result['success_count']}개")
    print(f"  • 실패: {batch_result['error_count']}개")
    print(f"  • 성공률: {(batch_result['success_count']/batch_result['total_items']*100):.1f}%")
    print(f"  • 처리 시간: {batch_result['processing_time']:.2f}초")
    print(f"  • 처리 속도: {batch_result['total_items']/batch_result['processing_time']:.2f} 제품/초")
    
    if batch_result['errors']:
        print(f"\n❌ 오류 목록:")
        for error in batch_result['errors'][:3]:  # Show first 3 errors
            print(f"  • {error['uid']}: {error['error'][:100]}...")
        if len(batch_result['errors']) > 3:
            print(f"  ... 그리고 {len(batch_result['errors'])-3}개 오류 더")
    
    # Estimate total time for all products
    if batch_result['processing_time'] > 0:
        speed = batch_result['total_items'] / batch_result['processing_time']
        total_time_estimate = 12873 / speed
        print(f"\n⏱️ 전체 12,873개 제품 예상 처리 시간: {total_time_estimate:.0f}초 ({total_time_estimate/60:.1f}분)")
    
    return batch_result['success_count'] > 0

if __name__ == "__main__":
    asyncio.run(test_small_batch()) 