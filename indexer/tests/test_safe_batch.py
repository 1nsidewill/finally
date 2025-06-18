import asyncio
import sys
import traceback
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from bulk_sync_with_checkpoints import BulkSynchronizer

async def test_safe_batch():
    """Test processing a small batch of 10 products with safe error handling"""
    print("🧪 안전한 소규모 배치 테스트 (10개 제품)")
    print("=" * 50)
    
    # Create synchronizer with small batch size
    synchronizer = BulkSynchronizer(batch_size=10)
    
    # Initialize
    if not await synchronizer.initialize():
        print("❌ 초기화 실패")
        return False
    
    # Get 10 products
    products = await synchronizer.get_products_batch(0, 10)
    print(f"📦 배치 로드됨: {len(products)}개 제품")
    
    if not products:
        print("⚠️ 처리할 제품이 없습니다.")
        return True
    
    # Show products
    print("\n🏍️ 처리될 제품들:")
    for i, product in enumerate(products):
        print(f"  {i+1}. UID {product['uid']}: {product['title'][:50]}...")
    
    print(f"\n🚀 개별 처리 시작...")
    
    success_count = 0
    error_count = 0
    
    for i, product in enumerate(products):
        try:
            print(f"\n📦 {i+1}/10 - UID {product['uid']} 처리 중...")
            
            # Check for potential issues
            if product['title'] is None or product['title'].strip() == "":
                raise ValueError("제목이 비어있습니다")
            
            if product['content'] is not None and len(str(product['content'])) > 10000:
                print(f"  ⚠️ 긴 컨텐츠 감지: {len(str(product['content']))} 문자")
            
            # Process the product
            result = await synchronizer.process_single_product(product)
            
            if result['status'] == 'success':
                print(f"  ✅ 성공")
                success_count += 1
            else:
                print(f"  ❌ 실패: {result['error'][:100]}...")
                error_count += 1
            
        except Exception as e:
            print(f"  💥 예외 발생:")
            print(f"    • 타입: {type(e).__name__}")
            print(f"    • 메시지: {str(e)[:100]}...")
            print(f"    • 제품 데이터:")
            print(f"      - UID: {product.get('uid', 'None')}")
            print(f"      - 제목: {str(product.get('title', 'None'))[:50]}...")
            print(f"      - 컨텐츠 길이: {len(str(product.get('content', '')))}")
            print(f"      - 브랜드: {product.get('brand', 'None')}")
            print(f"      - 가격: {product.get('price', 'None')}")
            print(f"      - 타입: {type(product.get('price', 'None'))}")
            error_count += 1
    
    print(f"\n📊 최종 결과:")
    print(f"  • 총 처리: {len(products)}개")
    print(f"  • 성공: {success_count}개")
    print(f"  • 실패: {error_count}개")
    print(f"  • 성공률: {(success_count/len(products)*100):.1f}%")
    
    return success_count > 0

if __name__ == "__main__":
    asyncio.run(test_safe_batch()) 