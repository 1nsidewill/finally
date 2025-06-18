#!/usr/bin/env python3
"""
빠른 부하 테스트 (1000개 매물)
전체 30k 테스트 전에 시스템 동작을 검증합니다.
"""

import asyncio
import sys
import os

# 현재 디렉토리를 Python path에 추가
sys.path.insert(0, os.getcwd())

from performance_load_test import LoadTestRunner

async def quick_test():
    """1000개 매물로 빠른 테스트"""
    print("🚀 빠른 부하 테스트 (1000개 매물)")
    print("=" * 40)
    
    runner = LoadTestRunner()
    
    # 작은 규모 설정
    total_products = 1000  # 1k 매물
    batch_size = 30       # 최적화된 배치 크기  
    num_workers = 5       # 워커 수 줄임
    
    print(f"설정:")
    print(f"  • 총 매물 수: {total_products:,}개")
    print(f"  • 배치 크기: {batch_size}")
    print(f"  • 워커 수: {num_workers}")
    print(f"  • 예상 소요 시간: 약 {total_products / (batch_size * 10):.1f}분")
    print()
    
    try:
        await runner.run_load_test(
            total_products=total_products,
            batch_size=batch_size,
            num_workers=num_workers
        )
        
        print("\n✅ 빠른 테스트 완료! 이제 30k 테스트를 실행할 수 있습니다.")
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(quick_test()) 