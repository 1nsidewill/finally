#!/usr/bin/env python3
# scripts/run_batch_indexing.py - 배치 인덱싱 실행 스크립트

import asyncio
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(str(Path(__file__).parent.parent))

from src.services.batch_processor import BatchProcessor, BatchConfig, create_batch_processor
from src.services.embedding_service import EmbeddingConfig

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('batch_indexing.log')
    ]
)
logger = logging.getLogger(__name__)

def create_progress_display(progress):
    """진행상황을 시각적으로 표시"""
    if progress.total_items == 0:
        return "진행률: 0%"
    
    completion = (progress.processed_items / progress.total_items) * 100
    success_rate = (progress.successful_items / progress.processed_items) * 100 if progress.processed_items > 0 else 0
    
    # 진행률 바 생성
    bar_length = 40
    filled = int(bar_length * progress.processed_items / progress.total_items)
    bar = '█' * filled + '░' * (bar_length - filled)
    
    return f"[{bar}] {completion:.1f}% (성공률: {success_rate:.1f}%)"

def progress_callback(progress):
    """진행상황 콜백 함수"""
    display = create_progress_display(progress)
    elapsed = datetime.now() - progress.start_time if progress.start_time else None
    
    print(f"\r{display}", end='', flush=True)
    
    # 배치 완료시 상세 정보 출력
    if progress.current_batch % 10 == 0:
        print()  # 새 줄
        logger.info(f"배치 {progress.current_batch} 완료 - "
                   f"처리: {progress.processed_items}/{progress.total_items}, "
                   f"성공: {progress.successful_items}, "
                   f"실패: {progress.failed_items}")
        if elapsed:
            logger.info(f"경과 시간: {elapsed}")

async def main():
    parser = argparse.ArgumentParser(description='매물 배치 인덱싱 스크립트')
    
    # 배치 설정 옵션
    parser.add_argument('--batch-size', type=int, default=50, 
                       help='배치 크기 (기본: 50)')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='배치 간 딜레이(초) (기본: 1.0)')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='최대 재시도 횟수 (기본: 3)')
    parser.add_argument('--retry-delay', type=float, default=5.0,
                       help='재시도 딜레이(초) (기본: 5.0)')
    
    # 진행상황 설정
    parser.add_argument('--save-every', type=int, default=10,
                       help='N개 배치마다 진행상황 저장 (기본: 10)')
    parser.add_argument('--log-every', type=int, default=5,
                       help='N개 배치마다 로그 출력 (기본: 5)')
    parser.add_argument('--progress-file', default='batch_progress.json',
                       help='진행상황 파일명 (기본: batch_progress.json)')
    
    # 실행 옵션
    parser.add_argument('--no-resume', action='store_true',
                       help='이전 진행상황 무시하고 처음부터 시작')
    parser.add_argument('--dry-run', action='store_true',
                       help='실제 처리 없이 설정만 확인')
    parser.add_argument('--embedding-batch-size', type=int, default=50,
                       help='임베딩 서비스 배치 크기 (기본: 50)')
    
    args = parser.parse_args()
    
    print("🚀 매물 배치 인덱싱 스크립트 시작!")
    print(f"설정:")
    print(f"  배치 크기: {args.batch_size}")
    print(f"  배치 간 딜레이: {args.delay}초")
    print(f"  최대 재시도: {args.max_retries}")
    print(f"  진행상황 저장: {args.save_every}배치마다")
    print(f"  재개 모드: {'비활성화' if args.no_resume else '활성화'}")
    print(f"  진행상황 파일: {args.progress_file}")
    
    if args.dry_run:
        print("\n🔍 DRY RUN 모드 - 실제 처리는 수행하지 않습니다.")
        return
    
    try:
        # 배치 설정 생성
        batch_config = BatchConfig(
            batch_size=args.batch_size,
            delay_between_batches=args.delay,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
            save_progress_every=args.save_every,
            log_every=args.log_every,
            progress_file=args.progress_file
        )
        
        # 임베딩 설정 생성
        embedding_config = EmbeddingConfig(
            batch_size=args.embedding_batch_size,
            model="text-embedding-3-large",
            dimensions=3072
        )
        
        print(f"\n📦 서비스 초기화 중...")
        
        # 배치 프로세서 생성
        processor = await create_batch_processor(config=batch_config)
        
        # 임베딩 서비스 설정 업데이트
        processor.embedding_service.config = embedding_config
        
        print("✅ 서비스 초기화 완료")
        
        # 진행상황 확인
        resume = not args.no_resume
        if resume and Path(args.progress_file).exists():
            print(f"\n📋 기존 진행상황 발견: {args.progress_file}")
            if processor.load_progress():
                completion = (processor.progress.processed_items / processor.progress.total_items) * 100 if processor.progress.total_items > 0 else 0
                print(f"이전 진행률: {completion:.1f}% ({processor.progress.processed_items}/{processor.progress.total_items})")
                
                user_input = input("이어서 계속 진행하시겠습니까? (y/N): ").strip().lower()
                if user_input not in ['y', 'yes']:
                    resume = False
                    print("처음부터 새로 시작합니다.")
        
        print(f"\n🔄 배치 처리 시작...")
        start_time = datetime.now()
        
        # 배치 처리 실행
        final_progress = await processor.process_all_listings(
            resume=resume,
            progress_callback=progress_callback
        )
        
        # 최종 결과 출력
        print()  # 새 줄
        end_time = datetime.now()
        total_time = end_time - start_time
        
        print("\n🎉 배치 처리 완료!")
        print(f"📊 최종 결과:")
        print(f"  총 매물: {final_progress.total_items}")
        print(f"  처리 완료: {final_progress.processed_items}")
        print(f"  성공: {final_progress.successful_items}")
        print(f"  실패: {final_progress.failed_items}")
        
        if final_progress.total_items > 0:
            completion = (final_progress.processed_items / final_progress.total_items) * 100
            success_rate = (final_progress.successful_items / final_progress.processed_items) * 100 if final_progress.processed_items > 0 else 0
            print(f"  완료율: {completion:.1f}%")
            print(f"  성공률: {success_rate:.1f}%")
        
        print(f"⏱️  총 소요 시간: {total_time}")
        
        if final_progress.failed_items > 0:
            print(f"⚠️  실패한 매물 ID: {final_progress.failed_item_ids[:10]}{'...' if len(final_progress.failed_item_ids) > 10 else ''}")
            print(f"   (상세 오류는 failed_operations 테이블 확인)")
        
        # 진행상황 파일 정리 (완료시)
        if final_progress.processed_items >= final_progress.total_items:
            progress_path = Path(args.progress_file)
            if progress_path.exists():
                backup_path = progress_path.with_suffix('.completed.json')
                progress_path.rename(backup_path)
                print(f"✅ 진행상황 파일을 {backup_path}로 백업했습니다.")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  사용자에 의해 중단되었습니다.")
        print("진행상황이 저장되었습니다. --resume 옵션으로 이어서 진행할 수 있습니다.")
        
    except Exception as e:
        logger.error(f"배치 처리 중 오류 발생: {e}")
        print(f"\n❌ 오류 발생: {e}")
        print("자세한 내용은 batch_indexing.log 파일을 확인하세요.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 