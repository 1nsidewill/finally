# test_failure_mechanism.py
import asyncio
import logging
import random
from datetime import datetime

from src.config import get_settings
from src.database.postgresql import postgres_manager
from src.services.failure_handler import failure_handler, OperationType
from src.workers.reliable_worker import reliable_worker

# 설정 로드
settings = get_settings()

# 로깅 설정
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL or "INFO"))
logger = logging.getLogger(__name__)

class TestException(Exception):
    """테스트용 예외"""
    pass

async def setup_test_environment():
    """테스트 환경 설정 (failed_operations 테이블 생성)"""
    try:
        # 테이블이 없으면 생성
        with open('migrations/create_failed_operations_table.sql', 'r', encoding='utf-8') as f:
            migration_sql = f.read()
        
        # 전체 SQL을 한 번에 실행 (psql 방식)
        try:
            await postgres_manager.execute_command(migration_sql)
        except Exception as e:
            # 이미 존재하는 테이블/인덱스는 무시
            if "already exists" not in str(e).lower():
                logger.warning(f"SQL 실행 경고: {e}")
        
        logger.info("✅ 테스트 환경 설정 완료")
        
    except Exception as e:
        logger.error(f"❌ 테스트 환경 설정 실패: {e}")
        raise

async def test_failure_logging():
    """실패 로깅 테스트"""
    logger.info("\n🔥 실패 로깅 테스트 시작")
    
    try:
        # 의도적으로 실패를 생성
        test_product_uid = 99999
        test_error = TestException("이것은 테스트 에러입니다")
        
        # 실패 로깅
        failure_id = await failure_handler.log_failure(
            operation_type=OperationType.SYNC,
            product_uid=test_product_uid,
            error=test_error,
            context={"test": True, "timestamp": datetime.now().isoformat()}
        )
        
        logger.info(f"✅ 실패 로깅 성공 - ID: {failure_id}")
        
        # 로깅된 실패 조회
        operations = await failure_handler.get_retryable_operations(1)
        if operations and operations[0].id == failure_id:
            logger.info(f"✅ 실패 작업 조회 성공 - 에러: {operations[0].error_message}")
        else:
            logger.error("❌ 실패 작업 조회 실패")
        
        return failure_id
        
    except Exception as e:
        logger.error(f"❌ 실패 로깅 테스트 실패: {e}")
        return None

async def test_retry_mechanism():
    """재시도 메커니즘 테스트"""
    logger.info("\n🔄 재시도 메커니즘 테스트 시작")
    
    try:
        # 실패한 작업들 조회
        operations = await failure_handler.get_retryable_operations(5)
        
        if not operations:
            logger.info("재시도할 작업이 없습니다")
            return
        
        logger.info(f"재시도할 작업 {len(operations)}개 발견")
        
        for operation in operations:
            logger.info(
                f"작업 ID: {operation.id}, "
                f"타입: {operation.operation_type.value}, "
                f"제품: {operation.product_uid}, "
                f"재시도 횟수: {operation.retry_count}/{operation.max_retries}"
            )
        
        # 첫 번째 작업을 테스트용으로 재시도 실패 처리
        first_operation = operations[0]
        
        # 재시도 실패로 업데이트
        await failure_handler.update_retry_attempt(
            first_operation.id,
            success=False,
            new_error=TestException("재시도 테스트 실패")
        )
        
        # 업데이트된 정보 확인
        updated_ops = await failure_handler.get_retryable_operations(1)
        if updated_ops:
            updated_op = updated_ops[0]
            logger.info(
                f"✅ 재시도 카운트 업데이트 성공 - "
                f"이전: {first_operation.retry_count} → 현재: {updated_op.retry_count}"
            )
        
    except Exception as e:
        logger.error(f"❌ 재시도 메커니즘 테스트 실패: {e}")

async def test_reliable_worker():
    """안정적인 워커 테스트"""
    logger.info("\n🛡️ 안정적인 워커 테스트 시작")
    
    try:
        # 실제 제품 데이터가 없으므로 모의 테스트만 수행
        test_product_uid = 88888
        
        # 실패 컨텍스트 테스트
        try:
            async with reliable_worker.failure_context(
                OperationType.SYNC, 
                test_product_uid,
                {"test": "reliable_worker_test"}
            ):
                # 의도적으로 예외 발생
                raise TestException("워커 테스트 예외")
                
        except TestException:
            logger.info("✅ 실패 컨텍스트 정상 작동 (예외 발생 및 로깅됨)")
        
        # 통계 조회 테스트
        stats = await reliable_worker.get_failure_statistics()
        logger.info(f"✅ 실패 통계 조회 성공: {stats}")
        
    except Exception as e:
        logger.error(f"❌ 안정적인 워커 테스트 실패: {e}")

async def test_failure_stats():
    """실패 통계 테스트"""
    logger.info("\n📊 실패 통계 테스트 시작")
    
    try:
        stats = await failure_handler.get_failure_stats()
        
        logger.info("실패 통계:")
        for operation_type, stat in stats.items():
            logger.info(f"  {operation_type}:")
            logger.info(f"    총 실패: {stat['total_failures']}")
            logger.info(f"    해결됨: {stat['resolved']}")
            logger.info(f"    영구 실패: {stat['permanent_failures']}")
            logger.info(f"    재시도 대기: {stat['pending_retries']}")
            logger.info(f"    평균 재시도 횟수: {stat['avg_retry_count']:.2f}")
        
        logger.info("✅ 실패 통계 조회 성공")
        
    except Exception as e:
        logger.error(f"❌ 실패 통계 테스트 실패: {e}")

async def test_success_marking():
    """성공 처리 테스트"""
    logger.info("\n✅ 성공 처리 테스트 시작")
    
    try:
        # 재시도 가능한 작업 중 하나를 성공으로 표시
        operations = await failure_handler.get_retryable_operations(1)
        
        if operations:
            operation = operations[0]
            
            # 성공으로 표시
            await failure_handler.update_retry_attempt(operation.id, success=True)
            
            logger.info(f"✅ 작업 {operation.id} 성공으로 표시됨")
            
            # 다시 조회해서 해당 작업이 재시도 목록에서 제거되었는지 확인
            remaining_ops = await failure_handler.get_retryable_operations(100)
            remaining_ids = [op.id for op in remaining_ops]
            
            if operation.id not in remaining_ids:
                logger.info("✅ 성공한 작업이 재시도 목록에서 제거됨")
            else:
                logger.error("❌ 성공한 작업이 여전히 재시도 목록에 있음")
        else:
            logger.info("성공 처리 테스트할 작업이 없습니다")
            
    except Exception as e:
        logger.error(f"❌ 성공 처리 테스트 실패: {e}")

async def main():
    """메인 테스트 함수"""
    logger.info("🚀 실패 처리 메커니즘 통합 테스트 시작")
    
    try:
        # 1. 테스트 환경 설정
        await setup_test_environment()
        
        # 2. 실패 로깅 테스트
        failure_id = await test_failure_logging()
        
        # 3. 재시도 메커니즘 테스트
        await test_retry_mechanism()
        
        # 4. 안정적인 워커 테스트
        await test_reliable_worker()
        
        # 5. 실패 통계 테스트
        await test_failure_stats()
        
        # 6. 성공 처리 테스트
        await test_success_marking()
        
        logger.info("\n🎉 모든 테스트 완료!")
        
        # 최종 통계 출력
        final_stats = await failure_handler.get_failure_stats()
        logger.info(f"\n최종 실패 통계: {final_stats}")
        
    except Exception as e:
        logger.error(f"❌ 테스트 실행 중 오류 발생: {e}")
    
    finally:
        # PostgreSQL 연결 정리
        await postgres_manager.close()

if __name__ == "__main__":
    asyncio.run(main()) 