"""
ErrorHandler 테스트

강력한 오류 처리, 로깅, failed_operations 테이블 기록을 테스트
"""

import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock
from typing import Dict, Any

from src.services.error_handler import (
    ErrorHandler, ErrorContext, FailedOperation, 
    ErrorCategory, ErrorSeverity
)
from src.database.postgresql import PostgreSQLManager

async def test_error_categorization():
    """오류 자동 분류 테스트"""
    print("🔥 오류 자동 분류 테스트!")
    
    error_handler = ErrorHandler()
    
    # 다양한 오류 타입별 테스트
    test_cases = [
        (ValueError("product_id가 필요합니다"), ErrorCategory.VALIDATION, ErrorSeverity.MEDIUM),
        (ConnectionError("PostgreSQL 연결 실패"), ErrorCategory.DATABASE, ErrorSeverity.HIGH),
        (Exception("API key is invalid"), ErrorCategory.AUTHENTICATION, ErrorSeverity.HIGH),
        (Exception("Rate limit exceeded"), ErrorCategory.RATE_LIMIT, ErrorSeverity.LOW),
        (TimeoutError("Network timeout"), ErrorCategory.NETWORK, ErrorSeverity.MEDIUM),
        (RuntimeError("알 수 없는 오류"), ErrorCategory.UNKNOWN, ErrorSeverity.MEDIUM),
    ]
    
    for exception, expected_category, expected_severity in test_cases:
        category, severity = error_handler._categorize_error(exception)
        assert category == expected_category, f"카테고리 불일치: {category} != {expected_category}"
        assert severity == expected_severity, f"심각도 불일치: {severity} != {expected_severity}"
        
        print(f"  ✅ {type(exception).__name__}: {category.value}({severity.value})")
    
    print(f"  🎯 {len(test_cases)}개 오류 분류 테스트 완료!")

async def test_error_context():
    """ErrorContext 데이터 구조 테스트"""
    print("\n🔥 ErrorContext 구조 테스트!")
    
    # 기본 컨텍스트
    context = ErrorContext(
        job_id="test_job_123",
        job_type="sync",
        product_id="product_456",
        operation_step="embedding_generation",
        additional_data={"retry_count": 1, "batch_id": "batch_789"}
    )
    
    assert context.job_id == "test_job_123"
    assert context.additional_data["retry_count"] == 1
    
    # 추가 데이터 없는 경우
    simple_context = ErrorContext(
        job_id="simple_job",
        job_type="delete",
        product_id="product_999",
        operation_step="vector_deletion"
    )
    
    assert simple_context.additional_data == {}
    
    print(f"  ✅ ErrorContext 구조 검증 완료")
    print(f"  - Job ID: {context.job_id}")
    print(f"  - Operation Step: {context.operation_step}")
    print(f"  - Additional Data: {context.additional_data}")

async def test_failed_operation_creation():
    """FailedOperation 객체 생성 테스트"""
    print("\n🔥 FailedOperation 생성 테스트!")
    
    failed_op = FailedOperation(
        id="failure_123",
        job_id="job_456",
        job_type="update",
        product_id="product_789",
        error_category=ErrorCategory.DATABASE,
        error_severity=ErrorSeverity.HIGH,
        error_message="PostgreSQL 연결 실패",
        error_details='{"exception_type": "ConnectionError", "traceback": "..."}',
        operation_step="data_fetch",
        retry_count=1,
        max_retries=3
    )
    
    assert failed_op.id == "failure_123"
    assert failed_op.error_category == ErrorCategory.DATABASE
    assert failed_op.error_severity == ErrorSeverity.HIGH
    assert failed_op.created_at is not None  # 자동 생성됨
    assert failed_op.additional_data == {}  # 기본값
    
    print(f"  ✅ FailedOperation 객체 생성 완료")
    print(f"  - ID: {failed_op.id}")
    print(f"  - 카테고리: {failed_op.error_category.value}")
    print(f"  - 심각도: {failed_op.error_severity.value}")
    print(f"  - 생성 시간: {failed_op.created_at}")

async def test_error_details_extraction():
    """오류 세부 정보 추출 테스트"""
    print("\n🔥 오류 세부 정보 추출 테스트!")
    
    error_handler = ErrorHandler()
    
    # 복잡한 예외 생성
    try:
        raise ValueError("잘못된 데이터 형식입니다")
    except Exception as e:
        error_details = error_handler._extract_error_details(e)
        
    # JSON 파싱 확인
    details_dict = json.loads(error_details)
    
    assert details_dict['exception_type'] == 'ValueError'
    assert 'traceback' in details_dict
    assert 'timestamp' in details_dict
    assert len(details_dict['exception_args']) > 0
    
    print(f"  ✅ 오류 세부 정보 추출 완료")
    print(f"  - Exception Type: {details_dict['exception_type']}")
    print(f"  - Args: {details_dict['exception_args']}")
    print(f"  - Timestamp: {details_dict['timestamp']}")

async def test_error_handling_workflow():
    """전체 오류 처리 워크플로우 테스트"""
    print("\n🔥 오류 처리 워크플로우 테스트!")
    
    # Mock PostgreSQL 매니저
    mock_postgresql = AsyncMock(spec=PostgreSQLManager)
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_postgresql.get_connection = AsyncMock(return_value=mock_conn)
    
    error_handler = ErrorHandler()
    error_handler.postgresql_manager = mock_postgresql
    
    # 테스트 예외와 컨텍스트
    test_exception = ConnectionError("Redis 연결 실패")
    context = ErrorContext(
        job_id="workflow_test_1",
        job_type="sync",
        product_id="test_product",
        operation_step="redis_polling",
        additional_data={"queue_name": "sync_queue"}
    )
    
    # 오류 처리 실행
    failed_op = await error_handler.handle_error(test_exception, context)
    
    # 결과 검증
    assert failed_op.job_id == "workflow_test_1"
    assert failed_op.error_category == ErrorCategory.NETWORK
    assert failed_op.error_severity == ErrorSeverity.MEDIUM
    assert "Redis 연결 실패" in failed_op.error_message
    
    # Mock 호출 검증
    mock_postgresql.get_connection.assert_called()
    mock_conn.execute.assert_called()  # INSERT 쿼리 실행됨
    
    print(f"  ✅ 워크플로우 테스트 완료")
    print(f"  - Failed Operation ID: {failed_op.id}")
    print(f"  - 분류: {failed_op.error_category.value}")
    print(f"  - DB 기록: 호출됨")

async def test_error_statistics():
    """오류 통계 기능 테스트"""
    print("\n🔥 오류 통계 테스트!")
    
    error_handler = ErrorHandler()
    
    # 초기 통계 확인
    initial_stats = error_handler.get_error_stats()
    assert initial_stats['total_errors'] == 0
    assert initial_stats['retry_success_rate'] == 0
    
    # 수동으로 통계 데이터 추가
    error_handler._update_error_stats(ErrorCategory.DATABASE, ErrorSeverity.HIGH)
    error_handler._update_error_stats(ErrorCategory.NETWORK, ErrorSeverity.MEDIUM)
    error_handler._update_error_stats(ErrorCategory.VALIDATION, ErrorSeverity.LOW)
    error_handler.error_stats['retry_success_count'] = 1
    
    # 통계 검증
    stats = error_handler.get_error_stats()
    assert stats['total_errors'] == 3
    assert stats['by_category']['database'] == 1
    assert stats['by_category']['network'] == 1
    assert stats['by_severity']['high'] == 1
    assert stats['retry_success_rate'] == 33.33333333333333  # 1/3 * 100
    
    print(f"  ✅ 통계 기능 검증 완료:")
    print(f"    - 총 오류: {stats['total_errors']}개")
    print(f"    - 데이터베이스 오류: {stats['by_category']['database']}개")
    print(f"    - 높은 심각도: {stats['by_severity']['high']}개")
    print(f"    - 재시도 성공률: {stats['retry_success_rate']:.1f}%")
    
    # 통계 초기화
    error_handler.reset_stats()
    reset_stats = error_handler.get_error_stats()
    assert reset_stats['total_errors'] == 0
    
    print(f"  ✅ 통계 초기화 완료")

async def test_logging_setup():
    """로깅 설정 테스트"""
    print("\n🔥 로깅 설정 테스트!")
    
    error_handler = ErrorHandler()
    
    # 로거 존재 확인
    assert error_handler.error_logger is not None
    assert error_handler.perf_logger is not None
    
    # 로그 파일 디렉토리 확인
    logs_dir = "logs"
    assert os.path.exists(logs_dir), f"로그 디렉토리가 없습니다: {logs_dir}"
    
    print(f"  ✅ 로깅 설정 검증 완료")
    print(f"  - Error Logger: 설정됨")
    print(f"  - Performance Logger: 설정됨")
    print(f"  - Logs Directory: {logs_dir}")

async def test_retry_mechanism():
    """재시도 메커니즘 테스트"""
    print("\n🔥 재시도 메커니즘 테스트!")
    
    # Mock PostgreSQL 매니저
    mock_postgresql = AsyncMock(spec=PostgreSQLManager)
    mock_conn = AsyncMock()
    
    # 재시도 가능한 실패 작업 데이터 Mock
    retry_data = [{
        'id': 'retry_test_1',
        'job_id': 'job_retry',
        'retry_count': 1,
        'max_retries': 3,
        'resolved_at': None
    }]
    
    mock_conn.fetch = AsyncMock(return_value=retry_data)
    mock_conn.execute = AsyncMock()
    mock_postgresql.get_connection = AsyncMock(return_value=mock_conn)
    
    error_handler = ErrorHandler()
    error_handler.postgresql_manager = mock_postgresql
    
    # 재시도 실행
    retry_result = await error_handler.retry_failed_operation('retry_test_1')
    
    # 결과 검증
    assert retry_result == True, "재시도가 실패했습니다"
    
    # Mock 호출 검증
    mock_conn.fetch.assert_called()  # SELECT 쿼리
    assert mock_conn.execute.call_count >= 1  # UPDATE 쿼리
    
    print(f"  ✅ 재시도 메커니즘 테스트 완료")
    print(f"  - 재시도 결과: {retry_result}")
    print(f"  - DB 업데이트: 호출됨")

async def test_comprehensive_error_scenarios():
    """포괄적인 오류 시나리오 테스트"""
    print("\n🔥 포괄적인 오류 시나리오 테스트!")
    
    error_scenarios = [
                 {
             'name': 'PostgreSQL 연결 실패',
             'exception': ConnectionError("PostgreSQL connection failed"),
             'expected_category': ErrorCategory.DATABASE,
             'expected_severity': ErrorSeverity.HIGH
         },
                 {
             'name': 'OpenAI API 키 오류',
             'exception': Exception("Invalid API key provided"),
             'expected_category': ErrorCategory.AUTHENTICATION,
             'expected_severity': ErrorSeverity.HIGH
         },
         {
             'name': '속도 제한 초과',
             'exception': Exception("Rate limit exceeded"),
             'expected_category': ErrorCategory.RATE_LIMIT,
             'expected_severity': ErrorSeverity.LOW
         },
         {
             'name': '데이터 검증 실패',
             'exception': ValueError("product_id is required"),
             'expected_category': ErrorCategory.VALIDATION,
             'expected_severity': ErrorSeverity.MEDIUM
         },
         {
             'name': '네트워크 타임아웃',
             'exception': TimeoutError("Request timed out"),
             'expected_category': ErrorCategory.NETWORK,
             'expected_severity': ErrorSeverity.MEDIUM
         }
    ]
    
    error_handler = ErrorHandler()
    mock_postgresql = AsyncMock(spec=PostgreSQLManager)
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_postgresql.get_connection = AsyncMock(return_value=mock_conn)
    error_handler.postgresql_manager = mock_postgresql
    
    processed_scenarios = []
    
    for scenario in error_scenarios:
        context = ErrorContext(
            job_id=f"scenario_{len(processed_scenarios)}",
            job_type="test",
            product_id="test_product",
            operation_step="scenario_test"
        )
        
        failed_op = await error_handler.handle_error(scenario['exception'], context)
        
        # 분류 검증
        assert failed_op.error_category == scenario['expected_category'], f"카테고리 불일치: {failed_op.error_category} != {scenario['expected_category']}"
        assert failed_op.error_severity == scenario['expected_severity'], f"심각도 불일치: {failed_op.error_severity} != {scenario['expected_severity']}"
        
        processed_scenarios.append({
            'name': scenario['name'],
            'category': failed_op.error_category.value,
            'severity': failed_op.error_severity.value,
            'success': True
        })
    
    print(f"  ✅ {len(processed_scenarios)}개 시나리오 테스트 완료:")
    for scenario in processed_scenarios:
        print(f"    ✅ {scenario['name']}: {scenario['category']}({scenario['severity']})")

async def main():
    """모든 테스트 실행"""
    print("🚀 ErrorHandler 종합 테스트 시작!\n")
    
    test_functions = [
        test_error_categorization,
        test_error_context,
        test_failed_operation_creation,
        test_error_details_extraction,
        test_error_handling_workflow,
        test_error_statistics,
        test_logging_setup,
        test_retry_mechanism,
        test_comprehensive_error_scenarios,
    ]
    
    passed = 0
    total = len(test_functions)
    
    for test_func in test_functions:
        try:
            await test_func()
            passed += 1
        except Exception as e:
            print(f"❌ {test_func.__name__} 실패: {e}")
    
    print(f"\n🎯 ErrorHandler 테스트 완료: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 테스트 통과! ErrorHandler 구현이 완벽합니다!")
    else:
        print(f"⚠️ {total - passed}개 테스트 실패")

if __name__ == "__main__":
    asyncio.run(main()) 