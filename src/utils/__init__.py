"""
Finance Data Vibe - 유틸리티 모듈 초기화
워런 버핏 스타일 가치투자 시스템의 유틸리티 함수들

이 모듈은 프로젝트 전반에서 사용되는 유틸리티 함수들과 클래스들을 
통합하여 쉽게 import할 수 있도록 합니다.

사용법:
    from src.utils import format_currency, validate_stock_code
    from src.utils import APIRateLimiter, MemoryMonitor
    from src.utils import get_logger, log_execution_time
"""

import sys
import os
from typing import Dict, Any, Optional, List, Union, Tuple
import warnings

# 버전 정보
__version__ = "1.0.0"
__author__ = "Finance Data Vibe Team"
__email__ = "admin@financedatavibe.com"

# 모듈 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# =============================================================================
# API 관련 유틸리티
# =============================================================================
try:
    from .api_utils import (
        # API 관리 클래스
        APIRateLimiter,
        APIRetryManager,
        APIHealthChecker,
        APIResponseValidator,
        
        # HTTP 유틸리티
        make_request,
        handle_api_error,
        parse_api_response,
        
        # 인증 및 보안
        generate_api_signature,
        validate_api_key,
        
        # 캐싱
        cache_api_response,
        get_cached_response,
        clear_api_cache,
        
        # 모니터링
        log_api_call,
        track_api_usage,
        get_api_stats
    )
except ImportError as e:
    warnings.warn(f"Failed to import api_utils: {e}")

# =============================================================================
# 계산 관련 유틸리티
# =============================================================================
try:
    from .calculation_utils import (
        # 기본 재무 계산
        calculate_roe,
        calculate_roa,
        calculate_per,
        calculate_pbr,
        calculate_peg,
        calculate_debt_ratio,
        calculate_current_ratio,
        calculate_quick_ratio,
        
        # 고급 재무 계산
        calculate_dcf_value,
        calculate_ddm_value,
        calculate_capm,
        calculate_wacc,
        calculate_eva,
        calculate_altman_z_score,
        
        # 수익률 계산
        calculate_returns,
        calculate_cumulative_returns,
        calculate_volatility,
        calculate_sharpe_ratio,
        calculate_max_drawdown,
        
        # 통계 계산
        calculate_correlation,
        calculate_beta,
        calculate_var,
        calculate_cvar,
        
        # 유틸리티 함수
        safe_divide,
        safe_percentage,
        normalize_values,
        weighted_average,
        compound_growth_rate,
        
        # 상수
        TRADING_DAYS_PER_YEAR,
        RISK_FREE_RATE,
        MARKET_RISK_PREMIUM
    )
except ImportError as e:
    warnings.warn(f"Failed to import calculation_utils: {e}")

# =============================================================================
# 차트 관련 유틸리티
# =============================================================================
try:
    from .chart_utils import (
        # 차트 생성 클래스
        BaseChart,
        LineChart,
        CandlestickChart,
        VolumeChart,
        FinancialChart,
        ComparisonChart,
        
        # 차트 유틸리티
        create_stock_chart,
        create_financial_chart,
        create_comparison_chart,
        create_portfolio_chart,
        create_analysis_chart,
        
        # 스타일 및 설정
        get_chart_colors,
        get_chart_layout,
        format_chart_data,
        
        # 인터랙티브 기능
        add_technical_indicators,
        add_annotations,
        add_trendlines,
        
        # 내보내기
        export_chart,
        save_chart_image,
        
        # 상수
        CHART_THEMES,
        DEFAULT_COLORS,
        CHART_SIZES
    )
except ImportError as e:
    warnings.warn(f"Failed to import chart_utils: {e}")

# =============================================================================
# 데이터 검증 유틸리티
# =============================================================================
try:
    from .data_validation import (
        # 검증 클래스
        DataValidator,
        StockDataValidator,
        FinancialDataValidator,
        NewsDataValidator,
        
        # 기본 검증 함수
        validate_stock_code,
        validate_date,
        validate_financial_data,
        validate_price_data,
        validate_volume_data,
        
        # 데이터 정리
        clean_financial_data,
        clean_price_data,
        clean_text_data,
        remove_outliers,
        
        # 데이터 품질 확인
        check_data_quality,
        check_data_completeness,
        check_data_consistency,
        
        # 예외 클래스
        ValidationError,
        DataQualityError,
        
        # 상수
        STOCK_CODE_PATTERN,
        VALID_EXCHANGES,
        DATA_QUALITY_THRESHOLDS
    )
except ImportError as e:
    warnings.warn(f"Failed to import data_validation: {e}")

# =============================================================================
# 날짜 관련 유틸리티
# =============================================================================
try:
    from .date_utils import (
        # 날짜 변환
        parse_date,
        format_date,
        to_datetime,
        to_timestamp,
        
        # 날짜 계산
        add_business_days,
        subtract_business_days,
        get_business_days_between,
        get_quarter_dates,
        get_year_dates,
        
        # 거래일 관련
        is_business_day,
        get_next_business_day,
        get_previous_business_day,
        get_business_days_in_period,
        
        # 기간 계산
        get_date_range,
        get_quarterly_dates,
        get_monthly_dates,
        get_weekly_dates,
        
        # 한국 시장 특화
        get_krx_trading_days,
        is_krx_holiday,
        get_krx_business_days,
        
        # 유틸리티
        get_current_quarter,
        get_fiscal_year,
        get_days_until_earnings,
        
        # 상수
        KRX_HOLIDAYS,
        BUSINESS_DAYS_PER_YEAR,
        TRADING_HOURS
    )
except ImportError as e:
    warnings.warn(f"Failed to import date_utils: {e}")

# =============================================================================
# 이메일 관련 유틸리티
# =============================================================================
try:
    from .email_utils import (
        # 이메일 클래스
        EmailSender,
        EmailTemplate,
        EmailScheduler,
        
        # 이메일 전송
        send_email,
        send_html_email,
        send_bulk_email,
        
        # 알림 이메일
        send_analysis_alert,
        send_portfolio_alert,
        send_error_alert,
        send_daily_report,
        
        # 템플릿
        create_analysis_report,
        create_portfolio_summary,
        create_screening_results,
        
        # 설정
        setup_email_config,
        validate_email_config,
        
        # 유틸리티
        validate_email_address,
        format_email_content,
        
        # 상수
        EMAIL_TEMPLATES,
        DEFAULT_EMAIL_CONFIG
    )
except ImportError as e:
    warnings.warn(f"Failed to import email_utils: {e}")

# =============================================================================
# 파일 관련 유틸리티
# =============================================================================
try:
    from .file_utils import (
        # 파일 관리 클래스
        FileManager,
        DataFileManager,
        ConfigFileManager,
        
        # 파일 읽기/쓰기
        read_file,
        write_file,
        read_json,
        write_json,
        read_csv,
        write_csv,
        read_excel,
        write_excel,
        
        # 파일 정보
        get_file_size,
        get_file_modified_time,
        get_file_extension,
        
        # 디렉토리 관리
        create_directory,
        remove_directory,
        list_files,
        find_files,
        
        # 파일 압축
        compress_file,
        decompress_file,
        create_archive,
        extract_archive,
        
        # 백업 관리
        backup_file,
        restore_file,
        cleanup_old_backups,
        
        # 보안
        encrypt_file,
        decrypt_file,
        calculate_file_hash,
        
        # 상수
        SUPPORTED_FILE_TYPES,
        MAX_FILE_SIZE,
        BACKUP_RETENTION_DAYS
    )
except ImportError as e:
    warnings.warn(f"Failed to import file_utils: {e}")

# =============================================================================
# 포맷팅 관련 유틸리티
# =============================================================================
try:
    from .formatting_utils import (
        # 숫자 포맷팅
        format_number,
        format_currency,
        format_percentage,
        format_large_number,
        format_scientific,
        
        # 재무 데이터 포맷팅
        format_financial_value,
        format_ratio,
        format_return,
        format_volatility,
        
        # 문자열 포맷팅
        format_stock_code,
        format_company_name,
        format_text,
        truncate_text,
        
        # 테이블 포맷팅
        format_table,
        format_dataframe,
        create_summary_table,
        
        # 리포트 포맷팅
        format_analysis_report,
        format_screening_results,
        format_portfolio_summary,
        
        # 색상 및 스타일
        get_color_for_value,
        get_trend_indicator,
        format_with_color,
        
        # 유틸리티
        clean_text,
        normalize_text,
        escape_html,
        
        # 상수
        CURRENCY_SYMBOLS,
        NUMBER_FORMATS,
        COLOR_SCHEMES
    )
except ImportError as e:
    warnings.warn(f"Failed to import formatting_utils: {e}")

# =============================================================================
# 로깅 관련 유틸리티
# =============================================================================
try:
    from .logging_utils import (
        # 로거 클래스
        LoggerManager,
        PerformanceLogger,
        InvestmentLogger,
        
        # 로거 인스턴스
        get_logger_manager,
        get_logger,
        
        # 데코레이터
        log_execution_time,
        log_function_call,
        
        # 컨텍스트 매니저
        log_context,
        
        # 편의 함수
        debug,
        info,
        warning,
        error,
        critical,
        
        # 설정
        initialize_logging,
        setup_error_logging,
        
        # 모니터링
        log_memory_usage,
        log_system_info,
        
        # 상수
        LOG_LEVELS
    )
except ImportError as e:
    warnings.warn(f"Failed to import logging_utils: {e}")

# =============================================================================
# 메모리 관련 유틸리티
# =============================================================================
try:
    from .memory_utils import (
        # 메모리 관리 클래스
        MemoryManager,
        MemoryMonitor,
        DataCache,
        
        # 메모리 모니터링
        get_memory_usage,
        monitor_memory,
        log_memory_stats,
        
        # 캐시 관리
        create_cache,
        get_from_cache,
        set_to_cache,
        clear_cache,
        
        # 메모리 최적화
        optimize_memory,
        cleanup_memory,
        garbage_collect,
        
        # 데이터 관리
        manage_large_dataset,
        chunk_data,
        stream_data,
        
        # 상수
        MEMORY_THRESHOLDS,
        CACHE_SIZES,
        CLEANUP_INTERVALS
    )
except ImportError as e:
    warnings.warn(f"Failed to import memory_utils: {e}")

# =============================================================================
# 보안 관련 유틸리티
# =============================================================================
try:
    from .security_utils import (
        # 암호화 클래스
        DataEncryption,
        PasswordManager,
        TokenManager,
        
        # 암호화/복호화
        encrypt_data,
        decrypt_data,
        hash_data,
        
        # 비밀번호 관리
        hash_password,
        verify_password,
        generate_password,
        
        # 토큰 관리
        generate_token,
        validate_token,
        refresh_token,
        
        # API 보안
        generate_api_key,
        validate_api_key,
        sign_request,
        
        # 데이터 보안
        sanitize_input,
        validate_input,
        escape_sql,
        
        # 상수
        ENCRYPTION_ALGORITHMS,
        HASH_ALGORITHMS,
        TOKEN_TYPES
    )
except ImportError as e:
    warnings.warn(f"Failed to import security_utils: {e}")

# =============================================================================
# 통합 유틸리티 함수들
# =============================================================================

def get_all_utils() -> Dict[str, Any]:
    """모든 유틸리티 함수와 클래스 목록 반환"""
    utils_dict = {}
    
    # 현재 모듈의 모든 공개 객체 수집
    current_module = sys.modules[__name__]
    for name in dir(current_module):
        if not name.startswith('_'):
            obj = getattr(current_module, name)
            if callable(obj) or isinstance(obj, type):
                utils_dict[name] = obj
    
    return utils_dict

def print_utils_summary():
    """유틸리티 모듈 요약 출력"""
    print("=" * 60)
    print("Finance Data Vibe - 유틸리티 모듈 요약")
    print("=" * 60)
    
    modules = [
        ("API 유틸리티", "api_utils"),
        ("계산 유틸리티", "calculation_utils"),
        ("차트 유틸리티", "chart_utils"),
        ("데이터 검증", "data_validation"),
        ("날짜 유틸리티", "date_utils"),
        ("이메일 유틸리티", "email_utils"),
        ("파일 유틸리티", "file_utils"),
        ("포맷팅 유틸리티", "formatting_utils"),
        ("로깅 유틸리티", "logging_utils"),
        ("메모리 유틸리티", "memory_utils"),
        ("보안 유틸리티", "security_utils"),
    ]
    
    for name, module in modules:
        try:
            mod = __import__(f"src.utils.{module}", fromlist=[module])
            print(f"✓ {name:15} - 사용 가능")
        except ImportError:
            print(f"✗ {name:15} - 사용 불가")
    
    print("=" * 60)
    print(f"버전: {__version__}")
    print(f"작성자: {__author__}")
    print("=" * 60)

def validate_utils_environment():
    """유틸리티 환경 검증"""
    issues = []
    
    # 필수 디렉토리 확인
    required_dirs = ['logs', 'data', 'config']
    for dir_name in required_dirs:
        if not os.path.exists(dir_name):
            issues.append(f"필수 디렉토리 누락: {dir_name}")
    
    # 필수 패키지 확인
    required_packages = ['pandas', 'numpy', 'plotly', 'requests']
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            issues.append(f"필수 패키지 누락: {package}")
    
    if issues:
        print("환경 검증 결과 - 문제 발견:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    else:
        print("환경 검증 완료 - 모든 요구사항 충족")
        return True

# =============================================================================
# 모듈 초기화
# =============================================================================

def initialize_utils():
    """유틸리티 모듈 초기화"""
    print("Finance Data Vibe 유틸리티 모듈 초기화 중...")
    
    # 환경 검증
    if not validate_utils_environment():
        warnings.warn("일부 유틸리티가 제대로 작동하지 않을 수 있습니다.")
    
    # 로깅 초기화
    try:
        initialize_logging()
        print("✓ 로깅 시스템 초기화 완료")
    except Exception as e:
        print(f"✗ 로깅 시스템 초기화 실패: {e}")
    
    # 메모리 모니터링 시작
    try:
        if 'MemoryMonitor' in locals():
            monitor = MemoryMonitor()
            monitor.start()
            print("✓ 메모리 모니터링 시작")
    except Exception as e:
        print(f"✗ 메모리 모니터링 시작 실패: {e}")
    
    print("유틸리티 모듈 초기화 완료!")

# =============================================================================
# 공통 상수 및 설정
# =============================================================================

# 프로젝트 설정
PROJECT_NAME = "Finance Data Vibe"
PROJECT_VERSION = __version__
PROJECT_AUTHOR = __author__

# 기본 설정
DEFAULT_TIMEZONE = "Asia/Seoul"
DEFAULT_CURRENCY = "KRW"
DEFAULT_LOCALE = "ko_KR"

# 성능 설정
MAX_WORKERS = 4
CHUNK_SIZE = 1000
BATCH_SIZE = 100

# 에러 메시지
ERROR_MESSAGES = {
    'INVALID_STOCK_CODE': "유효하지 않은 주식 코드입니다.",
    'INVALID_DATE': "유효하지 않은 날짜 형식입니다.",
    'DATA_NOT_FOUND': "데이터를 찾을 수 없습니다.",
    'API_ERROR': "API 호출 중 오류가 발생했습니다.",
    'CALCULATION_ERROR': "계산 중 오류가 발생했습니다.",
    'FILE_ERROR': "파일 처리 중 오류가 발생했습니다.",
    'NETWORK_ERROR': "네트워크 오류가 발생했습니다.",
    'PERMISSION_ERROR': "권한이 없습니다.",
    'VALIDATION_ERROR': "데이터 검증에 실패했습니다.",
    'CONFIGURATION_ERROR': "설정 오류가 발생했습니다."
}

# 성공 메시지
SUCCESS_MESSAGES = {
    'DATA_LOADED': "데이터가 성공적으로 로드되었습니다.",
    'ANALYSIS_COMPLETE': "분석이 완료되었습니다.",
    'REPORT_GENERATED': "리포트가 생성되었습니다.",
    'FILE_SAVED': "파일이 저장되었습니다.",
    'EMAIL_SENT': "이메일이 전송되었습니다.",
    'CACHE_CLEARED': "캐시가 정리되었습니다.",
    'BACKUP_CREATED': "백업이 생성되었습니다.",
    'VALIDATION_PASSED': "데이터 검증이 통과되었습니다.",
    'CONFIGURATION_UPDATED': "설정이 업데이트되었습니다.",
    'SYSTEM_HEALTHY': "시스템이 정상적으로 작동하고 있습니다."
}

# 모듈 정보
__all__ = [
    # 모듈 정보
    '__version__', '__author__', '__email__',
    
    # 초기화 함수
    'initialize_utils', 'validate_utils_environment', 'print_utils_summary',
    
    # 유틸리티 함수 (조건부 import)
    # API 유틸리티
    'APIRateLimiter', 'make_request', 'handle_api_error',
    
    # 계산 유틸리티
    'calculate_roe', 'calculate_roa', 'calculate_per', 'calculate_dcf_value',
    'calculate_returns', 'calculate_sharpe_ratio', 'safe_divide',
    
    # 차트 유틸리티
    'create_stock_chart', 'create_financial_chart', 'BaseChart',
    
    # 데이터 검증
    'validate_stock_code', 'validate_date', 'DataValidator',
    
    # 날짜 유틸리티
    'parse_date', 'format_date', 'add_business_days', 'is_business_day',
    
    # 이메일 유틸리티
    'send_email', 'send_analysis_alert', 'EmailSender',
    
    # 파일 유틸리티
    'read_file', 'write_file', 'read_json', 'write_json', 'FileManager',
    
    # 포맷팅 유틸리티
    'format_currency', 'format_percentage', 'format_number',
    
    # 로깅 유틸리티
    'get_logger', 'log_execution_time', 'initialize_logging',
    
    # 메모리 유틸리티
    'get_memory_usage', 'MemoryMonitor', 'DataCache',
    
    # 보안 유틸리티
    'encrypt_data', 'decrypt_data', 'hash_password',
    
    # 상수
    'PROJECT_NAME', 'PROJECT_VERSION', 'DEFAULT_TIMEZONE',
    'ERROR_MESSAGES', 'SUCCESS_MESSAGES'
]

# 자동 초기화 (환경 변수로 제어 가능)
if os.getenv('FINANCE_DATA_VIBE_AUTO_INIT', '1') == '1':
    try:
        initialize_utils()
    except Exception as e:
        warnings.warn(f"자동 초기화 중 오류 발생: {e}")

# 개발 모드에서 요약 출력
if os.getenv('FINANCE_DATA_VIBE_DEBUG', '0') == '1':
    print_utils_summary()