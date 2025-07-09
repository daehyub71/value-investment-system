"""
Finance Data Vibe - 로깅 유틸리티
워런 버핏 스타일 가치투자 시스템의 로깅 관리

주요 기능:
- 구조화된 로깅 설정
- 성능 모니터링 로깅
- 에러 추적 및 알림
- 투자 분석 로깅
- 로그 회전 및 압축
"""

import logging
import logging.handlers
import os
import sys
import json
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, Union, List
from pathlib import Path
import functools
import time
import threading
from contextlib import contextmanager
import inspect

# 로그 레벨 상수
LOG_LEVELS = {
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG
}

class ColoredFormatter(logging.Formatter):
    """컬러 포맷터 - 콘솔 출력용"""
    
    COLORS = {
        'DEBUG': '\033[36m',      # 청록색
        'INFO': '\033[32m',       # 초록색
        'WARNING': '\033[33m',    # 노란색
        'ERROR': '\033[31m',      # 빨간색
        'CRITICAL': '\033[35m',   # 자홍색
        'RESET': '\033[0m'        # 리셋
    }
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset_color = self.COLORS['RESET']
        
        # 메시지에 색상 적용
        record.levelname = f"{log_color}{record.levelname}{reset_color}"
        
        return super().format(record)

class JSONFormatter(logging.Formatter):
    """JSON 포맷터 - 구조화된 로깅"""
    
    def format(self, record):
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # 예외 정보 추가
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': traceback.format_exception(*record.exc_info)
            }
        
        # 추가 속성 처리
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 
                          'pathname', 'filename', 'module', 'lineno', 'funcName',
                          'created', 'msecs', 'relativeCreated', 'thread', 
                          'threadName', 'processName', 'process', 'message',
                          'exc_info', 'exc_text', 'stack_info']:
                log_entry[key] = value
        
        return json.dumps(log_entry, ensure_ascii=False, default=str)

class PerformanceLogger:
    """성능 로깅 클래스"""
    
    def __init__(self, logger_name: str = "performance"):
        self.logger = logging.getLogger(logger_name)
        self.timings = {}
        self.lock = threading.Lock()
    
    def log_execution_time(self, func_name: str, execution_time: float, 
                          context: Optional[Dict] = None):
        """실행 시간 로깅"""
        with self.lock:
            if func_name not in self.timings:
                self.timings[func_name] = []
            self.timings[func_name].append(execution_time)
        
        self.logger.info(
            f"Function '{func_name}' executed in {execution_time:.4f}s",
            extra={
                'execution_time': execution_time,
                'function_name': func_name,
                'context': context or {}
            }
        )
    
    def get_average_time(self, func_name: str) -> Optional[float]:
        """평균 실행 시간 반환"""
        with self.lock:
            if func_name in self.timings and self.timings[func_name]:
                return sum(self.timings[func_name]) / len(self.timings[func_name])
        return None
    
    def get_stats(self, func_name: str) -> Dict[str, float]:
        """실행 통계 반환"""
        with self.lock:
            if func_name not in self.timings or not self.timings[func_name]:
                return {}
            
            times = self.timings[func_name]
            return {
                'count': len(times),
                'min': min(times),
                'max': max(times),
                'avg': sum(times) / len(times),
                'total': sum(times)
            }

class InvestmentLogger:
    """투자 분석 전용 로거"""
    
    def __init__(self, logger_name: str = "investment"):
        self.logger = logging.getLogger(logger_name)
    
    def log_stock_analysis(self, stock_code: str, analysis_type: str, 
                          result: Dict[str, Any], score: Optional[float] = None):
        """주식 분석 로깅"""
        self.logger.info(
            f"Stock analysis completed: {stock_code} - {analysis_type}",
            extra={
                'stock_code': stock_code,
                'analysis_type': analysis_type,
                'result': result,
                'score': score,
                'timestamp': datetime.now().isoformat()
            }
        )
    
    def log_screening_result(self, criteria: Dict[str, Any], 
                           results: List[str], execution_time: float):
        """스크리닝 결과 로깅"""
        self.logger.info(
            f"Stock screening completed: {len(results)} stocks found",
            extra={
                'criteria': criteria,
                'results_count': len(results),
                'results': results,
                'execution_time': execution_time
            }
        )
    
    def log_portfolio_update(self, portfolio_id: str, action: str, 
                           details: Dict[str, Any]):
        """포트폴리오 업데이트 로깅"""
        self.logger.info(
            f"Portfolio update: {portfolio_id} - {action}",
            extra={
                'portfolio_id': portfolio_id,
                'action': action,
                'details': details,
                'timestamp': datetime.now().isoformat()
            }
        )

class LoggerManager:
    """로거 관리자"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
        self.loggers = {}
        self.performance_logger = PerformanceLogger()
        self.investment_logger = InvestmentLogger()
        
        # 로그 디렉토리 생성
        self._create_log_directories()
        
        # 기본 로거 설정
        self._setup_root_logger()
    
    def _default_config(self) -> Dict:
        """기본 로깅 설정"""
        return {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                },
                'detailed': {
                    'format': '%(asctime)s [%(levelname)s] %(name)s.%(funcName)s:%(lineno)d: %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'INFO',
                    'formatter': 'standard',
                    'stream': 'ext://sys.stdout'
                },
                'file': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'level': 'DEBUG',
                    'formatter': 'detailed',
                    'filename': 'logs/app.log',
                    'maxBytes': 10485760,  # 10MB
                    'backupCount': 5
                }
            },
            'loggers': {
                '': {  # root logger
                    'handlers': ['console', 'file'],
                    'level': 'DEBUG',
                    'propagate': False
                }
            }
        }
    
    def _create_log_directories(self):
        """로그 디렉토리 생성"""
        log_dirs = ['logs', 'logs/analysis', 'logs/performance', 'logs/errors']
        for log_dir in log_dirs:
            Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    def _setup_root_logger(self):
        """루트 로거 설정"""
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        
        # 기존 핸들러 제거
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # 콘솔 핸들러
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = ColoredFormatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # 파일 핸들러
        file_handler = logging.handlers.RotatingFileHandler(
            'logs/app.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s.%(funcName)s:%(lineno)d: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
    
    def get_logger(self, name: str, level: str = 'INFO') -> logging.Logger:
        """로거 인스턴스 반환"""
        if name not in self.loggers:
            logger = logging.getLogger(name)
            logger.setLevel(LOG_LEVELS.get(level.upper(), logging.INFO))
            self.loggers[name] = logger
        
        return self.loggers[name]
    
    def get_performance_logger(self) -> PerformanceLogger:
        """성능 로거 반환"""
        return self.performance_logger
    
    def get_investment_logger(self) -> InvestmentLogger:
        """투자 로거 반환"""
        return self.investment_logger

# 글로벌 로거 매니저 인스턴스
_logger_manager = None

def get_logger_manager() -> LoggerManager:
    """로거 매니저 싱글톤 인스턴스 반환"""
    global _logger_manager
    if _logger_manager is None:
        _logger_manager = LoggerManager()
    return _logger_manager

def get_logger(name: str, level: str = 'INFO') -> logging.Logger:
    """로거 인스턴스 반환"""
    return get_logger_manager().get_logger(name, level)

def log_execution_time(func):
    """실행 시간 로깅 데코레이터"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func_name = f"{func.__module__}.{func.__name__}"
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            # 함수 인자 정보 추가
            context = {
                'args_count': len(args),
                'kwargs_keys': list(kwargs.keys()) if kwargs else []
            }
            
            get_logger_manager().get_performance_logger().log_execution_time(
                func_name, execution_time, context
            )
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger = get_logger(func.__module__)
            logger.error(
                f"Function '{func_name}' failed after {execution_time:.4f}s: {str(e)}",
                extra={
                    'function_name': func_name,
                    'execution_time': execution_time,
                    'error': str(e),
                    'error_type': type(e).__name__
                }
            )
            raise
    
    return wrapper

@contextmanager
def log_context(logger: logging.Logger, message: str, level: str = 'INFO'):
    """컨텍스트 로깅"""
    start_time = time.time()
    log_level = LOG_LEVELS.get(level.upper(), logging.INFO)
    
    logger.log(log_level, f"Starting: {message}")
    
    try:
        yield
        execution_time = time.time() - start_time
        logger.log(log_level, f"Completed: {message} (took {execution_time:.4f}s)")
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(
            f"Failed: {message} (took {execution_time:.4f}s) - {str(e)}",
            exc_info=True
        )
        raise

def log_function_call(logger: logging.Logger, level: str = 'DEBUG'):
    """함수 호출 로깅 데코레이터"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = f"{func.__module__}.{func.__name__}"
            log_level = LOG_LEVELS.get(level.upper(), logging.DEBUG)
            
            # 함수 시그니처 정보
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            logger.log(
                log_level,
                f"Calling {func_name} with args: {dict(bound_args.arguments)}"
            )
            
            try:
                result = func(*args, **kwargs)
                logger.log(log_level, f"Function {func_name} returned successfully")
                return result
                
            except Exception as e:
                logger.error(
                    f"Function {func_name} raised {type(e).__name__}: {str(e)}",
                    exc_info=True
                )
                raise
        
        return wrapper
    return decorator

def setup_error_logging(email_config: Optional[Dict] = None):
    """에러 로깅 설정"""
    error_logger = get_logger('error')
    
    # 에러 파일 핸들러
    error_handler = logging.handlers.RotatingFileHandler(
        'logs/errors/error.log',
        maxBytes=5*1024*1024,  # 5MB
        backupCount=10,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_formatter = JSONFormatter()
    error_handler.setFormatter(error_formatter)
    error_logger.addHandler(error_handler)
    
    # 이메일 핸들러 (옵션)
    if email_config:
        try:
            email_handler = logging.handlers.SMTPHandler(
                mailhost=email_config['smtp_server'],
                fromaddr=email_config['from_addr'],
                toaddrs=email_config['to_addrs'],
                subject=email_config.get('subject', 'Finance Data Vibe - Error Alert'),
                credentials=(email_config['username'], email_config['password']),
                secure=()
            )
            email_handler.setLevel(logging.CRITICAL)
            email_handler.setFormatter(error_formatter)
            error_logger.addHandler(email_handler)
        except Exception as e:
            error_logger.warning(f"Failed to setup email handler: {e}")

def log_memory_usage(logger: logging.Logger, message: str = ""):
    """메모리 사용량 로깅"""
    try:
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        logger.info(
            f"Memory usage {message}: RSS={memory_info.rss / 1024 / 1024:.2f}MB, "
            f"VMS={memory_info.vms / 1024 / 1024:.2f}MB"
        )
    except ImportError:
        logger.warning("psutil not available for memory monitoring")

def log_system_info(logger: logging.Logger):
    """시스템 정보 로깅"""
    try:
        import platform
        import psutil
        
        logger.info(
            f"System info: {platform.platform()}, "
            f"Python {platform.python_version()}, "
            f"CPU count: {psutil.cpu_count()}, "
            f"Memory: {psutil.virtual_memory().total / 1024 / 1024 / 1024:.2f}GB"
        )
    except ImportError:
        logger.info(f"System info: {platform.platform()}, Python {platform.python_version()}")

# 편의 함수들
def debug(message: str, **kwargs):
    """디버그 로깅"""
    logger = get_logger('debug')
    logger.debug(message, extra=kwargs)

def info(message: str, **kwargs):
    """정보 로깅"""
    logger = get_logger('info')
    logger.info(message, extra=kwargs)

def warning(message: str, **kwargs):
    """경고 로깅"""
    logger = get_logger('warning')
    logger.warning(message, extra=kwargs)

def error(message: str, exc_info: bool = False, **kwargs):
    """에러 로깅"""
    logger = get_logger('error')
    logger.error(message, exc_info=exc_info, extra=kwargs)

def critical(message: str, exc_info: bool = False, **kwargs):
    """중요 에러 로깅"""
    logger = get_logger('critical')
    logger.critical(message, exc_info=exc_info, extra=kwargs)

# 초기화
def initialize_logging(config: Optional[Dict] = None):
    """로깅 시스템 초기화"""
    global _logger_manager
    _logger_manager = LoggerManager(config)
    
    # 시스템 정보 로깅
    root_logger = get_logger('system')
    log_system_info(root_logger)
    
    root_logger.info("Finance Data Vibe logging system initialized")

if __name__ == "__main__":
    # 테스트 코드
    initialize_logging()
    
    # 기본 로깅 테스트
    logger = get_logger('test')
    logger.info("Test message")
    
    # 성능 로깅 테스트
    @log_execution_time
    def test_function():
        time.sleep(0.1)
        return "test result"
    
    result = test_function()
    
    # 투자 로깅 테스트
    investment_logger = get_logger_manager().get_investment_logger()
    investment_logger.log_stock_analysis(
        "005930", "fundamental", {"roe": 15.5, "per": 12.3}, 85.5
    )
    
    print("Logging test completed")