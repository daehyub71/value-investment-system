"""
로깅 설정 파일
프로젝트 전체의 로깅 설정 관리
"""

import os
import logging
import logging.handlers
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import sys

# 환경변수 로드
load_dotenv()

class LoggingConfig:
    """로깅 설정 관리 클래스"""
    
    def __init__(self):
        # 로그 디렉토리 설정
        self.log_dir = Path('logs')
        self.log_dir.mkdir(exist_ok=True)
        
        # 기본 설정
        self.config = {
            'level': os.getenv('LOG_LEVEL', 'INFO').upper(),
            'file': self.log_dir / os.getenv('LOG_FILE', 'app.log').replace('logs/', ''),
            'max_size': self._parse_size(os.getenv('LOG_MAX_SIZE', '10MB')),
            'backup_count': int(os.getenv('LOG_BACKUP_COUNT', '5')),
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            'date_format': '%Y-%m-%d %H:%M:%S',
            'encoding': 'utf-8'
        }
        
        # 로거별 설정
        self.logger_configs = {
            'root': {
                'level': logging.INFO,
                'handlers': ['console', 'file'],
                'propagate': False
            },
            'DataCollector': {
                'level': logging.INFO,
                'handlers': ['console', 'file', 'data_file'],
                'propagate': False
            },
            'KISAPIClient': {
                'level': logging.INFO,
                'handlers': ['console', 'file', 'api_file'],
                'propagate': False
            },
            'DartCollector': {
                'level': logging.INFO,
                'handlers': ['console', 'file', 'api_file'],
                'propagate': False
            },
            'NewsCollector': {
                'level': logging.INFO,
                'handlers': ['console', 'file', 'api_file'],
                'propagate': False
            },
            'BuffettScorecard': {
                'level': logging.INFO,
                'handlers': ['console', 'file', 'analysis_file'],
                'propagate': False
            },
            'TechnicalAnalysis': {
                'level': logging.INFO,
                'handlers': ['console', 'file', 'analysis_file'],
                'propagate': False
            },
            'SentimentAnalysis': {
                'level': logging.INFO,
                'handlers': ['console', 'file', 'analysis_file'],
                'propagate': False
            },
            'StreamlitApp': {
                'level': logging.INFO,
                'handlers': ['console', 'file', 'app_file'],
                'propagate': False
            }
        }
        
        # 핸들러 설정
        self.handlers = {
            'console': {
                'class': logging.StreamHandler,
                'kwargs': {'stream': sys.stdout},
                'level': logging.INFO,
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
            'file': {
                'class': logging.handlers.RotatingFileHandler,
                'kwargs': {
                    'filename': str(self.config['file']),
                    'maxBytes': self.config['max_size'],
                    'backupCount': self.config['backup_count'],
                    'encoding': self.config['encoding']
                },
                'level': logging.DEBUG,
                'format': self.config['format']
            },
            'data_file': {
                'class': logging.handlers.RotatingFileHandler,
                'kwargs': {
                    'filename': str(self.log_dir / 'data_collection.log'),
                    'maxBytes': self.config['max_size'],
                    'backupCount': self.config['backup_count'],
                    'encoding': self.config['encoding']
                },
                'level': logging.DEBUG,
                'format': self.config['format']
            },
            'api_file': {
                'class': logging.handlers.RotatingFileHandler,
                'kwargs': {
                    'filename': str(self.log_dir / 'api_requests.log'),
                    'maxBytes': self.config['max_size'],
                    'backupCount': self.config['backup_count'],
                    'encoding': self.config['encoding']
                },
                'level': logging.DEBUG,
                'format': self.config['format']
            },
            'analysis_file': {
                'class': logging.handlers.RotatingFileHandler,
                'kwargs': {
                    'filename': str(self.log_dir / 'analysis.log'),
                    'maxBytes': self.config['max_size'],
                    'backupCount': self.config['backup_count'],
                    'encoding': self.config['encoding']
                },
                'level': logging.DEBUG,
                'format': self.config['format']
            },
            'app_file': {
                'class': logging.handlers.RotatingFileHandler,
                'kwargs': {
                    'filename': str(self.log_dir / 'streamlit_app.log'),
                    'maxBytes': self.config['max_size'],
                    'backupCount': self.config['backup_count'],
                    'encoding': self.config['encoding']
                },
                'level': logging.DEBUG,
                'format': self.config['format']
            },
            'error_file': {
                'class': logging.handlers.RotatingFileHandler,
                'kwargs': {
                    'filename': str(self.log_dir / 'errors.log'),
                    'maxBytes': self.config['max_size'],
                    'backupCount': self.config['backup_count'],
                    'encoding': self.config['encoding']
                },
                'level': logging.ERROR,
                'format': self.config['format']
            }
        }
        
        # 로깅 설정 적용
        self.setup_logging()
    
    def _parse_size(self, size_str: str) -> int:
        """크기 문자열을 바이트로 변환"""
        size_str = size_str.upper()
        
        if size_str.endswith('KB'):
            return int(size_str[:-2]) * 1024
        elif size_str.endswith('MB'):
            return int(size_str[:-2]) * 1024 * 1024
        elif size_str.endswith('GB'):
            return int(size_str[:-2]) * 1024 * 1024 * 1024
        else:
            return int(size_str)
    
    def setup_logging(self):
        """로깅 설정 적용"""
        # 기본 로깅 레벨 설정
        logging.basicConfig(
            level=getattr(logging, self.config['level']),
            format=self.config['format'],
            datefmt=self.config['date_format']
        )
        
        # 핸들러 생성
        self.created_handlers = {}
        for handler_name, handler_config in self.handlers.items():
            handler = handler_config['class'](**handler_config['kwargs'])
            handler.setLevel(handler_config['level'])
            
            # 포맷터 설정
            formatter = logging.Formatter(
                handler_config['format'],
                datefmt=self.config['date_format']
            )
            handler.setFormatter(formatter)
            
            self.created_handlers[handler_name] = handler
        
        # 로거 설정
        for logger_name, logger_config in self.logger_configs.items():
            if logger_name == 'root':
                logger = logging.getLogger()
            else:
                logger = logging.getLogger(logger_name)
            
            logger.setLevel(logger_config['level'])
            logger.propagate = logger_config['propagate']
            
            # 기존 핸들러 제거
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            
            # 새 핸들러 추가
            for handler_name in logger_config['handlers']:
                if handler_name in self.created_handlers:
                    logger.addHandler(self.created_handlers[handler_name])
        
        # 에러 핸들러를 모든 로거에 추가
        error_handler = self.created_handlers.get('error_file')
        if error_handler:
            for logger_name in self.logger_configs.keys():
                if logger_name == 'root':
                    logger = logging.getLogger()
                else:
                    logger = logging.getLogger(logger_name)
                logger.addHandler(error_handler)
    
    def get_logger(self, name: str) -> logging.Logger:
        """로거 반환"""
        return logging.getLogger(name)
    
    def set_log_level(self, level: str, logger_name: Optional[str] = None):
        """로그 레벨 설정"""
        log_level = getattr(logging, level.upper())
        
        if logger_name:
            logger = logging.getLogger(logger_name)
            logger.setLevel(log_level)
        else:
            # 모든 로거 레벨 변경
            for logger_name in self.logger_configs.keys():
                if logger_name == 'root':
                    logger = logging.getLogger()
                else:
                    logger = logging.getLogger(logger_name)
                logger.setLevel(log_level)
    
    def add_custom_handler(self, logger_name: str, handler: logging.Handler):
        """커스텀 핸들러 추가"""
        logger = logging.getLogger(logger_name)
        logger.addHandler(handler)
    
    def get_log_files(self) -> Dict[str, Path]:
        """로그 파일 목록 반환"""
        log_files = {}
        
        for handler_name, handler_config in self.handlers.items():
            if 'filename' in handler_config['kwargs']:
                log_files[handler_name] = Path(handler_config['kwargs']['filename'])
        
        return log_files
    
    def get_log_stats(self) -> Dict[str, Any]:
        """로그 파일 통계 반환"""
        stats = {}
        log_files = self.get_log_files()
        
        for handler_name, log_file in log_files.items():
            if log_file.exists():
                stats[handler_name] = {
                    'file': str(log_file),
                    'size': log_file.stat().st_size,
                    'size_mb': round(log_file.stat().st_size / (1024 * 1024), 2),
                    'modified': log_file.stat().st_mtime
                }
            else:
                stats[handler_name] = {
                    'file': str(log_file),
                    'size': 0,
                    'size_mb': 0,
                    'modified': None
                }
        
        return stats
    
    def cleanup_old_logs(self, days_to_keep: int = 30):
        """오래된 로그 파일 정리"""
        import time
        import glob
        
        current_time = time.time()
        cutoff_time = current_time - (days_to_keep * 24 * 60 * 60)
        
        cleaned_files = []
        
        # 백업 로그 파일들 확인
        for log_file in self.log_dir.glob('*.log.*'):
            if log_file.stat().st_mtime < cutoff_time:
                try:
                    log_file.unlink()
                    cleaned_files.append(str(log_file))
                except Exception as e:
                    logging.error(f"로그 파일 삭제 실패 {log_file}: {e}")
        
        return cleaned_files
    
    def rotate_logs(self):
        """로그 파일 수동 로테이션"""
        for handler_name, handler in self.created_handlers.items():
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                handler.doRollover()

# 로깅 설정 관리 클래스
class LoggerManager:
    """로거 관리 클래스"""
    
    def __init__(self):
        self.config = LoggingConfig()
    
    def get_logger(self, name: str) -> logging.Logger:
        """로거 반환"""
        return self.config.get_logger(name)
    
    def log_api_request(self, logger_name: str, method: str, url: str, 
                       status_code: int, response_time: float):
        """API 요청 로그"""
        logger = self.get_logger(logger_name)
        logger.info(f"API Request: {method} {url} - Status: {status_code} - Time: {response_time:.2f}s")
    
    def log_data_collection(self, logger_name: str, data_type: str, 
                           count: int, duration: float):
        """데이터 수집 로그"""
        logger = self.get_logger(logger_name)
        logger.info(f"Data Collection: {data_type} - Count: {count} - Duration: {duration:.2f}s")
    
    def log_analysis_result(self, logger_name: str, analysis_type: str, 
                           stock_code: str, result: Any):
        """분석 결과 로그"""
        logger = self.get_logger(logger_name)
        logger.info(f"Analysis: {analysis_type} - Stock: {stock_code} - Result: {result}")
    
    def log_error(self, logger_name: str, error: Exception, context: str = ""):
        """에러 로그"""
        logger = self.get_logger(logger_name)
        logger.error(f"Error in {context}: {type(error).__name__}: {str(error)}", exc_info=True)
    
    def log_performance(self, logger_name: str, function_name: str, 
                       execution_time: float, memory_usage: float = 0):
        """성능 로그"""
        logger = self.get_logger(logger_name)
        logger.info(f"Performance: {function_name} - Time: {execution_time:.2f}s - Memory: {memory_usage:.2f}MB")

# 글로벌 로거 관리자 인스턴스
logger_manager = LoggerManager()

# 편의 함수들
def get_logger(name: str) -> logging.Logger:
    """로거 반환"""
    return logger_manager.get_logger(name)

def setup_logger(name: str) -> logging.Logger:
    """로거 설정 및 반환"""
    return logger_manager.get_logger(name)

def log_api_request(logger_name: str, method: str, url: str, 
                   status_code: int, response_time: float):
    """API 요청 로그"""
    logger_manager.log_api_request(logger_name, method, url, status_code, response_time)

def log_data_collection(logger_name: str, data_type: str, count: int, duration: float):
    """데이터 수집 로그"""
    logger_manager.log_data_collection(logger_name, data_type, count, duration)

def log_analysis_result(logger_name: str, analysis_type: str, stock_code: str, result: Any):
    """분석 결과 로그"""
    logger_manager.log_analysis_result(logger_name, analysis_type, stock_code, result)

def log_error(logger_name: str, error: Exception, context: str = ""):
    """에러 로그"""
    logger_manager.log_error(logger_name, error, context)

def log_performance(logger_name: str, function_name: str, 
                   execution_time: float, memory_usage: float = 0):
    """성능 로그"""
    logger_manager.log_performance(logger_name, function_name, execution_time, memory_usage)

def setup_logging():
    """로깅 시스템 초기화 (호환성을 위한 함수)"""
    # 이미 LoggingConfig()에서 자동으로 설정되므로 pass
    pass

# 데코레이터
def log_execution_time(logger_name: str):
    """실행 시간 로깅 데코레이터"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            
            logger = get_logger(logger_name)
            logger.info(f"Starting {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(f"Completed {func.__name__} in {execution_time:.2f}s")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"Failed {func.__name__} after {execution_time:.2f}s: {e}")
                raise
        
        return wrapper
    return decorator

def log_method_calls(logger_name: str):
    """메서드 호출 로깅 데코레이터"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_logger(logger_name)
            logger.debug(f"Calling {func.__name__} with args: {args[1:]} kwargs: {kwargs}")
            
            try:
                result = func(*args, **kwargs)
                logger.debug(f"Method {func.__name__} returned: {type(result)}")
                return result
            except Exception as e:
                logger.error(f"Method {func.__name__} failed: {e}")
                raise
        
        return wrapper
    return decorator

# 사용 예시
if __name__ == "__main__":
    print("📝 로깅 시스템 테스트")
    print("=" * 50)
    
    # 각 로거 테스트
    loggers = [
        'DataCollector',
        'KISAPIClient',
        'BuffettScorecard',
        'StreamlitApp'
    ]
    
    for logger_name in loggers:
        logger = get_logger(logger_name)
        logger.info(f"{logger_name} 로거 테스트")
        logger.debug(f"{logger_name} 디버그 메시지")
        logger.warning(f"{logger_name} 경고 메시지")
        logger.error(f"{logger_name} 에러 메시지")
    
    # 로그 파일 통계 출력
    print("\n📊 로그 파일 통계:")
    stats = logger_manager.config.get_log_stats()
    
    for handler_name, stat in stats.items():
        print(f"{handler_name}: {stat['size_mb']}MB ({stat['file']})")
    
    print("\n✅ 로깅 시스템 설정 완료")