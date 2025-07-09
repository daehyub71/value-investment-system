"""
메인 설정 파일
프로젝트 전반적인 설정 관리
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any, Optional

# 환경변수 로드
load_dotenv()

# 프로젝트 루트 디렉토리
PROJECT_ROOT = Path(__file__).parent.parent

class Settings:
    """프로젝트 설정 클래스"""
    
    def __init__(self):
        # 기본 경로 설정
        self.PROJECT_ROOT = PROJECT_ROOT
        self.DATA_DIR = PROJECT_ROOT / 'data'
        self.DATABASE_DIR = self.DATA_DIR / 'databases'
        self.LOGS_DIR = PROJECT_ROOT / 'logs'
        
        # 디렉토리 생성
        self._create_directories()
        
        # API 설정
        self.api_config = self._load_api_config()
        
        # 데이터베이스 설정
        self.database_config = self._load_database_config()
        
        # 로깅 설정
        self.logging_config = self._load_logging_config()
        
        # 애플리케이션 설정
        self.app_config = self._load_app_config()
        
        # 투자 분석 설정
        self.analysis_config = self._load_analysis_config()
        
        # 보안 설정
        self.security_config = self._load_security_config()
        
        # 개발 환경 설정
        self.dev_config = self._load_dev_config()
        
        # 알림 설정
        self.notification_config = self._load_notification_config()
        
        # 성능 설정
        self.performance_config = self._load_performance_config()
    
    def _create_directories(self):
        """필요한 디렉토리 생성"""
        directories = [
            self.DATA_DIR,
            self.DATABASE_DIR,
            self.LOGS_DIR,
            self.DATA_DIR / 'raw' / 'dart',
            self.DATA_DIR / 'raw' / 'stock_prices',
            self.DATA_DIR / 'raw' / 'news',
            self.DATA_DIR / 'processed' / 'financial_ratios',
            self.DATA_DIR / 'processed' / 'technical_indicators',
            self.DATA_DIR / 'processed' / 'sentiment_scores',
            self.DATA_DIR / 'cache' / 'api_cache',
            self.DATA_DIR / 'cache' / 'analysis_cache'
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _load_api_config(self) -> Dict[str, Any]:
        """API 설정 로드"""
        return {
            # DART API 설정
            'dart': {
                'api_key': os.getenv('DART_API_KEY', ''),
                'base_url': 'https://opendart.fss.or.kr/api/',
                'request_delay': 1.0,
                'endpoints': {
                    'corp_code': 'corpCode.xml',
                    'financial_stmt': 'fnlttSinglAcntAll.json',
                    'disclosure': 'list.json'
                }
            },
            
            # 네이버 뉴스 API 설정
            'naver_news': {
                'client_id': os.getenv('NAVER_CLIENT_ID', ''),
                'client_secret': os.getenv('NAVER_CLIENT_SECRET', ''),
                'base_url': 'https://openapi.naver.com/v1/search/news.json',
                'daily_limit': 25000,
                'request_delay': 0.1
            },
            
            # KIS API 설정
            'kis': {
                'app_key': os.getenv('KIS_APP_KEY', '').strip('"'),
                'app_secret': os.getenv('KIS_APP_SECRET', '').strip('"'),
                'environment': os.getenv('KIS_ENVIRONMENT', 'VIRTUAL'),
                'url_base_real': os.getenv('KIS_URL_BASE_REAL', 'https://openapi.koreainvestment.com:9443'),
                'url_base_virtual': os.getenv('KIS_URL_BASE_VIRTUAL', 'https://openapivts.koreainvestment.com:29443'),
                'cano': os.getenv('KIS_CANO', '').strip('"'),
                'access_token': os.getenv('KIS_ACCESS_TOKEN', ''),
                'request_delay': float(os.getenv('KIS_REQUEST_DELAY', '0.05'))
            }
        }
    
    def _load_database_config(self) -> Dict[str, Any]:
        """데이터베이스 설정 로드"""
        db_path = os.getenv('DB_PATH', 'data/databases/')
        if not db_path.endswith('/'):
            db_path += '/'
        
        return {
            'path': self.DATABASE_DIR,
            'stock_db': self.DATABASE_DIR / os.getenv('STOCK_DB_NAME', 'stock_data.db'),
            'dart_db': self.DATABASE_DIR / os.getenv('DART_DB_NAME', 'dart_data.db'),
            'news_db': self.DATABASE_DIR / os.getenv('NEWS_DB_NAME', 'news_data.db'),
            'kis_db': self.DATABASE_DIR / os.getenv('KIS_DB_NAME', 'kis_data.db'),
            'connection_timeout': 30,
            'backup_enabled': True,
            'backup_interval': 86400  # 24시간
        }
    
    def _load_logging_config(self) -> Dict[str, Any]:
        """로깅 설정 로드"""
        return {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'file': self.LOGS_DIR / os.getenv('LOG_FILE', 'app.log').replace('logs/', ''),
            'max_size': os.getenv('LOG_MAX_SIZE', '10MB'),
            'backup_count': int(os.getenv('LOG_BACKUP_COUNT', '5')),
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'date_format': '%Y-%m-%d %H:%M:%S'
        }
    
    def _load_app_config(self) -> Dict[str, Any]:
        """애플리케이션 설정 로드"""
        return {
            'streamlit': {
                'port': int(os.getenv('STREAMLIT_PORT', '8501')),
                'debug': os.getenv('STREAMLIT_DEBUG', 'False').lower() == 'true',
                'title': 'Finance Data Vibe',
                'icon': '📊'
            },
            'cache': {
                'ttl': int(os.getenv('CACHE_TTL', '3600')),
                'max_size': int(os.getenv('CACHE_MAX_SIZE', '1000'))
            }
        }
    
    def _load_analysis_config(self) -> Dict[str, Any]:
        """투자 분석 설정 로드"""
        return {
            # 분석 비중 (기획서 기준)
            'weights': {
                'fundamental': float(os.getenv('ANALYSIS_WEIGHT_FUNDAMENTAL', '0.45')),
                'technical': float(os.getenv('ANALYSIS_WEIGHT_TECHNICAL', '0.30')),
                'sentiment': float(os.getenv('ANALYSIS_WEIGHT_SENTIMENT', '0.25'))
            },
            
            # 워런 버핏 스코어카드 점수 배분
            'buffett_scorecard': {
                'profitability': int(os.getenv('BUFFETT_SCORE_PROFITABILITY', '30')),
                'growth': int(os.getenv('BUFFETT_SCORE_GROWTH', '25')),
                'stability': int(os.getenv('BUFFETT_SCORE_STABILITY', '25')),
                'efficiency': int(os.getenv('BUFFETT_SCORE_EFFICIENCY', '10')),
                'valuation': int(os.getenv('BUFFETT_SCORE_VALUATION', '20'))
            },
            
            # 스크리닝 기준값
            'screening_criteria': {
                'roe_min': float(os.getenv('SCREENING_ROE_MIN', '0.15')),
                'debt_ratio_max': float(os.getenv('SCREENING_DEBT_RATIO_MAX', '0.50')),
                'profit_years': int(os.getenv('SCREENING_PROFIT_YEARS', '5')),
                'current_ratio_min': float(os.getenv('SCREENING_CURRENT_RATIO_MIN', '1.5')),
                'interest_coverage_min': float(os.getenv('SCREENING_INTEREST_COVERAGE_MIN', '5.0'))
            },
            
            # 안전마진 설정
            'safety_margin': float(os.getenv('SAFETY_MARGIN', '0.50')),
            
            # 기술적 분석 설정
            'technical_indicators': {
                'sma_periods': [20, 60, 120, 200],
                'ema_periods': [12, 26],
                'rsi_period': 14,
                'macd_fast': 12,
                'macd_slow': 26,
                'macd_signal': 9,
                'bollinger_period': 20,
                'bollinger_std': 2
            }
        }
    
    def _load_security_config(self) -> Dict[str, Any]:
        """보안 설정 로드"""
        return {
            'encryption_key': os.getenv('ENCRYPTION_KEY', ''),
            'jwt_secret': os.getenv('JWT_SECRET_KEY', ''),
            'jwt_expiration': int(os.getenv('JWT_EXPIRATION_TIME', '3600')),
            'api_key_encryption': True,
            'data_encryption': False
        }
    
    def _load_dev_config(self) -> Dict[str, Any]:
        """개발 환경 설정 로드"""
        return {
            'debug_mode': os.getenv('DEBUG_MODE', 'True').lower() == 'true',
            'development_mode': os.getenv('DEVELOPMENT_MODE', 'True').lower() == 'true',
            'test_mode': os.getenv('TEST_MODE', 'False').lower() == 'true',
            'mock_api_calls': os.getenv('MOCK_API_CALLS', 'False').lower() == 'true',
            'profiling_enabled': False
        }
    
    def _load_notification_config(self) -> Dict[str, Any]:
        """알림 설정 로드"""
        return {
            'email': {
                'enabled': bool(os.getenv('SMTP_USERNAME', '')),
                'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
                'smtp_port': int(os.getenv('SMTP_PORT', '587')),
                'username': os.getenv('SMTP_USERNAME', ''),
                'password': os.getenv('SMTP_PASSWORD', '').strip('"'),
                'from_email': os.getenv('SMTP_USERNAME', ''),
                'to_email': os.getenv('SMTP_USERNAME', '')
            },
            'slack': {
                'enabled': bool(os.getenv('SLACK_WEBHOOK_URL', '')),
                'webhook_url': os.getenv('SLACK_WEBHOOK_URL', '')
            }
        }
    
    def _load_performance_config(self) -> Dict[str, Any]:
        """성능 설정 로드"""
        return {
            'max_workers': int(os.getenv('MAX_WORKERS', '4')),
            'batch_size': int(os.getenv('BATCH_SIZE', '100')),
            'max_memory_usage': os.getenv('MAX_MEMORY_USAGE', '1GB'),
            'connection_pool_size': 10,
            'request_timeout': 30
        }
    
    def get_kis_config(self) -> Dict[str, Any]:
        """KIS API 설정 반환"""
        kis_config = self.api_config['kis'].copy()
        
        # 현재 환경에 따른 기본 URL 설정
        if kis_config['environment'] == 'REAL':
            kis_config['url_base'] = kis_config['url_base_real']
        else:
            kis_config['url_base'] = kis_config['url_base_virtual']
        
        return kis_config
    
    def get_database_path(self, db_name: str) -> Path:
        """데이터베이스 경로 반환"""
        db_map = {
            'stock': self.database_config['stock_db'],
            'dart': self.database_config['dart_db'],
            'news': self.database_config['news_db'],
            'kis': self.database_config['kis_db']
        }
        return db_map.get(db_name, self.database_config['stock_db'])
    
    def validate_config(self) -> list:
        """설정 유효성 검사"""
        errors = []
        
        # API 키 검증
        if not self.api_config['dart']['api_key']:
            errors.append("DART_API_KEY가 설정되지 않았습니다.")
        
        if not self.api_config['naver_news']['client_id']:
            errors.append("NAVER_CLIENT_ID가 설정되지 않았습니다.")
        
        if not self.api_config['naver_news']['client_secret']:
            errors.append("NAVER_CLIENT_SECRET이 설정되지 않았습니다.")
        
        # KIS API 검증
        kis_config = self.api_config['kis']
        if not kis_config['app_key']:
            errors.append("KIS_APP_KEY가 설정되지 않았습니다.")
        
        if not kis_config['app_secret']:
            errors.append("KIS_APP_SECRET이 설정되지 않았습니다.")
        
        if not kis_config['cano']:
            errors.append("KIS_CANO (계좌번호)가 설정되지 않았습니다.")
        
        if kis_config['environment'] not in ['REAL', 'VIRTUAL']:
            errors.append("KIS_ENVIRONMENT는 'REAL' 또는 'VIRTUAL'이어야 합니다.")
        
        # 분석 비중 검증
        weights = self.analysis_config['weights']
        total_weight = sum(weights.values())
        if abs(total_weight - 1.0) > 0.01:
            errors.append(f"분석 비중의 합이 1.0이 아닙니다. 현재: {total_weight}")
        
        # 워런 버핏 스코어카드 점수 검증
        buffett_scores = self.analysis_config['buffett_scorecard']
        if sum(buffett_scores.values()) != 110:
            errors.append("워런 버핏 스코어카드 점수의 합이 110점이 아닙니다.")
        
        return errors
    
    def print_config_summary(self):
        """설정 요약 출력"""
        print("=" * 60)
        print("🚀 Finance Data Vibe 설정 요약")
        print("=" * 60)
        
        # API 설정
        print("📊 API 설정:")
        print(f"  - DART API: {'✅ 설정됨' if self.api_config['dart']['api_key'] else '❌ 미설정'}")
        print(f"  - 네이버 뉴스: {'✅ 설정됨' if self.api_config['naver_news']['client_id'] else '❌ 미설정'}")
        print(f"  - KIS API: {'✅ 설정됨' if self.api_config['kis']['app_key'] else '❌ 미설정'}")
        print(f"  - KIS 환경: {self.api_config['kis']['environment']}")
        
        # 분석 설정
        print("\n📈 분석 설정:")
        weights = self.analysis_config['weights']
        print(f"  - 기본분석: {weights['fundamental']:.1%}")
        print(f"  - 기술분석: {weights['technical']:.1%}")
        print(f"  - 감정분석: {weights['sentiment']:.1%}")
        
        # 데이터베이스 설정
        print("\n💾 데이터베이스:")
        print(f"  - 저장 경로: {self.database_config['path']}")
        
        # 로깅 설정
        print("\n📝 로깅:")
        print(f"  - 레벨: {self.logging_config['level']}")
        print(f"  - 파일: {self.logging_config['file']}")
        
        # 알림 설정
        print("\n🔔 알림:")
        print(f"  - 이메일: {'✅ 설정됨' if self.notification_config['email']['enabled'] else '❌ 미설정'}")
        print(f"  - 슬랙: {'✅ 설정됨' if self.notification_config['slack']['enabled'] else '❌ 미설정'}")
        
        print("=" * 60)

# 글로벌 설정 인스턴스
settings = Settings()

# 편의 함수들
def get_dart_api_key() -> str:
    """DART API 키 반환"""
    return settings.api_config['dart']['api_key']

def get_naver_news_config() -> Dict[str, str]:
    """네이버 뉴스 API 설정 반환"""
    return settings.api_config['naver_news']

def get_kis_config() -> Dict[str, Any]:
    """KIS API 설정 반환"""
    return settings.get_kis_config()

def get_database_path(db_name: str) -> Path:
    """데이터베이스 경로 반환"""
    return settings.get_database_path(db_name)

def get_analysis_config() -> Dict[str, Any]:
    """분석 설정 반환"""
    return settings.analysis_config

def validate_all_configs() -> list:
    """모든 설정 유효성 검사"""
    return settings.validate_config()

# 사용 예시
if __name__ == "__main__":
    # 설정 요약 출력
    settings.print_config_summary()
    
    # 설정 검증
    errors = validate_all_configs()
    if errors:
        print("\n❌ 설정 오류:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("\n✅ 모든 설정이 올바르게 구성되었습니다.")