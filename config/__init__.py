"""
수정된 ConfigManager - ImportError 해결 버전
안전하고 단순한 설정 관리자
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

class ConfigManager:
    """안전한 통합 설정 관리자"""
    
    def __init__(self):
        """
        ConfigManager 초기화
        모든 설정을 안전하게 로드하고 오류 처리
        """
        # 프로젝트 루트 설정
        self.project_root = Path(__file__).parent.parent
        
        # 기본 로거 설정
        self._setup_basic_logger()
        
        try:
            # 각 설정을 개별적으로 로드 (오류 시 기본값 사용)
            self.dart_config = self._load_dart_config()
            self.naver_config = self._load_naver_config()
            self.kis_config = self._load_kis_config()
            self.database_config = self._load_database_config()
            self.logging_config = self._load_logging_config()
            self.analysis_config = self._load_analysis_config()
            
            # 필수 디렉토리 생성
            self._create_directories()
            
            self.logger.info("ConfigManager 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"ConfigManager 초기화 중 오류: {e}")
            # 최소한의 설정으로 진행
            self._set_minimal_config()
    
    def _setup_basic_logger(self):
        """기본 로거 설정"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('logs/config.log', encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger('ConfigManager')
    
    def _load_dart_config(self) -> Dict[str, Any]:
        """DART API 설정 로드"""
        try:
            return {
                'api_key': os.getenv('DART_API_KEY', '').strip('"'),
                'base_url': 'https://opendart.fss.or.kr/api',
                'endpoints': {
                    'corp_code': 'corpCode.xml',
                    'financial_stmt': 'fnlttSinglAcntAll.json',
                    'disclosure': 'list.json'
                },
                'request_delay': 1.0,
                'timeout': 30
            }
        except Exception as e:
            self.logger.warning(f"DART 설정 로드 실패: {e}")
            return {'api_key': '', 'base_url': 'https://opendart.fss.or.kr/api'}
    
    def _load_naver_config(self) -> Dict[str, Any]:
        """네이버 뉴스 API 설정 로드"""
        try:
            return {
                'client_id': os.getenv('NAVER_CLIENT_ID', '').strip('"'),
                'client_secret': os.getenv('NAVER_CLIENT_SECRET', '').strip('"'),
                'base_url': 'https://openapi.naver.com/v1/search/news.json',
                'daily_limit': 25000,
                'request_delay': 0.1
            }
        except Exception as e:
            self.logger.warning(f"네이버 설정 로드 실패: {e}")
            return {'client_id': '', 'client_secret': ''}
    
    def _load_kis_config(self) -> Dict[str, Any]:
        """KIS API 설정 로드"""
        try:
            environment = os.getenv('KIS_ENVIRONMENT', 'VIRTUAL').upper()
            
            config = {
                'app_key': os.getenv('KIS_APP_KEY', '').strip('"'),
                'app_secret': os.getenv('KIS_APP_SECRET', '').strip('"'),
                'environment': environment,
                'url_base_real': 'https://openapi.koreainvestment.com:9443',
                'url_base_virtual': 'https://openapivts.koreainvestment.com:29443',
                'cano': os.getenv('KIS_CANO', '').strip('"'),
                'access_token': os.getenv('KIS_ACCESS_TOKEN', ''),
                'request_delay': 0.05
            }
            
            # 환경에 따른 기본 URL 설정
            if environment == 'REAL':
                config['url_base'] = config['url_base_real']
            else:
                config['url_base'] = config['url_base_virtual']
            
            return config
            
        except Exception as e:
            self.logger.warning(f"KIS 설정 로드 실패: {e}")
            return {
                'app_key': '', 'app_secret': '', 'environment': 'VIRTUAL',
                'url_base': 'https://openapivts.koreainvestment.com:29443'
            }
    
    def _load_database_config(self) -> Dict[str, Any]:
        """데이터베이스 설정 로드"""
        try:
            base_path = self.project_root / 'data' / 'databases'
            
            return {
                'base_path': base_path,
                'dart_db': base_path / 'dart_data.db',
                'stock_db': base_path / 'stock_data.db',
                'news_db': base_path / 'news_data.db',
                'kis_db': base_path / 'kis_data.db',
                'buffett_db': base_path / 'buffett_scorecard.db',
                'timeout': 30
            }
        except Exception as e:
            self.logger.warning(f"데이터베이스 설정 로드 실패: {e}")
            base_path = Path('data/databases')
            return {'base_path': base_path, 'dart_db': base_path / 'dart_data.db'}
    
    def _load_logging_config(self) -> Dict[str, Any]:
        """로깅 설정 로드"""
        try:
            return {
                'level': os.getenv('LOG_LEVEL', 'INFO'),
                'file': self.project_root / 'logs' / 'app.log',
                'max_size': '10MB',
                'backup_count': 5,
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        except Exception as e:
            self.logger.warning(f"로깅 설정 로드 실패: {e}")
            return {'level': 'INFO', 'file': 'logs/app.log'}
    
    def _load_analysis_config(self) -> Dict[str, Any]:
        """분석 설정 로드"""
        try:
            return {
                # 분석 비중 (기획서 기준)
                'weights': {
                    'fundamental': 0.45,
                    'technical': 0.30,
                    'sentiment': 0.25
                },
                
                # 워런 버핏 스코어카드 점수 배분
                'buffett_scorecard': {
                    'profitability': 30,
                    'growth': 25,
                    'stability': 25,
                    'efficiency': 10,
                    'valuation': 20,
                    'max_score': 110
                },
                
                # 스크리닝 기준값
                'screening_criteria': {
                    'roe_min': 0.15,
                    'debt_ratio_max': 0.50,
                    'current_ratio_min': 1.5,
                    'interest_coverage_min': 5.0
                }
            }
        except Exception as e:
            self.logger.warning(f"분석 설정 로드 실패: {e}")
            return {'weights': {'fundamental': 0.45, 'technical': 0.30, 'sentiment': 0.25}}
    
    def _create_directories(self):
        """필수 디렉토리 생성"""
        try:
            directories = [
                self.project_root / 'data',
                self.project_root / 'data' / 'databases',
                self.project_root / 'logs',
                self.project_root / 'data' / 'raw',
                self.project_root / 'data' / 'processed',
                self.project_root / 'data' / 'cache'
            ]
            
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
                
        except Exception as e:
            self.logger.error(f"디렉토리 생성 실패: {e}")
    
    def _set_minimal_config(self):
        """최소한의 설정으로 fallback"""
        self.dart_config = {'api_key': '', 'base_url': 'https://opendart.fss.or.kr/api'}
        self.naver_config = {'client_id': '', 'client_secret': ''}
        self.kis_config = {'app_key': '', 'app_secret': ''}
        self.database_config = {'base_path': Path('data/databases')}
        self.logging_config = {'level': 'INFO'}
        self.analysis_config = {'weights': {'fundamental': 0.45, 'technical': 0.30, 'sentiment': 0.25}}
    
    # =============================================================================
    # 공개 메서드들
    # =============================================================================
    
    def get_dart_config(self) -> Dict[str, Any]:
        """DART API 설정 반환"""
        return self.dart_config
    
    def get_naver_news_config(self) -> Dict[str, Any]:
        """네이버 뉴스 API 설정 반환"""
        return self.naver_config
    
    def get_kis_config(self) -> Dict[str, Any]:
        """KIS API 설정 반환"""
        return self.kis_config
    
    def get_database_config(self) -> Dict[str, Any]:
        """데이터베이스 설정 반환"""
        return self.database_config
    
    def get_analysis_config(self) -> Dict[str, Any]:
        """분석 설정 반환"""
        return self.analysis_config
    
    def get_logger(self, name: str) -> logging.Logger:
        """로거 반환"""
        logger = logging.getLogger(name)
        if not logger.handlers:
            # 핸들러가 없으면 기본 설정 적용
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def get_database_path(self, db_name: str) -> Path:
        """데이터베이스 파일 경로 반환"""
        db_map = {
            'dart': 'dart_db',
            'stock': 'stock_db',
            'news': 'news_db',
            'kis': 'kis_db',
            'buffett': 'buffett_db'
        }
        
        config_key = db_map.get(db_name, 'stock_db')
        return self.database_config.get(config_key, Path('data/databases/stock_data.db'))
    
    def validate_config(self) -> List[str]:
        """설정 유효성 검사"""
        errors = []
        
        try:
            # API 키 검증
            if not self.dart_config.get('api_key'):
                errors.append("DART_API_KEY가 설정되지 않았습니다.")
            
            if not self.naver_config.get('client_id'):
                errors.append("NAVER_CLIENT_ID가 설정되지 않았습니다.")
            
            if not self.naver_config.get('client_secret'):
                errors.append("NAVER_CLIENT_SECRET이 설정되지 않았습니다.")
            
            # 필수 디렉토리 확인
            required_dirs = ['data', 'logs', 'data/databases']
            for dir_name in required_dirs:
                dir_path = self.project_root / dir_name
                if not dir_path.exists():
                    errors.append(f"필수 디렉토리가 없습니다: {dir_path}")
            
        except Exception as e:
            errors.append(f"설정 검증 중 오류: {e}")
        
        return errors
    
    def print_config_status(self):
        """설정 상태 출력"""
        print("=" * 60)
        print("🔧 Finance Data Vibe 설정 상태")
        print("=" * 60)
        
        # API 설정 상태
        print("📊 API 설정:")
        dart_status = "✅ 설정됨" if self.dart_config.get('api_key') else "❌ 미설정"
        naver_status = "✅ 설정됨" if self.naver_config.get('client_id') else "❌ 미설정"
        kis_status = "✅ 설정됨" if self.kis_config.get('app_key') else "❌ 미설정"
        
        print(f"  • DART API: {dart_status}")
        print(f"  • 네이버 뉴스: {naver_status}")
        print(f"  • KIS API: {kis_status}")
        
        # 데이터베이스 상태
        print("\n💾 데이터베이스:")
        db_path = self.database_config.get('base_path', 'Unknown')
        print(f"  • 저장 경로: {db_path}")
        print(f"  • 경로 존재: {'✅' if Path(db_path).exists() else '❌'}")
        
        # 설정 오류 확인
        errors = self.validate_config()
        if errors:
            print("\n⚠️ 설정 오류:")
            for error in errors:
                print(f"  • {error}")
        else:
            print("\n✅ 모든 설정이 정상적으로 로드되었습니다.")
        
        print("=" * 60)
    
    def is_ready(self) -> bool:
        """ConfigManager 사용 준비 상태 확인"""
        errors = self.validate_config()
        critical_errors = [
            error for error in errors 
            if "DART_API_KEY" in error or "디렉토리" in error
        ]
        return len(critical_errors) == 0

# =============================================================================
# 글로벌 인스턴스 및 편의 함수들
# =============================================================================

# 안전한 글로벌 인스턴스 생성
try:
    config_manager = ConfigManager()
    CONFIG_LOADED = True
except Exception as e:
    print(f"❌ ConfigManager 초기화 실패: {e}")
    print("⚠️ 최소한의 기능만 사용 가능합니다.")
    config_manager = None
    CONFIG_LOADED = False

# 편의 함수들
def get_dart_config() -> Dict[str, Any]:
    """DART API 설정 반환"""
    if config_manager:
        return config_manager.get_dart_config()
    return {'api_key': os.getenv('DART_API_KEY', ''), 'base_url': 'https://opendart.fss.or.kr/api'}

def get_naver_news_config() -> Dict[str, Any]:
    """네이버 뉴스 API 설정 반환"""
    if config_manager:
        return config_manager.get_naver_news_config()
    return {'client_id': os.getenv('NAVER_CLIENT_ID', ''), 'client_secret': os.getenv('NAVER_CLIENT_SECRET', '')}

def get_kis_config() -> Dict[str, Any]:
    """KIS API 설정 반환"""
    if config_manager:
        return config_manager.get_kis_config()
    return {'app_key': os.getenv('KIS_APP_KEY', ''), 'app_secret': os.getenv('KIS_APP_SECRET', '')}

def get_logger(name: str) -> logging.Logger:
    """로거 반환"""
    if config_manager:
        return config_manager.get_logger(name)
    
    # Fallback 로거
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

def get_database_path(db_name: str) -> Path:
    """데이터베이스 파일 경로 반환"""
    if config_manager:
        return config_manager.get_database_path(db_name)
    
    # Fallback 경로
    base_path = Path('data/databases')
    db_files = {
        'dart': 'dart_data.db',
        'stock': 'stock_data.db',
        'news': 'news_data.db',
        'kis': 'kis_data.db'
    }
    return base_path / db_files.get(db_name, 'stock_data.db')

def validate_all_configs() -> List[str]:
    """모든 설정 유효성 검사"""
    if config_manager:
        return config_manager.validate_config()
    return ["ConfigManager가 초기화되지 않았습니다."]

def is_config_ready() -> bool:
    """설정 준비 상태 확인"""
    return CONFIG_LOADED and config_manager and config_manager.is_ready()

# =============================================================================
# 테스트 및 진단 함수
# =============================================================================

def test_config_import():
    """ConfigManager 임포트 테스트"""
    print("🧪 ConfigManager 임포트 테스트")
    print("-" * 40)
    
    if CONFIG_LOADED:
        print("✅ ConfigManager 정상 로드")
        if config_manager:
            config_manager.print_config_status()
    else:
        print("❌ ConfigManager 로드 실패")
        print("💡 기본 기능만 사용 가능합니다.")

def create_sample_env():
    """샘플 .env 파일 생성"""
    sample_env = '''# Finance Data Vibe 환경변수 설정

# DART API 설정
DART_API_KEY="your_dart_api_key_here"

# 네이버 뉴스 API 설정
NAVER_CLIENT_ID="your_naver_client_id"
NAVER_CLIENT_SECRET="your_naver_client_secret"

# KIS API 설정 (선택사항)
KIS_APP_KEY="your_kis_app_key"
KIS_APP_SECRET="your_kis_app_secret"
KIS_ENVIRONMENT="VIRTUAL"
KIS_CANO="your_account_number"

# 데이터베이스 설정
DB_PATH="data/databases/"

# 로깅 설정
LOG_LEVEL="INFO"
LOG_FILE="logs/app.log"

# 기타 설정
DEBUG_MODE="True"
DEVELOPMENT_MODE="True"
'''
    
    env_path = Path('.env.sample')
    with open(env_path, 'w', encoding='utf-8') as f:
        f.write(sample_env)
    
    print(f"✅ 샘플 환경변수 파일 생성: {env_path}")
    print("💡 .env.sample을 .env로 복사하고 실제 API 키를 입력하세요.")

# 사용 예시
if __name__ == "__main__":
    test_config_import()
    
    # 설정 파일이 없으면 샘플 생성
    if not Path('.env').exists():
        print("\n.env 파일이 없습니다.")
        create_sample_env()
