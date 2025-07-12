"""
메인 설정 관리자
모든 설정 파일을 통합하여 관리하는 중앙 집중식 설정 관리자
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 프로젝트 루트 경로 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

# 설정 모듈 임포트
try:
    from config.settings import settings
    from config.api_config import api_config, get_kis_config  # kis_api_config 대신 api_config에서 임포트
    from config.database_config import database_config
    from config.logging_config import logger_manager
except ImportError as e:
    print(f"설정 모듈 임포트 실패: {e}")
    print("프로젝트 디렉토리 구조를 확인해주세요.")
    print("\n해결 방법:")
    print("1. config/api_config.py 파일이 존재하는지 확인")
    print("2. 환경변수(.env) 파일이 올바르게 설정되어 있는지 확인")
    print("3. Python 경로가 올바르게 설정되어 있는지 확인")
    sys.exit(1)

class ConfigManager:
    """통합 설정 관리자"""
    
    def __init__(self):
        self.project_root = PROJECT_ROOT
        self.settings = settings
        self.api_config = api_config
        self.database_config = database_config
        self.logger_manager = logger_manager
        
        # 설정 유효성 검사
        self.validation_errors = self._validate_all_configs()
        
        # 초기화 로그
        self.logger = self.logger_manager.get_logger('ConfigManager')
        self.logger.info("설정 관리자 초기화 완료")
    
    def _validate_all_configs(self) -> List[str]:
        """모든 설정 유효성 검사"""
        errors = []
        
        # 기본 설정 검증
        errors.extend(self.settings.validate_config())
        
        # API 설정 검증
        errors.extend(self.api_config.validate_all_configs())
        
        # 필수 디렉토리 검증
        required_dirs = [
            self.project_root / 'data',
            self.project_root / 'logs',
            self.project_root / 'data' / 'databases'
        ]
        
        for directory in required_dirs:
            if not directory.exists():
                errors.append(f"필수 디렉토리가 없습니다: {directory}")
                # 디렉토리 자동 생성
                try:
                    directory.mkdir(parents=True, exist_ok=True)
                    print(f"✅ 디렉토리 생성: {directory}")
                except Exception as e:
                    print(f"❌ 디렉토리 생성 실패: {directory} - {e}")
        
        return errors
    
    def get_config_summary(self) -> Dict[str, Any]:
        """설정 요약 정보 반환"""
        return {
            'project_root': str(self.project_root),
            'config_status': {
                'settings': 'loaded' if hasattr(self, 'settings') else 'failed',
                'api_config': 'loaded' if hasattr(self, 'api_config') else 'failed',
                'database_config': 'loaded' if hasattr(self, 'database_config') else 'failed',
                'logger_manager': 'loaded' if hasattr(self, 'logger_manager') else 'failed'
            },
            'validation_errors': self.validation_errors
        }
    
    def print_config_status(self):
        """설정 상태 출력"""
        print("=" * 60)
        print("🔧 Finance Data Vibe 설정 상태")
        print("=" * 60)
        
        summary = self.get_config_summary()
        
        print(f"📁 프로젝트 루트: {summary['project_root']}")
        print("\n📊 모듈 로딩 상태:")
        for module, status in summary['config_status'].items():
            emoji = "✅" if status == "loaded" else "❌"
            print(f"  {emoji} {module}: {status}")
        
        if summary['validation_errors']:
            print("\n⚠️  설정 오류:")
            for error in summary['validation_errors']:
                print(f"  - {error}")
        else:
            print("\n✅ 모든 설정이 정상적으로 로드되었습니다.")
        
        print("=" * 60)
    
    # 편의 메서드들 추가
    def get_logger(self, name: str):
        """로거 반환 (편의 메서드)"""
        return self.logger_manager.get_logger(name)
    
    def get_dart_config(self) -> Dict[str, Any]:
        """DART API 설정 반환"""
        return self.api_config.dart
    
    def get_naver_news_config(self) -> Dict[str, Any]:
        """네이버 뉴스 API 설정 반환"""
        return self.api_config.naver_news
    
    def get_kis_config(self) -> Dict[str, Any]:
        """KIS API 설정 반환"""
        return get_kis_config()
    
    def get_database_connection(self, db_name: str):
        """데이터베이스 연결 반환"""
        return self.database_config.get_connection(db_name)
    
    def get_analysis_config(self) -> Dict[str, Any]:
        """분석 설정 반환"""
        return self.settings.analysis_config
    
    def initialize_project(self) -> bool:
        """프로젝트 초기화"""
        try:
            # 필수 디렉토리 생성
            required_dirs = [
                self.project_root / 'data',
                self.project_root / 'logs',
                self.project_root / 'data' / 'databases'
            ]
            
            for directory in required_dirs:
                directory.mkdir(parents=True, exist_ok=True)
            
            self.logger.info("프로젝트 초기화 완료")
            return True
        except Exception as e:
            self.logger.error(f"프로젝트 초기화 실패: {e}")
            return False
    
    def is_development_mode(self) -> bool:
        """개발 모드 여부 반환"""
        return getattr(self.settings, 'dev_config', {}).get('development_mode', True)
    
    def is_debug_mode(self) -> bool:
        """디버그 모드 여부 반환"""
        return getattr(self.settings, 'dev_config', {}).get('debug_mode', False)

# 글로벌 설정 관리자 인스턴스
try:
    config_manager = ConfigManager()
except Exception as e:
    print(f"❌ 설정 관리자 초기화 실패: {e}")
    config_manager = None

# 편의 함수들
def get_dart_config() -> Dict[str, Any]:
    """DART API 설정 반환"""
    if config_manager:
        return config_manager.api_config.dart
    return {}

def get_naver_news_config() -> Dict[str, Any]:
    """네이버 뉴스 API 설정 반환"""
    if config_manager:
        return config_manager.api_config.naver_news
    return {}

def get_database_config() -> Dict[str, Any]:
    """데이터베이스 설정 반환"""
    if config_manager:
        return config_manager.database_config
    return {}

def validate_all_configs() -> List[str]:
    """모든 설정 유효성 검사"""
    if config_manager:
        return config_manager.validation_errors
    return ["설정 관리자가 초기화되지 않았습니다."]

# 사용 예시
if __name__ == "__main__":
    if config_manager:
        config_manager.print_config_status()
    else:
        print("❌ 설정 관리자를 초기화할 수 없습니다.")
