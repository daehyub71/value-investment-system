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
    from config.api_config import api_config
    from config.database_config import database_config
    from config.logging_config import logger_manager
    from config.kis_api_config import get_kis_config
except ImportError as e:
    print(f"설정 모듈 임포트 실패: {e}")
    print("프로젝트 디렉토리 구조를 확인해주세요.")
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
        
        return errors
    
    def get_config_summary(self) -> Dict[str, Any]:
        """설정 요약 정보 반환"""
        return {
            'project_root': str(self.project_root),
            'environment': os.getenv('KIS_ENVIRONMENT', 'VIRTUAL'),
            'debug_mode': self.settings.dev_config['debug_mode'],
            'log_level': self.settings.logging_config['level'],
            'database_path': str(self.database_config.base_path),
            'api_configs': {
                'dart_configured': bool(self.api_config.dart['api_key']),
                'naver_news_configured': bool(self.api_config.naver_news['client_id']),
                'kis_configured': bool(self.api_config.kis['app_key']),
                'kis_environment': self.api_config.kis['environment']
            },
            'analysis_weights': self.settings.analysis_config['weights'],
            'validation_errors': self.validation_errors
        }
    
    def print_config_status(self):
        """설정 상태 출력"""
        print("\n" + "="*60)
        print("🚀 Finance Data Vibe - 설정 상태")
        print("="*60)
        
        summary = self.get_config_summary()
        
        # 프로젝트 정보
        print(f"📁 프로젝트 경로: {summary['project_root']}")
        print(f"🔧 환경: {summary['environment']}")
        print(f"🐛 디버그 모드: {'✅' if summary['debug_mode'] else '❌'}")
        print(f"📝 로그 레벨: {summary['log_level']}")
        
        # API 설정 상태
        print(f"\n📊 API 설정:")
        api_configs = summary['api_configs']
        print(f"  - DART API: {'✅ 설정됨' if api_configs['dart_configured'] else '❌ 미설정'}")
        print(f"  - 네이버 뉴스: {'✅ 설정됨' if api_configs['naver_news_configured'] else '❌ 미설정'}")
        print(f"  - KIS API: {'✅ 설정됨' if api_configs['kis_configured'] else '❌ 미설정'}")
        print(f"  - KIS 환경: {api_configs['kis_environment']}")
        
        # 분석 비중
        print(f"\n📈 분석 비중:")
        weights = summary['analysis_weights']
        print(f"  - 기본분석: {weights['fundamental']:.1%}")
        print(f"  - 기술분석: {weights['technical']:.1%}")
        print(f"  - 감정분석: {weights['sentiment']:.1%}")
        
        # 데이터베이스 정보
        print(f"\n💾 데이터베이스:")
        print(f"  - 저장 경로: {summary['database_path']}")
        
        db_info = self.database_config.get_all_database_info()
        for db_name, info in db_info.items():
            status = "✅ 존재" if info['exists'] else "❌ 없음"
            print(f"  - {db_name}: {status} ({info['size']:,} bytes)")
        
        # 유효성 검사 결과
        if summary['validation_errors']:
            print(f"\n❌ 설정 오류 ({len(summary['validation_errors'])}개):")
            for error in summary['validation_errors']:
                print(f"  - {error}")
        else:
            print(f"\n✅ 모든 설정이 올바르게 구성되었습니다!")
        
        print("="*60)
    
    def initialize_project(self) -> bool:
        """프로젝트 초기화"""
        try:
            self.logger.info("프로젝트 초기화 시작")
            
            # 데이터베이스 초기화
            db_results = self.database_config.create_all_databases()
            
            success_count = sum(1 for result in db_results.values() if result)
            total_count = len(db_results)
            
            self.logger.info(f"데이터베이스 초기화 완료: {success_count}/{total_count}")
            
            # 설정 유효성 재검사
            self.validation_errors = self._validate_all_configs()
            
            if self.validation_errors:
                self.logger.warning(f"설정 유효성 검사 실패: {len(self.validation_errors)}개 오류")
                return False
            
            self.logger.info("프로젝트 초기화 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"프로젝트 초기화 실패: {e}")
            return False
    
    def get_dart_config(self) -> Dict[str, Any]:
        """DART API 설정 반환"""
        return self.api_config.dart
    
    def get_naver_news_config(self) -> Dict[str, Any]:
        """네이버 뉴스 API 설정 반환"""
        return self.api_config.naver_news
    
    def get_kis_config(self) -> Dict[str, Any]:
        """KIS API 설정 반환"""
        return self.api_config.kis
    
    def get_database_connection(self, db_name: str):
        """데이터베이스 연결 반환"""
        return self.database_config.get_connection(db_name)
    
    def get_logger(self, name: str):
        """로거 반환"""
        return self.logger_manager.get_logger(name)
    
    def get_analysis_config(self) -> Dict[str, Any]:
        """분석 설정 반환"""
        return self.settings.analysis_config
    
    def get_buffett_scorecard_config(self) -> Dict[str, int]:
        """워런 버핏 스코어카드 설정 반환"""
        return self.settings.analysis_config['buffett_scorecard']
    
    def get_screening_criteria(self) -> Dict[str, float]:
        """스크리닝 기준 반환"""
        return self.settings.analysis_config['screening_criteria']
    
    def get_technical_indicators_config(self) -> Dict[str, Any]:
        """기술적 지표 설정 반환"""
        return self.settings.analysis_config['technical_indicators']
    
    def get_notification_config(self) -> Dict[str, Any]:
        """알림 설정 반환"""
        return self.settings.notification_config
    
    def is_development_mode(self) -> bool:
        """개발 모드 여부 반환"""
        return self.settings.dev_config['development_mode']
    
    def is_debug_mode(self) -> bool:
        """디버그 모드 여부 반환"""
        return self.settings.dev_config['debug_mode']
    
    def get_streamlit_config(self) -> Dict[str, Any]:
        """Streamlit 설정 반환"""
        return self.settings.app_config['streamlit']
    
    def update_config(self, section: str, key: str, value: Any):
        """설정 업데이트"""
        try:
            if section == 'analysis':
                if key in self.settings.analysis_config:
                    self.settings.analysis_config[key] = value
                    self.logger.info(f"분석 설정 업데이트: {key} = {value}")
                else:
                    raise KeyError(f"분석 설정에 '{key}' 키가 없습니다.")
            
            elif section == 'app':
                if key in self.settings.app_config:
                    self.settings.app_config[key] = value
                    self.logger.info(f"앱 설정 업데이트: {key} = {value}")
                else:
                    raise KeyError(f"앱 설정에 '{key}' 키가 없습니다.")
            
            else:
                raise ValueError(f"지원하지 않는 설정 섹션: {section}")
                
        except Exception as e:
            self.logger.error(f"설정 업데이트 실패: {e}")
            raise
    
    def export_config(self, file_path: Optional[str] = None) -> str:
        """설정을 JSON 파일로 내보내기"""
        import json
        from datetime import datetime
        
        if file_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path = f"config_export_{timestamp}.json"
        
        config_data = {
            'timestamp': datetime.now().isoformat(),
            'project_root': str(self.project_root),
            'api_config': {
                'dart': {k: v for k, v in self.api_config.dart.items() if k != 'api_key'},
                'naver_news': {k: v for k, v in self.api_config.naver_news.items() if k not in ['client_id', 'client_secret']},
                'kis': {k: v for k, v in self.api_config.kis.items() if k not in ['app_key', 'app_secret', 'access_token']}
            },
            'database_config': {
                'base_path': str(self.database_config.base_path),
                'databases': {k: {'name': v['name'], 'description': v['description']} 
                            for k, v in self.database_config.databases.items()}
            },
            'analysis_config': self.settings.analysis_config,
            'app_config': self.settings.app_config,
            'validation_errors': self.validation_errors
        }
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"설정 내보내기 완료: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"설정 내보내기 실패: {e}")
            raise

# 글로벌 설정 관리자 인스턴스
config_manager = ConfigManager()

# 편의 함수들
def get_config_manager() -> ConfigManager:
    """설정 관리자 반환"""
    return config_manager

def get_dart_config() -> Dict[str, Any]:
    """DART API 설정 반환"""
    return config_manager.get_dart_config()

def get_naver_news_config() -> Dict[str, Any]:
    """네이버 뉴스 API 설정 반환"""
    return config_manager.get_naver_news_config()

def get_kis_config() -> Dict[str, Any]:
    """KIS API 설정 반환"""
    return config_manager.get_kis_config()

def get_database_connection(db_name: str):
    """데이터베이스 연결 반환"""
    return config_manager.get_database_connection(db_name)

def get_logger(name: str):
    """로거 반환"""
    return config_manager.get_logger(name)

def get_analysis_config() -> Dict[str, Any]:
    """분석 설정 반환"""
    return config_manager.get_analysis_config()

def initialize_project() -> bool:
    """프로젝트 초기화"""
    return config_manager.initialize_project()

def print_config_status():
    """설정 상태 출력"""
    config_manager.print_config_status()

def is_development_mode() -> bool:
    """개발 모드 여부 반환"""
    return config_manager.is_development_mode()

def is_debug_mode() -> bool:
    """디버그 모드 여부 반환"""
    return config_manager.is_debug_mode()

# 모듈 초기화 시 실행
if __name__ == "__main__":
    print("🔧 설정 관리자 테스트")
    print_config_status()
    
    # 프로젝트 초기화 테스트
    print("\n🚀 프로젝트 초기화 테스트...")
    success = initialize_project()
    
    if success:
        print("✅ 프로젝트 초기화 성공!")
    else:
        print("❌ 프로젝트 초기화 실패!")
        
    # 설정 내보내기 테스트
    print("\n📤 설정 내보내기 테스트...")
    try:
        export_file = config_manager.export_config()
        print(f"✅ 설정 내보내기 성공: {export_file}")
    except Exception as e:
        print(f"❌ 설정 내보내기 실패: {e}")
else:
    # 모듈 로드 시 자동 초기화
    if config_manager.validation_errors:
        print(f"⚠️  설정 검증 실패: {len(config_manager.validation_errors)}개 오류")
        for error in config_manager.validation_errors[:3]:  # 처음 3개만 출력
            print(f"  - {error}")
        if len(config_manager.validation_errors) > 3:
            print(f"  ... 및 {len(config_manager.validation_errors) - 3}개 추가 오류")
    else:
        print("✅ Finance Data Vibe 설정 로드 완료")