#!/usr/bin/env python3
"""
ConfigManager ImportError 진단 스크립트
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

print("🔍 ConfigManager ImportError 진단 시작")
print("=" * 60)

# 1. 기본 환경 확인
print("📁 프로젝트 경로:", project_root)
print("🐍 Python 버전:", sys.version)
print("📂 Python 경로:", sys.path[:3])

# 2. 개별 모듈 임포트 테스트
modules_to_test = [
    'config.settings',
    'config.api_config', 
    'config.database_config',
    'config.logging_config'
]

print("\n🧪 개별 모듈 임포트 테스트:")
print("-" * 40)

import_results = {}

for module_name in modules_to_test:
    try:
        exec(f"import {module_name}")
        print(f"✅ {module_name}: 성공")
        import_results[module_name] = "성공"
    except ImportError as e:
        print(f"❌ {module_name}: ImportError - {e}")
        import_results[module_name] = f"ImportError: {e}"
    except Exception as e:
        print(f"⚠️  {module_name}: 기타 오류 - {e}")
        import_results[module_name] = f"기타 오류: {e}"

# 3. ConfigManager 임포트 테스트
print("\n🎯 ConfigManager 임포트 테스트:")
print("-" * 40)

try:
    from config import ConfigManager
    print("✅ ConfigManager 임포트 성공!")
    
    # ConfigManager 인스턴스 생성 테스트
    try:
        config_manager = ConfigManager()
        print("✅ ConfigManager 인스턴스 생성 성공!")
        config_manager.print_config_status()
    except Exception as e:
        print(f"❌ ConfigManager 인스턴스 생성 실패: {e}")
        
except ImportError as e:
    print(f"❌ ConfigManager 임포트 실패: {e}")
    
    # 최소한의 대안 ConfigManager 구현
    print("\n🛠️  최소한의 대안 ConfigManager 구현:")
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        class MinimalConfigManager:
            def __init__(self):
                self.dart_api_key = os.getenv('DART_API_KEY', '')
                self.naver_client_id = os.getenv('NAVER_CLIENT_ID', '')
                self.naver_client_secret = os.getenv('NAVER_CLIENT_SECRET', '')
                self.db_path = os.getenv('DB_PATH', 'data/databases/')
                
            def get_dart_config(self):
                return {
                    'api_key': self.dart_api_key,
                    'base_url': 'https://opendart.fss.or.kr/api'
                }
            
            def get_logger(self, name):
                import logging
                logging.basicConfig(level=logging.INFO)
                return logging.getLogger(name)
        
        # 테스트
        minimal_config = MinimalConfigManager()
        print("✅ 최소한의 ConfigManager 생성 성공!")
        print(f"📊 DART API Key: {minimal_config.dart_api_key[:10]}..." if minimal_config.dart_api_key else "❌ DART API Key 없음")
        
    except Exception as e:
        print(f"❌ 최소 ConfigManager도 실패: {e}")

except Exception as e:
    print(f"❌ 기타 오류: {e}")

# 4. 결과 요약
print("\n📊 진단 결과 요약:")
print("=" * 60)

success_count = sum(1 for result in import_results.values() if result == "성공")
total_count = len(import_results)

print(f"모듈 임포트 성공률: {success_count}/{total_count}")

if success_count < total_count:
    print("\n🔧 권장 해결 방법:")
    print("1. 실패한 모듈 내부 오류 수정")
    print("2. 누락된 의존성 패키지 설치")
    print("3. 최소한의 ConfigManager 사용")
    print("4. 개별 모듈 직접 임포트")

print("\n🎯 다음 단계:")
print("- 실패한 모듈 개별 수정")
print("- ConfigManager 간소화")
print("- 에러 처리 강화")
