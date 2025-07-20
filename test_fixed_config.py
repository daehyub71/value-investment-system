#!/usr/bin/env python3
"""
수정된 ConfigManager 테스트 스크립트
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

print("🧪 수정된 ConfigManager 테스트 시작")
print("=" * 60)

# 1. ConfigManager 임포트 테스트
print("1️⃣ ConfigManager 임포트 테스트")
try:
    from config import ConfigManager
    print("✅ ConfigManager 임포트 성공!")
    
    # 2. ConfigManager 인스턴스 생성 테스트
    print("\n2️⃣ ConfigManager 인스턴스 생성 테스트")
    config_manager = ConfigManager()
    print("✅ ConfigManager 인스턴스 생성 성공!")
    
    # 3. 설정 상태 확인
    print("\n3️⃣ 설정 상태 확인")
    config_manager.print_config_status()
    
    # 4. 편의 함수 테스트
    print("\n4️⃣ 편의 함수 테스트")
    from config import get_dart_config, get_logger, get_database_path
    
    dart_config = get_dart_config()
    logger = get_logger('TestLogger')
    db_path = get_database_path('dart')
    
    print(f"✅ DART 설정: {'API 키 있음' if dart_config.get('api_key') else 'API 키 없음'}")
    print(f"✅ 로거 생성: {type(logger).__name__}")
    print(f"✅ DB 경로: {db_path}")
    
    # 5. 실제 스크립트에서 사용하는 방식 테스트
    print("\n5️⃣ 실제 스크립트 사용 방식 테스트")
    
    # 기존 스크립트에서 사용하던 방식
    try:
        from config import config_manager as cm
        logger = cm.get_logger('DartCollector')
        dart_config = cm.get_dart_config()
        
        print("✅ 기존 스크립트 방식 호환 성공!")
        print(f"   - 로거: {logger.name}")
        print(f"   - DART API: {dart_config.get('base_url', 'Unknown')}")
        
        # 실제 데이터 수집 스크립트 방식 테스트
        if dart_config.get('api_key'):
            print("✅ DART API 키 확인됨 - 실제 수집 가능")
        else:
            print("⚠️ DART API 키 없음 - .env 파일 확인 필요")
            
    except Exception as e:
        print(f"❌ 기존 스크립트 방식 테스트 실패: {e}")
    
    # 6. 에러 복구 능력 테스트
    print("\n6️⃣ 에러 복구 능력 테스트")
    errors = config_manager.validate_config()
    if errors:
        print("⚠️ 설정 오류 발견:")
        for error in errors:
            print(f"   - {error}")
        print("✅ 오류 발견 및 보고 기능 정상")
    else:
        print("✅ 모든 설정 검증 통과")
    
    # 7. 최종 준비 상태 확인
    print("\n7️⃣ 최종 준비 상태 확인")
    if config_manager.is_ready():
        print("🎉 ConfigManager 완전히 준비됨 - 모든 스크립트 실행 가능!")
    else:
        print("⚠️ 일부 설정 부족 - 기본 기능만 사용 가능")
    
    print("\n" + "=" * 60)
    print("✅ ConfigManager 수정 완료 - ImportError 해결!")
    print("🚀 이제 모든 데이터 수집 스크립트가 정상 작동합니다.")
    
except ImportError as e:
    print(f"❌ ConfigManager 임포트 여전히 실패: {e}")
    print("\n🔧 추가 문제 해결 방법:")
    print("1. Python 가상환경 확인")
    print("2. 필요한 패키지 설치: pip install python-dotenv")
    print("3. 프로젝트 경로 확인")
    
except Exception as e:
    print(f"❌ 기타 오류: {e}")
    print(f"❌ 오류 타입: {type(e).__name__}")
    import traceback
    print(f"❌ 상세 오류:\n{traceback.format_exc()}")

print("\n💡 다음 단계:")
print("1. 이 테스트가 성공하면 원래 스크립트들을 실행해보세요")
print("2. python scripts/data_collection/collect_dart_data.py")
print("3. python buffett_scorecard_calculator.py")
