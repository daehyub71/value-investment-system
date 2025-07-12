#!/usr/bin/env python3
"""
DART API 연결 테스트 스크립트
기업코드 다운로드 문제 진단용
"""

import sys
import os
import requests
import zipfile
import io
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import ConfigManager

def test_dart_api():
    """DART API 연결 테스트"""
    print("🔍 DART API 연결 테스트 시작")
    print("=" * 50)
    
    # 설정 로드
    config_manager = ConfigManager()
    dart_config = config_manager.get_dart_config()
    api_key = dart_config.get('api_key')
    base_url = dart_config.get('base_url', "https://opendart.fss.or.kr/api")
    
    print(f"📋 API 키: {api_key[:10]}...{api_key[-5:] if len(api_key) > 15 else api_key}")
    print(f"🌐 Base URL: {base_url}")
    
    # 기업코드 다운로드 테스트
    url = f"{base_url}/corpCode.xml"
    params = {'crtfc_key': api_key}
    
    print(f"\n📡 요청 URL: {url}")
    print(f"📋 파라미터: {params}")
    
    try:
        print("\n⏳ 기업코드 파일 다운로드 중...")
        response = requests.get(url, params=params, timeout=30)
        
        print(f"📊 응답 상태: {response.status_code}")
        print(f"📦 응답 크기: {len(response.content)} bytes")
        print(f"📄 Content-Type: {response.headers.get('content-type', 'unknown')}")
        
        # 응답 내용 일부 출력
        print(f"\n🔍 응답 시작 부분 (50바이트):")
        print(response.content[:50])
        
        # JSON 에러 응답 확인
        if response.headers.get('content-type', '').startswith('application/json'):
            try:
                error_data = response.json()
                print(f"\n❌ JSON 에러 응답:")
                print(f"  - status: {error_data.get('status')}")
                print(f"  - message: {error_data.get('message')}")
                return False
            except:
                pass
        
        # ZIP 파일 확인
        if response.content.startswith(b'PK'):
            print("\n✅ ZIP 파일 형식 확인됨")
            
            try:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    file_list = zip_file.namelist()
                    print(f"📂 ZIP 파일 내용: {file_list}")
                    
                    if 'CORPCODE.xml' in file_list:
                        xml_content = zip_file.read('CORPCODE.xml')
                        print(f"📄 XML 파일 크기: {len(xml_content)} bytes")
                        print(f"🔍 XML 시작 부분: {xml_content[:100]}")
                        print("\n✅ 기업코드 다운로드 성공!")
                        return True
                    else:
                        print("\n❌ CORPCODE.xml 파일이 ZIP에 없습니다.")
                        return False
                        
            except zipfile.BadZipFile as e:
                print(f"\n❌ ZIP 파일 해제 실패: {e}")
                return False
        else:
            print("\n❌ ZIP 파일이 아닙니다.")
            print(f"응답 내용 (처음 200바이트):")
            try:
                print(response.content[:200].decode('utf-8', errors='ignore'))
            except:
                print(response.content[:200])
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"\n❌ 요청 실패: {e}")
        return False
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        return False

if __name__ == "__main__":
    success = test_dart_api()
    
    if success:
        print("\n🎉 테스트 완료: DART API 연결 성공!")
        print("이제 collect_dart_data.py를 실행할 수 있습니다.")
    else:
        print("\n💥 테스트 실패: DART API 연결 문제")
        print("\n🔧 확인사항:")
        print("1. DART_API_KEY가 올바르게 설정되었는지 확인")
        print("2. API 키가 유효한지 DART 홈페이지에서 확인")
        print("3. 네트워크 연결 상태 확인")
        print("4. DART API 서비스 상태 확인")
