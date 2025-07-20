#!/usr/bin/env python3
"""
DART API 문제 진단 및 해결 스크립트
"""

import requests
import os
from dotenv import load_dotenv

load_dotenv()

def test_dart_api_detailed():
    """DART API 상세 테스트"""
    print("🔍 DART API 상세 진단 시작")
    print("=" * 50)
    
    api_key = os.getenv('DART_API_KEY', '').strip('"')
    print(f"📊 API 키: {api_key[:10]}...{api_key[-10:]}")
    
    # 1. 기본 API 키 유효성 테스트
    print("\n1️⃣ API 키 유효성 테스트")
    test_url = "https://opendart.fss.or.kr/api/list.json"
    
    # 수정된 파라미터 - 삼성전자로 직접 테스트
    params = {
        'crtfc_key': api_key,
        'corp_cls': 'Y',      # 유가증권
        'bgn_de': '20240101', # 시작일
        'end_de': '20241231', # 종료일
        'page_no': 1,
        'page_count': 10
    }
    
    try:
        response = requests.get(test_url, params=params, timeout=10)
        print(f"응답 코드: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"응답 상태: {data.get('status', 'Unknown')}")
            print(f"응답 메시지: {data.get('message', 'No message')}")
            
            if data.get('status') == '000':
                print("✅ API 키 유효성 확인됨")
                if 'list' in data and data['list']:
                    print(f"✅ 공시 데이터 {len(data['list'])}건 조회 성공")
                    return True
                else:
                    print("⚠️ API는 정상이지만 해당 기간 공시 데이터 없음")
            elif data.get('status') == '013':
                print("❌ API 키가 유효하지 않습니다")
            elif data.get('status') == '020':
                print("⚠️ 조회된 데이타가 없습니다 (기간 조정 필요)")
            else:
                print(f"❌ API 오류: {data.get('message', 'Unknown error')}")
        else:
            print(f"❌ HTTP 오류: {response.status_code}")
            
    except Exception as e:
        print(f"❌ 요청 실패: {e}")
        return False
    
    # 2. 기업코드 다운로드 테스트
    print("\n2️⃣ 기업코드 다운로드 테스트")
    corp_url = "https://opendart.fss.or.kr/api/corpCode.xml"
    corp_params = {'crtfc_key': api_key}
    
    try:
        response = requests.get(corp_url, params=corp_params, timeout=30)
        print(f"응답 코드: {response.status_code}")
        print(f"Content-Type: {response.headers.get('content-type', 'unknown')}")
        print(f"응답 크기: {len(response.content)} bytes")
        
        if response.content.startswith(b'PK'):
            print("✅ ZIP 파일 다운로드 성공")
            return True
        else:
            print("❌ ZIP 파일이 아닙니다")
            print(f"응답 시작 부분: {response.content[:100]}")
            
    except Exception as e:
        print(f"❌ 기업코드 다운로드 실패: {e}")
    
    # 3. 삼성전자 재무데이터 직접 테스트
    print("\n3️⃣ 삼성전자 재무데이터 테스트")
    
    # 삼성전자 corp_code: 00126380
    financial_url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    financial_params = {
        'crtfc_key': api_key,
        'corp_code': '00126380',  # 삼성전자
        'bsns_year': '2023',      # 2023년
        'reprt_code': '11011'     # 사업보고서
    }
    
    try:
        response = requests.get(financial_url, params=financial_params, timeout=30)
        data = response.json()
        
        print(f"응답 상태: {data.get('status', 'Unknown')}")
        print(f"응답 메시지: {data.get('message', 'No message')}")
        
        if data.get('status') == '000':
            print("✅ 삼성전자 재무데이터 조회 성공")
            if 'list' in data:
                print(f"✅ 재무항목 {len(data['list'])}개 조회됨")
                
                # 주요 계정과목 확인
                accounts = [item['account_nm'] for item in data['list'][:10]]
                print("주요 계정과목:", accounts[:5])
                return True
        else:
            print(f"❌ 재무데이터 조회 실패: {data.get('message')}")
            
    except Exception as e:
        print(f"❌ 재무데이터 테스트 실패: {e}")
    
    return False

def suggest_fixes():
    """해결 방안 제시"""
    print("\n🔧 해결 방안:")
    print("1. API 파라미터 수정")
    print("2. 날짜 범위 조정")
    print("3. 다른 기업 코드로 테스트")
    print("4. 재무데이터 수집 방식 변경")

if __name__ == "__main__":
    success = test_dart_api_detailed()
    
    if not success:
        suggest_fixes()
        
        print("\n💡 임시 해결책:")
        print("기존 데이터베이스의 데이터를 사용하여 스코어카드 테스트:")
        print("python buffett_scorecard_calculator_fixed.py")
