#!/usr/bin/env python3
"""
KIS API 연결 테스트 스크립트
API 키 설정 및 인증 테스트
"""

import os
import requests
import json
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

def test_kis_api_settings():
    """KIS API 설정 확인"""
    print("🔍 KIS API 설정 확인")
    print("=" * 50)
    
    app_key = os.getenv('KIS_APP_KEY', '').strip('"')
    app_secret = os.getenv('KIS_APP_SECRET', '').strip('"')
    environment = os.getenv('KIS_ENVIRONMENT', 'VIRTUAL')
    cano = os.getenv('KIS_CANO', '').strip('"')
    
    print(f"APP_KEY: {app_key[:10]}...{app_key[-10:] if len(app_key) > 20 else app_key}")
    print(f"APP_SECRET: {'설정됨' if app_secret else '미설정'} ({len(app_secret)} 글자)")
    print(f"Environment: {environment}")
    print(f"계좌번호: {cano}")
    
    # 기본 검증
    errors = []
    
    if not app_key:
        errors.append("KIS_APP_KEY가 설정되지 않았습니다.")
    elif len(app_key) < 20:
        errors.append("KIS_APP_KEY가 너무 짧습니다.")
    
    if not app_secret:
        errors.append("KIS_APP_SECRET이 설정되지 않았습니다.")
    elif len(app_secret) < 50:
        errors.append("KIS_APP_SECRET이 너무 짧습니다.")
    
    if environment not in ['REAL', 'VIRTUAL']:
        errors.append("KIS_ENVIRONMENT는 'REAL' 또는 'VIRTUAL'이어야 합니다.")
    
    if not cano:
        errors.append("KIS_CANO (계좌번호)가 설정되지 않았습니다.")
    
    if errors:
        print("\n❌ 설정 오류:")
        for error in errors:
            print(f"   - {error}")
        return False
    else:
        print("\n✅ 기본 설정 확인 완료")
        return True

def get_kis_access_token_simple():
    """간단한 KIS API 인증 테스트"""
    print("\n🔐 KIS API 인증 테스트")
    print("=" * 50)
    
    app_key = os.getenv('KIS_APP_KEY', '').strip('"')
    app_secret = os.getenv('KIS_APP_SECRET', '').strip('"')
    environment = os.getenv('KIS_ENVIRONMENT', 'VIRTUAL')
    
    # URL 설정
    if environment == 'REAL':
        base_url = 'https://openapi.koreainvestment.com:9443'
    else:
        base_url = 'https://openapivts.koreainvestment.com:29443'
    
    # 인증 요청
    url = f"{base_url}/oauth2/tokenP"
    
    headers = {
        'content-type': 'application/json; charset=utf-8'
    }
    
    data = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret": app_secret
    }
    
    print(f"요청 URL: {url}")
    print(f"APP_KEY: {app_key[:10]}...")
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        print(f"\nHTTP 상태 코드: {response.status_code}")
        print(f"응답 헤더: {dict(response.headers)}")
        
        if response.status_code == 200:
            result = response.json()
            print("\n✅ 인증 성공!")
            print(f"액세스 토큰: {result.get('access_token', 'N/A')[:20]}...")
            print(f"토큰 타입: {result.get('token_type', 'N/A')}")
            print(f"만료 시간: {result.get('expires_in', 'N/A')}초")
            return result.get('access_token')
        else:
            print(f"\n❌ 인증 실패: {response.status_code}")
            try:
                error_data = response.json()
                print(f"오류 메시지: {error_data}")
            except:
                print(f"응답 내용: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"\n❌ 네트워크 오류: {e}")
        return None
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        return None

def test_simple_api_call(access_token):
    """간단한 API 호출 테스트"""
    if not access_token:
        print("\n⏭️  액세스 토큰이 없어 API 호출 테스트를 건너뜁니다.")
        return
    
    print("\n📊 간단한 API 호출 테스트")
    print("=" * 50)
    
    app_key = os.getenv('KIS_APP_KEY', '').strip('"')
    app_secret = os.getenv('KIS_APP_SECRET', '').strip('"')
    environment = os.getenv('KIS_ENVIRONMENT', 'VIRTUAL')
    
    # URL 설정
    if environment == 'REAL':
        base_url = 'https://openapi.koreainvestment.com:9443'
    else:
        base_url = 'https://openapivts.koreainvestment.com:29443'
    
    # 삼성전자 주가 조회 테스트
    url = f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
    
    headers = {
        'content-type': 'application/json; charset=utf-8',
        'authorization': f'Bearer {access_token}',
        'appkey': app_key,
        'appsecret': app_secret,
        'tr_id': 'FHKST01010100',
        'custtype': 'P'
    }
    
    params = {
        'FID_COND_MRKT_DIV_CODE': 'J',
        'FID_INPUT_ISCD': '005930'  # 삼성전자
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        print(f"HTTP 상태 코드: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('rt_cd') == '0':
                output = result.get('output', {})
                current_price = output.get('stck_prpr', 'N/A')
                company_name = output.get('hts_kor_isnm', 'N/A')
                
                print("✅ API 호출 성공!")
                print(f"종목명: {company_name}")
                print(f"현재가: {current_price}원")
                print(f"전일대비: {output.get('prdy_vrss', 'N/A')}원")
                print(f"등락률: {output.get('prdy_ctrt', 'N/A')}%")
            else:
                print(f"❌ API 응답 오류: {result.get('msg1', 'Unknown error')}")
        else:
            print(f"❌ HTTP 오류: {response.status_code}")
            try:
                error_data = response.json()
                print(f"오류 내용: {error_data}")
            except:
                print(f"응답 내용: {response.text[:200]}...")
                
    except Exception as e:
        print(f"❌ API 호출 오류: {e}")

def main():
    """메인 테스트 함수"""
    print("🚀 KIS API 연결 테스트 시작")
    print("=" * 60)
    
    # 1단계: 설정 확인
    if not test_kis_api_settings():
        print("\n💡 해결 방법:")
        print("1. .env 파일에서 KIS API 설정을 확인하세요")
        print("2. 한국투자증권 OpenAPI 사이트에서 발급받은 정확한 키를 입력하세요")
        print("3. 모의투자 계좌가 개설되어 있는지 확인하세요")
        return
    
    # 2단계: 인증 테스트
    access_token = get_kis_access_token_simple()
    
    # 3단계: API 호출 테스트
    test_simple_api_call(access_token)
    
    print("\n" + "=" * 60)
    print("테스트 완료!")
    
    if access_token:
        print("\n✅ KIS API 연결 성공! 이제 실제 데이터 수집을 시작할 수 있습니다.")
        print("\n다음 명령어로 데이터 수집을 시작하세요:")
        print("python scripts/data_collection/collect_kis_data.py --market_indicators")
    else:
        print("\n❌ KIS API 연결 실패. 다음을 확인해주세요:")
        print("1. 한국투자증권 OpenAPI 사이트에서 발급받은 정확한 API 키인지 확인")
        print("2. 모의투자 환경에 계좌가 개설되어 있는지 확인")
        print("3. API 사용 신청이 승인되었는지 확인")
        print("4. .env 파일의 키 값에 불필요한 따옴표나 공백이 없는지 확인")

if __name__ == "__main__":
    main()
