#!/usr/bin/env python3
"""
KIS API 문제 진단 및 해결 스크립트
500 서버 오류 원인 분석 및 대안 제시
"""

import os
import sys
import requests
import json
from datetime import datetime, timedelta
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.insert(0, str(Path(__file__).parent))

def check_market_open_status():
    """한국 주식시장 개장 여부 확인"""
    now = datetime.now()
    
    # 주말 확인
    if now.weekday() >= 5:  # 토요일(5), 일요일(6)
        return False, "주말 (휴장일)"
    
    # 시간 확인 (9:00 - 15:30)
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    if market_open <= now <= market_close:
        return True, "정규 장중"
    elif now < market_open:
        return False, "장 시작 전"
    else:
        return False, "장 마감 후"

def test_kis_api_basic():
    """KIS API 기본 연결 테스트"""
    print("🔐 KIS API 기본 연결 테스트")
    print("-" * 40)
    
    from dotenv import load_dotenv
    load_dotenv()
    
    app_key = os.getenv('KIS_APP_KEY', '').strip('"')
    app_secret = os.getenv('KIS_APP_SECRET', '').strip('"')
    environment = os.getenv('KIS_ENVIRONMENT', 'VIRTUAL')
    
    if not app_key or not app_secret:
        print("❌ KIS API 키가 설정되지 않았습니다.")
        return False
    
    # 환경에 따른 URL 설정
    if environment == 'REAL':
        base_url = 'https://openapi.koreainvestment.com:9443'
    else:
        base_url = 'https://openapivts.koreainvestment.com:29443'
    
    # 토큰 발급 테스트
    try:
        url = f"{base_url}/oauth2/tokenP"
        headers = {'content-type': 'application/json; charset=utf-8'}
        data = {
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret
        }
        
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('access_token'):
                print("✅ KIS API 인증 성공")
                return True, result['access_token']
            else:
                print(f"❌ 토큰 발급 실패: {result}")
                return False, None
        else:
            print(f"❌ HTTP 오류: {response.status_code}")
            print(f"응답: {response.text}")
            return False, None
            
    except Exception as e:
        print(f"❌ 연결 오류: {e}")
        return False, None

def test_alternative_kis_api(access_token):
    """대안 KIS API 엔드포인트 테스트"""
    if not access_token:
        return False
        
    print("\n📊 대안 KIS API 테스트")
    print("-" * 40)
    
    from dotenv import load_dotenv
    load_dotenv()
    
    app_key = os.getenv('KIS_APP_KEY', '').strip('"')
    app_secret = os.getenv('KIS_APP_SECRET', '').strip('"')
    environment = os.getenv('KIS_ENVIRONMENT', 'VIRTUAL')
    
    if environment == 'REAL':
        base_url = 'https://openapi.koreainvestment.com:9443'
    else:
        base_url = 'https://openapivts.koreainvestment.com:29443'
    
    # 삼성전자 개별 종목 조회 테스트
    try:
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
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('rt_cd') == '0':
                output = result.get('output', {})
                print("✅ 개별 종목 조회 성공")
                print(f"종목: {output.get('hts_kor_isnm', 'N/A')}")
                print(f"현재가: {output.get('stck_prpr', 'N/A')}원")
                return True
            else:
                print(f"⚠️ API 응답 경고: {result.get('msg1', 'Unknown')}")
                return False
        else:
            print(f"❌ 개별 종목 조회 실패: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ 개별 종목 조회 오류: {e}")
        return False

def test_financedatareader():
    """FinanceDataReader 대안 테스트"""
    print("\n📈 FinanceDataReader 대안 테스트")
    print("-" * 40)
    
    try:
        import FinanceDataReader as fdr
        
        # KOSPI 지수 조회
        kospi = fdr.DataReader('KS11', '2025-07-10', '2025-07-13')
        if len(kospi) > 0:
            latest = kospi.iloc[-1]
            print("✅ FinanceDataReader 작동 확인")
            print(f"KOSPI 최신가: {latest['Close']:.2f}")
            print(f"일자: {kospi.index[-1].strftime('%Y-%m-%d')}")
            return True
        else:
            print("⚠️ FinanceDataReader 데이터 없음")
            return False
            
    except ImportError:
        print("❌ FinanceDataReader 라이브러리 없음")
        print("설치: pip install finance-datareader")
        return False
    except Exception as e:
        print(f"❌ FinanceDataReader 오류: {e}")
        return False

def suggest_alternatives():
    """대안 방법 제시"""
    print("\n💡 권장 해결 방안")
    print("=" * 50)
    
    # 시장 상태 확인
    is_open, status = check_market_open_status()
    print(f"📅 현재 시장 상태: {status}")
    
    if not is_open:
        print("\n🔔 휴장일 대응 방안:")
        print("1. 평일 9:00-15:30에 다시 시도")
        print("2. FinanceDataReader로 과거 데이터 수집")
        print("3. DART API로 재무제표 데이터 수집")
    
    print("\n🚀 즉시 실행 가능한 대안들:")
    print("1. FinanceDataReader 사용:")
    print("   python scripts/data_collection/collect_stock_data.py")
    
    print("\n2. DART 재무데이터 수집:")
    print("   python scripts/data_collection/collect_dart_data.py --year=2024 --quarter=2")
    
    print("\n3. 뉴스 데이터 수집:")
    print("   python scripts/data_collection/collect_news_data.py --stock_code=005930 --days=7")
    
    print("\n4. KIS API 개별 종목 시도:")
    print("   python scripts/data_collection/collect_kis_data.py --stock_code=005930 --realtime_quotes")
    
    print("\n5. 통합 데이터 수집 (KIS 제외):")
    print("   python scripts/data_collection/collect_all_data.py")

def main():
    """메인 진단 함수"""
    print("🔍 KIS API 문제 진단 및 해결 도구")
    print("=" * 60)
    
    # 1. 시장 상태 확인
    is_open, status = check_market_open_status()
    print(f"📅 한국 주식시장 상태: {status}")
    
    if not is_open:
        print("⚠️ 현재 휴장 시간입니다. 이것이 500 오류의 주요 원인일 수 있습니다.")
    
    # 2. KIS API 기본 연결 테스트
    success, access_token = test_kis_api_basic()
    
    # 3. 대안 API 테스트
    if success:
        test_alternative_kis_api(access_token)
    
    # 4. FinanceDataReader 테스트
    test_financedatareader()
    
    # 5. 대안 방법 제시
    suggest_alternatives()
    
    print("\n" + "=" * 60)
    print("진단 완료!")

if __name__ == "__main__":
    main()
