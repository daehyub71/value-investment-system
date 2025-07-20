#!/usr/bin/env python3
"""
DART API 키 로드 문제 해결 및 2025년 1분기 데이터 수집
"""

import requests
import sqlite3
import pandas as pd
import os
from datetime import datetime
from pathlib import Path

# .env 파일 직접 로드 
from dotenv import load_dotenv

def load_env_safely():
    """환경변수 안전하게 로드"""
    
    print("🔧 환경변수 로드 중...")
    
    # 현재 디렉토리에서 .env 파일 찾기
    env_path = Path('.env')
    
    if env_path.exists():
        print(f"✅ .env 파일 발견: {env_path.absolute()}")
        load_dotenv(env_path)
        
        # API 키 확인
        api_key = os.environ.get('DART_API_KEY')
        if api_key:
            print(f"✅ DART API 키 로드 성공: {api_key[:10]}***")
            return api_key
        else:
            print("❌ DART_API_KEY 환경변수가 없습니다.")
    else:
        print(f"❌ .env 파일이 없습니다: {env_path.absolute()}")
    
    # .env 파일에서 직접 읽기 (백업 방법)
    try:
        print("🔄 .env 파일 직접 파싱 시도...")
        with open('.env', 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('DART_API_KEY'):
                    api_key = line.split('=')[1].strip().strip('"')
                    print(f"✅ 직접 파싱으로 API 키 확보: {api_key[:10]}***")
                    return api_key
    except Exception as e:
        print(f"❌ .env 파일 직접 읽기 실패: {e}")
    
    return None

def test_dart_api_connection(api_key):
    """DART API 연결 테스트"""
    
    print("\n🔍 DART API 연결 테스트")
    print("-" * 30)
    
    if not api_key:
        print("❌ API 키가 없습니다.")
        return False
    
    try:
        # 삼성전자 기업정보 요청 테스트
        test_url = "https://opendart.fss.or.kr/api/company.json"
        params = {
            'crtfc_key': api_key,
            'corp_code': '00126380'  # 삼성전자
        }
        
        print(f"📡 API 요청: {test_url}")
        print(f"📝 파라미터: corp_code=00126380, crtfc_key={api_key[:10]}***")
        
        response = requests.get(test_url, params=params, timeout=10)
        
        print(f"📊 응답 상태: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"📋 응답 데이터: {data}")
            
            if data.get('status') == '000':
                print("✅ DART API 연결 성공!")
                print(f"   회사명: {data.get('corp_name', 'N/A')}")
                print(f"   대표자: {data.get('ceo_nm', 'N/A')}")
                return True
            else:
                print(f"❌ API 응답 오류: {data.get('message', 'Unknown error')}")
                print(f"   상태코드: {data.get('status', 'N/A')}")
                return False
        else:
            print(f"❌ HTTP 오류: {response.status_code}")
            print(f"   응답 내용: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ 연결 오류: {e}")
        return False

def get_existing_corp_codes():
    """기존 기업코드 데이터 확인"""
    
    print("\n📊 기존 기업코드 데이터 확인")
    print("-" * 30)
    
    try:
        db_path = Path('data/databases/dart_data.db')
        if not db_path.exists():
            print(f"❌ 데이터베이스 파일이 없습니다: {db_path}")
            return pd.DataFrame()
        
        conn = sqlite3.connect(db_path)
        
        # 주요 기업 기업코드 확인
        query = """
        SELECT corp_code, corp_name, stock_code 
        FROM corp_codes 
        WHERE corp_name IN (
            '삼성전자', 'SK하이닉스', 'NAVER', 'LG에너지솔루션', 
            'LG화학', '카카오', 'POSCO홀딩스', '현대자동차', 'KT&G'
        )
        ORDER BY corp_name
        """
        
        result = pd.read_sql(query, conn)
        conn.close()
        
        if not result.empty:
            print("✅ 기존 기업코드 데이터:")
            for _, row in result.iterrows():
                print(f"   🏢 {row['corp_name']} ({row['stock_code']}): {row['corp_code']}")
            return result
        else:
            print("❌ 주요 기업의 기업코드가 없습니다.")
            
            # 전체 기업코드 수 확인
            conn = sqlite3.connect(db_path)
            total_count = pd.read_sql("SELECT COUNT(*) as count FROM corp_codes", conn).iloc[0]['count']
            conn.close()
            
            print(f"📊 전체 기업코드 수: {total_count:,}개")
            
            if total_count > 0:
                print("🔄 임의 기업으로 테스트 진행...")
                conn = sqlite3.connect(db_path)
                sample_result = pd.read_sql("SELECT corp_code, corp_name, stock_code FROM corp_codes LIMIT 5", conn)
                conn.close()
                return sample_result
            
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ 기업코드 조회 오류: {e}")
        return pd.DataFrame()

def collect_sample_financial_data(api_key, corp_codes_df):
    """샘플 재무데이터 수집 테스트"""
    
    if corp_codes_df.empty:
        print("❌ 수집할 기업이 없습니다.")
        return False
    
    print(f"\n🚀 샘플 재무데이터 수집 테스트")
    print("-" * 40)
    
    # 첫 번째 기업으로 테스트
    test_company = corp_codes_df.iloc[0]
    corp_code = test_company['corp_code']
    corp_name = test_company['corp_name']
    
    print(f"🎯 테스트 대상: {corp_name} ({corp_code})")
    
    try:
        # 2025년 1분기 재무제표 요청
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        params = {
            'crtfc_key': api_key,
            'corp_code': corp_code,
            'bsns_year': '2025',
            'reprt_code': '11013',  # 1분기
            'fs_div': 'CFS'  # 연결재무제표
        }
        
        print(f"📡 재무제표 API 요청...")
        print(f"   연도: 2025, 분기: 1분기, 기업: {corp_name}")
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f"📊 API 응답 상태: {data.get('status', 'N/A')}")
            print(f"📋 응답 메시지: {data.get('message', 'N/A')}")
            
            if data.get('status') == '000' and 'list' in data:
                records = data['list']
                print(f"✅ {corp_name} 재무데이터 수집 성공: {len(records)}건")
                
                # 주요 계정과목 확인
                key_accounts = ['매출액', '영업이익', '당기순이익', '총자산', '자본총계']
                found_accounts = []
                
                for record in records[:10]:  # 처음 10개만 확인
                    account_nm = record.get('account_nm', '')
                    amount = record.get('thstrm_amount', '')
                    if any(key in account_nm for key in key_accounts):
                        found_accounts.append(f"{account_nm}: {amount}")
                
                if found_accounts:
                    print("🏆 주요 재무지표 발견:")
                    for account in found_accounts:
                        print(f"   💰 {account}")
                    return True
                else:
                    print("⚠️ 주요 재무지표를 찾을 수 없습니다.")
                    return False
                    
            elif data.get('status') == '013':
                print(f"⚠️ {corp_name}: 2025년 1분기 보고서가 아직 제출되지 않았습니다.")
                
                # 2024년 데이터로 테스트
                print("🔄 2024년 데이터로 테스트...")
                params['bsns_year'] = '2024'
                params['reprt_code'] = '11011'  # 사업보고서
                
                response2 = requests.get(url, params=params, timeout=30)
                if response2.status_code == 200:
                    data2 = response2.json()
                    if data2.get('status') == '000' and 'list' in data2:
                        print(f"✅ {corp_name} 2024년 데이터 확인: {len(data2['list'])}건")
                        return True
                
                return False
            else:
                print(f"❌ {corp_name}: API 오류 - {data.get('message', 'Unknown')}")
                return False
        else:
            print(f"❌ HTTP 오류: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ 데이터 수집 오류: {e}")
        return False

def main():
    """메인 실행"""
    
    print("🚀 DART API 연결 및 데이터 수집 진단")
    print("=" * 60)
    
    # 1. 환경변수 로드
    api_key = load_env_safely()
    if not api_key:
        print("\n❌ API 키를 가져올 수 없습니다.")
        print("💡 .env 파일의 DART_API_KEY를 확인하세요.")
        return
    
    # 2. API 연결 테스트
    if not test_dart_api_connection(api_key):
        print("\n❌ DART API 연결 실패")
        print("💡 인터넷 연결 또는 API 키를 확인하세요.")
        return
    
    # 3. 기존 기업코드 확인
    corp_codes = get_existing_corp_codes()
    if corp_codes.empty:
        print("\n❌ 기업코드 데이터가 없습니다.")
        print("💡 먼저 기업코드를 수집해야 합니다.")
        return
    
    # 4. 샘플 재무데이터 수집 테스트
    success = collect_sample_financial_data(api_key, corp_codes)
    
    if success:
        print("\n✅ DART API 연결 및 데이터 수집 가능 확인!")
        print("🚀 이제 전체 데이터 수집을 진행할 수 있습니다.")
        print("\n다음 단계:")
        print("   python scripts/data_collection/collect_dart_data_final.py --year=2025")
    else:
        print("\n⚠️ 2025년 1분기 데이터가 아직 없을 수 있습니다.")
        print("🔄 2024년 데이터로 워런 버핏 스코어카드를 먼저 테스트해보세요.")

if __name__ == "__main__":
    main()
