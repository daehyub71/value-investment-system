#!/usr/bin/env python3
"""
DART API 연결 진단 및 2025년 1분기 데이터 수집
"""

import requests
import sqlite3
import pandas as pd
import os
from datetime import datetime
import zipfile
import xml.etree.ElementTree as ET
import io

def test_dart_api_connection():
    """DART API 연결 테스트"""
    
    print("🔍 DART API 연결 진단")
    print("=" * 50)
    
    # 환경변수에서 API 키 확인
    api_key = os.environ.get('DART_API_KEY')
    if not api_key:
        print("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        print("💡 .env 파일을 확인하세요.")
        return False
    
    print(f"✅ API Key 확인: {api_key[:10]}***")
    
    # 1. 기본 연결 테스트
    try:
        test_url = "https://opendart.fss.or.kr/api/company.json"
        params = {
            'crtfc_key': api_key,
            'corp_code': '00126380'  # 삼성전자 테스트
        }
        
        response = requests.get(test_url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == '000':
                print("✅ DART API 기본 연결 성공")
                print(f"   테스트 결과: {data.get('corp_name', 'N/A')}")
                return True
            else:
                print(f"❌ API 응답 오류: {data.get('message', 'Unknown error')}")
                return False
        else:
            print(f"❌ HTTP 오류: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ 연결 오류: {e}")
        return False

def get_existing_corp_codes():
    """기존 데이터베이스에서 기업코드 가져오기"""
    
    print("\n📊 기존 기업코드 데이터 활용")
    print("-" * 30)
    
    try:
        conn = sqlite3.connect('data/databases/dart_data.db')
        
        # 기존 corp_codes 테이블에서 주요 기업 확인
        query = """
        SELECT corp_code, corp_name, stock_code 
        FROM corp_codes 
        WHERE corp_name IN ('삼성전자', 'SK하이닉스', 'NAVER', 'LG에너지솔루션', '카카오')
        ORDER BY corp_name
        """
        
        result = pd.read_sql(query, conn)
        conn.close()
        
        if not result.empty:
            print("✅ 기존 기업코드 데이터 발견:")
            for _, row in result.iterrows():
                print(f"   🏢 {row['corp_name']} ({row['stock_code']}): {row['corp_code']}")
            return result
        else:
            print("❌ 기존 기업코드 데이터가 없습니다.")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ 기업코드 조회 오류: {e}")
        return pd.DataFrame()

def collect_2025q1_with_existing_codes(corp_codes_df):
    """기존 기업코드로 2025년 1분기 데이터 수집"""
    
    if corp_codes_df.empty:
        print("❌ 수집할 기업코드가 없습니다.")
        return
    
    print(f"\n🚀 2025년 1분기 재무데이터 수집 시작 ({len(corp_codes_df)}개 기업)")
    print("-" * 50)
    
    api_key = os.environ.get('DART_API_KEY')
    if not api_key:
        print("❌ API 키가 없습니다.")
        return
    
    conn = sqlite3.connect('data/databases/dart_data.db')
    success_count = 0
    
    for _, company in corp_codes_df.iterrows():
        corp_code = company['corp_code']
        corp_name = company['corp_name']
        
        try:
            print(f"📊 {corp_name} 데이터 수집 중...")
            
            # 2025년 1분기 재무제표 요청
            url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
            params = {
                'crtfc_key': api_key,
                'corp_code': corp_code,
                'bsns_year': '2025',
                'reprt_code': '11013',  # 1분기 보고서
                'fs_div': 'CFS'  # 연결재무제표
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == '000' and 'list' in data:
                    # 데이터베이스에 저장
                    records = []
                    for item in data['list']:
                        records.append({
                            'corp_code': corp_code,
                            'bsns_year': 2025,
                            'reprt_code': '11013',
                            'fs_div': item.get('fs_div'),
                            'fs_nm': item.get('fs_nm'),
                            'account_nm': item.get('account_nm'),
                            'thstrm_amount': float(item.get('thstrm_amount', 0)) if item.get('thstrm_amount') else None,
                            'frmtrm_amount': float(item.get('frmtrm_amount', 0)) if item.get('frmtrm_amount') else None,
                            'bfefrmtrm_amount': float(item.get('bfefrmtrm_amount', 0)) if item.get('bfefrmtrm_amount') else None,
                            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                    
                    if records:
                        df = pd.DataFrame(records)
                        df.to_sql('financial_statements', conn, if_exists='append', index=False)
                        print(f"   ✅ {corp_name}: {len(records)}건 저장")
                        success_count += 1
                    else:
                        print(f"   ⚠️ {corp_name}: 데이터 없음")
                        
                elif data.get('status') == '013':
                    print(f"   ⚠️ {corp_name}: 2025년 1분기 보고서 미제출")
                else:
                    print(f"   ❌ {corp_name}: API 오류 - {data.get('message', 'Unknown')}")
            else:
                print(f"   ❌ {corp_name}: HTTP 오류 {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ {corp_name}: 수집 오류 - {e}")
        
        # API 요청 간격 (초당 10회 제한)
        import time
        time.sleep(0.2)
    
    conn.close()
    
    print(f"\n🎉 수집 완료: {success_count}/{len(corp_codes_df)}개 기업 성공")
    
    return success_count > 0

def main():
    """메인 실행"""
    
    print("🚀 2025년 1분기 DART 데이터 긴급 수집")
    print("=" * 60)
    
    # 1. API 연결 테스트
    if not test_dart_api_connection():
        print("\n❌ DART API 연결 실패")
        print("💡 인터넷 연결 또는 API 키를 확인하세요.")
        return
    
    # 2. 기존 기업코드 활용
    corp_codes = get_existing_corp_codes()
    
    if corp_codes.empty:
        print("\n❌ 수집할 기업이 없습니다.")
        print("💡 먼저 기업코드 데이터를 수집하세요.")
        return
    
    # 3. 2025년 1분기 데이터 수집
    success = collect_2025q1_with_existing_codes(corp_codes)
    
    if success:
        print("\n✅ 2025년 1분기 데이터 수집 완료!")
        print("🔄 이제 워런 버핏 스코어카드를 계산할 수 있습니다.")
        print("\n다음 단계:")
        print("   python check_2025q1_data.py  # 수집 결과 확인")
        print("   python buffett_scorecard_final.py --stock_code=005930  # 스코어카드 계산")
    else:
        print("\n❌ 데이터 수집 실패")
        print("💡 수동으로 다른 수집 방법을 시도하세요.")

if __name__ == "__main__":
    main()
