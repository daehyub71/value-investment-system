#!/usr/bin/env python3
"""
주요 상장기업 대상 DART 데이터 수집 스크립트
확실한 재무데이터가 있는 기업들만 대상으로 수집
"""

import sys
import sqlite3
from pathlib import Path
import requests
from datetime import datetime
import time

# 프로젝트 루트 경로 추가
sys.path.insert(0, str(Path(__file__).parent))

def get_major_listed_companies():
    """주요 상장기업 코드 및 이름 반환"""
    major_companies = [
        # KOSPI 대형주 (재무데이터 확실)
        ('00126380', '005930', '삼성전자'),
        ('00164779', '000660', 'SK하이닉스'),
        ('00401731', '035420', 'NAVER'),
        ('00138167', '005380', '현대자동차'),
        ('00114991', '006400', '삼성SDI'),
        ('00166971', '005490', 'POSCO홀딩스'),
        ('00117967', '051910', 'LG화학'),
        ('00140312', '035720', '카카오'),
        ('00139967', '003550', 'LG'),
        ('00101312', '012330', '현대모비스'),
        ('00126701', '000270', '기아'),
        ('00113570', '096770', 'SK이노베이션'),
        ('00164445', '009150', '삼성전기'),
        ('00164470', '010130', '고려아연'),
        ('00152467', '034730', 'SK'),
        ('00100883', '005830', 'DB손해보험'),
        ('00191965', '018260', '삼성에스디에스'),
        ('00117629', '066570', 'LG전자'),
        ('00111467', '017670', 'SK텔레콤'),
        ('00164394', '032830', '삼성생명')
    ]
    return major_companies

def test_dart_api_access():
    """DART API 접근 테스트"""
    try:
        from config import ConfigManager
        config_manager = ConfigManager()
        dart_config = config_manager.get_dart_config()
        api_key = dart_config.get('api_key')
        
        if not api_key:
            print("❌ DART API 키가 설정되지 않았습니다.")
            return False
            
        # 기업 개요 API 테스트 (삼성전자)
        test_url = f"https://opendart.fss.or.kr/api/company.json"
        params = {
            'crtfc_key': api_key,
            'corp_code': '00126380'  # 삼성전자
        }
        
        response = requests.get(test_url, params=params)
        data = response.json()
        
        if data.get('status') == '000':
            print("✅ DART API 연결 정상")
            return True
        else:
            print(f"❌ DART API 오류: {data.get('message', '알 수 없는 오류')}")
            return False
            
    except Exception as e:
        print(f"❌ DART API 테스트 실패: {e}")
        return False

def collect_major_companies_data(year=2022, limit=10):
    """주요 기업들의 DART 데이터 수집"""
    
    if not test_dart_api_access():
        return False
    
    print(f"🎯 주요 상장기업 DART 데이터 수집 시작")
    print(f"📅 대상 연도: {year}")
    print(f"📊 수집 기업 수: {limit}개")
    print("=" * 60)
    
    companies = get_major_listed_companies()[:limit]
    
    # DART 데이터베이스에 기업 코드 삽입
    db_path = Path('data/databases/dart_data.db')
    if not db_path.exists():
        print("❌ dart_data.db가 없습니다. 먼저 데이터베이스를 생성해주세요.")
        return False
    
    try:
        with sqlite3.connect(db_path) as conn:
            # corp_codes 테이블 확인/생성
            conn.execute('''
                CREATE TABLE IF NOT EXISTS corp_codes (
                    corp_code TEXT PRIMARY KEY,
                    corp_name TEXT,
                    stock_code TEXT,
                    modify_date TEXT
                )
            ''')
            
            # 기업 코드 삽입
            for corp_code, stock_code, corp_name in companies:
                conn.execute('''
                    INSERT OR REPLACE INTO corp_codes 
                    (corp_code, corp_name, stock_code, modify_date)
                    VALUES (?, ?, ?, ?)
                ''', (corp_code, corp_name, stock_code, datetime.now().strftime('%Y%m%d')))
            
            conn.commit()
            
        print(f"✅ {len(companies)}개 주요 기업 코드 등록 완료")
        
        # 이제 improved_dart_collector 실행 권장
        print("\n🚀 다음 명령어로 데이터 수집을 실행하세요:")
        print(f"python improved_dart_collector.py --companies={limit} --year={year}")
        
        return True
        
    except Exception as e:
        print(f"❌ 기업 코드 등록 실패: {e}")
        return False

def quick_test_collection():
    """빠른 테스트 수집 (삼성전자 1개만)"""
    print("🧪 빠른 테스트: 삼성전자 데이터만 수집")
    print("=" * 50)
    
    try:
        from config import ConfigManager
        config_manager = ConfigManager()
        dart_config = config_manager.get_dart_config()
        api_key = dart_config.get('api_key')
        
        # 기업 개요 API 호출
        company_url = "https://opendart.fss.or.kr/api/company.json"
        params = {
            'crtfc_key': api_key,
            'corp_code': '00126380'  # 삼성전자
        }
        
        print("📞 삼성전자 기업개요 API 호출...")
        response = requests.get(company_url, params=params)
        data = response.json()
        
        if data.get('status') == '000':
            print("✅ 기업개요 수집 성공")
            print(f"   회사명: {data.get('corp_name', 'N/A')}")
            print(f"   종목코드: {data.get('stock_code', 'N/A')}")
            
            # 재무제표 API 호출 테스트
            fs_url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
            fs_params = {
                'crtfc_key': api_key,
                'corp_code': '00126380',
                'bsns_year': '2022',
                'reprt_code': '11011'  # 사업보고서
            }
            
            print("📊 삼성전자 재무제표 API 호출...")
            time.sleep(1)  # API 제한 고려
            
            fs_response = requests.get(fs_url, params=fs_params)
            fs_data = fs_response.json()
            
            if fs_data.get('status') == '000':
                print("✅ 재무제표 수집 성공")
                print(f"   재무항목 수: {len(fs_data.get('list', []))}개")
                return True
            else:
                print(f"❌ 재무제표 수집 실패: {fs_data.get('message', 'N/A')}")
                return False
                
        else:
            print(f"❌ 기업개요 수집 실패: {data.get('message', 'N/A')}")
            return False
            
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        return False

if __name__ == "__main__":
    print("🎯 DART 데이터 수집 준비 스크립트")
    print("=" * 60)
    
    # 1. API 연결 테스트
    if not test_dart_api_access():
        print("\n❌ DART API 연결에 문제가 있습니다. .env 파일의 API 키를 확인해주세요.")
        exit(1)
    
    # 2. 빠른 테스트
    print("\n🧪 1단계: 빠른 API 테스트")
    if quick_test_collection():
        print("✅ API 테스트 성공!")
    else:
        print("❌ API 테스트 실패. 연도를 변경하거나 다른 기업으로 시도해보세요.")
        exit(1)
    
    # 3. 주요 기업 코드 등록
    print("\n📊 2단계: 주요 기업 코드 등록")
    if collect_major_companies_data(year=2022, limit=10):
        print("\n✅ 모든 준비 완료!")
        print("\n🚀 이제 다음 명령어를 실행하세요:")
        print("python improved_dart_collector.py --companies=10 --year=2022")
    else:
        print("\n❌ 준비 실패. 오류를 확인해주세요.")