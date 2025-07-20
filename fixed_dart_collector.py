#!/usr/bin/env python3
"""
수정된 DART 재무데이터 수집기
Config import 오류 해결 및 단순화된 버전

실행 예시:
python fixed_dart_collector.py --year=2023 --companies=10
python fixed_dart_collector.py --year=2022,2023 --companies=100
"""

import os
import sys
import time
import requests
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import logging
import zipfile
import io
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

class DartCollectorFixed:
    """수정된 DART 수집기"""
    
    def __init__(self, request_delay: float = 1.5):
        self.logger = self.setup_logging()
        
        # API 설정 (환경변수에서 직접 로드)
        self.api_key = os.getenv('DART_API_KEY')
        self.base_url = "https://opendart.fss.or.kr/api"
        
        if not self.api_key:
            raise ValueError("DART_API_KEY 환경변수가 설정되지 않았습니다.")
        
        self.request_delay = request_delay
        self.session = requests.Session()
        self.stock_to_corp_mapping = {}
        
        # 데이터베이스 경로
        self.db_path = Path('data/databases')
        self.dart_db_path = self.db_path / 'dart_data.db'
        self.stock_db_path = self.db_path / 'stock_data.db'
        
        # 디렉토리 생성
        self.db_path.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("✅ DART 수집기 초기화 완료")
    
    def setup_logging(self):
        """로깅 설정"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger('DartCollectorFixed')
    
    def init_dart_database(self):
        """DART 데이터베이스 초기화"""
        try:
            with sqlite3.connect(self.dart_db_path) as conn:
                # corp_codes 테이블
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS corp_codes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        corp_code TEXT UNIQUE NOT NULL,
                        corp_name TEXT NOT NULL,
                        stock_code TEXT,
                        modify_date TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # financial_statements 테이블
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS financial_statements (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        corp_code TEXT NOT NULL,
                        stock_code TEXT,
                        company_name TEXT,
                        bsns_year TEXT NOT NULL,
                        reprt_code TEXT NOT NULL,
                        account_nm TEXT,
                        account_id TEXT,
                        fs_div TEXT,
                        fs_nm TEXT,
                        sj_div TEXT,
                        sj_nm TEXT,
                        thstrm_nm TEXT,
                        thstrm_amount TEXT,
                        thstrm_add_amount TEXT,
                        frmtrm_nm TEXT,
                        frmtrm_amount TEXT,
                        frmtrm_q_nm TEXT,
                        frmtrm_q_amount TEXT,
                        frmtrm_add_amount TEXT,
                        bfefrmtrm_nm TEXT,
                        bfefrmtrm_amount TEXT,
                        ord INTEGER,
                        currency TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # 인덱스 생성
                conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_financial_corp_year 
                    ON financial_statements(corp_code, bsns_year)
                ''')
                
                conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_financial_stock_year 
                    ON financial_statements(stock_code, bsns_year)
                ''')
                
                conn.commit()
                self.logger.info("✅ DART 데이터베이스 초기화 완료")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ DART 데이터베이스 초기화 실패: {e}")
            return False
    
    def download_corp_codes(self) -> bool:
        """DART corp_codes.xml 다운로드"""
        try:
            self.logger.info("📡 DART corp_codes.xml 다운로드 중...")
            
            url = f"{self.base_url}/corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # ZIP 파일 처리
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_content = zip_file.read('CORPCODE.xml')
            
            # XML 파싱
            root = ET.fromstring(xml_content)
            
            self.logger.info("🔍 corp_codes.xml 파싱 중...")
            
            mapping_count = 0
            for corp in root.findall('list'):
                corp_code = corp.find('corp_code').text if corp.find('corp_code') is not None else None
                stock_code = corp.find('stock_code').text if corp.find('stock_code') is not None else None
                corp_name = corp.find('corp_name').text if corp.find('corp_name') is not None else None
                
                if corp_code and stock_code and stock_code.strip():
                    self.stock_to_corp_mapping[stock_code.strip()] = {
                        'corp_code': corp_code.strip(),
                        'corp_name': corp_name.strip() if corp_name else ''
                    }
                    mapping_count += 1
            
            self.logger.info(f"✅ 매핑 테이블 생성 완료: {mapping_count:,}개 상장기업")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ corp_codes.xml 다운로드 실패: {e}")
            return False
    
    def get_companies_from_stock_db(self) -> List[Tuple[str, str]]:
        """stock_data.db에서 종목 리스트 가져오기"""
        try:
            if not self.stock_db_path.exists():
                self.logger.error(f"❌ stock_data.db가 존재하지 않습니다: {self.stock_db_path}")
                return []
            
            with sqlite3.connect(self.stock_db_path) as conn:
                cursor = conn.execute('''
                    SELECT stock_code, company_name 
                    FROM company_info 
                    WHERE stock_code IS NOT NULL 
                      AND stock_code != '' 
                    ORDER BY stock_code
                ''')
                
                companies = cursor.fetchall()
                company_list = [(row[0], row[1]) for row in companies]
                
                self.logger.info(f"📋 stock_data.db에서 {len(company_list):,}개 종목 조회 완료")
                return company_list
                
        except Exception as e:
            self.logger.error(f"❌ stock_data.db 조회 실패: {e}")
            return []
    
    def get_financial_statements(self, corp_code: str, stock_code: str, company_name: str, year: int) -> Optional[List[Dict]]:
        """재무제표 데이터 가져오기"""
        try:
            url = f"{self.base_url}/fnlttSinglAcntAll.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': str(year),
                'reprt_code': '11011'  # 사업보고서
            }
            
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                self.logger.warning(f"⚠️ HTTP 오류 {response.status_code}: {corp_code}")
                return None
            
            data = response.json()
            
            if data.get('status') == '000' and data.get('list'):
                # 기본 정보 추가
                for item in data['list']:
                    item['corp_code'] = corp_code
                    item['stock_code'] = stock_code
                    item['company_name'] = company_name
                
                return data['list']
            elif data.get('status') == '013':  # 검색된 데이터가 없습니다
                self.logger.debug(f"📭 데이터 없음: {company_name} ({year})")
                return None
            else:
                self.logger.warning(f"⚠️ API 오류 {data.get('status')}: {company_name} - {data.get('message')}")
                return None
                
        except Exception as e:
            self.logger.warning(f"⚠️ API 호출 실패: {company_name} ({year}) - {e}")
            return None
    
    def save_financial_data(self, financial_data_list: List[Dict]) -> bool:
        """재무데이터 저장"""
        if not financial_data_list:
            return False
            
        try:
            with sqlite3.connect(self.dart_db_path) as conn:
                saved_count = 0
                
                for data in financial_data_list:
                    try:
                        conn.execute('''
                            INSERT OR REPLACE INTO financial_statements (
                                corp_code, stock_code, company_name, bsns_year, reprt_code,
                                account_nm, account_id, fs_div, fs_nm, sj_div, sj_nm,
                                thstrm_nm, thstrm_amount, thstrm_add_amount,
                                frmtrm_nm, frmtrm_amount, frmtrm_q_nm, frmtrm_q_amount, frmtrm_add_amount,
                                bfefrmtrm_nm, bfefrmtrm_amount, ord, currency
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            data.get('corp_code'), data.get('stock_code'), data.get('company_name'),
                            data.get('bsns_year'), data.get('reprt_code'),
                            data.get('account_nm'), data.get('account_id'), data.get('fs_div'), data.get('fs_nm'),
                            data.get('sj_div'), data.get('sj_nm'),
                            data.get('thstrm_nm'), data.get('thstrm_amount'), data.get('thstrm_add_amount'),
                            data.get('frmtrm_nm'), data.get('frmtrm_amount'), data.get('frmtrm_q_nm'), 
                            data.get('frmtrm_q_amount'), data.get('frmtrm_add_amount'),
                            data.get('bfefrmtrm_nm'), data.get('bfefrmtrm_amount'), data.get('ord'), data.get('currency')
                        ))
                        saved_count += 1
                    except Exception as e:
                        self.logger.debug(f"데이터 저장 오류: {e}")
                        continue
                
                conn.commit()
                
                if saved_count > 0:
                    self.logger.debug(f"✅ {saved_count}건 저장 완료")
                    return True
                
        except Exception as e:
            self.logger.error(f"❌ 데이터 저장 실패: {e}")
            
        return False
    
    def collect_year_data(self, year: int, max_companies: Optional[int] = None) -> Dict[str, int]:
        """특정 연도 데이터 수집"""
        
        self.logger.info(f"📊 {year}년 재무데이터 수집 시작")
        
        # 회사 목록 가져오기
        companies = self.get_companies_from_stock_db()
        
        if not companies:
            self.logger.error("❌ 수집할 회사 목록이 없습니다")
            return {'success': 0, 'fail': 0, 'records': 0}
        
        # corp_codes 매핑 다운로드
        if not self.download_corp_codes():
            self.logger.error("❌ corp_codes 다운로드 실패")
            return {'success': 0, 'fail': 0, 'records': 0}
        
        # 수집 제한
        if max_companies:
            companies = companies[:max_companies]
            self.logger.info(f"📋 수집 제한: {len(companies):,}개 회사")
        
        success_count = 0
        fail_count = 0
        total_records = 0
        
        for i, (stock_code, company_name) in enumerate(companies, 1):
            try:
                # corp_code 찾기
                if stock_code not in self.stock_to_corp_mapping:
                    self.logger.debug(f"❌ corp_code 없음: {stock_code} {company_name}")
                    fail_count += 1
                    continue
                
                corp_info = self.stock_to_corp_mapping[stock_code]
                corp_code = corp_info['corp_code']
                
                # 재무데이터 수집
                financial_data = self.get_financial_statements(corp_code, stock_code, company_name, year)
                
                if financial_data:
                    if self.save_financial_data(financial_data):
                        success_count += 1
                        total_records += len(financial_data)
                        self.logger.debug(f"✅ {i:,}/{len(companies):,} {company_name}: {len(financial_data)}건")
                    else:
                        fail_count += 1
                        self.logger.debug(f"❌ 저장 실패: {company_name}")
                else:
                    fail_count += 1
                    self.logger.debug(f"📭 데이터 없음: {company_name}")
                
                # 진행률 출력 (100개마다)
                if i % 100 == 0:
                    progress = (i / len(companies)) * 100
                    self.logger.info(f"📊 진행률: {progress:.1f}% ({success_count:,}성공, {fail_count:,}실패)")
                
                # API 제한 대응
                time.sleep(self.request_delay)
                
            except Exception as e:
                self.logger.error(f"❌ 처리 실패: {company_name} - {e}")
                fail_count += 1
                continue
        
        self.logger.info(f"✅ {year}년 재무데이터 수집 완료")
        self.logger.info(f"📊 최종 결과: {success_count:,}/{len(companies):,}건 성공 ({success_count/(success_count+fail_count)*100:.1f}%)")
        
        if success_count == 0:
            self.logger.error(f"❌ {year}년 재무데이터 수집 실패")
            self.logger.info("💡 해결방안: 2023년 데이터로 테스트해보세요")
            self.logger.info("   python fixed_dart_collector.py --year=2023 --companies=10")
        
        return {
            'success': success_count,
            'fail': fail_count,
            'records': total_records
        }
    
    def collect_multi_year_data(self, years: List[int], max_companies: Optional[int] = None) -> bool:
        """다년도 데이터 수집"""
        
        self.logger.info(f"🚀 다년도 DART 수집 시작: {years}")
        
        # 데이터베이스 초기화
        if not self.init_dart_database():
            return False
        
        overall_success = 0
        overall_fail = 0
        year_results = {}
        
        for year in years:
            self.logger.info(f"\n📅 {year}년 데이터 수집 시작")
            
            result = self.collect_year_data(year, max_companies)
            
            year_results[year] = result
            overall_success += result['success']
            overall_fail += result['fail']
            
            self.logger.info(f"✅ {year}년 완료: {result['success']:,}성공, {result['records']:,}건 데이터")
        
        # 최종 결과
        total_processed = overall_success + overall_fail
        success_rate = (overall_success / total_processed * 100) if total_processed > 0 else 0
        
        self.logger.info(f"\n🎉 다년도 수집 완료!")
        self.logger.info(f"📊 전체 결과: {overall_success:,}/{total_processed:,} 성공 ({success_rate:.1f}%)")
        
        for year, result in year_results.items():
            self.logger.info(f"  {year}년: {result['success']:,}개 기업, {result['records']:,}건 데이터")
        
        return overall_success > 0


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='수정된 DART 재무데이터 수집기')
    parser.add_argument('--year', type=str, required=True,
                       help='수집할 연도 (쉼표로 구분, 예: 2022,2023)')
    parser.add_argument('--companies', type=int,
                       help='수집할 기업 수 (기본: 전체)')
    parser.add_argument('--delay', type=float, default=1.5,
                       help='요청 간격 (초, 기본: 1.5)')
    
    args = parser.parse_args()
    
    try:
        # 연도 파싱
        years = [int(year.strip()) for year in args.year.split(',')]
        
        print(f"🚀 DART 재무데이터 수집기 시작")
        print(f"📅 대상 연도: {', '.join(map(str, years))}")
        print(f"⏱️ API 간격: {args.delay}초")
        if args.companies:
            print(f"📊 수집 제한: {args.companies:,}개 종목")
        print("=" * 60)
        
        collector = DartCollectorFixed(request_delay=args.delay)
        
        success = collector.collect_multi_year_data(
            years=years,
            max_companies=args.companies
        )
        
        if success:
            print("\n✅ 데이터 수집이 완료되었습니다!")
            print("💡 다음 단계: 워런 버핏 스코어카드 계산")
        else:
            print("\n❌ 데이터 수집에 실패했습니다.")
            print("💡 해결방안:")
            print("  1. DART_API_KEY 환경변수 확인")
            print("  2. 2023년 데이터로 테스트: --year=2023 --companies=10")
            print("  3. 네트워크 연결 확인")
            
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 실행 실패: {e}")


if __name__ == "__main__":
    main()
