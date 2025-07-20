#!/usr/bin/env python3
"""
DART 재무데이터 수집 스크립트 (코스피/코스닥 전용)
코스피/코스닥 상장 종목만 대상으로 DART 재무제표 수집

실행 방법:
python dart_collector_fixed.py --financial --year=2024
python dart_collector_fixed.py --financial --year=2025
"""

import sys
import os
import argparse
import sqlite3
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import zipfile
import io
from datetime import datetime, timedelta
from pathlib import Path
import logging
import time

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from config import ConfigManager
except ImportError:
    print("ConfigManager를 찾을 수 없습니다. 기본 설정으로 진행합니다.")
    ConfigManager = None

class DartDataCollectorFixed:
    """DART 데이터 수집 클래스 (수정된 버전)"""
    
    def __init__(self):
        # ConfigManager를 통한 통합 설정 관리
        if ConfigManager:
            self.config_manager = ConfigManager()
            self.logger = self.config_manager.get_logger('DartCollectorFixed')
            
            # API 설정 가져오기
            dart_config = self.config_manager.get_dart_config()
            self.api_key = dart_config.get('api_key')
            self.base_url = dart_config.get('base_url', "https://opendart.fss.or.kr/api")
        else:
            # 기본 로깅 설정
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)
            self.api_key = os.getenv('DART_API_KEY')
            self.base_url = "https://opendart.fss.or.kr/api"
        
        if not self.api_key:
            raise ValueError("DART API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
        
        # 데이터베이스 경로
        self.dart_db_path = Path('data/databases/dart_data.db')
        self.stock_db_path = Path('data/databases/stock_data.db')
        
        self.logger.info("DART 데이터 수집기 초기화 완료 (코스피/코스닥 전용)")
    
    def get_kospi_kosdaq_companies(self):
        """코스피/코스닥 상장 종목 목록 조회"""
        try:
            companies = []
            
            # 1. stock_data.db에서 코스피/코스닥 종목 조회
            if self.stock_db_path.exists():
                with sqlite3.connect(self.stock_db_path) as conn:
                    query = """
                        SELECT DISTINCT stock_code, company_name, market_type
                        FROM company_info 
                        WHERE market_type IN ('KOSPI', 'KOSDAQ')
                        AND stock_code IS NOT NULL 
                        AND stock_code != ''
                        ORDER BY market_type, stock_code
                    """
                    
                    stock_companies = pd.read_sql(query, conn)
                    self.logger.info(f"📊 Stock DB에서 코스피/코스닥 종목 조회: {len(stock_companies)}개")
                    
                    for _, row in stock_companies.iterrows():
                        companies.append({
                            'stock_code': row['stock_code'],
                            'company_name': row['company_name'],
                            'market_type': row['market_type']
                        })
            
            # 2. DART DB에서 corp_code 매핑 정보 조회
            if self.dart_db_path.exists():
                with sqlite3.connect(self.dart_db_path) as conn:
                    # 기존 기업코드 테이블 확인
                    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
                    
                    if 'corp_codes' in tables['name'].values:
                        corp_mapping = pd.read_sql("""
                            SELECT stock_code, corp_code, corp_name
                            FROM corp_codes 
                            WHERE stock_code IS NOT NULL AND stock_code != ''
                        """, conn)
                        
                        self.logger.info(f"📋 DART DB에서 기업코드 매핑 조회: {len(corp_mapping)}개")
                        
                        # stock_code 기준으로 corp_code 추가
                        for company in companies:
                            matching_corp = corp_mapping[corp_mapping['stock_code'] == company['stock_code']]
                            if not matching_corp.empty:
                                company['corp_code'] = matching_corp.iloc[0]['corp_code']
                            else:
                                company['corp_code'] = None
                    else:
                        self.logger.warning("⚠️ corp_codes 테이블이 없습니다. 기업코드를 먼저 수집하세요.")
                        return []
            else:
                self.logger.warning("⚠️ DART DB가 없습니다. 기업코드를 먼저 수집하세요.")
                return []
            
            # corp_code가 있는 회사들만 필터링
            valid_companies = [c for c in companies if c.get('corp_code')]
            
            self.logger.info(f"✅ 최종 대상 기업: {len(valid_companies)}개 (코스피/코스닥 상장 + DART 등록)")
            
            # 시장별 통계
            kospi_count = len([c for c in valid_companies if c['market_type'] == 'KOSPI'])
            kosdaq_count = len([c for c in valid_companies if c['market_type'] == 'KOSDAQ'])
            self.logger.info(f"📈 KOSPI: {kospi_count}개, KOSDAQ: {kosdaq_count}개")
            
            return valid_companies
            
        except Exception as e:
            self.logger.error(f"❌ 코스피/코스닥 종목 조회 실패: {e}")
            return []
    
    def get_financial_statements(self, corp_code, bsns_year, reprt_code='11011'):
        """재무제표 데이터 수집"""
        try:
            url = f"{self.base_url}/fnlttSinglAcntAll.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': bsns_year,
                'reprt_code': reprt_code,  # 11011: 사업보고서, 11012: 반기보고서, 11013: 1분기, 11014: 3분기
                'fs_div': 'OFS'  # OFS: 개별재무제표, CFS: 연결재무제표
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') != '000':
                if data.get('message') == '조회된 데이터가 없습니다.':
                    self.logger.debug(f"📊 데이터 없음 ({corp_code}, {bsns_year}, {reprt_code})")
                else:
                    self.logger.warning(f"⚠️ 재무제표 조회 실패 ({corp_code}, {bsns_year}): {data.get('message', 'Unknown error')}")
                return pd.DataFrame()
            
            # 재무제표 데이터 처리
            financial_data = []
            for item in data.get('list', []):
                fs_info = {
                    'corp_code': corp_code,
                    'bsns_year': int(bsns_year),
                    'reprt_code': reprt_code,
                    'fs_div': item.get('fs_div', ''),
                    'fs_nm': item.get('fs_nm', ''),
                    'account_nm': item.get('account_nm', ''),
                    'thstrm_amount': self._parse_amount(item.get('thstrm_amount', '')),  # 당기금액
                    'frmtrm_amount': self._parse_amount(item.get('frmtrm_amount', '')),   # 전기금액
                    'bfefrmtrm_amount': self._parse_amount(item.get('bfefrmtrm_amount', '')),  # 전전기금액
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                financial_data.append(fs_info)
            
            self.logger.debug(f"📊 재무제표 수집 완료 ({corp_code}, {bsns_year}): {len(financial_data)}개 계정")
            return pd.DataFrame(financial_data)
            
        except Exception as e:
            self.logger.error(f"❌ 재무제표 수집 실패 ({corp_code}, {bsns_year}): {e}")
            return pd.DataFrame()
    
    def _parse_amount(self, amount_str):
        """금액 문자열을 숫자로 변환"""
        if not amount_str or amount_str == '-':
            return None
        
        try:
            # 콤마 제거하고 숫자로 변환
            clean_amount = amount_str.replace(',', '').replace('(', '-').replace(')', '')
            return float(clean_amount)
        except:
            return None
    
    def create_financial_tables(self, conn):
        """재무제표 테이블 생성 (스키마 오류 수정)"""
        # 기존 테이블 구조 확인
        cursor = conn.execute("PRAGMA table_info(financial_statements)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'created_at' not in columns:
            self.logger.info("📋 financial_statements 테이블에 created_at 컬럼 추가")
            try:
                conn.execute("ALTER TABLE financial_statements ADD COLUMN created_at TEXT")
                conn.commit()
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    self.logger.warning(f"⚠️ 컬럼 추가 실패: {e}")
        
        # 테이블이 없는 경우 생성
        conn.execute('''
            CREATE TABLE IF NOT EXISTS financial_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                corp_code TEXT NOT NULL,
                bsns_year INTEGER NOT NULL,
                reprt_code TEXT NOT NULL,
                fs_div TEXT,
                fs_nm TEXT,
                account_nm TEXT,
                thstrm_amount REAL,
                frmtrm_amount REAL,
                bfefrmtrm_amount REAL,
                created_at TEXT,
                updated_at TEXT,
                UNIQUE(corp_code, bsns_year, reprt_code, fs_div, account_nm)
            )
        ''')
        
        # 인덱스 생성
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_financial_corp_year 
            ON financial_statements(corp_code, bsns_year)
        ''')
        
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_financial_year 
            ON financial_statements(bsns_year)
        ''')
    
    def save_financial_data(self, financial_data):
        """재무제표 데이터 저장 (스키마 오류 수정)"""
        if financial_data.empty:
            return False
        
        try:
            # 데이터베이스 디렉토리 생성
            self.dart_db_path.parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(self.dart_db_path) as conn:
                # 테이블 생성/수정
                self.create_financial_tables(conn)
                
                saved_count = 0
                for _, row in financial_data.iterrows():
                    try:
                        # created_at 컬럼 추가
                        row_dict = row.to_dict()
                        if 'created_at' not in row_dict:
                            row_dict['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        conn.execute('''
                            INSERT OR REPLACE INTO financial_statements 
                            (corp_code, bsns_year, reprt_code, fs_div, fs_nm, account_nm,
                             thstrm_amount, frmtrm_amount, bfefrmtrm_amount, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            row_dict['corp_code'],
                            row_dict['bsns_year'],
                            row_dict['reprt_code'],
                            row_dict['fs_div'],
                            row_dict['fs_nm'],
                            row_dict['account_nm'],
                            row_dict['thstrm_amount'],
                            row_dict['frmtrm_amount'],
                            row_dict['bfefrmtrm_amount'],
                            row_dict['created_at'],
                            row_dict['updated_at']
                        ))
                        saved_count += 1
                    except sqlite3.Error as e:
                        self.logger.debug(f"⚠️ 재무데이터 저장 실패: {e}")
                        continue
                
                conn.commit()
                self.logger.debug(f"💾 재무데이터 저장 완료: {saved_count}/{len(financial_data)}건")
                return saved_count > 0
                
        except Exception as e:
            self.logger.error(f"❌ 재무데이터 저장 실패: {e}")
            return False
    
    def collect_financial_data_by_year(self, year):
        """연도별 재무데이터 수집 (코스피/코스닥만)"""
        try:
            # 코스피/코스닥 종목 조회
            companies = self.get_kospi_kosdaq_companies()
            
            if not companies:
                self.logger.error("❌ 수집할 종목이 없습니다.")
                return False
            
            # 분기별 보고서 코드
            reprt_codes = {
                1: '11013',  # 1분기
                2: '11012',  # 반기
                3: '11014',  # 3분기  
                4: '11011'   # 사업보고서
            }
            
            total_count = len(companies) * len(reprt_codes)
            current_count = 0
            success_count = 0
            
            self.logger.info(f"🚀 {year}년 재무데이터 수집 시작: {len(companies)}개 기업 × 4분기 = {total_count}건")
            
            for company in companies:
                corp_code = company['corp_code']
                company_name = company['company_name']
                market_type = company['market_type']
                
                for quarter, reprt_code in reprt_codes.items():
                    current_count += 1
                    
                    quarter_name = {1: '1분기', 2: '2분기', 3: '3분기', 4: '4분기'}[quarter]
                    
                    self.logger.info(f"📊 진행률: {current_count}/{total_count} - {company_name}({market_type}) {year}년 {quarter_name}")
                    
                    # 재무제표 수집
                    financial_data = self.get_financial_statements(corp_code, year, reprt_code)
                    
                    if not financial_data.empty:
                        if self.save_financial_data(financial_data):
                            success_count += 1
                    
                    # API 호출 제한 대응 (초당 10회 제한)
                    time.sleep(0.12)
                    
                    # 진행률 표시 (100건마다)
                    if current_count % 100 == 0:
                        progress = (current_count / total_count) * 100
                        self.logger.info(f"🔄 중간 진행률: {progress:.1f}% 완료, 성공: {success_count}건")
            
            final_progress = (success_count / total_count) * 100
            self.logger.info(f"✅ {year}년 재무데이터 수집 완료")
            self.logger.info(f"📊 최종 결과: {success_count}/{total_count}건 성공 ({final_progress:.1f}%)")
            
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"❌ {year}년 재무데이터 수집 실패: {e}")
            return False
    
    def download_corp_codes(self):
        """기업 고유번호 다운로드 및 파싱 (전체 기업)"""
        try:
            # DART 기업코드 ZIP 파일 다운로드
            url = f"{self.base_url}/corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            self.logger.info("📥 기업코드 파일 다운로드 중...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # ZIP 파일 압축 해제
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_content = zip_file.read('CORPCODE.xml')
            
            # XML 파싱
            root = ET.fromstring(xml_content)
            
            corp_data = []
            for corp in root.findall('list'):
                corp_info = {
                    'corp_code': corp.find('corp_code').text if corp.find('corp_code') is not None else '',
                    'corp_name': corp.find('corp_name').text if corp.find('corp_name') is not None else '',
                    'stock_code': corp.find('stock_code').text if corp.find('stock_code') is not None else '',
                    'modify_date': corp.find('modify_date').text if corp.find('modify_date') is not None else '',
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                corp_data.append(corp_info)
            
            self.logger.info(f"📋 기업코드 파싱 완료: {len(corp_data)}개 기업")
            
            # 데이터베이스 저장
            with sqlite3.connect(self.dart_db_path) as conn:
                # 테이블 생성
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS corp_codes (
                        corp_code TEXT PRIMARY KEY,
                        corp_name TEXT,
                        stock_code TEXT,
                        modify_date TEXT,
                        created_at TEXT,
                        updated_at TEXT
                    )
                ''')
                
                # 데이터 저장
                corp_df = pd.DataFrame(corp_data)
                corp_df.to_sql('corp_codes', conn, if_exists='replace', index=False)
                
                self.logger.info(f"💾 기업코드 저장 완료: {len(corp_data)}건")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 기업코드 다운로드 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='DART 재무데이터 수집 스크립트 (코스피/코스닥 전용)')
    parser.add_argument('--corp_codes', action='store_true', help='기업코드 수집')
    parser.add_argument('--financial', action='store_true', help='재무데이터 수집 (코스피/코스닥만)')
    parser.add_argument('--year', type=int, help='수집 연도')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    try:
        # 수집기 초기화
        collector = DartDataCollectorFixed()
        
        # 로그 레벨 설정
        if args.log_level:
            logging.getLogger().setLevel(getattr(logging, args.log_level))
            collector.logger.setLevel(getattr(logging, args.log_level))
        
        if args.corp_codes:
            # 기업코드 수집
            if collector.download_corp_codes():
                collector.logger.info("✅ 기업코드 수집 성공")
            else:
                collector.logger.error("❌ 기업코드 수집 실패")
                sys.exit(1)
                
        elif args.financial and args.year:
            # 재무데이터 수집 (코스피/코스닥만)
            if collector.collect_financial_data_by_year(args.year):
                collector.logger.info(f"✅ {args.year}년 재무데이터 수집 성공")
            else:
                collector.logger.error(f"❌ {args.year}년 재무데이터 수집 실패")
                sys.exit(1)
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        print(f"❌ 예기치 못한 오류: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
