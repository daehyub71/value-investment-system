#!/usr/bin/env python3
"""
DART 데이터베이스 저장 오류 수정 버전
저장 실패 문제 해결 및 디버깅 강화

실행 예시:
python dart_collector_save_fixed.py --year=2023 --companies=10
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

class DartCollectorSaveFixed:
    """저장 오류 수정된 DART 수집기"""
    
    def __init__(self, request_delay: float = 1.5):
        self.logger = self.setup_logging()
        
        # API 설정
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
        return logging.getLogger('DartCollectorSaveFixed')
    
    def init_dart_database(self):
        """DART 데이터베이스 초기화 - 단순화된 스키마"""
        try:
            with sqlite3.connect(self.dart_db_path) as conn:
                # 기존 테이블 삭제 (스키마 문제 방지)
                conn.execute('DROP TABLE IF EXISTS financial_statements')
                
                # 단순화된 financial_statements 테이블
                conn.execute('''
                    CREATE TABLE financial_statements (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        corp_code TEXT NOT NULL,
                        stock_code TEXT,
                        company_name TEXT,
                        bsns_year TEXT NOT NULL,
                        reprt_code TEXT NOT NULL,
                        fs_div TEXT,
                        account_nm TEXT,
                        thstrm_amount TEXT,
                        frmtrm_amount TEXT,
                        currency TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # 인덱스 생성
                conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_financial_corp_year 
                    ON financial_statements(corp_code, bsns_year)
                ''')
                
                conn.commit()
                self.logger.info("✅ DART 데이터베이스 초기화 완료 (단순화된 스키마)")
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
    
    def get_financial_statements_all_types(self, corp_code: str, stock_code: str, company_name: str, year: int) -> Optional[List[Dict]]:
        """재무제표 데이터 가져오기 - 모든 fs_div 유형 시도"""
        
        # fs_div 유형들
        fs_div_types = ['CFS', 'OFS']  # CFS: 연결재무제표, OFS: 별도재무제표
        
        all_financial_data = []
        
        for fs_div in fs_div_types:
            try:
                url = f"{self.base_url}/fnlttSinglAcntAll.json"
                params = {
                    'crtfc_key': self.api_key,
                    'corp_code': corp_code,
                    'bsns_year': str(year),
                    'reprt_code': '11011',  # 사업보고서
                    'fs_div': fs_div  # 필수 파라미터 추가
                }
                
                self.logger.debug(f"🔍 {company_name} {year}년 {fs_div} 재무제표 조회...")
                
                response = self.session.get(url, params=params, timeout=10)
                
                if response.status_code != 200:
                    self.logger.debug(f"⚠️ HTTP 오류 {response.status_code}: {corp_code} ({fs_div})")
                    continue
                
                data = response.json()
                
                if data.get('status') == '000' and data.get('list'):
                    # 기본 정보 추가
                    for item in data['list']:
                        item['corp_code'] = corp_code
                        item['stock_code'] = stock_code
                        item['company_name'] = company_name
                        item['fs_div_used'] = fs_div  # 사용한 fs_div 기록
                    
                    all_financial_data.extend(data['list'])
                    self.logger.info(f"✅ {company_name} {fs_div}: {len(data['list'])}건 수집")
                    
                    # 첫 번째 성공한 fs_div로 충분하다면 break (선택사항)
                    break
                    
                elif data.get('status') == '013':  # 검색된 데이터가 없습니다
                    self.logger.debug(f"📭 {fs_div} 데이터 없음: {company_name} ({year})")
                    continue
                else:
                    self.logger.debug(f"⚠️ {fs_div} API 오류 {data.get('status')}: {company_name} - {data.get('message')}")
                    continue
                
                # API 제한 대응
                time.sleep(0.3)
                
            except Exception as e:
                self.logger.debug(f"⚠️ {fs_div} API 호출 실패: {company_name} ({year}) - {e}")
                continue
        
        if all_financial_data:
            self.logger.info(f"📊 {company_name}: 총 {len(all_financial_data)}건 수집")
            return all_financial_data
        else:
            self.logger.info(f"📭 모든 fs_div에서 데이터 없음: {company_name} ({year})")
            return None
    
    def save_financial_data(self, financial_data_list: List[Dict]) -> bool:
        """재무데이터 저장 - 단순화된 스키마"""
        if not financial_data_list:
            self.logger.warning("저장할 데이터가 없습니다")
            return False
        
        self.logger.info(f"💾 {len(financial_data_list)}건 데이터 저장 시작...")
        
        try:
            with sqlite3.connect(self.dart_db_path) as conn:
                saved_count = 0
                error_count = 0
                
                for i, data in enumerate(financial_data_list):
                    try:
                        # 데이터 검증 및 정제
                        corp_code = str(data.get('corp_code', '')).strip()
                        stock_code = str(data.get('stock_code', '')).strip()
                        company_name = str(data.get('company_name', '')).strip()
                        bsns_year = str(data.get('bsns_year', '')).strip()
                        reprt_code = str(data.get('reprt_code', '')).strip()
                        fs_div = str(data.get('fs_div', '')).strip()
                        account_nm = str(data.get('account_nm', '')).strip()
                        thstrm_amount = str(data.get('thstrm_amount', '')).strip()
                        frmtrm_amount = str(data.get('frmtrm_amount', '')).strip()
                        currency = str(data.get('currency', '')).strip()
                        
                        # 필수 필드 검증
                        if not corp_code or not bsns_year or not reprt_code:
                            self.logger.debug(f"필수 필드 누락: {i+1}번째 데이터")
                            error_count += 1
                            continue
                        
                        # 단순화된 INSERT
                        conn.execute('''
                            INSERT OR REPLACE INTO financial_statements (
                                corp_code, stock_code, company_name, bsns_year, reprt_code,
                                fs_div, account_nm, thstrm_amount, frmtrm_amount, currency
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            corp_code, stock_code, company_name, bsns_year, reprt_code,
                            fs_div, account_nm, thstrm_amount, frmtrm_amount, currency
                        ))
                        
                        saved_count += 1
                        
                        # 진행률 출력 (100개마다)
                        if (i + 1) % 100 == 0:
                            self.logger.debug(f"저장 진행률: {i+1}/{len(financial_data_list)}")
                        
                    except Exception as e:
                        error_count += 1
                        self.logger.debug(f"개별 데이터 저장 오류 ({i+1}번째): {e}")
                        continue
                
                # 커밋
                conn.commit()
                
                # 결과 리포트
                total_processed = saved_count + error_count
                success_rate = (saved_count / total_processed * 100) if total_processed > 0 else 0
                
                self.logger.info(f"💾 저장 완료: {saved_count:,}/{total_processed:,}건 ({success_rate:.1f}%)")
                
                if error_count > 0:
                    self.logger.warning(f"⚠️ 저장 오류: {error_count:,}건")
                
                return saved_count > 0
                
        except Exception as e:
            self.logger.error(f"❌ 데이터베이스 저장 실패: {e}")
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
                self.logger.info(f"\n🔍 {i:,}/{len(companies):,} 처리 중: {company_name} ({stock_code})")
                
                # corp_code 찾기
                if stock_code not in self.stock_to_corp_mapping:
                    self.logger.warning(f"❌ corp_code 없음: {stock_code} {company_name}")
                    fail_count += 1
                    continue
                
                corp_info = self.stock_to_corp_mapping[stock_code]
                corp_code = corp_info['corp_code']
                
                # 재무데이터 수집 (모든 fs_div 유형 시도)
                financial_data = self.get_financial_statements_all_types(corp_code, stock_code, company_name, year)
                
                if financial_data:
                    if self.save_financial_data(financial_data):
                        success_count += 1
                        total_records += len(financial_data)
                        self.logger.info(f"✅ {company_name}: {len(financial_data)}건 저장 성공")
                    else:
                        fail_count += 1
                        self.logger.error(f"❌ {company_name}: 저장 실패")
                else:
                    fail_count += 1
                    self.logger.info(f"📭 {company_name}: 데이터 없음")
                
                # API 제한 대응
                time.sleep(self.request_delay)
                
            except Exception as e:
                self.logger.error(f"❌ 처리 실패: {company_name} - {e}")
                fail_count += 1
                continue
        
        # 최종 결과
        total_processed = success_count + fail_count
        success_rate = (success_count / total_processed * 100) if total_processed > 0 else 0
        
        self.logger.info(f"\n🎉 {year}년 수집 완료!")
        self.logger.info(f"📊 최종 결과: {success_count:,}/{total_processed:,}건 성공 ({success_rate:.1f}%)")
        self.logger.info(f"📈 총 수집 데이터: {total_records:,}건")
        
        if success_count == 0:
            self.logger.error(f"❌ {year}년 재무데이터 수집 실패")
            self.logger.info("💡 가능한 원인:")
            self.logger.info("   1. 해당 연도 재무제표가 아직 제출되지 않음")
            self.logger.info("   2. 종목들이 재무제표 제출 의무가 없는 종목들")
            self.logger.info("   3. 데이터베이스 저장 권한 문제")
        else:
            self.logger.info(f"🎉 성공! {success_count}개 기업의 재무데이터를 수집했습니다")
        
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
    
    parser = argparse.ArgumentParser(description='저장 오류 수정된 DART 재무데이터 수집기')
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
        
        print(f"🚀 DART 재무데이터 수집기 시작 (저장 오류 수정)")
        print(f"📅 대상 연도: {', '.join(map(str, years))}")
        print(f"⏱️ API 간격: {args.delay}초")
        if args.companies:
            print(f"📊 수집 제한: {args.companies:,}개 종목")
        print("🔧 수정사항: 단순화된 스키마 + 강화된 저장 로직")
        print("=" * 70)
        
        collector = DartCollectorSaveFixed(request_delay=args.delay)
        
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
            print("  1. 다른 연도 시도: --year=2022 또는 --year=2021")
            print("  2. 더 큰 기업들 대상: 수집 종목 수 늘리기")
            print("  3. 데이터베이스 권한 확인")
            
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 실행 실패: {e}")


if __name__ == "__main__":
    main()
