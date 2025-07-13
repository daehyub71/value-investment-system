#!/usr/bin/env python3
"""
Company_Info 테이블 기반 전 종목 DART 재무제표 수집기

특징:
- stock_data.db의 company_info에서 전체 종목 리스트 가져오기
- DART corp_codes.xml로 stock_code → corp_code 매핑
- 전 종목 재무제표 체계적 수집
- 진행률 및 성공률 실시간 표시
"""

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
from functools import wraps
import zipfile
import io

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import ConfigManager

class CompanyInfoBasedDartCollector:
    """Company_Info 테이블 기반 전 종목 DART 수집기"""
    
    def __init__(self, request_delay: float = 1.5):
        self.config_manager = ConfigManager()
        self.logger = self.config_manager.get_logger('CompanyInfoDartCollector')
        
        # API 설정
        dart_config = self.config_manager.get_dart_config()
        self.api_key = dart_config.get('api_key')
        self.base_url = "https://opendart.fss.or.kr/api"
        
        if not self.api_key:
            raise ValueError("DART API 키가 설정되지 않았습니다.")
        
        self.request_delay = request_delay
        self.session = requests.Session()
        
        # stock_code → corp_code 매핑 테이블
        self.stock_to_corp_mapping = {}
        
        self.logger.info("Company_Info 기반 전 종목 DART 수집기 초기화 완료")
    
    def check_company_info_table(self) -> Tuple[bool, int]:
        """company_info 테이블 존재 여부 및 레코드 수 확인"""
        try:
            stock_conn = self.config_manager.get_database_connection('stock')
            
            # 테이블 존재 확인
            cursor = stock_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='company_info'"
            )
            table_exists = cursor.fetchone() is not None
            
            if not table_exists:
                stock_conn.close()
                return False, 0
            
            # 레코드 수 확인
            cursor = stock_conn.execute("SELECT COUNT(*) FROM company_info")
            record_count = cursor.fetchone()[0]
            
            # 샘플 데이터 확인
            cursor = stock_conn.execute("SELECT * FROM company_info LIMIT 3")
            sample_data = cursor.fetchall()
            
            stock_conn.close()
            
            print(f"📊 company_info 테이블 현황:")
            print(f"  - 총 레코드 수: {record_count:,}개")
            print(f"  - 샘플 데이터:")
            
            for row in sample_data:
                # row는 sqlite3.Row 객체이므로 컬럼명으로 접근 가능
                print(f"    {row[1]} ({row[0]})")  # company_name (stock_code)
            
            return True, record_count
            
        except Exception as e:
            self.logger.error(f"company_info 테이블 확인 실패: {e}")
            return False, 0
    
    def download_corp_codes(self) -> bool:
        """DART corp_codes.xml 다운로드 및 매핑 테이블 생성"""
        try:
            print("📡 DART corp_codes.xml 다운로드 중...")
            
            url = f"{self.base_url}/corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # ZIP 파일 압축 해제
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_content = zip_file.read('CORPCODE.xml')
            
            # XML 파싱
            root = ET.fromstring(xml_content)
            
            print("🔍 corp_codes.xml 파싱 및 매핑 테이블 생성 중...")
            
            mapping_count = 0
            for corp in root.findall('list'):
                corp_code = corp.find('corp_code').text if corp.find('corp_code') is not None else None
                stock_code = corp.find('stock_code').text if corp.find('stock_code') is not None else None
                corp_name = corp.find('corp_name').text if corp.find('corp_name') is not None else None
                
                # stock_code가 있는 경우만 매핑에 추가 (상장기업)
                if corp_code and stock_code and stock_code.strip():
                    self.stock_to_corp_mapping[stock_code.strip()] = {
                        'corp_code': corp_code.strip(),
                        'corp_name': corp_name.strip() if corp_name else ''
                    }
                    mapping_count += 1
            
            print(f"✅ 매핑 테이블 생성 완료: {mapping_count:,}개 상장기업")
            
            # DART 데이터베이스에 저장
            self._save_corp_codes_to_db()
            
            return True
            
        except Exception as e:
            self.logger.error(f"corp_codes.xml 다운로드 실패: {e}")
            print(f"❌ corp_codes.xml 다운로드 실패: {e}")
            return False
    
    def _save_corp_codes_to_db(self):
        """corp_codes 매핑을 데이터베이스에 저장"""
        try:
            dart_conn = self.config_manager.get_database_connection('dart')
            
            # corp_codes 테이블 생성 (존재하지 않는 경우)
            dart_conn.execute('''
                CREATE TABLE IF NOT EXISTS corp_codes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    corp_code TEXT UNIQUE NOT NULL,
                    corp_name TEXT NOT NULL,
                    stock_code TEXT,
                    modify_date TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 기존 데이터 삭제
            dart_conn.execute("DELETE FROM corp_codes")
            
            # 새 데이터 삽입
            for stock_code, info in self.stock_to_corp_mapping.items():
                dart_conn.execute('''
                    INSERT INTO corp_codes (corp_code, corp_name, stock_code, modify_date)
                    VALUES (?, ?, ?, ?)
                ''', (
                    info['corp_code'],
                    info['corp_name'],
                    stock_code,
                    datetime.now().strftime('%Y%m%d')
                ))
            
            dart_conn.commit()
            dart_conn.close()
            
            print(f"💾 corp_codes 테이블에 {len(self.stock_to_corp_mapping):,}개 저장 완료")
            
        except Exception as e:
            self.logger.error(f"corp_codes 데이터베이스 저장 실패: {e}")
    
    def get_all_companies_from_info_table(self) -> List[Tuple[str, str]]:
        """company_info 테이블에서 전체 종목 리스트 가져오기"""
        try:
            stock_conn = self.config_manager.get_database_connection('stock')
            
            # 전체 종목 조회 (stock_code와 company_name)
            cursor = stock_conn.execute('''
                SELECT stock_code, company_name 
                FROM company_info 
                WHERE stock_code IS NOT NULL 
                  AND stock_code != '' 
                ORDER BY stock_code
            ''')
            
            companies = cursor.fetchall()
            stock_conn.close()
            
            # Tuple 형태로 변환
            company_list = [(row[0], row[1]) for row in companies]
            
            print(f"📋 company_info에서 {len(company_list):,}개 종목 조회 완료")
            return company_list
            
        except Exception as e:
            self.logger.error(f"company_info 테이블 조회 실패: {e}")
            return []
    
    def get_financial_statements(self, corp_code: str, stock_code: str, company_name: str, year: int) -> pd.DataFrame:
        """재무제표 데이터 가져오기"""
        try:
            url = f"{self.base_url}/fnlttSinglAcnt.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': str(year),
                'reprt_code': '11011'  # 사업보고서
            }
            
            response = self.session.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get('status') == '000' and data.get('list'):
                df = pd.DataFrame(data['list'])
                
                # 메타데이터 추가 (스키마
    
    def collect_all_companies_financial_data(self, year: int = 2022, 
                                           max_companies: Optional[int] = None,
                                           start_from: int = 0) -> bool:
        """전체 종목 재무제표 데이터 수집"""
        try:
            # 1. company_info 테이블 확인
            table_exists, total_companies = self.check_company_info_table()
            if not table_exists:
                print("❌ company_info 테이블이 존재하지 않습니다.")
                return False
            
            # 2. corp_codes 다운로드 및 매핑 생성
            if not self.download_corp_codes():
                return False
            
            # 3. 전체 종목 리스트 가져오기
            all_companies = self.get_all_companies_from_info_table()
            if not all_companies:
                print("❌ company_info에서 종목을 가져올 수 없습니다.")
                return False
            
            # 4. 수집 대상 결정
            if max_companies:
                target_companies = all_companies[start_from:start_from + max_companies]
            else:
                target_companies = all_companies[start_from:]
            
            print(f"\n🚀 전체 종목 재무제표 수집 시작")
            print(f"📊 대상: {len(target_companies):,}개 종목 (전체 {len(all_companies):,}개 중)")
            print(f"📅 수집 연도: {year}년")
            print(f"⏱️ 예상 소요시간: {len(target_companies) * self.request_delay / 60:.0f}분")
            print("=" * 70)
            
            # 5. 실제 수집 진행
            success_count = 0
            mapping_fail_count = 0
            api_fail_count = 0
            total_financial_records = 0
            
            dart_conn = self.config_manager.get_database_connection('dart')
            
            for idx, (stock_code, company_name) in enumerate(target_companies):
                print(f"\n📊 진행률: {idx+1:,}/{len(target_companies):,} ({idx+1/len(target_companies)*100:.1f}%) - {company_name} ({stock_code})")
                
                try:
                    # corp_code 매핑 확인
                    if stock_code not in self.stock_to_corp_mapping:
                        print(f"  ⚠️ corp_code 매핑 없음")
                        mapping_fail_count += 1
                        continue
                    
                    corp_code = self.stock_to_corp_mapping[stock_code]['corp_code']
                    
                    # 재무제표 수집
                    financial_data = self.get_financial_statements(corp_code, year)
                    
                    if not financial_data.empty:
                        # 데이터베이스 저장
                        financial_data.to_sql('financial_statements', dart_conn, 
                                            if_exists='append', index=False)
                        
                        success_count += 1
                        total_financial_records += len(financial_data)
                        print(f"  ✅ 재무데이터 저장: {len(financial_data)}건")
                    else:
                        print(f"  ❌ 재무데이터 없음")
                        api_fail_count += 1
                
                except Exception as e:
                    print(f"  ❌ 처리 실패: {e}")
                    api_fail_count += 1
                    continue
                
                # 진행상황 중간 출력
                if (idx + 1) % 50 == 0:
                    print(f"\n📈 중간 결과 ({idx+1:,}개 처리):")
                    print(f"  ✅ 성공: {success_count}개")
                    print(f"  ⚠️ 매핑실패: {mapping_fail_count}개")
                    print(f"  ❌ API실패: {api_fail_count}개")
                    print(f"  📋 총 수집: {total_financial_records:,}건")
                    print(f"  🎯 성공률: {success_count/(idx+1)*100:.1f}%")
                
                # API 호출 제한
                time.sleep(self.request_delay)
            
            dart_conn.commit()
            dart_conn.close()
            
            # 최종 결과
            print(f"\n🎉 전체 종목 재무제표 수집 완료!")
            print("=" * 70)
            print(f"📊 처리 결과:")
            print(f"  📋 총 처리: {len(target_companies):,}개 종목")
            print(f"  ✅ 성공: {success_count:,}개 ({success_count/len(target_companies)*100:.1f}%)")
            print(f"  ⚠️ 매핑실패: {mapping_fail_count:,}개")
            print(f"  ❌ API실패: {api_fail_count:,}개")
            print(f"  📈 총 수집: {total_financial_records:,}건 재무데이터")
            
            if success_count > 0:
                print(f"\n✅ 성공적으로 완료되었습니다!")
                print(f"📊 다음 단계: 워런 버핏 스코어카드 계산")
                print(f"python scripts/analysis/run_buffett_analysis.py --all_stocks")
            
            self.logger.info(f"전체 종목 수집 완료: {success_count}/{len(target_companies)}개 성공")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"전체 종목 수집 실패: {e}")
            return False
    
    def collect_batch_companies(self, batch_size: int = 100, year: int = 2022) -> bool:
        """배치 단위로 종목 수집 (대량 데이터 처리용)"""
        try:
            # 전체 종목 수 확인
            table_exists, total_companies = self.check_company_info_table()
            if not table_exists:
                return False
            
            print(f"🚀 배치 수집 모드: {batch_size}개씩 처리")
            print(f"📊 전체 예상 배치 수: {total_companies // batch_size + 1}개")
            
            current_start = 0
            batch_num = 1
            
            while current_start < total_companies:
                print(f"\n📦 배치 {batch_num} 시작 (위치: {current_start+1:,}~{min(current_start+batch_size, total_companies):,})")
                
                success = self.collect_all_companies_financial_data(
                    year=year,
                    max_companies=batch_size,
                    start_from=current_start
                )
                
                if not success:
                    print(f"❌ 배치 {batch_num} 실패")
                
                current_start += batch_size
                batch_num += 1
                
                # 배치 간 대기
                if current_start < total_companies:
                    print(f"⏱️ 다음 배치까지 30초 대기...")
                    time.sleep(30)
            
            return True
            
        except Exception as e:
            self.logger.error(f"배치 수집 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Company_Info 기반 전 종목 DART 수집기')
    parser.add_argument('--companies', type=int, 
                       help='수집할 기업 수 (기본: 전체)')
    parser.add_argument('--year', type=int, default=2022,
                       help='수집할 연도 (기본: 2022)')
    parser.add_argument('--delay', type=float, default=1.5,
                       help='요청 간격 (초, 기본: 1.5)')
    parser.add_argument('--start-from', type=int, default=0,
                       help='시작 위치 (기본: 0)')
    parser.add_argument('--batch-mode', action='store_true',
                       help='배치 모드 활성화 (100개씩 처리)')
    
    args = parser.parse_args()
    
    try:
        print(f"🚀 Company_Info 기반 전 종목 DART 수집기 시작")
        print(f"📅 수집 연도: {args.year}")
        print(f"⏱️ API 간격: {args.delay}초")
        if args.companies:
            print(f"📊 수집 제한: {args.companies:,}개 종목")
        if args.start_from > 0:
            print(f"📍 시작 위치: {args.start_from:,}번째부터")
        print("=" * 70)
        
        collector = CompanyInfoBasedDartCollector(request_delay=args.delay)
        
        if args.batch_mode:
            success = collector.collect_batch_companies(batch_size=100, year=args.year)
        else:
            success = collector.collect_all_companies_financial_data(
                year=args.year,
                max_companies=args.companies,
                start_from=args.start_from
            )
        
        if success:
            print("\n✅ 전체 종목 데이터 수집이 완료되었습니다!")
        else:
            print("\n❌ 데이터 수집에 실패했습니다.")
            
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 실행 실패: {e}")

if __name__ == "__main__":
    main()