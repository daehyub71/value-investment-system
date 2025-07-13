#!/usr/bin/env python3
"""
전 종목 다년도 DART 재무데이터 수집기
2022년~2024년 전 종목 재무제표 체계적 수집

실행 예시:
python multi_year_dart_collector.py --years=2022,2023,2024 --companies=100
python multi_year_dart_collector.py --years=2022,2023 --batch-size=50
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
import zipfile
import io

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import ConfigManager

class MultiYearDartCollector:
    """전 종목 다년도 DART 수집기"""
    
    def __init__(self, request_delay: float = 1.5):
        self.config_manager = ConfigManager()
        self.logger = self.config_manager.get_logger('MultiYearDartCollector')
        
        # API 설정
        dart_config = self.config_manager.get_dart_config()
        self.api_key = dart_config.get('api_key')
        self.base_url = "https://opendart.fss.or.kr/api"
        
        if not self.api_key:
            raise ValueError("DART API 키가 설정되지 않았습니다.")
        
        self.request_delay = request_delay
        self.session = requests.Session()
        self.stock_to_corp_mapping = {}
        
        # 연도별 데이터 가용성 정보
        self.year_availability = {
            2022: {'status': 'complete', 'expected_coverage': 95, 'description': '완전 가능'},
            2023: {'status': 'complete', 'expected_coverage': 90, 'description': '완전 가능'},
            2024: {'status': 'mostly', 'expected_coverage': 80, 'description': '대부분 가능'},
            2025: {'status': 'partial', 'expected_coverage': 0, 'description': '연간 데이터 없음 (분기만)'}
        }
        
        self.logger.info("다년도 DART 수집기 초기화 완료")
    
    def check_year_feasibility(self, years: List[int]) -> Dict[int, Dict]:
        """연도별 수집 가능성 분석"""
        
        print("📊 연도별 데이터 가용성 분석")
        print("=" * 60)
        
        feasible_years = []
        warnings = []
        
        for year in years:
            if year in self.year_availability:
                info = self.year_availability[year]
                print(f"📅 {year}년: {info['description']} (예상 커버리지: {info['expected_coverage']}%)")
                
                if info['status'] in ['complete', 'mostly']:
                    feasible_years.append(year)
                elif info['status'] == 'partial':
                    warnings.append(f"{year}년은 연간 데이터가 없습니다 (분기 데이터만 가능)")
            else:
                if year > 2025:
                    warnings.append(f"{year}년 데이터는 아직 제출되지 않았습니다")
                elif year < 2015:
                    warnings.append(f"{year}년은 너무 오래된 데이터입니다")
        
        if warnings:
            print(f"\n⚠️ 주의사항:")
            for warning in warnings:
                print(f"  - {warning}")
        
        return {
            'feasible_years': feasible_years,
            'warnings': warnings,
            'total_expected_companies': len(feasible_years) * 2000  # 대략적 추정
        }
    
    def download_corp_codes(self) -> bool:
        """DART corp_codes.xml 다운로드"""
        try:
            print("\n📡 DART corp_codes.xml 다운로드 중...")
            
            url = f"{self.base_url}/corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_content = zip_file.read('CORPCODE.xml')
            
            root = ET.fromstring(xml_content)
            
            print("🔍 corp_codes.xml 파싱 중...")
            
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
            
            print(f"✅ 매핑 테이블 생성 완료: {mapping_count:,}개 상장기업")
            return True
            
        except Exception as e:
            self.logger.error(f"corp_codes.xml 다운로드 실패: {e}")
            return False
    
    def get_all_companies_from_info_table(self) -> List[Tuple[str, str]]:
        """company_info에서 전체 종목 가져오기"""
        try:
            stock_conn = self.config_manager.get_database_connection('stock')
            
            cursor = stock_conn.execute('''
                SELECT stock_code, company_name 
                FROM company_info 
                WHERE stock_code IS NOT NULL 
                  AND stock_code != '' 
                ORDER BY stock_code
            ''')
            
            companies = cursor.fetchall()
            stock_conn.close()
            
            company_list = [(row[0], row[1]) for row in companies]
            
            print(f"📋 company_info에서 {len(company_list):,}개 종목 조회 완료")
            return company_list
            
        except Exception as e:
            self.logger.error(f"company_info 테이블 조회 실패: {e}")
            return []
    
    def check_existing_data(self, years: List[int]) -> Dict[int, int]:
        """기존 수집된 데이터 확인"""
        
        try:
            dart_conn = self.config_manager.get_database_connection('dart')
            existing_data = {}
            
            print("\n📊 기존 수집 데이터 확인")
            print("-" * 40)
            
            for year in years:
                cursor = dart_conn.execute('''
                    SELECT COUNT(DISTINCT corp_code) 
                    FROM financial_statements 
                    WHERE year = ? OR bsns_year = ?
                ''', (year, str(year)))
                
                count = cursor.fetchone()[0]
                existing_data[year] = count
                
                print(f"  {year}년: {count:,}개 기업 데이터 보유")
            
            dart_conn.close()
            return existing_data
            
        except Exception as e:
            self.logger.error(f"기존 데이터 확인 실패: {e}")
            return {}
    
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
                
                # 메타데이터 추가
                df['stock_code'] = stock_code
                df['company_name'] = company_name  
                df['year'] = year
                df['collected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"재무제표 조회 오류 ({corp_code}): {e}")
            return pd.DataFrame()
    
    def collect_multi_year_data(self, years: List[int], max_companies: Optional[int] = None, 
                               start_from: int = 0, skip_existing: bool = True) -> bool:
        """다년도 재무제표 데이터 수집"""
        
        try:
            # 1. 연도별 가능성 분석
            feasibility = self.check_year_feasibility(years)
            feasible_years = feasibility['feasible_years']
            
            if not feasible_years:
                print("❌ 수집 가능한 연도가 없습니다.")
                return False
            
            # 2. 기존 데이터 확인
            existing_data = self.check_existing_data(feasible_years)
            
            # 3. corp_codes 다운로드
            if not self.download_corp_codes():
                return False
            
            # 4. 전체 종목 가져오기
            all_companies = self.get_all_companies_from_info_table()
            if not all_companies:
                print("❌ company_info에서 종목을 가져올 수 없습니다.")
                return False
            
            # 5. 수집 대상 결정
            if max_companies:
                target_companies = all_companies[start_from:start_from + max_companies]
            else:
                target_companies = all_companies[start_from:]
            
            # 6. 수집 시작
            total_combinations = len(target_companies) * len(feasible_years)
            
            print(f"\n🚀 다년도 DART 재무제표 수집 시작")
            print(f"📊 대상 종목: {len(target_companies):,}개")
            print(f"📅 대상 연도: {', '.join(map(str, feasible_years))}")
            print(f"🎯 총 수집 조합: {total_combinations:,}개 (종목 × 연도)")
            print(f"⏱️ 예상 소요시간: {total_combinations * self.request_delay / 3600:.1f}시간")
            print("=" * 80)
            
            # 7. 수집 진행
            dart_conn = self.config_manager.get_database_connection('dart')
            
            overall_success = 0
            overall_fail = 0
            year_results = {year: {'success': 0, 'fail': 0, 'records': 0} for year in feasible_years}
            
            for year in feasible_years:
                print(f"\n📅 {year}년 데이터 수집 시작")
                print(f"💾 기존 보유: {existing_data.get(year, 0):,}개 기업")
                print("-" * 60)
                
                year_success = 0
                year_fail = 0
                year_records = 0
                
                for idx, (stock_code, company_name) in enumerate(target_companies):
                    current_combination = (len(feasible_years) * idx) + (feasible_years.index(year) + 1)
                    
                    print(f"📊 진행률: {current_combination:,}/{total_combinations:,} ({current_combination/total_combinations*100:.1f}%) - {year}년 {company_name} ({stock_code})")
                    
                    try:
                        # 기존 데이터 스킵 체크
                        if skip_existing:
                            cursor = dart_conn.execute('''
                                SELECT COUNT(*) FROM financial_statements 
                                WHERE stock_code = ? AND (year = ? OR bsns_year = ?)
                            ''', (stock_code, year, str(year)))
                            
                            if cursor.fetchone()[0] > 0:
                                print(f"  ⏭️ 기존 데이터 존재 - 스킵")
                                continue
                        
                        # corp_code 매핑 확인
                        if stock_code not in self.stock_to_corp_mapping:
                            print(f"  ⚠️ corp_code 매핑 없음")
                            year_fail += 1
                            continue
                        
                        corp_code = self.stock_to_corp_mapping[stock_code]['corp_code']
                        
                        # 재무제표 수집
                        financial_data = self.get_financial_statements(corp_code, stock_code, company_name, year)
                        
                        if not financial_data.empty:
                            # 데이터베이스 저장
                            financial_data.to_sql('financial_statements', dart_conn, 
                                                if_exists='append', index=False)
                            
                            year_success += 1
                            year_records += len(financial_data)
                            print(f"  ✅ 저장: {len(financial_data)}건")
                        else:
                            print(f"  ❌ 데이터 없음")
                            year_fail += 1
                    
                    except Exception as e:
                        print(f"  ❌ 오류: {e}")
                        year_fail += 1
                        continue
                    
                    # 중간 결과 (50개마다)
                    if (idx + 1) % 50 == 0:
                        print(f"\n📈 {year}년 중간 결과 ({idx+1}개 처리):")
                        print(f"  ✅ 성공: {year_success}개")
                        print(f"  ❌ 실패: {year_fail}개")
                        print(f"  📋 수집: {year_records:,}건")
                        print(f"  🎯 성공률: {year_success/(idx+1)*100:.1f}%")
                    
                    time.sleep(self.request_delay)
                
                # 연도별 결과 저장
                year_results[year] = {
                    'success': year_success,
                    'fail': year_fail,
                    'records': year_records
                }
                
                overall_success += year_success
                overall_fail += year_fail
                
                print(f"\n✅ {year}년 수집 완료:")
                print(f"  📊 성공: {year_success:,}개 기업")
                print(f"  📋 수집: {year_records:,}건 재무데이터")
                print(f"  🎯 성공률: {year_success/(year_success+year_fail)*100:.1f}%")
            
            dart_conn.commit()
            dart_conn.close()
            
            # 최종 결과 출력
            print(f"\n🎉 다년도 DART 수집 완료!")
            print("=" * 80)
            print(f"📊 전체 결과:")
            print(f"  📋 총 처리: {overall_success + overall_fail:,}개 조합")
            print(f"  ✅ 성공: {overall_success:,}개")
            print(f"  ❌ 실패: {overall_fail:,}개")
            print(f"  🎯 전체 성공률: {overall_success/(overall_success+overall_fail)*100:.1f}%")
            
            print(f"\n📅 연도별 상세 결과:")
            total_records = 0
            for year, result in year_results.items():
                print(f"  {year}년: {result['success']:,}개 기업, {result['records']:,}건 데이터")
                total_records += result['records']
            
            print(f"\n📈 총 수집 데이터: {total_records:,}건")
            
            if overall_success > 0:
                print(f"\n✅ 다년도 수집이 성공적으로 완료되었습니다!")
                print(f"📊 다음 단계: 워런 버핏 스코어카드 계산")
            
            return overall_success > 0
            
        except Exception as e:
            self.logger.error(f"다년도 수집 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='전 종목 다년도 DART 수집기')
    parser.add_argument('--years', type=str, required=True,
                       help='수집할 연도들 (쉼표로 구분, 예: 2022,2023,2024)')
    parser.add_argument('--companies', type=int,
                       help='수집할 기업 수 (기본: 전체)')
    parser.add_argument('--delay', type=float, default=1.5,
                       help='요청 간격 (초, 기본: 1.5)')
    parser.add_argument('--start-from', type=int, default=0,
                       help='시작 위치 (기본: 0)')
    parser.add_argument('--include-existing', action='store_true',
                       help='기존 데이터도 다시 수집')
    
    args = parser.parse_args()
    
    try:
        # 연도 파싱
        years = [int(year.strip()) for year in args.years.split(',')]
        
        print(f"🚀 전 종목 다년도 DART 수집기 시작")
        print(f"📅 대상 연도: {', '.join(map(str, years))}")
        print(f"⏱️ API 간격: {args.delay}초")
        if args.companies:
            print(f"📊 수집 제한: {args.companies:,}개 종목")
        if args.start_from > 0:
            print(f"📍 시작 위치: {args.start_from:,}번째부터")
        print("=" * 80)
        
        collector = MultiYearDartCollector(request_delay=args.delay)
        
        success = collector.collect_multi_year_data(
            years=years,
            max_companies=args.companies,
            start_from=args.start_from,
            skip_existing=not args.include_existing
        )
        
        if success:
            print("\n✅ 다년도 데이터 수집이 완료되었습니다!")
        else:
            print("\n❌ 데이터 수집에 실패했습니다.")
            
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 실행 실패: {e}")

if __name__ == "__main__":
    main()