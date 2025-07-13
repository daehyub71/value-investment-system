#!/usr/bin/env python3
"""
분기+연간 데이터 통합 DART 수집기
연간 사업보고서 + 분기보고서 모두 수집

실행 예시:
python quarterly_annual_dart_collector.py --years=2022,2023 --reports=annual,quarterly --companies=50
python quarterly_annual_dart_collector.py --years=2024 --reports=quarterly --companies=100
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

class QuarterlyAnnualDartCollector:
    """분기+연간 데이터 통합 DART 수집기"""
    
    def __init__(self, request_delay: float = 1.5):
        self.config_manager = ConfigManager()
        self.logger = self.config_manager.get_logger('QuarterlyAnnualDartCollector')
        
        # API 설정
        dart_config = self.config_manager.get_dart_config()
        self.api_key = dart_config.get('api_key')
        self.base_url = "https://opendart.fss.or.kr/api"
        
        if not self.api_key:
            raise ValueError("DART API 키가 설정되지 않았습니다.")
        
        self.request_delay = request_delay
        self.session = requests.Session()
        self.stock_to_corp_mapping = {}
        
        # 보고서 유형 정의
        self.report_types = {
            'annual': {
                'code': '11011',
                'name': '사업보고서',
                'quarter': 'Y',
                'description': '연간 재무제표'
            },
            'q1': {
                'code': '11013', 
                'name': '1분기보고서',
                'quarter': 'Q1',
                'description': '1분기 재무제표'
            },
            'q2': {
                'code': '11012',
                'name': '반기보고서', 
                'quarter': 'Q2',
                'description': '2분기/반기 재무제표'
            },
            'q3': {
                'code': '11014',
                'name': '3분기보고서',
                'quarter': 'Q3', 
                'description': '3분기 재무제표'
            }
        }
        
        # 연도별 가용성
        self.availability = {
            2022: {'annual': True, 'quarterly': True, 'coverage': '95%'},
            2023: {'annual': True, 'quarterly': True, 'coverage': '90%'},
            2024: {'annual': True, 'quarterly': True, 'coverage': '85%'},
            2025: {'annual': False, 'quarterly': True, 'coverage': '분기만'}
        }
        
        self.logger.info("분기+연간 DART 수집기 초기화 완료")
    
    def show_collection_plan(self, years: List[int], report_types: List[str]) -> None:
        """수집 계획 표시"""
        
        print("📋 수집 계획 분석")
        print("=" * 70)
        
        total_combinations = 0
        
        for year in years:
            print(f"\n📅 {year}년:")
            year_combinations = 0
            
            if 'annual' in report_types:
                if self.availability.get(year, {}).get('annual', False):
                    print(f"  ✅ 연간 데이터 (사업보고서)")
                    year_combinations += 1
                else:
                    print(f"  ❌ 연간 데이터 없음")
            
            if 'quarterly' in report_types:
                if self.availability.get(year, {}).get('quarterly', False):
                    quarters = ['Q1', 'Q2', 'Q3'] if year == 2025 else ['Q1', 'Q2', 'Q3'] 
                    print(f"  ✅ 분기 데이터 ({', '.join(quarters)})")
                    year_combinations += len(quarters)
                else:
                    print(f"  ❌ 분기 데이터 없음")
            
            coverage = self.availability.get(year, {}).get('coverage', 'Unknown')
            print(f"  📊 예상 커버리지: {coverage}")
            print(f"  🎯 연도별 수집 조합: {year_combinations}개")
            
            total_combinations += year_combinations
        
        print(f"\n📈 전체 수집 조합: {total_combinations}개 (연도별 보고서 유형)")
        
        # 예상 소요시간 계산
        companies_estimate = 2500  # 대략적 추정
        total_requests = companies_estimate * total_combinations
        estimated_hours = total_requests * self.request_delay / 3600
        
        print(f"⏱️ 예상 소요시간: {estimated_hours:.1f}시간 (약 {companies_estimate:,}개 기업 기준)")
    
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
            return company_list
            
        except Exception as e:
            self.logger.error(f"company_info 테이블 조회 실패: {e}")
            return []
    
    def get_financial_statements(self, corp_code: str, stock_code: str, company_name: str, 
                               year: int, report_type: str) -> pd.DataFrame:
        """재무제표 데이터 가져오기 (분기/연간 구분)"""
        try:
            report_info = self.report_types[report_type]
            
            url = f"{self.base_url}/fnlttSinglAcnt.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': str(year),
                'reprt_code': report_info['code']
            }
            
            response = self.session.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get('status') == '000' and data.get('list'):
                df = pd.DataFrame(data['list'])
                
                # 메타데이터 추가
                df['stock_code'] = stock_code
                df['company_name'] = company_name  
                df['year'] = year
                df['quarter'] = report_info['quarter']
                df['collected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"재무제표 조회 오류 ({corp_code}, {report_type}): {e}")
            return pd.DataFrame()
    
    def collect_comprehensive_data(self, years: List[int], report_types: List[str], 
                                 max_companies: Optional[int] = None, start_from: int = 0) -> bool:
        """포괄적 재무데이터 수집 (분기+연간)"""
        
        try:
            # 1. 수집 계획 표시
            self.show_collection_plan(years, report_types)
            
            # 2. corp_codes 다운로드
            if not self.download_corp_codes():
                return False
            
            # 3. 전체 종목 가져오기
            all_companies = self.get_all_companies_from_info_table()
            if not all_companies:
                print("❌ company_info에서 종목을 가져올 수 없습니다.")
                return False
            
            # 4. 수집 대상 결정
            if max_companies:
                target_companies = all_companies[start_from:start_from + max_companies]
            else:
                target_companies = all_companies[start_from:]
            
            print(f"\n🚀 포괄적 DART 재무제표 수집 시작")
            print(f"📊 대상 종목: {len(target_companies):,}개")
            print(f"📅 대상 연도: {', '.join(map(str, years))}")
            print(f"📋 수집 유형: {', '.join(report_types)}")
            print("=" * 80)
            
            # 5. 수집 진행
            dart_conn = self.config_manager.get_database_connection('dart')
            
            overall_stats = {
                'total_attempts': 0,
                'total_success': 0,
                'total_records': 0,
                'by_year': {},
                'by_type': {}
            }
            
            for year in years:
                print(f"\n📅 {year}년 데이터 수집 시작")
                print("=" * 60)
                
                year_stats = {'success': 0, 'fail': 0, 'records': 0}
                
                # 연간 데이터 수집
                if 'annual' in report_types and self.availability.get(year, {}).get('annual', False):
                    print(f"\n📊 {year}년 연간 데이터 (사업보고서) 수집")
                    stats = self._collect_by_report_type(dart_conn, target_companies, year, 'annual')
                    year_stats['success'] += stats['success']
                    year_stats['fail'] += stats['fail'] 
                    year_stats['records'] += stats['records']
                
                # 분기 데이터 수집
                if 'quarterly' in report_types and self.availability.get(year, {}).get('quarterly', False):
                    quarters = ['q1', 'q2', 'q3'] if year == 2025 else ['q1', 'q2', 'q3']
                    
                    for quarter in quarters:
                        print(f"\n📊 {year}년 {quarter.upper()} 데이터 수집")
                        stats = self._collect_by_report_type(dart_conn, target_companies, year, quarter)
                        year_stats['success'] += stats['success']
                        year_stats['fail'] += stats['fail']
                        year_stats['records'] += stats['records']
                
                overall_stats['by_year'][year] = year_stats
                overall_stats['total_success'] += year_stats['success']
                overall_stats['total_records'] += year_stats['records']
                
                print(f"\n✅ {year}년 수집 완료:")
                print(f"  📊 성공: {year_stats['success']:,}개")
                print(f"  📋 수집: {year_stats['records']:,}건")
            
            dart_conn.commit()
            dart_conn.close()
            
            # 최종 결과
            self._show_final_results(overall_stats, years, report_types)
            
            return overall_stats['total_success'] > 0
            
        except Exception as e:
            self.logger.error(f"포괄적 데이터 수집 실패: {e}")
            return False
    
    def _collect_by_report_type(self, conn, companies: List[Tuple[str, str]], 
                               year: int, report_type: str) -> Dict:
        """특정 보고서 유형별 수집"""
        
        stats = {'success': 0, 'fail': 0, 'records': 0}
        report_info = self.report_types[report_type]
        
        for idx, (stock_code, company_name) in enumerate(companies):
            
            try:
                if stock_code not in self.stock_to_corp_mapping:
                    stats['fail'] += 1
                    continue
                
                corp_code = self.stock_to_corp_mapping[stock_code]['corp_code']
                
                # 기존 데이터 체크
                cursor = conn.execute('''
                    SELECT COUNT(*) FROM financial_statements 
                    WHERE stock_code = ? AND year = ? AND quarter = ?
                ''', (stock_code, year, report_info['quarter']))
                
                if cursor.fetchone()[0] > 0:
                    continue  # 이미 존재하는 데이터 스킵
                
                # 재무제표 수집
                financial_data = self.get_financial_statements(corp_code, stock_code, company_name, year, report_type)
                
                if not financial_data.empty:
                    financial_data.to_sql('financial_statements', conn, if_exists='append', index=False)
                    stats['success'] += 1
                    stats['records'] += len(financial_data)
                    
                    if idx % 100 == 0:
                        print(f"  진행률: {idx+1}/{len(companies)} - {company_name}: ✅ {len(financial_data)}건")
                else:
                    stats['fail'] += 1
                    
            except Exception as e:
                stats['fail'] += 1
                continue
            
            time.sleep(self.request_delay)
        
        return stats
    
    def _show_final_results(self, stats: Dict, years: List[int], report_types: List[str]) -> None:
        """최종 결과 출력"""
        
        print(f"\n🎉 포괄적 DART 수집 완료!")
        print("=" * 80)
        print(f"📊 전체 결과:")
        print(f"  ✅ 총 성공: {stats['total_success']:,}개")
        print(f"  📋 총 수집: {stats['total_records']:,}건 재무데이터")
        
        print(f"\n📅 연도별 결과:")
        for year, year_stats in stats['by_year'].items():
            print(f"  {year}년: {year_stats['success']:,}개 성공, {year_stats['records']:,}건 수집")
        
        print(f"\n📋 수집된 데이터 유형:")
        for report_type in report_types:
            if report_type == 'annual':
                print(f"  📊 연간 데이터 (사업보고서)")
            elif report_type == 'quarterly':  
                print(f"  📊 분기 데이터 (1분기, 반기, 3분기)")
        
        print(f"\n✅ 이제 분기별 + 연간 상세 분석이 가능합니다!")

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='분기+연간 데이터 통합 DART 수집기')
    parser.add_argument('--years', type=str, required=True,
                       help='수집할 연도들 (쉼표로 구분, 예: 2022,2023,2024)')
    parser.add_argument('--reports', type=str, default='annual,quarterly',
                       help='수집할 보고서 유형 (annual, quarterly, 또는 둘 다)')
    parser.add_argument('--companies', type=int,
                       help='수집할 기업 수 (기본: 전체)')
    parser.add_argument('--delay', type=float, default=1.5,
                       help='요청 간격 (초, 기본: 1.5)')
    parser.add_argument('--start-from', type=int, default=0,
                       help='시작 위치 (기본: 0)')
    
    args = parser.parse_args()
    
    try:
        # 입력 파싱
        years = [int(year.strip()) for year in args.years.split(',')]
        report_types = [rtype.strip() for rtype in args.reports.split(',')]
        
        print(f"🚀 분기+연간 데이터 통합 DART 수집기 시작")
        print(f"📅 대상 연도: {', '.join(map(str, years))}")
        print(f"📋 보고서 유형: {', '.join(report_types)}")
        print(f"⏱️ API 간격: {args.delay}초")
        if args.companies:
            print(f"📊 수집 제한: {args.companies:,}개 종목")
        print("=" * 80)
        
        collector = QuarterlyAnnualDartCollector(request_delay=args.delay)
        
        success = collector.collect_comprehensive_data(
            years=years,
            report_types=report_types,
            max_companies=args.companies,
            start_from=args.start_from
        )
        
        if success:
            print("\n✅ 분기+연간 데이터 수집이 완료되었습니다!")
        else:
            print("\n❌ 데이터 수집에 실패했습니다.")
            
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 실행 실패: {e}")

if __name__ == "__main__":
    main()