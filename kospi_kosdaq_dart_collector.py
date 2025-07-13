#!/usr/bin/env python3
"""
KOSPI/KOSDAQ 상장기업 전용 DART 데이터 수집기
재무제표가 있는 기업만 선별하여 효율적으로 수집

특징:
- KOSPI/KOSDAQ 상장기업만 대상
- 재무제표 존재 여부 사전 확인
- 데이터 없는 기업 자동 스킵
- 상장기업 우선순위 정렬 (시가총액 기준)
"""

import sys
import time
import requests
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import logging
from functools import wraps
import json

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import ConfigManager

class ListedCompanyDartCollector:
    """KOSPI/KOSDAQ 상장기업 전용 DART 수집기"""
    
    def __init__(self, request_delay: float = 2.0):
        self.config_manager = ConfigManager()
        self.logger = self.config_manager.get_logger('ListedCompanyDartCollector')
        
        # API 설정
        dart_config = self.config_manager.get_dart_config()
        self.api_key = dart_config.get('api_key')
        self.base_url = "https://opendart.fss.or.kr/api"
        
        if not self.api_key:
            raise ValueError("DART API 키가 설정되지 않았습니다.")
        
        self.request_delay = request_delay
        self.session = requests.Session()
        
        # 주요 KOSPI/KOSDAQ 기업 목록 (시가총액 상위부터)
        self.major_listed_companies = self._get_major_listed_companies()
        
        self.logger.info("KOSPI/KOSDAQ 상장기업 전용 DART 수집기 초기화 완료")
    
    def _get_major_listed_companies(self) -> List[Tuple[str, str, str, str]]:
        """주요 KOSPI/KOSDAQ 상장기업 목록 반환 (corp_code, stock_code, name, market)"""
        return [
            # KOSPI 대형주 (시가총액 순)
            ('00126380', '005930', '삼성전자', 'KOSPI'),
            ('00164779', '000660', 'SK하이닉스', 'KOSPI'),
            ('00401731', '035420', 'NAVER', 'KOSPI'),
            ('00138167', '005380', '현대자동차', 'KOSPI'),
            ('00114991', '006400', '삼성SDI', 'KOSPI'),
            ('00166971', '005490', 'POSCO홀딩스', 'KOSPI'),
            ('00117967', '051910', 'LG화학', 'KOSPI'),
            ('00140312', '035720', '카카오', 'KOSPI'),
            ('00139967', '003550', 'LG', 'KOSPI'),
            ('00101312', '012330', '현대모비스', 'KOSPI'),
            ('00126701', '000270', '기아', 'KOSPI'),
            ('00113570', '096770', 'SK이노베이션', 'KOSPI'),
            ('00164445', '009150', '삼성전기', 'KOSPI'),
            ('00164470', '010130', '고려아연', 'KOSPI'),
            ('00152467', '034730', 'SK', 'KOSPI'),
            ('00191965', '018260', '삼성에스디에스', 'KOSPI'),
            ('00117629', '066570', 'LG전자', 'KOSPI'),
            ('00111467', '017670', 'SK텔레콤', 'KOSPI'),
            ('00164394', '032830', '삼성생명', 'KOSPI'),
            ('00117733', '105560', 'KB금융', 'KOSPI'),
            
            # KOSPI 중형주
            ('00101592', '003490', '대한항공', 'KOSPI'),
            ('00113931', '015760', '한국전력', 'KOSPI'),
            ('00100840', '009540', 'HD한국조선해양', 'KOSPI'),
            ('00164906', '047050', '포스코인터내셔널', 'KOSPI'),
            ('00122975', '028260', '삼성물산', 'KOSPI'),
            ('00148460', '055550', '신한지주', 'KOSPI'),
            ('00191965', '018260', '삼성에스디에스', 'KOSPI'),
            ('00147572', '352820', 'HYBE', 'KOSPI'),
            ('00102385', '002790', '아모레G', 'KOSPI'),
            ('00154449', '000720', '현대건설', 'KOSPI'),
            
            # KOSDAQ 대형주
            ('00256624', '086520', '에코프로', 'KOSDAQ'),
            ('00892071', '247540', '에코프로비엠', 'KOSDAQ'),
            ('00430886', '091990', ' 셀트리온헬스케어', 'KOSDAQ'),
            ('00164779', '196170', '알테오젠', 'KOSDAQ'),
            ('00125097', '058470', '리노공업', 'KOSDAQ'),
            ('00351090', '121600', '나노신소재', 'KOSDAQ'),
            ('00352984', '112040', '위메이드', 'KOSDAQ'),
            ('00893302', '357780', '솔브레인', 'KOSDAQ'),
            ('00101826', '039030', '이오테크닉스', 'KOSDAQ'),
            ('00118031', '084370', '유진테크', 'KOSDAQ'),
            
            # KOSDAQ 중형주
            ('00110923', '067310', '하나마이크론', 'KOSDAQ'),
            ('00159838', '036930', '주성엔지니어링', 'KOSDAQ'),
            ('00289527', '094170', '동운아나텍', 'KOSDAQ'),
            ('00351694', '166090', '하나머티리얼즈', 'KOSDAQ'),
            ('00101826', '048410', '현대바이오', 'KOSDAQ'),
            ('00124524', '065350', '신성델타테크', 'KOSDAQ'),
            ('00892071', '393890', '더블유씨피', 'KOSDAQ'),
            ('00154449', '450080', '에코프로머티', 'KOSDAQ'),
            ('00351090', '293490', '카카오게임즈', 'KOSDAQ'),
            ('00125097', '900140', '엘브이엠씨', 'KOSDAQ')
        ]
    
    def check_financial_data_availability(self, corp_code: str, year: int) -> bool:
        """재무제표 데이터 존재 여부 확인"""
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
            
            if data.get('status') == '000':
                # 재무제표 데이터가 있는 경우
                return len(data.get('list', [])) > 0
            elif data.get('status') == '013':
                # 조회된 데이터가 없는 경우
                return False
            else:
                # 기타 오류
                self.logger.warning(f"재무데이터 확인 실패 ({corp_code}): {data.get('message', 'Unknown error')}")
                return False
                
        except Exception as e:
            self.logger.error(f"재무데이터 확인 오류 ({corp_code}): {e}")
            return False
    
    def get_company_outline(self, corp_code: str) -> Optional[Dict]:
        """기업 개요 정보 가져오기"""
        try:
            url = f"{self.base_url}/company.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code
            }
            
            response = self.session.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get('status') == '000':
                return data
            else:
                self.logger.warning(f"기업개요 조회 실패 ({corp_code}): {data.get('message', 'Unknown')}")
                return None
                
        except Exception as e:
            self.logger.error(f"기업개요 조회 오류 ({corp_code}): {e}")
            return None
    
    def get_financial_statements(self, corp_code: str, year: int) -> pd.DataFrame:
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
                df['corp_code'] = corp_code
                df['year'] = year
                df['collected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"재무제표 조회 오류 ({corp_code}): {e}")
            return pd.DataFrame()
    
    def filter_companies_with_financial_data(self, companies: List[Tuple], year: int) -> List[Tuple]:
        """재무제표가 있는 기업만 필터링"""
        valid_companies = []
        
        print(f"🔍 {len(companies)}개 기업의 재무데이터 존재 여부 확인 중... (연도: {year})")
        print("-" * 60)
        
        for i, (corp_code, stock_code, company_name, market) in enumerate(companies):
            print(f"  {i+1:2d}/{len(companies)} - {company_name} ({market}): ", end="", flush=True)
            
            if self.check_financial_data_availability(corp_code, year):
                print("✅ 재무데이터 있음")
                valid_companies.append((corp_code, stock_code, company_name, market))
            else:
                print("❌ 재무데이터 없음")
            
            # API 호출 제한 고려
            time.sleep(self.request_delay)
        
        print(f"\n📊 결과: {len(valid_companies)}/{len(companies)}개 기업에서 재무데이터 확인")
        return valid_companies
    
    def collect_listed_companies_data(self, year: int = 2022, max_companies: int = 20, 
                                    filter_financial_data: bool = True) -> bool:
        """상장기업 DART 데이터 수집"""
        try:
            self.logger.info(f"상장기업 DART 데이터 수집 시작: {year}년, 최대 {max_companies}개")
            
            # 대상 기업 선정
            target_companies = self.major_listed_companies[:max_companies]
            
            # 재무데이터 필터링 (선택적)
            if filter_financial_data:
                target_companies = self.filter_companies_with_financial_data(target_companies, year)
                
                if not target_companies:
                    print("❌ 재무데이터가 있는 기업이 없습니다.")
                    return False
            
            print(f"\n🚀 실제 데이터 수집 시작: {len(target_companies)}개 기업")
            print("=" * 70)
            
            success_count = 0
            total_financial_records = 0
            
            # 데이터베이스 연결
            conn = self.config_manager.get_database_connection('dart')
            
            for idx, (corp_code, stock_code, company_name, market) in enumerate(target_companies):
                print(f"\n📊 진행률: {idx+1}/{len(target_companies)} - {company_name} ({market})")
                
                try:
                    # 1. 기업 개요 수집
                    outline_data = self.get_company_outline(corp_code)
                    if outline_data:
                        print(f"  ✅ 기업개요 수집 완료")
                    
                    time.sleep(self.request_delay)
                    
                    # 2. 재무제표 수집
                    financial_data = self.get_financial_statements(corp_code, year)
                    
                    if not financial_data.empty:
                        # 데이터베이스 저장
                        financial_data.to_sql('financial_statements', conn, 
                                            if_exists='append', index=False)
                        
                        # 기업개요 저장 (있는 경우)
                        if outline_data:
                            outline_df = pd.DataFrame([{
                                'corp_code': corp_code,
                                'corp_name': company_name,
                                'stock_code': stock_code,
                                'market_type': market,
                                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                **outline_data
                            }])
                            outline_df.to_sql('company_outlines', conn, 
                                            if_exists='append', index=False)
                        
                        success_count += 1
                        total_financial_records += len(financial_data)
                        print(f"  ✅ 재무데이터 저장: {len(financial_data)}건")
                    else:
                        print(f"  ⚠️ 재무데이터 없음")
                
                except Exception as e:
                    print(f"  ❌ 처리 실패: {e}")
                    continue
                
                # 진행상황 출력
                if (idx + 1) % 5 == 0:
                    print(f"\n📈 중간 결과: {success_count}/{idx+1}개 기업 성공, {total_financial_records}건 수집")
                
                time.sleep(self.request_delay)
            
            conn.commit()
            conn.close()
            
            # 최종 결과
            print(f"\n🎉 수집 완료!")
            print(f"📊 성공: {success_count}/{len(target_companies)}개 기업")
            print(f"📋 총 재무데이터: {total_financial_records}건")
            print(f"🎯 성공률: {success_count/len(target_companies)*100:.1f}%")
            
            self.logger.info(f"상장기업 데이터 수집 완료: {success_count}/{len(target_companies)}개 기업")
            return True
            
        except Exception as e:
            self.logger.error(f"데이터 수집 실패: {e}")
            return False
    
    def collect_kospi_top50(self, year: int = 2022) -> bool:
        """KOSPI 상위 50개 기업 수집"""
        kospi_companies = [comp for comp in self.major_listed_companies if comp[3] == 'KOSPI'][:50]
        return self.collect_listed_companies_data_from_list(kospi_companies, year)
    
    def collect_kosdaq_top30(self, year: int = 2022) -> bool:
        """KOSDAQ 상위 30개 기업 수집"""
        kosdaq_companies = [comp for comp in self.major_listed_companies if comp[3] == 'KOSDAQ'][:30]
        return self.collect_listed_companies_data_from_list(kosdaq_companies, year)
    
    def collect_listed_companies_data_from_list(self, companies: List[Tuple], year: int) -> bool:
        """지정된 기업 리스트로 데이터 수집"""
        print(f"🎯 지정 기업 리스트 데이터 수집: {len(companies)}개 기업")
        
        # 재무데이터 필터링
        valid_companies = self.filter_companies_with_financial_data(companies, year)
        
        if not valid_companies:
            print("❌ 재무데이터가 있는 기업이 없습니다.")
            return False
        
        # 실제 수집 진행
        success_count = 0
        conn = self.config_manager.get_database_connection('dart')
        
        for idx, (corp_code, stock_code, company_name, market) in enumerate(valid_companies):
            try:
                print(f"\n📊 {idx+1}/{len(valid_companies)} - {company_name} 수집 중...")
                
                # 재무제표 수집
                financial_data = self.get_financial_statements(corp_code, year)
                
                if not financial_data.empty:
                    financial_data.to_sql('financial_statements', conn, 
                                        if_exists='append', index=False)
                    success_count += 1
                    print(f"  ✅ 성공: {len(financial_data)}건")
                
                time.sleep(self.request_delay)
                
            except Exception as e:
                print(f"  ❌ 실패: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 수집 완료: {success_count}/{len(valid_companies)}개 기업 성공")
        return success_count > 0

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='KOSPI/KOSDAQ 상장기업 DART 데이터 수집기')
    parser.add_argument('--companies', type=int, default=20, 
                       help='수집할 기업 수 (기본: 20개)')
    parser.add_argument('--year', type=int, default=2022,
                       help='수집할 연도 (기본: 2022)')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='요청 간격 (초, 기본: 2.0)')
    parser.add_argument('--market', choices=['kospi', 'kosdaq', 'all'], default='all',
                       help='수집할 시장 (기본: all)')
    parser.add_argument('--no-filter', action='store_true',
                       help='재무데이터 사전 필터링 비활성화')
    
    args = parser.parse_args()
    
    try:
        print(f"🚀 KOSPI/KOSDAQ 상장기업 DART 데이터 수집기 시작")
        print(f"📋 설정: {args.companies}개 기업, {args.year}년, {args.delay}초 간격")
        print(f"📈 대상 시장: {args.market.upper()}")
        print("=" * 70)
        
        collector = ListedCompanyDartCollector(request_delay=args.delay)
        
        if args.market == 'kospi':
            success = collector.collect_kospi_top50(args.year)
        elif args.market == 'kosdaq':
            success = collector.collect_kosdaq_top30(args.year)
        else:
            success = collector.collect_listed_companies_data(
                year=args.year,
                max_companies=args.companies,
                filter_financial_data=not args.no_filter
            )
        
        if success:
            print("\n✅ 데이터 수집이 성공적으로 완료되었습니다!")
            print("\n📊 다음 단계: 워런 버핏 스코어카드 계산")
            print("python scripts/analysis/run_buffett_analysis.py --all_stocks")
        else:
            print("\n❌ 데이터 수집에 실패했습니다.")
            
    except Exception as e:
        print(f"\n❌ 실행 실패: {e}")

if __name__ == "__main__":
    main()