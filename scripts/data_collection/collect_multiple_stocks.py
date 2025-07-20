#!/usr/bin/env python3
"""
KOSPI/KOSDAQ 주요 종목 재무데이터 일괄 수집
삼성전자 외 추가 종목들도 수집
"""

import sys
import os
import sqlite3
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
import logging
import time

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from config import ConfigManager
    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False

class MultiStockDartCollector:
    """다수 종목 DART 재무데이터 수집기"""
    
    def __init__(self):
        """초기화"""
        if CONFIG_AVAILABLE:
            try:
                self.config_manager = ConfigManager()
                self.logger = self.config_manager.get_logger('MultiStockCollector')
                dart_config = self.config_manager.get_dart_config()
                self.api_key = dart_config.get('api_key')
                self.base_url = dart_config.get('base_url', "https://opendart.fss.or.kr/api")
                self.db_path = self.config_manager.get_database_path('dart')
            except Exception:
                self._use_fallback_config()
        else:
            self._use_fallback_config()
    
    def _use_fallback_config(self):
        """Fallback 설정"""
        from dotenv import load_dotenv
        load_dotenv()
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('MultiStockCollector')
        
        self.api_key = os.getenv('DART_API_KEY', '').strip('"')
        self.base_url = "https://opendart.fss.or.kr/api"
        self.db_path = Path('data/databases/dart_data.db')
    
    def get_major_stocks_info(self):
        """주요 종목 정보 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # KOSPI 주요 종목들 (시가총액 상위)
                major_stocks_query = """
                SELECT corp_code, corp_name, stock_code 
                FROM corp_codes 
                WHERE stock_code IN (
                    '005930',  -- 삼성전자
                    '000660',  -- SK하이닉스
                    '373220',  -- LG에너지솔루션
                    '207940',  -- 삼성바이오로직스
                    '005380',  -- 현대차
                    '000270',  -- 기아
                    '051910',  -- LG화학
                    '006400',  -- 삼성SDI
                    '035420',  -- NAVER
                    '035720',  -- 카카오
                    '028260',  -- 삼성물산
                    '066570',  -- LG전자
                    '003670',  -- 포스코홀딩스
                    '096770',  -- SK이노베이션
                    '034730',  -- SK
                    '017670',  -- SK텔레콤
                    '030200',  -- KT
                    '015760',  -- 한국전력
                    '009150',  -- 삼성전기
                    '011200'   -- HMM
                )
                AND corp_code != ''
                ORDER BY 
                    CASE stock_code
                        WHEN '005930' THEN 1  -- 삼성전자 우선
                        WHEN '000660' THEN 2  -- SK하이닉스
                        WHEN '373220' THEN 3  -- LG에너지솔루션
                        ELSE 4
                    END
                """
                
                stocks_df = pd.read_sql_query(major_stocks_query, conn)
                self.logger.info(f"📊 주요 종목 {len(stocks_df)}개 확인")
                
                return stocks_df
                
        except Exception as e:
            self.logger.error(f"주요 종목 정보 조회 실패: {e}")
            return pd.DataFrame()
    
    def collect_stock_financial_data(self, corp_code, corp_name, stock_code):
        """개별 종목 재무데이터 수집"""
        try:
            self.logger.info(f"💰 {corp_name}({stock_code}) 재무데이터 수집 시작")
            
            # 2023년 사업보고서 연결재무제표만 수집 (시간 절약)
            year_report_fs_combinations = [
                ('2023', '11011', 'CFS'),  # 2023년 사업보고서 연결
                ('2022', '11011', 'CFS'),  # 2022년 사업보고서 연결
            ]
            
            all_data = []
            
            for year, report_code, fs_div in year_report_fs_combinations:
                try:
                    url = f"{self.base_url}/fnlttSinglAcntAll.json"
                    params = {
                        'crtfc_key': self.api_key,
                        'corp_code': corp_code,
                        'bsns_year': year,
                        'reprt_code': report_code,
                        'fs_div': fs_div
                    }
                    
                    response = requests.get(url, params=params, timeout=30)
                    data = response.json()
                    
                    if data.get('status') == '000' and 'list' in data and data['list']:
                        financial_df = pd.DataFrame(data['list'])
                        
                        # 추가 정보
                        financial_df['stock_code'] = stock_code
                        financial_df['corp_name'] = corp_name
                        financial_df['collect_year'] = year
                        financial_df['report_code'] = report_code
                        financial_df['fs_div'] = fs_div
                        financial_df['collect_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        all_data.append(financial_df)
                        self.logger.info(f"✅ {corp_name} {year}년: {len(financial_df)}건 수집")
                        
                    else:
                        self.logger.warning(f"⚠️ {corp_name} {year}년: {data.get('message', 'No data')}")
                    
                    time.sleep(0.3)  # API 제한 고려
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ {corp_name} {year}년 수집 실패: {e}")
                    continue
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                self.logger.info(f"✅ {corp_name} 총 {len(combined_df)}건 수집 완료")
                return combined_df
            else:
                self.logger.warning(f"❌ {corp_name} 재무데이터 수집 실패")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"❌ {corp_name} 수집 중 오류: {e}")
            return pd.DataFrame()
    
    def collect_multiple_stocks(self, max_stocks=5):
        """다수 종목 재무데이터 수집"""
        try:
            self.logger.info(f"🚀 주요 종목 {max_stocks}개 재무데이터 수집 시작")
            
            # 1. 주요 종목 목록 가져오기
            stocks_df = self.get_major_stocks_info()
            
            if stocks_df.empty:
                self.logger.error("❌ 수집할 종목 목록을 가져올 수 없습니다")
                return False
            
            # 2. 제한된 수의 종목만 수집
            limited_stocks = stocks_df.head(max_stocks)
            
            all_financial_data = []
            success_count = 0
            
            for idx, row in limited_stocks.iterrows():
                corp_code = row['corp_code']
                corp_name = row['corp_name']
                stock_code = row['stock_code']
                
                self.logger.info(f"📊 진행률: {idx + 1}/{len(limited_stocks)} - {corp_name}")
                
                # 개별 종목 데이터 수집
                stock_data = self.collect_stock_financial_data(corp_code, corp_name, stock_code)
                
                if not stock_data.empty:
                    all_financial_data.append(stock_data)
                    success_count += 1
                    self.logger.info(f"✅ {corp_name} 수집 성공 ({success_count}/{len(limited_stocks)})")
                else:
                    self.logger.warning(f"⚠️ {corp_name} 수집 실패")
                
                # 진행 상황 출력
                if (idx + 1) % 3 == 0:
                    self.logger.info(f"🔄 중간 진행률: {success_count}/{idx + 1} 성공")
            
            # 3. 모든 데이터 통합 및 저장
            if all_financial_data:
                combined_df = pd.concat(all_financial_data, ignore_index=True)
                
                with sqlite3.connect(self.db_path) as conn:
                    combined_df.to_sql('multi_stock_financial_statements', conn, if_exists='replace', index=False)
                    
                    # 저장 확인
                    count_query = "SELECT COUNT(*) as count FROM multi_stock_financial_statements"
                    count_result = pd.read_sql_query(count_query, conn)
                    
                    self.logger.info(f"🎉 다종목 재무데이터 수집 완료!")
                    self.logger.info(f"✅ 성공한 종목: {success_count}개")
                    self.logger.info(f"✅ 총 데이터: {count_result.iloc[0]['count']}건")
                    
                    # 종목별 요약
                    summary_query = """
                    SELECT corp_name, stock_code, COUNT(*) as count
                    FROM multi_stock_financial_statements
                    GROUP BY corp_name, stock_code
                    ORDER BY count DESC
                    """
                    summary = pd.read_sql_query(summary_query, conn)
                    
                    self.logger.info("📋 종목별 수집 현황:")
                    for _, row in summary.iterrows():
                        self.logger.info(f"   • {row['corp_name']}({row['stock_code']}): {row['count']}건")
                
                return True
            else:
                self.logger.error("❌ 모든 종목 수집 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 다종목 수집 실행 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    print("🚀 KOSPI/KOSDAQ 주요 종목 재무데이터 일괄 수집")
    print("=" * 60)
    
    try:
        collector = MultiStockDartCollector()
        
        # 주요 종목 5개만 우선 수집 (시간 절약)
        success = collector.collect_multiple_stocks(max_stocks=5)
        
        if success:
            print("\n🎉 다종목 재무데이터 수집 성공!")
            print("✅ 수집 완료:")
            print("   • 기존: 삼성전자 (samsung_financial_statements)")
            print("   • 신규: 주요 5개 종목 (multi_stock_financial_statements)")
            print("\n🎯 이제 여러 종목 워런 버핏 스코어카드 계산 가능!")
            print("   python buffett_scorecard_calculator_fixed.py")
            
        else:
            print("\n❌ 다종목 수집 실패")
            print("🔧 대안: 삼성전자 데이터로 테스트")
            print("   python buffett_scorecard_calculator_fixed.py")
        
        return success
        
    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")
        return False

if __name__ == "__main__":
    main()
