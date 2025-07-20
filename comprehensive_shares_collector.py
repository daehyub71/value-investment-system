#!/usr/bin/env python3
"""
전체 종목 상장주식수 종합 수집 시스템

다중 데이터 소스를 활용하여 나머지 3,840개 종목의 상장주식수를 수집합니다.

데이터 소스:
1. KRX 공식 데이터 (우선순위 1)
2. 네이버 금융 크롤링 (우선순위 2)  
3. 다음 금융 크롤링 (우선순위 3)
4. 야후 파이낸스 (우선순위 4)
5. 추정값 계산 (최후 수단)

실행 방법:
python comprehensive_shares_collector.py --method=all
python comprehensive_shares_collector.py --method=krx_only
python comprehensive_shares_collector.py --batch_size=50 --delay=0.1
"""

import sys
import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# 추가 라이브러리 설치 확인
try:
    import FinanceDataReader as fdr
    import yfinance as yf
except ImportError:
    print("필요한 라이브러리 설치:")
    print("pip install finance-datareader yfinance")

class ComprehensiveSharesCollector:
    """종합 상장주식수 수집 시스템"""
    
    def __init__(self, batch_size=50, delay=0.1, max_workers=5):
        self.logger = self.setup_logging()
        self.db_path = Path('data/databases/stock_data.db')
        self.batch_size = batch_size
        self.delay = delay
        self.max_workers = max_workers
        
        # 세션 설정
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # 통계 추적
        self.stats = {
            'total_missing': 0,
            'collected_krx': 0,
            'collected_naver': 0,
            'collected_daum': 0,
            'collected_yahoo': 0,
            'estimated': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None
        }
        
        # 캐시
        self.krx_cache = {}
        self.failed_cache = set()
    
    def setup_logging(self):
        """로깅 설정"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('comprehensive_shares_collection.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def get_missing_shares_stocks(self):
        """상장주식수가 없는 종목 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                missing_stocks = pd.read_sql("""
                    SELECT stock_code, company_name, market_type
                    FROM company_info 
                    WHERE shares_outstanding IS NULL OR shares_outstanding = 0
                    ORDER BY 
                        CASE market_type 
                            WHEN 'KOSPI' THEN 1 
                            WHEN 'KOSDAQ' THEN 2 
                            WHEN 'KONEX' THEN 3 
                            ELSE 4 
                        END,
                        stock_code
                """, conn)
                
                self.stats['total_missing'] = len(missing_stocks)
                self.logger.info(f"📋 상장주식수 없는 종목: {len(missing_stocks):,}개")
                
                # 시장별 분포
                market_dist = missing_stocks['market_type'].value_counts()
                for market, count in market_dist.items():
                    self.logger.info(f"   {market}: {count:,}개")
                
                return missing_stocks
                
        except Exception as e:
            self.logger.error(f"❌ 누락 종목 조회 실패: {e}")
            return pd.DataFrame()
    
    def collect_from_krx_api(self, stock_codes):
        """KRX 공식 API에서 상장주식수 수집"""
        self.logger.info("🏛️ KRX 공식 데이터 수집 시작...")
        
        collected_data = {}
        
        try:
            # KRX 상장법인목록 API 호출
            krx_url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
            
            # KOSPI 데이터
            kospi_payload = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
                'mktId': 'STK',
                'trdDd': datetime.now().strftime('%Y%m%d'),
                'money': '1',
                'csvxls_isNo': 'false'
            }
            
            response = self.session.post(krx_url, data=kospi_payload, timeout=30)
            if response.status_code == 200:
                kospi_data = response.json()
                for item in kospi_data.get('OutBlock_1', []):
                    stock_code = item.get('ISU_SRT_CD', '').zfill(6)
                    shares = item.get('LIST_SHRS', 0)
                    
                    if stock_code and shares:
                        try:
                            shares_int = int(str(shares).replace(',', ''))
                            if shares_int > 0:
                                collected_data[stock_code] = {
                                    'shares': shares_int,
                                    'source': 'KRX_KOSPI',
                                    'company_name': item.get('ISU_ABBRV', '')
                                }
                        except:
                            continue
            
            # KOSDAQ 데이터
            kosdaq_payload = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
                'mktId': 'KSQ',
                'trdDd': datetime.now().strftime('%Y%m%d'),
                'money': '1',
                'csvxls_isNo': 'false'
            }
            
            response = self.session.post(krx_url, data=kosdaq_payload, timeout=30)
            if response.status_code == 200:
                kosdaq_data = response.json()
                for item in kosdaq_data.get('OutBlock_1', []):
                    stock_code = item.get('ISU_SRT_CD', '').zfill(6)
                    shares = item.get('LIST_SHRS', 0)
                    
                    if stock_code and shares:
                        try:
                            shares_int = int(str(shares).replace(',', ''))
                            if shares_int > 0:
                                collected_data[stock_code] = {
                                    'shares': shares_int,
                                    'source': 'KRX_KOSDAQ',
                                    'company_name': item.get('ISU_ABBRV', '')
                                }
                        except:
                            continue
            
            self.stats['collected_krx'] = len(collected_data)
            self.logger.info(f"✅ KRX 데이터 수집: {len(collected_data):,}개")
            
            return collected_data
            
        except Exception as e:
            self.logger.error(f"❌ KRX 데이터 수집 실패: {e}")
            return {}
    
    def collect_from_naver_finance(self, stock_code, company_name):
        """네이버 금융에서 상장주식수 수집"""
        try:
            url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 상장주식수 찾기 (여러 패턴 시도)
            patterns = [
                # 패턴 1: 테이블에서 찾기
                {'selector': '.sub_section table tr', 'text': '상장주식수'},
                {'selector': '.section table tr', 'text': '상장주식'},
                {'selector': '.tb_type1 tr', 'text': '상장주식수'},
                # 패턴 2: 기업개요에서 찾기
                {'selector': '.company_info tr', 'text': '상장주식'},
                {'selector': '.info_table tr', 'text': '주식수'},
            ]
            
            for pattern in patterns:
                rows = soup.select(pattern['selector'])
                for row in rows:
                    if pattern['text'] in row.get_text():
                        # 숫자 추출
                        text = row.get_text()
                        numbers = re.findall(r'[\d,]+', text)
                        for num_str in numbers:
                            try:
                                num = int(num_str.replace(',', ''))
                                # 상장주식수 범위 검증 (10만 ~ 100억주)
                                if 100000 <= num <= 10000000000:
                                    return {
                                        'shares': num,
                                        'source': 'NAVER',
                                        'company_name': company_name
                                    }
                            except:
                                continue
            
            return None
            
        except Exception as e:
            return None
    
    def collect_from_daum_finance(self, stock_code, company_name):
        """다음 금융에서 상장주식수 수집"""
        try:
            url = f"https://finance.daum.net/quotes/A{stock_code}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 상장주식수 찾기
            info_items = soup.select('.info_major dl')
            for item in info_items:
                dt = item.find('dt')
                dd = item.find('dd')
                
                if dt and dd and '상장주식수' in dt.get_text():
                    text = dd.get_text()
                    numbers = re.findall(r'[\d,]+', text)
                    for num_str in numbers:
                        try:
                            num = int(num_str.replace(',', ''))
                            if 100000 <= num <= 10000000000:
                                return {
                                    'shares': num,
                                    'source': 'DAUM',
                                    'company_name': company_name
                                }
                        except:
                            continue
            
            return None
            
        except Exception as e:
            return None
    
    def collect_from_yahoo_finance(self, stock_code, company_name):
        """야후 파이낸스에서 상장주식수 수집"""
        try:
            # 한국 종목은 .KS 또는 .KQ 접미사 필요
            yahoo_symbol = f"{stock_code}.KS"  # KOSPI 기본
            
            ticker = yf.Ticker(yahoo_symbol)
            info = ticker.info
            
            # 상장주식수 필드들 확인
            shares_fields = ['sharesOutstanding', 'impliedSharesOutstanding', 'floatShares']
            
            for field in shares_fields:
                if field in info and info[field]:
                    shares = info[field]
                    if isinstance(shares, (int, float)) and shares > 100000:
                        return {
                            'shares': int(shares),
                            'source': 'YAHOO',
                            'company_name': company_name
                        }
            
            # KOSDAQ 시도
            if not info or 'sharesOutstanding' not in info:
                yahoo_symbol = f"{stock_code}.KQ"
                ticker = yf.Ticker(yahoo_symbol)
                info = ticker.info
                
                for field in shares_fields:
                    if field in info and info[field]:
                        shares = info[field]
                        if isinstance(shares, (int, float)) and shares > 100000:
                            return {
                                'shares': int(shares),
                                'source': 'YAHOO_KQ',
                                'company_name': company_name
                            }
            
            return None
            
        except Exception as e:
            return None
    
    def estimate_shares_from_market_cap(self, stock_code, company_name, market_cap):
        """시가총액을 이용한 상장주식수 추정"""
        try:
            # 현재가 조회
            current_price = self.get_current_price(stock_code)
            
            if current_price and current_price > 0 and market_cap and market_cap > 0:
                estimated_shares = int(market_cap / current_price)
                
                # 합리적 범위 검증
                if 100000 <= estimated_shares <= 10000000000:
                    return {
                        'shares': estimated_shares,
                        'source': 'ESTIMATED',
                        'company_name': company_name
                    }
            
            return None
            
        except Exception as e:
            return None
    
    def get_current_price(self, stock_code):
        """현재가 조회"""
        try:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
            
            data = fdr.DataReader(stock_code, start_date, end_date)
            
            if not data.empty:
                return float(data['Close'].iloc[-1])
            return None
            
        except Exception as e:
            return None
    
    def collect_single_stock_shares(self, stock_info):
        """단일 종목 상장주식수 수집 (멀티소스)"""
        stock_code = stock_info['stock_code']
        company_name = stock_info['company_name']
        market_type = stock_info.get('market_type', '')
        
        # 실패 캐시 확인
        if stock_code in self.failed_cache:
            return None
        
        # 우선순위별 수집 시도
        sources = [
            ('KRX', lambda: self.krx_cache.get(stock_code)),
            ('NAVER', lambda: self.collect_from_naver_finance(stock_code, company_name)),
            ('DAUM', lambda: self.collect_from_daum_finance(stock_code, company_name)),
            ('YAHOO', lambda: self.collect_from_yahoo_finance(stock_code, company_name)),
        ]
        
        for source_name, collect_func in sources:
            try:
                result = collect_func()
                if result and result.get('shares'):
                    self.logger.debug(f"✅ {stock_code} {company_name}: {result['shares']:,}주 ({source_name})")
                    return {
                        'stock_code': stock_code,
                        'shares_outstanding': result['shares'],
                        'data_source': result['source']
                    }
                
                # API 제한 대응
                time.sleep(self.delay)
                
            except Exception as e:
                self.logger.debug(f"⚠️ {stock_code} {source_name} 실패: {e}")
                continue
        
        # 모든 소스 실패 시 실패 캐시에 추가
        self.failed_cache.add(stock_code)
        self.stats['failed'] += 1
        return None
    
    def batch_collect_shares(self, missing_stocks_df):
        """배치 단위로 상장주식수 수집"""
        self.logger.info(f"🚀 배치 수집 시작: {len(missing_stocks_df):,}개 종목")
        
        # KRX 데이터 미리 로드
        self.krx_cache = self.collect_from_krx_api(missing_stocks_df['stock_code'].tolist())
        
        collected_data = []
        
        # 배치 단위로 처리
        for i in range(0, len(missing_stocks_df), self.batch_size):
            batch = missing_stocks_df.iloc[i:i+self.batch_size]
            
            self.logger.info(f"📊 배치 {i//self.batch_size + 1}/{(len(missing_stocks_df)-1)//self.batch_size + 1} 처리 중...")
            
            # 멀티스레딩으로 병렬 처리
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for _, stock_info in batch.iterrows():
                    future = executor.submit(self.collect_single_stock_shares, stock_info.to_dict())
                    futures.append(future)
                
                # 결과 수집
                for future in as_completed(futures):
                    try:
                        result = future.result(timeout=30)
                        if result:
                            collected_data.append(result)
                    except Exception as e:
                        self.logger.debug(f"Future 처리 실패: {e}")
                        continue
            
            # 진행률 출력
            progress = (i + self.batch_size) / len(missing_stocks_df) * 100
            collected_count = len(collected_data)
            self.logger.info(f"진행률: {progress:.1f}% ({collected_count:,}개 수집)")
            
            # 배치 간 휴식
            time.sleep(1)
        
        return collected_data
    
    def save_collected_shares(self, collected_data):
        """수집된 상장주식수 데이터 저장"""
        if not collected_data:
            self.logger.warning("저장할 데이터가 없습니다")
            return False
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                updated_count = 0
                
                for data in collected_data:
                    stock_code = data['stock_code']
                    shares = data['shares_outstanding']
                    source = data.get('data_source', 'UNKNOWN')
                    
                    conn.execute("""
                        UPDATE company_info 
                        SET shares_outstanding = ?, 
                            updated_at = ?,
                            data_source = ?
                        WHERE stock_code = ?
                    """, (
                        shares,
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        source,
                        stock_code
                    ))
                    
                    updated_count += 1
                
                conn.commit()
                
            self.logger.info(f"✅ 데이터 저장 완료: {updated_count:,}개 종목")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 저장 실패: {e}")
            return False
    
    def generate_collection_report(self):
        """수집 결과 리포트 생성"""
        self.stats['end_time'] = datetime.now()
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds() / 60
        
        print("\n" + "="*80)
        print("📊 전체 종목 상장주식수 수집 완료 리포트")
        print("="*80)
        
        print(f"⏱️  소요 시간: {duration:.1f}분")
        print(f"📋 총 대상 종목: {self.stats['total_missing']:,}개")
        print()
        
        total_collected = (self.stats['collected_krx'] + self.stats['collected_naver'] + 
                          self.stats['collected_daum'] + self.stats['collected_yahoo'] + 
                          self.stats['estimated'])
        
        print("📈 소스별 수집 현황:")
        print(f"   🏛️ KRX 공식: {self.stats['collected_krx']:,}개")
        print(f"   🌐 네이버: {self.stats['collected_naver']:,}개")
        print(f"   🔍 다음: {self.stats['collected_daum']:,}개")
        print(f"   📊 야후: {self.stats['collected_yahoo']:,}개")
        print(f"   🧮 추정값: {self.stats['estimated']:,}개")
        print(f"   ❌ 실패: {self.stats['failed']:,}개")
        print()
        
        success_rate = (total_collected / self.stats['total_missing'] * 100) if self.stats['total_missing'] > 0 else 0
        print(f"✅ 전체 성공률: {success_rate:.1f}% ({total_collected:,}/{self.stats['total_missing']:,})")
        
        # 최종 데이터베이스 상태 확인
        self.check_final_database_status()
    
    def check_final_database_status(self):
        """최종 데이터베이스 상태 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                stats = pd.read_sql("""
                    SELECT 
                        COUNT(*) as total_stocks,
                        COUNT(CASE WHEN shares_outstanding IS NOT NULL AND shares_outstanding > 0 THEN 1 END) as has_shares,
                        COUNT(CASE WHEN market_cap IS NOT NULL AND market_cap > 0 THEN 1 END) as has_market_cap
                    FROM company_info
                """, conn).iloc[0]
                
                print("\n📊 최종 데이터베이스 상태:")
                print(f"   전체 종목: {stats['total_stocks']:,}개")
                print(f"   상장주식수 있음: {stats['has_shares']:,}개 ({stats['has_shares']/stats['total_stocks']*100:.1f}%)")
                print(f"   시가총액 있음: {stats['has_market_cap']:,}개 ({stats['has_market_cap']/stats['total_stocks']*100:.1f}%)")
                
                # 상위 종목들 확인
                top_stocks = pd.read_sql("""
                    SELECT stock_code, company_name, market_cap, shares_outstanding, market_type
                    FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                    ORDER BY market_cap DESC 
                    LIMIT 10
                """, conn)
                
                if not top_stocks.empty:
                    print("\n🏆 시가총액 상위 10개 종목:")
                    for i, row in top_stocks.iterrows():
                        market_cap_trillion = row['market_cap'] / 1e12
                        shares_million = row['shares_outstanding'] / 1e6 if row['shares_outstanding'] else 0
                        print(f"   {i+1:2d}. {row['stock_code']} {row['company_name']} ({row['market_type']}) - {market_cap_trillion:.1f}조 ({shares_million:.0f}M주)")
                
        except Exception as e:
            print(f"❌ 최종 상태 확인 실패: {e}")
    
    def run_comprehensive_collection(self, method='all'):
        """종합 수집 실행"""
        self.stats['start_time'] = datetime.now()
        
        print("🚀 전체 종목 상장주식수 종합 수집 시작")
        print("="*80)
        
        # 1. 누락 종목 조회
        missing_stocks = self.get_missing_shares_stocks()
        
        if missing_stocks.empty:
            print("✅ 모든 종목의 상장주식수가 이미 수집되었습니다!")
            return True
        
        # 2. 배치 수집 실행
        collected_data = self.batch_collect_shares(missing_stocks)
        
        # 3. 데이터 저장
        if collected_data:
            self.save_collected_shares(collected_data)
        
        # 4. 리포트 생성
        self.generate_collection_report()
        
        return True


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='전체 종목 상장주식수 종합 수집 시스템')
    parser.add_argument('--method', choices=['all', 'krx_only', 'web_only'], default='all', help='수집 방법')
    parser.add_argument('--batch_size', type=int, default=50, help='배치 크기')
    parser.add_argument('--delay', type=float, default=0.1, help='API 호출 간 지연시간(초)')
    parser.add_argument('--max_workers', type=int, default=5, help='최대 동시 작업 수')
    parser.add_argument('--test_mode', action='store_true', help='테스트 모드 (10개 종목만)')
    
    args = parser.parse_args()
    
    try:
        collector = ComprehensiveSharesCollector(
            batch_size=args.batch_size,
            delay=args.delay,
            max_workers=args.max_workers
        )
        
        if args.test_mode:
            collector.batch_size = 10
            print("🧪 테스트 모드: 10개 종목만 처리")
        
        success = collector.run_comprehensive_collection(method=args.method)
        
        if success:
            print("\n✅ 전체 수집 프로세스 완료!")
            print("\n💡 다음 단계:")
            print("1. python forecast_data_analyzer_fixed.py --check_db  # 결과 확인")
            print("2. python forecast_data_analyzer_fixed.py --collect_top 20  # 대형주 추정실적 수집")
        else:
            print("\n❌ 수집 프로세스 실패")
            
    except KeyboardInterrupt:
        print("\n\n⏹️ 사용자에 의해 중단되었습니다")
        print("진행된 데이터는 저장되었습니다")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        return False
    
    return True


if __name__ == "__main__":
    main()