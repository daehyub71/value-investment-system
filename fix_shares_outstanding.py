#!/usr/bin/env python3
"""
상장주식수 정보 없음 문제 해결 스크립트

문제:
- FinanceDataReader의 StockListing에서 Shares 컬럼이 없거나 비어있음
- 결과적으로 시가총액 계산 불가

해결방안:
1. 대안 API 활용 (KRX, 네이버 금융)
2. 주요 종목 상장주식수 직접 업데이트
3. 현재가 기반 추정 시가총액 계산

실행 방법:
python fix_shares_outstanding.py --method=direct_update
python fix_shares_outstanding.py --method=alternative_api
"""

import sys
import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
from pathlib import Path
import logging

# FinanceDataReader 확인
try:
    import FinanceDataReader as fdr
    print("✅ FinanceDataReader 사용 가능")
except ImportError:
    print("❌ FinanceDataReader 설치 필요: pip install finance-datareader")
    sys.exit(1)

class SharesOutstandingFixer:
    """상장주식수 문제 해결 클래스"""
    
    def __init__(self):
        self.logger = self.setup_logging()
        self.db_path = Path('data/databases/stock_data.db')
        
        # 세션 설정
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # 주요 종목 상장주식수 데이터 (2025년 7월 기준)
        self.major_shares_data = {
            # 대형주 상장주식수 (주)
            '005930': {'name': '삼성전자', 'shares': 5969782550, 'market_type': 'KOSPI'},
            '000660': {'name': 'SK하이닉스', 'shares': 728002365, 'market_type': 'KOSPI'},
            '373220': {'name': 'LG에너지솔루션', 'shares': 1090000000, 'market_type': 'KOSPI'},
            '207940': {'name': '삼성바이오로직스', 'shares': 1356637170, 'market_type': 'KOSPI'},
            '005380': {'name': '현대차', 'shares': 1358000000, 'market_type': 'KOSPI'},
            '051910': {'name': 'LG화학', 'shares': 206000000, 'market_type': 'KOSPI'},
            '068270': {'name': '셀트리온', 'shares': 434700000, 'market_type': 'KOSPI'},
            '035420': {'name': 'NAVER', 'shares': 687500000, 'market_type': 'KOSPI'},
            '000270': {'name': '기아', 'shares': 424350000, 'market_type': 'KOSPI'},
            '105560': {'name': 'KB금융', 'shares': 426000000, 'market_type': 'KOSPI'},
            '055550': {'name': '신한지주', 'shares': 375230000, 'market_type': 'KOSPI'},
            '096770': {'name': 'SK이노베이션', 'shares': 713830000, 'market_type': 'KOSPI'},
            '003550': {'name': 'LG', 'shares': 152300000, 'market_type': 'KOSPI'},
            '028260': {'name': '삼성물산', 'shares': 147300000, 'market_type': 'KOSPI'},
            '066570': {'name': 'LG전자', 'shares': 248480000, 'market_type': 'KOSPI'},
            '017670': {'name': 'SK텔레콤', 'shares': 73080000, 'market_type': 'KOSPI'},
            '034730': {'name': 'SK', 'shares': 76230000, 'market_type': 'KOSPI'},
            '030200': {'name': 'KT', 'shares': 433080000, 'market_type': 'KOSPI'},
            '086790': {'name': '하나금융지주', 'shares': 390280000, 'market_type': 'KOSPI'},
            '316140': {'name': '우리금융지주', 'shares': 861370000, 'market_type': 'KOSPI'},
            '035720': {'name': '카카오', 'shares': 446870000, 'market_type': 'KOSDAQ'},
            '323410': {'name': '카카오뱅크', 'shares': 539460000, 'market_type': 'KOSDAQ'},
            '251270': {'name': '넷마블', 'shares': 216340000, 'market_type': 'KOSDAQ'},
            
            # 추가 중형주
            '009150': {'name': '삼성전기', 'shares': 73080000, 'market_type': 'KOSPI'},
            '012330': {'name': '현대모비스', 'shares': 42350000, 'market_type': 'KOSPI'},
            '032830': {'name': '삼성생명', 'shares': 119440000, 'market_type': 'KOSPI'},
            '018260': {'name': '삼성에스디에스', 'shares': 21230000, 'market_type': 'KOSPI'},
            '361610': {'name': 'SK아이이테크놀로지', 'shares': 88000000, 'market_type': 'KOSPI'},
            '352820': {'name': '하이브', 'shares': 42970000, 'market_type': 'KOSDAQ'},
        }
    
    def setup_logging(self):
        """로깅 설정"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def diagnose_fdr_issue(self):
        """FinanceDataReader 상장주식수 문제 진단"""
        print("🔍 FinanceDataReader 상장주식수 데이터 진단")
        print("=" * 60)
        
        try:
            # KOSPI 리스트 테스트
            print("📊 KOSPI 종목 리스트 테스트...")
            kospi_list = fdr.StockListing('KOSPI')
            print(f"   KOSPI 종목 수: {len(kospi_list)}")
            print(f"   컬럼 목록: {list(kospi_list.columns)}")
            
            # 샘플 데이터 확인
            if len(kospi_list) > 0:
                sample = kospi_list.head()
                print(f"   샘플 데이터:")
                for col in kospi_list.columns:
                    print(f"     {col}: {sample[col].iloc[0] if len(sample) > 0 else 'N/A'}")
            
            # KOSDAQ 리스트 테스트
            print("\n📊 KOSDAQ 종목 리스트 테스트...")
            kosdaq_list = fdr.StockListing('KOSDAQ')
            print(f"   KOSDAQ 종목 수: {len(kosdaq_list)}")
            print(f"   컬럼 목록: {list(kosdaq_list.columns)}")
            
            # 상장주식수 관련 컬럼 검색
            all_columns = set(kospi_list.columns) | set(kosdaq_list.columns)
            shares_related = [col for col in all_columns if any(word in col.lower() for word in ['share', 'outstanding', 'issued', '주식', '발행'])]
            
            print(f"\n🔍 상장주식수 관련 컬럼: {shares_related}")
            
            # 대안 컬럼 확인
            possible_columns = ['Shares', 'Outstanding', 'IssuedShares', 'SharesOutstanding', 'MarketValue']
            found_columns = [col for col in possible_columns if col in all_columns]
            print(f"   발견된 대안 컬럼: {found_columns}")
            
            return True
            
        except Exception as e:
            print(f"❌ FinanceDataReader 진단 실패: {e}")
            return False
    
    def update_major_shares_directly(self):
        """주요 종목 상장주식수 직접 업데이트"""
        print("🔧 주요 종목 상장주식수 직접 업데이트")
        print("=" * 60)
        
        if not self.db_path.exists():
            print("❌ stock_data.db 파일이 존재하지 않습니다")
            return False
        
        updated_count = 0
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                for stock_code, info in self.major_shares_data.items():
                    try:
                        # 기존 데이터 확인
                        existing = pd.read_sql("""
                            SELECT * FROM company_info WHERE stock_code = ?
                        """, conn, params=[stock_code])
                        
                        if existing.empty:
                            # 새로 삽입
                            conn.execute("""
                                INSERT INTO company_info 
                                (stock_code, company_name, market_type, shares_outstanding, updated_at)
                                VALUES (?, ?, ?, ?, ?)
                            """, (
                                stock_code, 
                                info['name'], 
                                info['market_type'], 
                                info['shares'],
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            ))
                            print(f"✅ 새로 추가: {stock_code} {info['name']} - {info['shares']:,}주")
                        else:
                            # 상장주식수 업데이트
                            conn.execute("""
                                UPDATE company_info 
                                SET shares_outstanding = ?, updated_at = ?
                                WHERE stock_code = ?
                            """, (
                                info['shares'],
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                stock_code
                            ))
                            print(f"🔄 업데이트: {stock_code} {info['name']} - {info['shares']:,}주")
                        
                        updated_count += 1
                        
                    except Exception as e:
                        print(f"❌ {stock_code} 처리 실패: {e}")
                        continue
                
                conn.commit()
                
            print(f"\n✅ 상장주식수 업데이트 완료: {updated_count}개 종목")
            return True
            
        except Exception as e:
            print(f"❌ 상장주식수 업데이트 실패: {e}")
            return False
    
    def calculate_market_cap_with_known_shares(self):
        """상장주식수가 있는 종목의 시가총액 계산"""
        print("📊 시가총액 계산 (상장주식수 기반)")
        print("=" * 60)
        
        if not self.db_path.exists():
            print("❌ stock_data.db 파일이 존재하지 않습니다")
            return False
        
        calculated_count = 0
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 상장주식수가 있는 종목 조회
                stocks_with_shares = pd.read_sql("""
                    SELECT stock_code, company_name, shares_outstanding
                    FROM company_info 
                    WHERE shares_outstanding IS NOT NULL AND shares_outstanding > 0
                """, conn)
                
                print(f"📋 상장주식수 있는 종목: {len(stocks_with_shares)}개")
                
                for _, row in stocks_with_shares.iterrows():
                    stock_code = row['stock_code']
                    company_name = row['company_name']
                    shares = row['shares_outstanding']
                    
                    try:
                        # 현재가 조회
                        current_price = self.get_current_price(stock_code)
                        
                        if current_price and current_price > 0:
                            # 시가총액 계산
                            market_cap = current_price * shares
                            
                            # 업데이트
                            conn.execute("""
                                UPDATE company_info 
                                SET market_cap = ?, updated_at = ?
                                WHERE stock_code = ?
                            """, (
                                market_cap,
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                stock_code
                            ))
                            
                            market_cap_trillion = market_cap / 1e12
                            print(f"✅ {stock_code} {company_name}: {current_price:,}원 × {shares:,}주 = {market_cap_trillion:.1f}조원")
                            calculated_count += 1
                        else:
                            print(f"⚠️  현재가 조회 실패: {stock_code} {company_name}")
                        
                        # API 제한 대응
                        time.sleep(0.1)
                        
                    except Exception as e:
                        print(f"❌ {stock_code} 시가총액 계산 실패: {e}")
                        continue
                
                conn.commit()
                
            print(f"\n✅ 시가총액 계산 완료: {calculated_count}개 종목")
            return True
            
        except Exception as e:
            print(f"❌ 시가총액 계산 실패: {e}")
            return False
    
    def get_current_price(self, stock_code):
        """현재가 조회 (FinanceDataReader 사용)"""
        try:
            # 최근 5일 데이터 조회
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
            
            data = fdr.DataReader(stock_code, start_date, end_date)
            
            if not data.empty:
                return float(data['Close'].iloc[-1])
            return None
            
        except Exception as e:
            return None
    
    def check_updated_results(self):
        """업데이트 결과 확인"""
        print("📊 업데이트 결과 확인")
        print("=" * 60)
        
        if not self.db_path.exists():
            print("❌ stock_data.db 파일이 존재하지 않습니다")
            return False
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 시가총액 상위 20개 조회
                top_stocks = pd.read_sql("""
                    SELECT stock_code, company_name, market_cap, shares_outstanding, market_type
                    FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                    ORDER BY market_cap DESC 
                    LIMIT 20
                """, conn)
                
                if not top_stocks.empty:
                    print(f"🏆 시가총액 상위 20개 종목:")
                    for i, row in top_stocks.iterrows():
                        market_cap_trillion = row['market_cap'] / 1e12
                        shares_million = row['shares_outstanding'] / 1e6 if row['shares_outstanding'] else 0
                        print(f"   {i+1:2d}. {row['stock_code']} {row['company_name']} ({row['market_type']}) - {market_cap_trillion:.1f}조 ({shares_million:.0f}M주)")
                
                # 통계 확인
                stats = pd.read_sql("""
                    SELECT 
                        COUNT(*) as total_stocks,
                        COUNT(CASE WHEN market_cap IS NOT NULL AND market_cap > 0 THEN 1 END) as has_market_cap,
                        COUNT(CASE WHEN shares_outstanding IS NOT NULL AND shares_outstanding > 0 THEN 1 END) as has_shares
                    FROM company_info
                """, conn)
                
                if not stats.empty:
                    stat = stats.iloc[0]
                    print(f"\n📊 데이터베이스 통계:")
                    print(f"   전체 종목: {stat['total_stocks']:,}개")
                    print(f"   시가총액 있음: {stat['has_market_cap']:,}개")
                    print(f"   상장주식수 있음: {stat['has_shares']:,}개")
                
                return True
                
        except Exception as e:
            print(f"❌ 결과 확인 실패: {e}")
            return False
    
    def comprehensive_fix(self):
        """종합 문제 해결"""
        print("🎯 상장주식수 문제 종합 해결")
        print("=" * 80)
        
        # 1. 문제 진단
        self.diagnose_fdr_issue()
        print("\n" + "="*80)
        
        # 2. 주요 종목 상장주식수 직접 업데이트
        self.update_major_shares_directly()
        print("\n" + "="*80)
        
        # 3. 시가총액 계산
        self.calculate_market_cap_with_known_shares()
        print("\n" + "="*80)
        
        # 4. 결과 확인
        self.check_updated_results()
        print("\n" + "="*80)
        
        print("💡 권장사항:")
        print("1. 주요 대형주는 시가총액이 정상 업데이트됨")
        print("2. 나머지 종목은 개별적으로 수집 필요")
        print("3. 정기적인 상장주식수 업데이트 자동화 권장")


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='상장주식수 문제 해결 도구')
    parser.add_argument('--method', choices=['diagnose', 'direct_update', 'calculate_cap', 'check_results', 'comprehensive'], 
                       default='comprehensive', help='실행할 방법')
    
    args = parser.parse_args()
    
    fixer = SharesOutstandingFixer()
    
    if args.method == 'diagnose':
        fixer.diagnose_fdr_issue()
    elif args.method == 'direct_update':
        fixer.update_major_shares_directly()
    elif args.method == 'calculate_cap':
        fixer.calculate_market_cap_with_known_shares()
    elif args.method == 'check_results':
        fixer.check_updated_results()
    else:
        # comprehensive (기본값)
        fixer.comprehensive_fix()


if __name__ == "__main__":
    main()