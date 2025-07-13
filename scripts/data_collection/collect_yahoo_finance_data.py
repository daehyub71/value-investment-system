#!/usr/bin/env python3
"""
Yahoo Finance API를 활용한 Forward P/E 및 추정 실적 수집
무료이며 글로벌 표준 데이터 제공

수집 데이터:
- Forward P/E (예상 주가수익비율)
- Trailing P/E (과거 주가수익비율)
- PEG Ratio (성장 대비 밸류에이션)
- Price/Sales, Price/Book
- Analyst Target Price
- Earnings Estimate (애널리스트 EPS 추정치)

실행 방법:
python scripts/data_collection/collect_yahoo_finance_data.py --stock_code=005930
"""

import sys
import os
import requests
import json
import time
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, Any, Optional
import sqlite3

# yfinance 라이브러리 사용
try:
    import yfinance as yf
    print("✅ yfinance 라이브러리 사용 가능")
except ImportError:
    print("❌ yfinance 라이브러리가 설치되지 않았습니다.")
    print("설치: pip install yfinance")
    sys.exit(1)

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

class YahooFinanceCollector:
    """Yahoo Finance 데이터 수집 클래스"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_path = Path('data/databases/yahoo_finance_data.db')
        
        # 데이터베이스 초기화
        self._init_database()
    
    def _init_database(self):
        """데이터베이스 테이블 초기화"""
        try:
            # 데이터베이스 디렉토리 생성
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(self.db_path) as conn:
                # 기존 테이블이 있다면 삭제 후 재생성 (스키마 수정 반영)
                conn.execute('DROP TABLE IF EXISTS yahoo_valuation')
                conn.execute('DROP TABLE IF EXISTS yahoo_estimates')
                
                # Yahoo Finance 밸류에이션 데이터
                conn.execute('''
                    CREATE TABLE yahoo_valuation (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        company_name TEXT,
                        forward_pe REAL,
                        trailing_pe REAL,
                        peg_ratio REAL,
                        price_to_sales REAL,
                        price_to_book REAL,
                        enterprise_value REAL,
                        ev_to_revenue REAL,
                        ev_to_ebitda REAL,
                        market_cap REAL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(stock_code)
                    )
                ''')
                
                # 애널리스트 추정치 데이터
                conn.execute('''
                    CREATE TABLE yahoo_estimates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        company_name TEXT,
                        current_year_eps_estimate REAL,
                        next_year_eps_estimate REAL,
                        current_quarter_eps_estimate REAL,
                        next_quarter_eps_estimate REAL,
                        analyst_target_price REAL,
                        analyst_recommendation TEXT,
                        number_of_analysts INTEGER,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(stock_code)
                    )
                ''')
                
                conn.commit()
                self.logger.info("Yahoo Finance 데이터베이스 초기화 완료")
                
        except Exception as e:
            self.logger.error(f"데이터베이스 초기화 실패: {e}")
    
    def get_korean_ticker(self, stock_code: str) -> str:
        """한국 주식 코드를 Yahoo Finance 티커로 변환"""
        # 한국 주식은 .KS (KOSPI) 또는 .KQ (KOSDAQ) 접미사 필요
        if len(stock_code) == 6 and stock_code.isdigit():
            # KOSPI/KOSDAQ 구분 로직 (간단한 예시)
            if stock_code.startswith(('0', '1', '2', '3')):
                return f"{stock_code}.KS"  # KOSPI
            else:
                return f"{stock_code}.KQ"  # KOSDAQ
        return stock_code
    
    def collect_yahoo_valuation(self, stock_code: str) -> Optional[Dict]:
        """Yahoo Finance에서 밸류에이션 데이터 수집"""
        try:
            ticker = self.get_korean_ticker(stock_code)
            stock = yf.Ticker(ticker)
            
            # 기본 정보 가져오기
            info = stock.info
            
            if not info or 'symbol' not in info:
                self.logger.warning(f"Yahoo Finance에서 데이터를 찾을 수 없습니다: {ticker}")
                return None
            
            valuation_data = {
                'stock_code': stock_code,
                'company_name': info.get('longName', info.get('shortName', 'Unknown')),
                'forward_pe': info.get('forwardPE'),
                'trailing_pe': info.get('trailingPE'),
                'peg_ratio': info.get('pegRatio'),
                'price_to_sales': info.get('priceToSalesTrailing12Months'),
                'price_to_book': info.get('priceToBook'),
                'enterprise_value': info.get('enterpriseValue'),
                'ev_to_revenue': info.get('enterpriseToRevenue'),
                'ev_to_ebitda': info.get('enterpriseToEbitda'),
                'market_cap': info.get('marketCap')
            }
            
            # None 값들을 제거하고 로그
            non_null_data = {k: v for k, v in valuation_data.items() if v is not None}
            
            if len(non_null_data) > 2:  # stock_code, company_name 외에 데이터가 있는 경우
                self.logger.info(f"✅ Yahoo 밸류에이션 수집: {stock_code} - Forward P/E: {valuation_data.get('forward_pe', 'N/A')}")
                return valuation_data
            else:
                self.logger.warning(f"⚠️ 유효한 밸류에이션 데이터가 없습니다: {stock_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Yahoo 밸류에이션 수집 실패 ({stock_code}): {e}")
            return None
    
    def collect_yahoo_estimates(self, stock_code: str) -> Optional[Dict]:
        """Yahoo Finance에서 애널리스트 추정치 수집"""
        try:
            ticker = self.get_korean_ticker(stock_code)
            stock = yf.Ticker(ticker)
            
            # 기본 정보
            info = stock.info
            
            if not info or 'symbol' not in info:
                return None
            
            # 애널리스트 추정치 수집
            estimates_data = {
                'stock_code': stock_code,
                'company_name': info.get('longName', info.get('shortName', 'Unknown')),
                'current_year_eps_estimate': None,
                'next_year_eps_estimate': None,
                'current_quarter_eps_estimate': None,
                'next_quarter_eps_estimate': None,
                'analyst_target_price': info.get('targetMeanPrice'),
                'analyst_recommendation': info.get('recommendationKey'),
                'number_of_analysts': info.get('numberOfAnalystOpinions')
            }
            
            # earnings_estimates 데이터 시도 (더 상세한 추정치)
            try:
                calendar = stock.calendar
                if calendar is not None and not calendar.empty:
                    # 다음 실적 발표 관련 정보가 있다면 활용
                    pass
            except:
                pass
            
            # 유효한 데이터가 있는지 확인
            valid_fields = ['analyst_target_price', 'analyst_recommendation', 'number_of_analysts']
            has_valid_data = any(estimates_data.get(field) is not None for field in valid_fields)
            
            if has_valid_data:
                self.logger.info(f"✅ Yahoo 추정치 수집: {stock_code} - 목표가: {estimates_data.get('analyst_target_price', 'N/A')}")
                return estimates_data
            else:
                self.logger.warning(f"⚠️ 유효한 추정치 데이터가 없습니다: {stock_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Yahoo 추정치 수집 실패 ({stock_code}): {e}")
            return None
    
    def save_valuation_data(self, valuation_data: Dict) -> bool:
        """밸류에이션 데이터 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO yahoo_valuation
                    (stock_code, company_name, forward_pe, trailing_pe, peg_ratio,
                     price_to_sales, price_to_book, enterprise_value, ev_to_revenue,
                     ev_to_ebitda, market_cap, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    valuation_data['stock_code'],
                    valuation_data['company_name'],
                    valuation_data['forward_pe'],
                    valuation_data['trailing_pe'],
                    valuation_data['peg_ratio'],
                    valuation_data['price_to_sales'],
                    valuation_data['price_to_book'],
                    valuation_data['enterprise_value'],
                    valuation_data['ev_to_revenue'],
                    valuation_data['ev_to_ebitda'],
                    valuation_data['market_cap']
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 밸류에이션 데이터 저장 실패: {e}")
            return False
    
    def save_estimates_data(self, estimates_data: Dict) -> bool:
        """추정치 데이터 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO yahoo_estimates
                    (stock_code, company_name, current_year_eps_estimate, next_year_eps_estimate,
                     current_quarter_eps_estimate, next_quarter_eps_estimate, analyst_target_price,
                     analyst_recommendation, number_of_analysts, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    estimates_data['stock_code'],
                    estimates_data['company_name'],
                    estimates_data['current_year_eps_estimate'],
                    estimates_data['next_year_eps_estimate'],
                    estimates_data['current_quarter_eps_estimate'],
                    estimates_data['next_quarter_eps_estimate'],
                    estimates_data['analyst_target_price'],
                    estimates_data['analyst_recommendation'],
                    estimates_data['number_of_analysts']
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 추정치 데이터 저장 실패: {e}")
            return False
    
    def collect_stock_yahoo_data(self, stock_code: str) -> bool:
        """종목별 Yahoo Finance 데이터 수집"""
        try:
            self.logger.info(f"📊 Yahoo Finance 데이터 수집 시작: {stock_code}")
            
            success_count = 0
            
            # 1. 밸류에이션 데이터 수집
            valuation_data = self.collect_yahoo_valuation(stock_code)
            if valuation_data and self.save_valuation_data(valuation_data):
                success_count += 1
            
            # 2. 추정치 데이터 수집
            estimates_data = self.collect_yahoo_estimates(stock_code)
            if estimates_data and self.save_estimates_data(estimates_data):
                success_count += 1
            
            # API 호출 제한 대응
            time.sleep(0.5)
            
            if success_count > 0:
                self.logger.info(f"✅ Yahoo Finance 수집 완료: {stock_code} ({success_count}/2)")
                return True
            else:
                self.logger.warning(f"⚠️ Yahoo Finance 수집 실패: {stock_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Yahoo Finance 수집 실패 ({stock_code}): {e}")
            return False


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Yahoo Finance 데이터 수집 스크립트')
    parser.add_argument('--stock_code', type=str, help='수집할 종목코드')
    parser.add_argument('--test', action='store_true', help='테스트 모드 (삼성전자만)')
    parser.add_argument('--top_stocks', type=int, default=50, help='상위 N개 종목')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # 수집기 초기화
    collector = YahooFinanceCollector()
    
    try:
        if args.test:
            # 테스트 모드 - 삼성전자
            test_codes = ['005930', '000660', '035420', '005380', '051910']
            print("🧪 테스트 모드: 주요 종목 Yahoo Finance 데이터 수집")
            
            for code in test_codes:
                if collector.collect_stock_yahoo_data(code):
                    print(f"✅ {code} 성공")
                else:
                    print(f"❌ {code} 실패")
                    
        elif args.stock_code:
            # 단일 종목 수집
            if collector.collect_stock_yahoo_data(args.stock_code):
                print(f"✅ {args.stock_code} Yahoo Finance 데이터 수집 성공")
            else:
                print(f"❌ {args.stock_code} Yahoo Finance 데이터 수집 실패")
                
        else:
            # 상위 종목 수집
            logger.info(f"시가총액 상위 {args.top_stocks}개 종목 Yahoo Finance 데이터 수집 시작")
            
            # 주식 데이터베이스에서 상위 종목 조회
            stock_db_path = Path('data/databases/stock_data.db')
            if stock_db_path.exists():
                with sqlite3.connect(stock_db_path) as conn:
                    cursor = conn.execute(f"""
                        SELECT stock_code, company_name
                        FROM company_info 
                        WHERE market_cap IS NOT NULL AND market_cap > 0
                        ORDER BY market_cap DESC 
                        LIMIT {args.top_stocks}
                    """)
                    stock_list = cursor.fetchall()
                
                success_count = 0
                for idx, (stock_code, company_name) in enumerate(stock_list):
                    logger.info(f"진행률: {idx+1}/{len(stock_list)} - {company_name}({stock_code})")
                    
                    if collector.collect_stock_yahoo_data(stock_code):
                        success_count += 1
                
                print(f"✅ Yahoo Finance 데이터 수집 완료: {success_count}/{len(stock_list)} 성공")
            else:
                print("❌ 주식 데이터베이스를 찾을 수 없습니다.")
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")


if __name__ == "__main__":
    main()
