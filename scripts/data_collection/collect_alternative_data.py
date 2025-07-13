#!/usr/bin/env python3
"""
대안 데이터 수집 스크립트 (KIS API 대신 FinanceDataReader 사용)
"""

import sys
import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import argparse

# FinanceDataReader import
try:
    import FinanceDataReader as fdr
    print("✅ FinanceDataReader 사용 가능")
except ImportError:
    print("❌ FinanceDataReader가 설치되지 않았습니다.")
    print("설치: pip install finance-datareader")
    sys.exit(1)

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

class AlternativeDataCollector:
    """대안 데이터 수집 클래스"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_path = Path('data/databases/stock_data.db')
        
    def collect_realtime_quote(self, stock_code: str):
        """실시간 주가 정보 수집 (FinanceDataReader 사용)"""
        try:
            # FinanceDataReader로 최신 데이터 가져오기
            today = datetime.now().strftime('%Y-%m-%d')
            yesterday = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
            
            # 최근 3일간 데이터 가져오기 (공휴일 대응)
            df = fdr.DataReader(stock_code, yesterday, today)
            
            if df.empty:
                self.logger.warning(f"데이터가 없습니다: {stock_code}")
                return None
            
            # 최신 데이터 추출
            latest = df.iloc[-1]
            prev_close = df.iloc[-2]['Close'] if len(df) > 1 else latest['Close']
            
            quote_data = {
                'stock_code': stock_code,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'current_price': float(latest['Close']),
                'change_price': float(latest['Close'] - prev_close),
                'change_rate': float((latest['Close'] - prev_close) / prev_close * 100),
                'volume': int(latest['Volume']),
                'high_price': float(latest['High']),
                'low_price': float(latest['Low']),
                'open_price': float(latest['Open']),
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.logger.info(f"✅ 주가 수집 완료: {stock_code} - {quote_data['current_price']:,.0f}원")
            return quote_data
            
        except Exception as e:
            self.logger.error(f"❌ 주가 수집 실패 ({stock_code}): {e}")
            return None
    
    def collect_market_indicators(self):
        """시장 지표 수집"""
        try:
            indicators = []
            market_codes = [
                ('KOSPI', 'KOSPI'),
                ('KOSDAQ', 'KOSDAQ')
            ]
            
            today = datetime.now().strftime('%Y-%m-%d')
            yesterday = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
            
            for code, name in market_codes:
                try:
                    df = fdr.DataReader(code, yesterday, today)
                    
                    if df.empty:
                        continue
                    
                    latest = df.iloc[-1]
                    prev_close = df.iloc[-2]['Close'] if len(df) > 1 else latest['Close']
                    
                    indicator_data = {
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'index_name': name,
                        'index_code': code,
                        'close_price': float(latest['Close']),
                        'change_price': float(latest['Close'] - prev_close),
                        'change_rate': float((latest['Close'] - prev_close) / prev_close * 100),
                        'volume': int(latest['Volume']),
                        'high_price': float(latest['High']),
                        'low_price': float(latest['Low']),
                        'open_price': float(latest['Open']),
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    indicators.append(indicator_data)
                    
                    self.logger.info(f"✅ 시장지표 수집 완료: {name} - {indicator_data['close_price']:,.2f}")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ {name} 지표 수집 실패: {e}")
            
            return indicators
            
        except Exception as e:
            self.logger.error(f"❌ 시장 지표 수집 실패: {e}")
            return []
    
    def save_to_database(self, realtime_quotes=None, market_indicators=None):
        """데이터베이스에 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 실시간 주가 저장
                if realtime_quotes:
                    for quote in realtime_quotes:
                        conn.execute('''
                            INSERT OR REPLACE INTO daily_prices 
                            (stock_code, date, open_price, high_price, low_price, close_price,
                             volume, change_price, change_rate, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            quote['stock_code'], 
                            datetime.now().strftime('%Y-%m-%d'),
                            quote['open_price'], quote['high_price'], quote['low_price'],
                            quote['current_price'], quote['volume'],
                            quote['change_price'], quote['change_rate'],
                            quote['created_at']
                        ))
                    
                    self.logger.info(f"✅ 주가 데이터 저장 완료: {len(realtime_quotes)}건")
                
                # 시장 지표는 별도 테이블이 없으므로 로그만 출력
                if market_indicators:
                    self.logger.info(f"✅ 시장 지표 수집 완료: {len(market_indicators)}건")
                    for indicator in market_indicators:
                        self.logger.info(f"   {indicator['index_name']}: {indicator['close_price']:,.2f} ({indicator['change_rate']:+.2f}%)")
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 데이터베이스 저장 실패: {e}")
            return False
    
    def collect_stock_data(self, stock_code: str):
        """개별 종목 데이터 수집"""
        try:
            self.logger.info(f"📊 데이터 수집 시작: {stock_code}")
            
            # 주가 데이터 수집
            quote_data = self.collect_realtime_quote(stock_code)
            if not quote_data:
                return False
            
            # 데이터베이스 저장
            success = self.save_to_database(realtime_quotes=[quote_data])
            
            if success:
                self.logger.info(f"✅ 데이터 수집 완료: {stock_code}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 수집 실패 ({stock_code}): {e}")
            return False
    
    def collect_market_data(self):
        """시장 데이터 수집"""
        try:
            self.logger.info("📊 시장 데이터 수집 시작")
            
            # 시장 지표 수집
            market_indicators = self.collect_market_indicators()
            
            if not market_indicators:
                self.logger.warning("⚠️ 시장 지표를 수집할 수 없습니다")
                return False
            
            # 로그 출력 (별도 저장 테이블 없음)
            self.save_to_database(market_indicators=market_indicators)
            
            self.logger.info("✅ 시장 데이터 수집 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 시장 데이터 수집 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='대안 데이터 수집 스크립트 (FinanceDataReader)')
    parser.add_argument('--stock_code', type=str, help='수집할 종목코드')
    parser.add_argument('--market_data', action='store_true', help='시장 지표 수집')
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
    collector = AlternativeDataCollector()
    
    try:
        if args.stock_code:
            # 특정 종목 데이터 수집
            if collector.collect_stock_data(args.stock_code):
                print("✅ 주가 데이터 수집 성공")
            else:
                print("❌ 주가 데이터 수집 실패")
                sys.exit(1)
                
        elif args.market_data:
            # 시장 데이터 수집
            if collector.collect_market_data():
                print("✅ 시장 데이터 수집 성공")
            else:
                print("❌ 시장 데이터 수집 실패")
                sys.exit(1)
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
