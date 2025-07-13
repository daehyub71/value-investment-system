#!/usr/bin/env python3
"""
대안 실시간 데이터 수집 스크립트
KIS API 대신 FinanceDataReader 활용으로 DART 시차 극복
"""

import sys
import os
import argparse
import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging

# FinanceDataReader 설치 확인
try:
    import FinanceDataReader as fdr
except ImportError:
    print("❌ FinanceDataReader가 설치되지 않았습니다.")
    print("다음 명령어로 설치하세요: pip install finance-datareader")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

class AlternativeRealtimeCollector:
    """대안 실시간 데이터 수집 클래스 (FinanceDataReader 활용)"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_path = Path('data/databases/kis_data.db')
        self.stock_db_path = Path('data/databases/stock_data.db')
        
        # 데이터베이스 디렉토리 생성
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def collect_market_indicators(self):
        """시장 지표 수집 (KOSPI, KOSDAQ)"""
        try:
            self.logger.info("시장 지표 수집 시작 (FinanceDataReader)")
            
            indicators = []
            today = datetime.now().strftime('%Y-%m-%d')
            
            # KOSPI 지수 (KS11)
            try:
                self.logger.info("KOSPI 지수 수집 중...")
                # 최근 5일 데이터 조회 (오늘 데이터가 없을 수 있으므로)
                start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
                kospi = fdr.DataReader('KS11', start=start_date)
                
                if not kospi.empty:
                    latest_kospi = kospi.iloc[-1]
                    prev_kospi = kospi.iloc[-2] if len(kospi) > 1 else latest_kospi
                    
                    kospi_data = {
                        'date': today,
                        'index_name': 'KOSPI',
                        'index_code': 'KS11',
                        'close_price': float(latest_kospi['Close']),
                        'change_price': float(latest_kospi['Close'] - prev_kospi['Close']),
                        'change_rate': float((latest_kospi['Close'] - prev_kospi['Close']) / prev_kospi['Close'] * 100),
                        'volume': int(latest_kospi.get('Volume', 0)),
                        'high_price': float(latest_kospi['High']),
                        'low_price': float(latest_kospi['Low']),
                        'open_price': float(latest_kospi['Open']),
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    indicators.append(kospi_data)
                    self.logger.info(f"✅ KOSPI 수집 완료: {kospi_data['close_price']:.2f} ({kospi_data['change_rate']:+.2f}%)")
                else:
                    self.logger.warning("❌ KOSPI 데이터를 가져올 수 없습니다")
                    
            except Exception as e:
                self.logger.warning(f"KOSPI 수집 실패: {e}")
            
            # KOSDAQ 지수 (KQ11)
            try:
                self.logger.info("KOSDAQ 지수 수집 중...")
                start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
                kosdaq = fdr.DataReader('KQ11', start=start_date)
                
                if not kosdaq.empty:
                    latest_kosdaq = kosdaq.iloc[-1]
                    prev_kosdaq = kosdaq.iloc[-2] if len(kosdaq) > 1 else latest_kosdaq
                    
                    kosdaq_data = {
                        'date': today,
                        'index_name': 'KOSDAQ',
                        'index_code': 'KQ11',
                        'close_price': float(latest_kosdaq['Close']),
                        'change_price': float(latest_kosdaq['Close'] - prev_kosdaq['Close']),
                        'change_rate': float((latest_kosdaq['Close'] - prev_kosdaq['Close']) / prev_kosdaq['Close'] * 100),
                        'volume': int(latest_kosdaq.get('Volume', 0)),
                        'high_price': float(latest_kosdaq['High']),
                        'low_price': float(latest_kosdaq['Low']),
                        'open_price': float(latest_kosdaq['Open']),
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    indicators.append(kosdaq_data)
                    self.logger.info(f"✅ KOSDAQ 수집 완료: {kosdaq_data['close_price']:.2f} ({kosdaq_data['change_rate']:+.2f}%)")
                else:
                    self.logger.warning("❌ KOSDAQ 데이터를 가져올 수 없습니다")
                    
            except Exception as e:
                self.logger.warning(f"KOSDAQ 수집 실패: {e}")
            
            return indicators
            
        except Exception as e:
            self.logger.error(f"시장 지표 수집 실패: {e}")
            return []
    
    def collect_realtime_quote(self, stock_code: str):
        """개별 종목 실시간 주가 수집"""
        try:
            self.logger.info(f"주가 데이터 수집 중: {stock_code}")
            
            # 최근 5일 데이터 조회
            start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
            stock_data = fdr.DataReader(stock_code, start=start_date)
            
            if stock_data.empty:
                self.logger.warning(f"주가 데이터를 찾을 수 없습니다: {stock_code}")
                return None
            
            # 최신 데이터 추출
            latest_data = stock_data.iloc[-1]
            prev_data = stock_data.iloc[-2] if len(stock_data) > 1 else latest_data
            
            quote_data = {
                'stock_code': stock_code,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'current_price': float(latest_data['Close']),
                'change_price': float(latest_data['Close'] - prev_data['Close']),
                'change_rate': float((latest_data['Close'] - prev_data['Close']) / prev_data['Close'] * 100),
                'volume': int(latest_data.get('Volume', 0)),
                'high_price': float(latest_data['High']),
                'low_price': float(latest_data['Low']),
                'open_price': float(latest_data['Open']),
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.logger.info(f"✅ 주가 수집 완료: {stock_code} - {quote_data['current_price']:.0f}원 ({quote_data['change_rate']:+.2f}%)")
            return quote_data
            
        except Exception as e:
            self.logger.error(f"주가 수집 실패 ({stock_code}): {e}")
            return None
    
    def save_to_database(self, realtime_quotes=None, market_indicators=None):
        """데이터베이스에 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 실시간 주가 저장
                if realtime_quotes:
                    for quote in realtime_quotes:
                        conn.execute('''
                            INSERT OR REPLACE INTO realtime_quotes 
                            (stock_code, timestamp, current_price, change_price, change_rate,
                             volume, high_price, low_price, open_price, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            quote['stock_code'], quote['timestamp'], quote['current_price'],
                            quote['change_price'], quote['change_rate'], quote['volume'],
                            quote['high_price'], quote['low_price'], quote['open_price'],
                            quote['created_at']
                        ))
                    
                    self.logger.info(f"📁 실시간 주가 저장 완료: {len(realtime_quotes)}건")
                
                # 시장 지표 저장
                if market_indicators:
                    for indicator in market_indicators:
                        conn.execute('''
                            INSERT OR REPLACE INTO market_indicators
                            (date, index_name, index_code, close_price, change_price, change_rate,
                             volume, high_price, low_price, open_price, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            indicator['date'], indicator['index_name'], indicator['index_code'],
                            indicator['close_price'], indicator['change_price'], indicator['change_rate'],
                            indicator['volume'], indicator['high_price'], indicator['low_price'],
                            indicator['open_price'], indicator['created_at']
                        ))
                    
                    self.logger.info(f"📁 시장 지표 저장 완료: {len(market_indicators)}건")
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"데이터베이스 저장 실패: {e}")
            return False
    
    def collect_stock_realtime_data(self, stock_code: str):
        """개별 종목 실시간 데이터 수집"""
        try:
            # 실시간 주가 수집
            quote_data = self.collect_realtime_quote(stock_code)
            if not quote_data:
                return False
            
            # 데이터베이스 저장
            success = self.save_to_database(realtime_quotes=[quote_data])
            
            if success:
                self.logger.info(f"🎯 실시간 데이터 수집 완료: {stock_code}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"실시간 데이터 수집 실패 ({stock_code}): {e}")
            return False
    
    def collect_all_market_data(self):
        """전체 시장 데이터 수집"""
        try:
            self.logger.info("🌐 전체 시장 데이터 수집 시작")
            
            # 시장 지표 수집
            market_indicators = self.collect_market_indicators()
            
            if not market_indicators:
                self.logger.warning("⚠️  시장 지표를 수집할 수 없습니다")
                return False
            
            # 데이터베이스 저장
            success = self.save_to_database(market_indicators=market_indicators)
            
            if success:
                self.logger.info("🎉 전체 시장 데이터 수집 완료")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"전체 시장 데이터 수집 실패: {e}")
            return False
    
    def update_top_stocks_realtime(self, limit: int = 50):
        """상위 종목 실시간 데이터 업데이트"""
        try:
            # 시가총액 상위 종목 조회
            if not self.stock_db_path.exists():
                self.logger.error("❌ 주식 데이터베이스를 찾을 수 없습니다.")
                return False
                
            with sqlite3.connect(self.stock_db_path) as conn:
                if limit == 0:  # limit이 0이면 전체 조회
                    cursor = conn.execute("""
                        SELECT stock_code, company_name, market_cap
                        FROM company_info 
                        WHERE market_cap IS NOT NULL AND market_cap > 0
                        ORDER BY market_cap DESC
                    """)
                else:
                    cursor = conn.execute(f"""
                        SELECT stock_code, company_name, market_cap
                        FROM company_info 
                        WHERE market_cap IS NOT NULL AND market_cap > 0
                        ORDER BY market_cap DESC 
                        LIMIT {limit}
                    """)
                stock_list = cursor.fetchall()
            
            if not stock_list:
                self.logger.error("❌ 종목 리스트를 찾을 수 없습니다.")
                return False
            
            if limit == 0:
                self.logger.info(f"📊 company_info 테이블 전체 {len(stock_list)}개 종목 실시간 데이터 업데이트 시작")
            else:
                self.logger.info(f"📊 상위 {len(stock_list)}개 종목 실시간 데이터 업데이트 시작")
            
            success_count = 0
            for idx, (stock_code, company_name, market_cap) in enumerate(stock_list):
                self.logger.info(f"📈 진행률: {idx+1}/{len(stock_list)} - {company_name}({stock_code})")
                
                if self.collect_stock_realtime_data(stock_code):
                    success_count += 1
                
                # 딜레이 (서버 부하 방지)
                time.sleep(0.1)
            
            if limit == 0:
                self.logger.info(f"🏁 company_info 테이블 전체 종목 실시간 데이터 업데이트 완료: {success_count}/{len(stock_list)} 성공")
            else:
                self.logger.info(f"🏁 상위 종목 실시간 데이터 업데이트 완료: {success_count}/{len(stock_list)} 성공")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"상위 종목 실시간 데이터 업데이트 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='대안 실시간 데이터 수집 스크립트 (FinanceDataReader)')
    parser.add_argument('--stock_code', type=str, help='실시간 데이터 수집할 종목코드')
    parser.add_argument('--realtime_quotes', action='store_true', help='실시간 주가 수집')
    parser.add_argument('--market_indicators', action='store_true', help='시장 지표 수집')
    parser.add_argument('--all_stocks', action='store_true', help='상위 종목 전체 업데이트')
    parser.add_argument('--limit', type=int, default=50, help='처리할 종목 수 제한 (0을 입력하면 company_info 테이블 전체 종목 처리)')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 기본 로깅 설정
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # 수집기 초기화
    collector = AlternativeRealtimeCollector()
    
    try:
        if args.stock_code and args.realtime_quotes:
            # 특정 종목 실시간 주가 수집
            if collector.collect_stock_realtime_data(args.stock_code):
                logger.info("✅ 실시간 주가 데이터 수집 성공 (FinanceDataReader)")
            else:
                logger.error("❌ 실시간 주가 데이터 수집 실패")
                sys.exit(1)
                
        elif args.market_indicators:
            # 시장 지표 수집
            if collector.collect_all_market_data():
                logger.info("✅ 시장 지표 데이터 수집 성공 (FinanceDataReader)")
            else:
                logger.error("❌ 시장 지표 데이터 수집 실패")
                sys.exit(1)
                
        elif args.all_stocks:
            # 상위 종목 또는 전체 종목 실시간 데이터 업데이트
            if collector.update_top_stocks_realtime(args.limit):
                if args.limit == 0:
                    logger.info("✅ company_info 테이블 전체 종목 실시간 데이터 업데이트 성공 (FinanceDataReader)")
                else:
                    logger.info("✅ 상위 종목 실시간 데이터 업데이트 성공 (FinanceDataReader)")
            else:
                if args.limit == 0:
                    logger.error("❌ company_info 테이블 전체 종목 실시간 데이터 업데이트 실패")
                else:
                    logger.error("❌ 상위 종목 실시간 데이터 업데이트 실패")
                sys.exit(1)
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
