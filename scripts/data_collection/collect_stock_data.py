#!/usr/bin/env python3
"""
주가 데이터 수집 스크립트
FinanceDataReader를 활용한 주가 데이터 수집 및 SQLite 저장

실행 방법:
python scripts/data_collection/collect_stock_data.py --stock_code=005930 --days=30
python scripts/data_collection/collect_stock_data.py --all_stocks --days=7
"""

import sys
import os
import argparse
import sqlite3
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from pathlib import Path
import logging

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import ConfigManager

class StockDataCollector:
    """주가 데이터 수집 클래스"""
    
    def __init__(self):
        # ConfigManager를 통한 통합 설정 관리
        self.config_manager = ConfigManager()
        self.logger = self.config_manager.get_logger('StockDataCollector')
        
        # 데이터베이스 연결 설정
        self.db_path = Path('data/databases/stock_data.db')
        
    def get_kospi_kosdaq_list(self):
        """KOSPI, KOSDAQ 전 종목 리스트 조회"""
        try:
            # KOSPI 종목 리스트
            kospi_list = fdr.StockListing('KOSPI')
            kospi_list['Market'] = 'KOSPI'
            
            # KOSDAQ 종목 리스트  
            kosdaq_list = fdr.StockListing('KOSDAQ')
            kosdaq_list['Market'] = 'KOSDAQ'
            
            # 통합
            all_stocks = pd.concat([kospi_list, kosdaq_list], ignore_index=True)
            
            self.logger.info(f"전체 종목 수: {len(all_stocks)} (KOSPI: {len(kospi_list)}, KOSDAQ: {len(kosdaq_list)})")
            return all_stocks
            
        except Exception as e:
            self.logger.error(f"종목 리스트 조회 실패: {e}")
            return pd.DataFrame()
    
    def collect_stock_prices(self, stock_code, start_date, end_date):
        """개별 종목 주가 데이터 수집"""
        try:
            # FinanceDataReader로 주가 데이터 수집
            df = fdr.DataReader(stock_code, start_date, end_date)
            
            if df.empty:
                self.logger.warning(f"주가 데이터 없음: {stock_code}")
                return pd.DataFrame()
            
            # 컬럼명 정리
            df = df.reset_index()
            df['stock_code'] = stock_code
            df['date'] = df['Date'].dt.strftime('%Y-%m-%d')
            
            # 필요한 컬럼만 선택하고 이름 변경
            price_data = pd.DataFrame({
                'stock_code': df['stock_code'],
                'date': df['date'],
                'open_price': df['Open'],
                'high_price': df['High'], 
                'low_price': df['Low'],
                'close_price': df['Close'],
                'volume': df['Volume'],
                'amount': df.get('Amount', df['Volume'] * df['Close']),  # 거래대금 계산
                'adjusted_close': df.get('Adj Close', df['Close']),  # 수정종가
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            self.logger.info(f"주가 데이터 수집 완료: {stock_code}, {len(price_data)}건")
            return price_data
            
        except Exception as e:
            self.logger.error(f"주가 데이터 수집 실패 ({stock_code}): {e}")
            return pd.DataFrame()
    
    def collect_company_info(self, stock_list_df):
        """기업 기본정보 수집"""
        try:
            company_data = []
            
            for _, row in stock_list_df.iterrows():
                # 시가총액 계산 (최신 주가 * 상장주식수)
                try:
                    latest_price = fdr.DataReader(row['Code'])
                    if not latest_price.empty:
                        current_price = latest_price['Close'].iloc[-1]
                        market_cap = int(current_price * row.get('Shares', 0)) if pd.notna(row.get('Shares', 0)) else None
                    else:
                        market_cap = None
                except:
                    market_cap = None
                
                company_info = {
                    'stock_code': row['Code'],
                    'company_name': row['Name'],
                    'market_type': row.get('Market', ''),
                    'sector': row.get('Sector', ''),
                    'industry': row.get('Industry', ''),
                    'listing_date': row.get('ListingDate', ''),
                    'market_cap': market_cap,
                    'shares_outstanding': row.get('Shares', None),
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                company_data.append(company_info)
            
            return pd.DataFrame(company_data)
            
        except Exception as e:
            self.logger.error(f"기업정보 수집 실패: {e}")
            return pd.DataFrame()
    
    def save_to_database(self, price_data, company_data=None):
        """데이터베이스에 저장"""
        try:
            # SQLite 데이터베이스 연결
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(self.db_path) as conn:
                # 데이터베이스 테이블 생성 (없는 경우)
                self._create_tables(conn)
                
                # 주가 데이터 저장 (중복 방지)
                if not price_data.empty:
                    saved_count = 0
                    for _, row in price_data.iterrows():
                        try:
                            conn.execute('''
                                INSERT OR REPLACE INTO stock_prices 
                                (stock_code, date, open_price, high_price, low_price, close_price, 
                                 volume, amount, adjusted_close, created_at, updated_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                row['stock_code'], row['date'], row['open_price'], row['high_price'],
                                row['low_price'], row['close_price'], row['volume'], row['amount'],
                                row['adjusted_close'], row['created_at'], row['updated_at']
                            ))
                            saved_count += 1
                        except Exception as e:
                            self.logger.warning(f"주가 데이터 저장 실패 ({row['stock_code']}, {row['date']}): {e}")
                            continue
                    
                    self.logger.info(f"주가 데이터 저장 완료: {saved_count}/{len(price_data)}건")
                
                # 기업정보 저장
                if company_data is not None and not company_data.empty:
                    company_saved_count = 0
                    for _, row in company_data.iterrows():
                        try:
                            conn.execute('''
                                INSERT OR REPLACE INTO company_info 
                                (stock_code, company_name, market_type, sector, industry, 
                                 listing_date, market_cap, shares_outstanding, created_at, updated_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', tuple(row))
                            company_saved_count += 1
                        except Exception as e:
                            self.logger.warning(f"기업정보 저장 실패 ({row['stock_code']}): {e}")
                            continue
                    
                    self.logger.info(f"기업정보 저장 완료: {company_saved_count}/{len(company_data)}건")
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"데이터베이스 저장 실패: {e}")
            return False
    
    def _create_tables(self, conn):
        """데이터베이스 테이블 생성"""
        # 주가 데이터 테이블
        conn.execute('''
            CREATE TABLE IF NOT EXISTS stock_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date TEXT NOT NULL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume INTEGER,
                amount REAL,
                adjusted_close REAL,
                created_at TEXT,
                updated_at TEXT,
                UNIQUE(stock_code, date)
            )
        ''')
        
        # 기업 정보 테이블
        conn.execute('''
            CREATE TABLE IF NOT EXISTS company_info (
                stock_code TEXT PRIMARY KEY,
                company_name TEXT,
                market_type TEXT,
                sector TEXT,
                industry TEXT,
                listing_date TEXT,
                market_cap REAL,
                shares_outstanding REAL,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
        
        # 인덱스 생성
        conn.execute('CREATE INDEX IF NOT EXISTS idx_stock_prices_code_date ON stock_prices(stock_code, date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_company_info_code ON company_info(stock_code)')
    
    def collect_single_stock(self, stock_code, days=30):
        """단일 종목 데이터 수집"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        self.logger.info(f"단일 종목 수집 시작: {stock_code} ({start_date.date()} ~ {end_date.date()})")
        
        # 주가 데이터 수집
        price_data = self.collect_stock_prices(
            stock_code, 
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        if not price_data.empty:
            # 데이터베이스 저장
            success = self.save_to_database(price_data)
            if success:
                self.logger.info(f"종목 {stock_code} 데이터 수집 완료")
                return True
        
        return False
    
    def collect_all_stocks(self, days=7):
        """전체 종목 데이터 수집"""
        self.logger.info(f"전체 종목 데이터 수집 시작 (최근 {days}일)")
        
        # 종목 리스트 조회
        stock_list = self.get_kospi_kosdaq_list()
        if stock_list.empty:
            self.logger.error("종목 리스트 조회 실패")
            return False
        
        # 기업 기본정보 수집 및 저장
        company_data = self.collect_company_info(stock_list)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        success_count = 0
        total_count = len(stock_list)
        
        for idx, row in stock_list.iterrows():
            stock_code = row['Code']
            
            try:
                self.logger.info(f"진행률: {idx+1}/{total_count} - {stock_code}")
                
                # 주가 데이터 수집
                price_data = self.collect_stock_prices(
                    stock_code,
                    start_date.strftime('%Y-%m-%d'),
                    end_date.strftime('%Y-%m-%d')
                )
                
                if not price_data.empty:
                    # 개별 저장 (메모리 효율성)
                    if self.save_to_database(price_data):
                        success_count += 1
                
                # 진행률 로깅 (10개마다)
                if (idx + 1) % 10 == 0:
                    self.logger.info(f"진행률: {idx+1}/{total_count} ({(idx+1)/total_count*100:.1f}%) - 성공: {success_count}")
                    
            except Exception as e:
                self.logger.error(f"종목 {stock_code} 처리 실패: {e}")
                continue
        
        # 기업정보 저장 (마지막에 일괄)
        if not company_data.empty:
            self.save_to_database(pd.DataFrame(), company_data)
        
        self.logger.info(f"전체 수집 완료: {success_count}/{total_count} 성공")
        return success_count > 0
    
    def collect_single_stock_period(self, stock_code, start_date, end_date):
        """단일 종목 지정 기간 데이터 수집"""
        self.logger.info(f"단일 종목 수집 시작: {stock_code} ({start_date} ~ {end_date})")
        
        # 주가 데이터 수집
        price_data = self.collect_stock_prices(stock_code, start_date, end_date)
        
        if not price_data.empty:
            # 데이터베이스 저장
            success = self.save_to_database(price_data)
            if success:
                self.logger.info(f"종목 {stock_code} 데이터 수집 완료")
                return True
        
        return False
    
    def collect_all_stocks_period(self, start_date, end_date):
        """전체 종목 지정 기간 데이터 수집"""
        self.logger.info(f"전체 종목 데이터 수집 시작 ({start_date} ~ {end_date})")
        
        # 종목 리스트 조회
        stock_list = self.get_kospi_kosdaq_list()
        if stock_list.empty:
            self.logger.error("종목 리스트 조회 실패")
            return False
        
        # 기업 기본정보 수집 및 저장
        company_data = self.collect_company_info(stock_list)
        
        success_count = 0
        total_count = len(stock_list)
        
        for idx, row in stock_list.iterrows():
            stock_code = row['Code']
            
            try:
                self.logger.info(f"진행률: {idx+1}/{total_count} - {stock_code}")
                
                # 주가 데이터 수집
                price_data = self.collect_stock_prices(stock_code, start_date, end_date)
                
                if not price_data.empty:
                    # 개별 저장 (메모리 효율성)
                    if self.save_to_database(price_data):
                        success_count += 1
                
                # 진행률 로깅 (10개마다)
                if (idx + 1) % 10 == 0:
                    self.logger.info(f"진행률: {idx+1}/{total_count} ({(idx+1)/total_count*100:.1f}%) - 성공: {success_count}")
                    
            except Exception as e:
                self.logger.error(f"종목 {stock_code} 처리 실패: {e}")
                continue
        
        # 기업정보 저장 (마지막에 일괄)
        if not company_data.empty:
            self.save_to_database(pd.DataFrame(), company_data)
        
        self.logger.info(f"전체 수집 완료: {success_count}/{total_count} 성공")
        return success_count > 0


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='주가 데이터 수집 스크립트')
    parser.add_argument('--stock_code', type=str, help='수집할 종목코드 (예: 005930)')
    parser.add_argument('--all_stocks', action='store_true', help='전체 종목 수집')
    parser.add_argument('--start_date', type=str, help='수집 시작일 (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, help='수집 종료일 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=30, help='수집 기간 (일수, 기본값: 30)')
    
    args = parser.parse_args()
    
    # 수집기 초기화
    collector = StockDataCollector()
    
    try:
        if args.stock_code:
            # 단일 종목 수집
            if args.start_date and args.end_date:
                # 사용자 지정 기간
                success = collector.collect_single_stock_period(args.stock_code, args.start_date, args.end_date)
            else:
                # 기본 기간 (days 사용)
                success = collector.collect_single_stock(args.stock_code, args.days)
                
            if success:
                collector.logger.info("✅ 주가 데이터 수집 성공")
            else:
                collector.logger.error("❌ 주가 데이터 수집 실패")
                sys.exit(1)
                
        elif args.all_stocks:
            # 전체 종목 수집
            success = collector.collect_all_stocks(args.days)
            if success:
                collector.logger.info("✅ 전체 주가 데이터 수집 성공")
            else:
                collector.logger.error("❌ 전체 주가 데이터 수집 실패")
                sys.exit(1)
                
        elif args.start_date:
            # 시작날짜만 지정된 경우 전체 종목 수집
            end_date = args.end_date if args.end_date else datetime.now().strftime('%Y-%m-%d')
            success = collector.collect_all_stocks_period(args.start_date, end_date)
            if success:
                collector.logger.info("✅ 기간별 주가 데이터 수집 성공")
            else:
                collector.logger.error("❌ 기간별 주가 데이터 수집 실패")
                sys.exit(1)
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        collector.logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        if 'collector' in locals():
            collector.logger.error(f"예기치 못한 오류: {e}")
        else:
            print(f"오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()