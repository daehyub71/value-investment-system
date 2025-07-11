#!/usr/bin/env python3
"""
워런 버핏 스코어카드 시스템을 위한 향상된 데이터 업데이트 시스템
- 일일 증분 업데이트
- 특정 기간 누락 데이터 보완
- 자동화된 스케줄링
- 데이터 품질 검증
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, time
import pandas as pd
import sqlite3
import time as time_module
import logging
import schedule
from typing import List, Dict, Optional, Tuple
import argparse

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config import get_db_connection
import FinanceDataReader as fdr

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_updater.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    """데이터 품질 검증 클래스"""
    
    def __init__(self):
        pass
    
    def find_missing_dates(self, stock_code: str, start_date: str, end_date: str) -> List[str]:
        """누락된 날짜 찾기"""
        try:
            with get_db_connection('stock') as conn:
                query = """
                    SELECT DISTINCT date 
                    FROM stock_prices 
                    WHERE stock_code = ? AND date BETWEEN ? AND ?
                    ORDER BY date
                """
                existing_dates = pd.read_sql(query, conn, params=(stock_code, start_date, end_date))
            
            # 전체 영업일 생성 (주말 제외)
            date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # B = 영업일
            all_dates = [d.strftime('%Y-%m-%d') for d in date_range]
            
            # 기존 날짜와 비교하여 누락된 날짜 찾기
            existing_date_list = existing_dates['date'].tolist() if not existing_dates.empty else []
            missing_dates = [d for d in all_dates if d not in existing_date_list]
            
            return missing_dates
            
        except Exception as e:
            logger.error(f"누락 날짜 검색 실패 ({stock_code}): {e}")
            return []
    
    def find_missing_stocks_for_date(self, target_date: str) -> List[str]:
        """특정 날짜에 누락된 종목 찾기"""
        try:
            with get_db_connection('stock') as conn:
                # 전체 종목 목록
                all_stocks = pd.read_sql("SELECT stock_code FROM company_info", conn)
                
                # 해당 날짜에 데이터가 있는 종목
                existing_stocks = pd.read_sql(
                    "SELECT DISTINCT stock_code FROM stock_prices WHERE date = ?", 
                    conn, params=(target_date,)
                )
            
            all_stock_list = all_stocks['stock_code'].tolist()
            existing_stock_list = existing_stocks['stock_code'].tolist() if not existing_stocks.empty else []
            
            missing_stocks = [s for s in all_stock_list if s not in existing_stock_list]
            return missing_stocks
            
        except Exception as e:
            logger.error(f"누락 종목 검색 실패 ({target_date}): {e}")
            return []
    
    def validate_data_integrity(self, stock_code: str, date: str) -> Dict[str, bool]:
        """데이터 무결성 검증"""
        try:
            with get_db_connection('stock') as conn:
                query = """
                    SELECT open_price, high_price, low_price, close_price, volume
                    FROM stock_prices 
                    WHERE stock_code = ? AND date = ?
                """
                data = pd.read_sql(query, conn, params=(stock_code, date))
            
            if data.empty:
                return {'exists': False}
            
            row = data.iloc[0]
            
            checks = {
                'exists': True,
                'valid_prices': all(row[col] > 0 for col in ['open_price', 'high_price', 'low_price', 'close_price']),
                'valid_high_low': row['high_price'] >= row['low_price'],
                'valid_ohlc': row['low_price'] <= row['open_price'] <= row['high_price'] and row['low_price'] <= row['close_price'] <= row['high_price'],
                'has_volume': row['volume'] >= 0
            }
            
            return checks
            
        except Exception as e:
            logger.error(f"데이터 무결성 검증 실패 ({stock_code}, {date}): {e}")
            return {'exists': False}

class SmartDataUpdater:
    """스마트 데이터 업데이트 클래스"""
    
    def __init__(self):
        self.quality_checker = DataQualityChecker()
    
    def update_daily_stock_prices(self, target_date: str = None) -> bool:
        """일일 주가 데이터 업데이트"""
        try:
            if target_date is None:
                target_date = datetime.now().strftime('%Y-%m-%d')
            
            logger.info(f"=== 일일 주가 업데이트 시작: {target_date} ===")
            
            # 누락된 종목 찾기
            missing_stocks = self.quality_checker.find_missing_stocks_for_date(target_date)
            
            if not missing_stocks:
                logger.info(f"{target_date} 모든 종목 데이터가 최신 상태입니다.")
                return True
            
            logger.info(f"업데이트 대상: {len(missing_stocks)}개 종목")
            
            success_count = 0
            for i, stock_code in enumerate(missing_stocks):
                logger.info(f"진행: {i+1}/{len(missing_stocks)} - {stock_code}")
                
                if self._update_single_stock_price(stock_code, target_date, target_date):
                    success_count += 1
                
                time_module.sleep(0.1)  # API 호출 제한
            
            logger.info(f"일일 주가 업데이트 완료: {success_count}/{len(missing_stocks)}")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"일일 주가 업데이트 실패: {e}")
            return False
    
    def update_period_stock_prices(self, start_date: str, end_date: str, stock_codes: List[str] = None) -> bool:
        """특정 기간 주가 데이터 업데이트"""
        try:
            logger.info(f"=== 기간 주가 업데이트: {start_date} ~ {end_date} ===")
            
            # 종목 목록 가져오기
            if stock_codes is None:
                with get_db_connection('stock') as conn:
                    stock_df = pd.read_sql("SELECT stock_code FROM company_info", conn)
                    stock_codes = stock_df['stock_code'].tolist()
            
            total_count = len(stock_codes)
            success_count = 0
            
            for i, stock_code in enumerate(stock_codes):
                logger.info(f"진행: {i+1}/{total_count} - {stock_code}")
                
                # 누락된 날짜 찾기
                missing_dates = self.quality_checker.find_missing_dates(stock_code, start_date, end_date)
                
                if missing_dates:
                    logger.info(f"{stock_code}: {len(missing_dates)}개 날짜 누락")
                    
                    if self._update_single_stock_price(stock_code, start_date, end_date):
                        success_count += 1
                else:
                    logger.debug(f"{stock_code}: 데이터 완료")
                    success_count += 1
                
                time_module.sleep(0.1)
            
            logger.info(f"기간 주가 업데이트 완료: {success_count}/{total_count}")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"기간 주가 업데이트 실패: {e}")
            return False
    
    def _update_single_stock_price(self, stock_code: str, start_date: str, end_date: str) -> bool:
        """단일 종목 주가 데이터 업데이트"""
        try:
            # 주가 데이터 수집
            df = fdr.DataReader(stock_code, start_date, end_date)
            
            if df.empty:
                logger.warning(f"주가 데이터 없음: {stock_code}")
                return False
            
            # 데이터 정리
            df = df.reset_index()
            df['stock_code'] = stock_code
            df = df.rename(columns={
                'Date': 'date',
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
                'Volume': 'volume'
            })
            
            # 필요한 컬럼만 선택
            required_columns = [
                'stock_code', 'date', 'open_price', 'high_price', 
                'low_price', 'close_price', 'volume'
            ]
            df = df[required_columns]
            
            # 추가 계산
            df['adjusted_close'] = df['close_price']
            df['amount'] = df['volume'] * df['close_price']
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            
            # 데이터베이스 저장
            with get_db_connection('stock') as conn:
                # 기존 데이터 삭제 (중복 방지)
                conn.execute(
                    "DELETE FROM stock_prices WHERE stock_code = ? AND date BETWEEN ? AND ?", 
                    (stock_code, start_date, end_date)
                )
                
                # 새 데이터 삽입
                df.to_sql('stock_prices', conn, if_exists='append', index=False)
            
            logger.debug(f"저장 완료: {stock_code} - {len(df)}개 레코드")
            return True
            
        except Exception as e:
            logger.error(f"주가 업데이트 실패 ({stock_code}): {e}")
            return False
    
    def update_daily_news(self, target_date: str = None, limit: int = None) -> bool:
        """일일 뉴스 데이터 업데이트 (25% 비중)"""
        try:
            if target_date is None:
                target_date = datetime.now().strftime('%Y-%m-%d')
            
            logger.info(f"=== 일일 뉴스 업데이트: {target_date} ===")
            
            # 기존 뉴스 수집 스크립트 활용
            from scripts.data_collection.collect_news_data import NewsCollector
            
            collector = NewsCollector()
            success = collector.collect_all_stock_news(days=1, limit=limit)
            
            if success:
                logger.info("일일 뉴스 업데이트 완료")
            else:
                logger.error("일일 뉴스 업데이트 실패")
            
            return success
            
        except Exception as e:
            logger.error(f"일일 뉴스 업데이트 실패: {e}")
            return False
    
    def update_weekly_dart_data(self) -> bool:
        """주간 DART 데이터 업데이트 (45% 비중)"""
        try:
            logger.info("=== 주간 DART 데이터 업데이트 ===")
            
            # 기존 DART 수집 스크립트 활용
            from scripts.data_collection.collect_dart_data import DartCollector
            
            collector = DartCollector()
            
            # 최근 공시 데이터만 수집
            current_year = datetime.now().year
            success = collector.collect_all_financial_data(years=[str(current_year)])
            
            if success:
                logger.info("주간 DART 데이터 업데이트 완료")
            else:
                logger.error("주간 DART 데이터 업데이트 실패")
            
            return success
            
        except Exception as e:
            logger.error(f"주간 DART 데이터 업데이트 실패: {e}")
            return False
    
    def repair_missing_data(self, days_back: int = 30) -> Dict[str, bool]:
        """누락 데이터 자동 보수"""
        try:
            logger.info(f"=== 누락 데이터 보수 시작 (최근 {days_back}일) ===")
            
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            results = {}
            
            # 1. 주가 데이터 보수
            logger.info("1. 주가 데이터 보수 중...")
            results['stock_prices'] = self.update_period_stock_prices(start_date, end_date)
            
            # 2. 뉴스 데이터 보수
            logger.info("2. 뉴스 데이터 보수 중...")
            results['news_data'] = self.update_daily_news(limit=50)  # 제한적 업데이트
            
            logger.info(f"누락 데이터 보수 완료: {results}")
            return results
            
        except Exception as e:
            logger.error(f"누락 데이터 보수 실패: {e}")
            return {'error': True}

class DataUpdateScheduler:
    """데이터 업데이트 스케줄러"""
    
    def __init__(self):
        self.updater = SmartDataUpdater()
    
    def setup_daily_schedule(self):
        """일일 스케줄 설정"""
        # 매일 오후 6시 주가 데이터 업데이트 (장 마감 후)
        schedule.every().day.at("18:00").do(self._daily_stock_update)
        
        # 매일 오전 9시 뉴스 데이터 업데이트
        schedule.every().day.at("09:00").do(self._daily_news_update)
        
        # 매주 월요일 오전 10시 DART 데이터 업데이트
        schedule.every().monday.at("10:00").do(self._weekly_dart_update)
        
        # 매주 일요일 오후 8시 누락 데이터 보수
        schedule.every().sunday.at("20:00").do(self._weekly_repair)
        
        logger.info("데이터 업데이트 스케줄 설정 완료")
    
    def _daily_stock_update(self):
        """일일 주가 업데이트 작업"""
        logger.info("⏰ 스케줄된 일일 주가 업데이트 시작")
        self.updater.update_daily_stock_prices()
    
    def _daily_news_update(self):
        """일일 뉴스 업데이트 작업"""
        logger.info("⏰ 스케줄된 일일 뉴스 업데이트 시작")
        self.updater.update_daily_news()
    
    def _weekly_dart_update(self):
        """주간 DART 업데이트 작업"""
        logger.info("⏰ 스케줄된 주간 DART 업데이트 시작")
        self.updater.update_weekly_dart_data()
    
    def _weekly_repair(self):
        """주간 누락 데이터 보수"""
        logger.info("⏰ 스케줄된 주간 데이터 보수 시작")
        self.updater.repair_missing_data()
    
    def run_scheduler(self):
        """스케줄러 실행"""
        logger.info("🚀 데이터 업데이트 스케줄러 시작")
        
        while True:
            schedule.run_pending()
            time_module.sleep(60)  # 1분마다 체크

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='워런 버핏 시스템 데이터 업데이트')
    parser.add_argument('--mode', choices=['daily', 'period', 'repair', 'schedule'], 
                       default='daily', help='업데이트 모드')
    parser.add_argument('--start-date', help='시작 날짜 (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='종료 날짜 (YYYY-MM-DD)')
    parser.add_argument('--target-date', help='대상 날짜 (YYYY-MM-DD)')
    parser.add_argument('--stock-codes', nargs='+', help='특정 종목 코드들')
    parser.add_argument('--days-back', type=int, default=30, help='보수할 일수')
    
    args = parser.parse_args()
    
    updater = SmartDataUpdater()
    
    try:
        if args.mode == 'daily':
            print("📈 일일 데이터 업데이트를 시작합니다...")
            success = updater.update_daily_stock_prices(args.target_date)
            
            if success:
                print("✅ 일일 업데이트 완료!")
            else:
                print("❌ 일일 업데이트 실패!")
        
        elif args.mode == 'period':
            if not args.start_date or not args.end_date:
                print("❌ 기간 업데이트는 --start-date와 --end-date가 필요합니다.")
                return False
            
            print(f"📊 기간 데이터 업데이트: {args.start_date} ~ {args.end_date}")
            success = updater.update_period_stock_prices(
                args.start_date, args.end_date, args.stock_codes
            )
            
            if success:
                print("✅ 기간 업데이트 완료!")
            else:
                print("❌ 기간 업데이트 실패!")
        
        elif args.mode == 'repair':
            print(f"🔧 누락 데이터 보수 (최근 {args.days_back}일)")
            results = updater.repair_missing_data(args.days_back)
            
            if not results.get('error'):
                print("✅ 데이터 보수 완료!")
                print(f"결과: {results}")
            else:
                print("❌ 데이터 보수 실패!")
        
        elif args.mode == 'schedule':
            print("⏰ 자동 스케줄러를 시작합니다...")
            scheduler = DataUpdateScheduler()
            scheduler.setup_daily_schedule()
            scheduler.run_scheduler()
        
        return True
        
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 중단되었습니다.")
        return True
    except Exception as e:
        logger.error(f"실행 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)