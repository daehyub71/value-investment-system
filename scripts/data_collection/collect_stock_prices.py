# =============================================================================
# 3. scripts/data_collection/collect_stock_prices.py
# =============================================================================

#!/usr/bin/env python3
"""
주가 데이터 수집 스크립트
실행: python scripts/data_collection/collect_stock_prices.py
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import FinanceDataReader as fdr
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
import logging
from config import get_db_connection

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockPriceCollector:
    def __init__(self):
        pass
    
    def collect_stock_prices(self, stock_code, start_date, end_date):
        """개별 종목 주가 데이터 수집"""
        try:
            df = fdr.DataReader(stock_code, start_date, end_date)
            
            if df.empty:
                logger.warning(f"주가 데이터 없음: {stock_code}")
                return pd.DataFrame()
            
            # 컬럼명 정리 및 종목코드 추가
            df = df.reset_index()
            df['stock_code'] = stock_code
            df = df.rename(columns={
                'Date': 'date',
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
                'Volume': 'volume',
                'Change': 'change_rate'
            })
            
            # 필요한 컬럼만 선택
            required_columns = [
                'stock_code', 'date', 'open_price', 'high_price', 
                'low_price', 'close_price', 'volume'
            ]
            
            # 존재하는 컬럼만 사용
            available_columns = [col for col in required_columns if col in df.columns]
            df = df[available_columns]
            
            # 수정종가 및 거래대금 계산
            df['adjusted_close'] = df['close_price']
            df['amount'] = df['volume'] * df['close_price']
            
            # 날짜 형식 통일
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            
            return df
            
        except Exception as e:
            logger.error(f"주가 데이터 수집 실패 ({stock_code}): {e}")
            return pd.DataFrame()
    
    def collect_all_stock_prices(self, start_date, end_date, limit=None):
        """전종목 주가 데이터 수집"""
        try:
            # 종목코드 목록 가져오기
            with get_db_connection('stock') as conn:
                query = "SELECT stock_code, company_name FROM company_info"
                if limit:
                    query += f" LIMIT {limit}"
                
                stocks = pd.read_sql(query, conn)
            
            if stocks.empty:
                logger.error("종목 정보가 없습니다. 먼저 종목 기본정보를 수집해주세요.")
                return False
            
            total_count = len(stocks)
            success_count = 0
            
            for i, row in stocks.iterrows():
                stock_code = row['stock_code']
                company_name = row['company_name']
                
                logger.info(f"주가 수집: {i+1}/{total_count} - {stock_code} ({company_name})")
                
                # 주가 데이터 수집
                price_data = self.collect_stock_prices(stock_code, start_date, end_date)
                
                if not price_data.empty:
                    # 데이터베이스에 저장
                    with get_db_connection('stock') as conn:
                        # 기존 데이터 삭제
                        conn.execute(
                            "DELETE FROM stock_prices WHERE stock_code = ? AND date BETWEEN ? AND ?", 
                            (stock_code, start_date, end_date)
                        )
                        # 새 데이터 입력
                        price_data.to_sql('stock_prices', conn, if_exists='append', index=False)
                    
                    success_count += 1
                    logger.info(f"저장 완료: {stock_code} - {len(price_data)}개 레코드")
                
                # API 호출 제한
                time.sleep(0.1)
            
            logger.info(f"전종목 주가 수집 완료: {success_count}/{total_count}")
            return True
            
        except Exception as e:
            logger.error(f"전종목 주가 수집 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        collector = StockPriceCollector()
        
        print("📈 주가 데이터 수집을 시작합니다...")
        
        # 기본 날짜 설정 (최근 2년)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
        
        print(f"수집 기간: {start_date} ~ {end_date}")
        
        # 테스트 모드 여부 확인
        test_mode = input("테스트 모드로 실행하시겠습니까? (10개 종목만 수집) (y/N): ")
        limit = 10 if test_mode.lower() == 'y' else None
        
        if not test_mode.lower() == 'y':
            print("⚠️  전종목 주가 수집은 시간이 오래 걸립니다 (1-2시간)...")
            user_input = input("계속 진행하시겠습니까? (y/N): ")
            if user_input.lower() != 'y':
                print("주가 데이터 수집을 취소합니다.")
                return False
        
        # 주가 데이터 수집
        success = collector.collect_all_stock_prices(start_date, end_date, limit)
        
        if success:
            print("✅ 주가 데이터 수집 완료!")
        else:
            print("❌ 주가 데이터 수집 실패!")
            
        return success
        
    except Exception as e:
        logger.error(f"주가 데이터 수집 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
