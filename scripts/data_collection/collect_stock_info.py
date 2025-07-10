# =============================================================================
# 1. scripts/data_collection/collect_stock_info.py
# =============================================================================

#!/usr/bin/env python3
"""
전종목 기본정보 수집 스크립트
실행: python scripts/data_collection/collect_stock_info.py
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import sqlite3
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime
import logging
from config import get_db_connection

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def collect_stock_info():
    """전종목 기본정보 수집"""
    try:
        logger.info("=== 전종목 기본정보 수집 시작 ===")
        
        # KOSPI 종목 수집
        logger.info("KOSPI 종목 수집 중...")
        kospi_stocks = fdr.StockListing('KOSPI')
        kospi_stocks['market_type'] = 'KOSPI'
        
        # KOSDAQ 종목 수집
        logger.info("KOSDAQ 종목 수집 중...")
        kosdaq_stocks = fdr.StockListing('KOSDAQ')
        kosdaq_stocks['market_type'] = 'KOSDAQ'
        
        # 통합
        all_stocks = pd.concat([kospi_stocks, kosdaq_stocks], ignore_index=True)
        
        # 컬럼명 정리
        all_stocks = all_stocks.rename(columns={
            'Code': 'stock_code',
            'Name': 'company_name',
            'Market': 'market_type',
            'Sector': 'sector',
            'Industry': 'industry',
            'ListingDate': 'listing_date',
            'Marcap': 'market_cap',
            'Stocks': 'shares_outstanding'
        })
        
        # 필요한 컬럼만 선택
        columns_to_keep = [
            'stock_code', 'company_name', 'market_type', 
            'sector', 'industry', 'listing_date', 
            'market_cap', 'shares_outstanding'
        ]
        
        # 존재하는 컬럼만 필터링
        available_columns = [col for col in columns_to_keep if col in all_stocks.columns]
        all_stocks = all_stocks[available_columns]
        
        logger.info(f"총 {len(all_stocks)}개 종목 수집 완료")
        
        # 데이터베이스에 저장
        with get_db_connection('stock') as conn:
            # 기존 데이터 삭제
            conn.execute("DELETE FROM company_info")
            
            # 새 데이터 입력
            all_stocks.to_sql('company_info', conn, if_exists='append', index=False)
            
            logger.info(f"데이터베이스 저장 완료: {len(all_stocks)}개 종목")
        
        # 결과 출력
        print(f"\n📊 수집 결과:")
        print(f"  - KOSPI: {len(kospi_stocks)}개 종목")
        print(f"  - KOSDAQ: {len(kosdaq_stocks)}개 종목")
        print(f"  - 총합: {len(all_stocks)}개 종목")
        
        return True
        
    except Exception as e:
        logger.error(f"전종목 기본정보 수집 실패: {e}")
        return False

if __name__ == "__main__":
    success = collect_stock_info()
    if success:
        print("✅ 전종목 기본정보 수집 성공!")
    else:
        print("❌ 전종목 기본정보 수집 실패!")
        sys.exit(1)