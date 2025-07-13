#!/usr/bin/env python3
"""
DART 데이터 빠른 수집 스크립트 (상위 기업 100개만)
"""

import sys
import sqlite3
import pandas as pd
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.data_collection.collect_dart_data import DartDataCollector

def collect_top_companies_data():
    """상위 100개 기업 데이터 수집"""
    try:
        collector = DartDataCollector()
        logger = collector.logger
        
        logger.info("상위 기업 대상 DART 데이터 수집 시작")
        
        # 상위 100개 기업 선별
        conn = collector.config_manager.get_database_connection('dart')
        query = """
        SELECT corp_code, corp_name, stock_code 
        FROM corp_codes 
        WHERE stock_code != '' 
        ORDER BY corp_name 
        LIMIT 100
        """
        corp_df = pd.read_sql(query, conn)
        conn.close()
        
        if corp_df.empty:
            logger.error("대상 기업이 없습니다.")
            return False
        
        logger.info(f"대상 기업: {len(corp_df)}개")
        
        # 재무데이터 수집 (2023년 4분기만)
        success_count = 0
        for idx, corp_row in corp_df.iterrows():
            corp_code = corp_row['corp_code']
            corp_name = corp_row['corp_name']
            
            logger.info(f"진행률: {idx+1}/{len(corp_df)} - {corp_name}({corp_code})")
            
            # 재무제표 수집
            financial_data = collector.get_financial_statements(corp_code, 2023, '11011')
            
            if not financial_data.empty:
                collector.save_to_database(financial_data=financial_data)
                success_count += 1
                logger.info(f"✅ {corp_name} 재무데이터 저장 완료")
            else:
                logger.warning(f"⚠️ {corp_name} 재무데이터 없음")
            
            # API 호출 제한 대응
            import time
            time.sleep(0.1)
        
        logger.info(f"✅ 재무데이터 수집 완료: {success_count}/{len(corp_df)}개 기업")
        
        # 공시정보 수집 (상위 20개 기업만)
        top20_df = corp_df.head(20)
        from datetime import datetime, timedelta
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        for idx, corp_row in top20_df.iterrows():
            corp_code = corp_row['corp_code']
            corp_name = corp_row['corp_name']
            
            logger.info(f"공시정보 수집: {idx+1}/{len(top20_df)} - {corp_name}")
            
            disclosure_data = collector.get_disclosures(
                corp_code,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            
            if not disclosure_data.empty:
                collector.save_to_database(disclosure_data=disclosure_data)
            
            time.sleep(0.1)
        
        logger.info("✅ 전체 수집 완료")
        return True
        
    except Exception as e:
        print(f"❌ 수집 실패: {e}")
        return False

if __name__ == "__main__":
    collect_top_companies_data()
