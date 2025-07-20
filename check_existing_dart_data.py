#!/usr/bin/env python3
"""
2025년 1분기 DART 재무데이터 긴급 수집
"""

import sqlite3
import pandas as pd

def check_existing_data():
    """현재 보유 데이터 확인"""
    
    print("🔍 현재 DART 데이터 현황 확인")
    print("=" * 50)
    
    try:
        conn = sqlite3.connect('data/databases/dart_data.db')
        
        # 연도별 데이터 현황
        query = """
        SELECT 
            bsns_year as 연도,
            COUNT(DISTINCT corp_code) as 기업수,
            COUNT(*) as 총레코드수,
            MIN(created_at) as 최초수집일,
            MAX(created_at) as 최근수집일
        FROM financial_statements 
        GROUP BY bsns_year 
        ORDER BY bsns_year DESC
        LIMIT 10
        """
        
        result = pd.read_sql(query, conn)
        
        if not result.empty:
            print("📊 연도별 보유 데이터:")
            for _, row in result.iterrows():
                print(f"   {row['연도']}년: {row['기업수']:,}개 기업, {row['총레코드수']:,}건")
                if pd.notna(row['최근수집일']):
                    print(f"          수집일: {row['최근수집일']}")
        else:
            print("❌ 재무데이터가 없습니다.")
        
        # 최신 데이터 연도 확인
        latest_query = "SELECT MAX(bsns_year) FROM financial_statements"
        latest_year = pd.read_sql(latest_query, conn).iloc[0, 0]
        
        print(f"\n📅 현재 최신 데이터: {latest_year}년")
        print(f"🎯 필요한 데이터: 2025년 1분기")
        print(f"⚠️  데이터 갭: {2025 - latest_year}년")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    check_existing_data()
