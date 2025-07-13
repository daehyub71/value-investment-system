#!/usr/bin/env python3
"""
company_info 테이블 구조 확인 스크립트
"""

import sqlite3
from pathlib import Path

def check_company_info_table():
    """company_info 테이블 구조 및 데이터 확인"""
    
    db_path = Path('data/databases/stock_data.db')
    
    if not db_path.exists():
        print("❌ stock_data.db 파일이 없습니다.")
        return
    
    with sqlite3.connect(db_path) as conn:
        # 테이블 구조 확인
        print("📊 company_info 테이블 구조:")
        print("-" * 50)
        
        cursor = conn.execute("PRAGMA table_info(company_info)")
        columns = cursor.fetchall()
        
        for col in columns:
            print(f"  {col[1]} ({col[2]}) - NULL: {bool(col[3])}")
        
        print("\n📈 데이터 샘플 (상위 10개):")
        print("-" * 50)
        
        cursor = conn.execute("SELECT * FROM company_info LIMIT 10")
        rows = cursor.fetchall()
        
        # 컬럼명 출력
        column_names = [description[0] for description in cursor.description]
        print("  " + " | ".join(column_names))
        print("  " + "-" * (len(" | ".join(column_names))))
        
        for row in rows:
            print("  " + " | ".join(str(cell)[:15] if cell else "NULL" for cell in row))
        
        # market_cap 컬럼 존재 여부 및 데이터 확인
        print("\n💰 market_cap 컬럼 분석:")
        print("-" * 50)
        
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM company_info WHERE market_cap IS NOT NULL")
            non_null_count = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM company_info")
            total_count = cursor.fetchone()[0]
            
            print(f"  전체 기업: {total_count}개")
            print(f"  market_cap 값이 있는 기업: {non_null_count}개")
            
            if non_null_count > 0:
                cursor = conn.execute("SELECT stock_code, company_name, market_cap FROM company_info WHERE market_cap IS NOT NULL ORDER BY market_cap DESC LIMIT 5")
                top_companies = cursor.fetchall()
                
                print("\n  시가총액 상위 5개 기업:")
                for stock_code, company_name, market_cap in top_companies:
                    print(f"    {stock_code} | {company_name} | {market_cap:,}")
            
        except sqlite3.OperationalError as e:
            print(f"  ❌ market_cap 컬럼 없음: {e}")
            
            print("\n  📋 대안: stock_code로 종목 조회")
            cursor = conn.execute("SELECT stock_code, company_name FROM company_info ORDER BY stock_code LIMIT 10")
            companies = cursor.fetchall()
            
            for stock_code, company_name in companies:
                print(f"    {stock_code} | {company_name}")

if __name__ == "__main__":
    check_company_info_table()
