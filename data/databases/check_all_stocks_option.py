#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock_data.db 구조 확인 및 --all_stocks 옵션 동작 검증
"""

import sqlite3
import os
from pathlib import Path

def main():
    """메인 실행 함수"""
    print("🔍 stock_data.db 및 --all_stocks 옵션 검증")
    
    db_path = 'C:/data_analysis/value-investment-system/value-investment-system/data/databases/stock_data.db'
    
    if not os.path.exists(db_path):
        print("❌ stock_data.db 파일을 찾을 수 없습니다.")
        return
    
    print("=" * 60)
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # 1. 테이블 목록 확인
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            print(f"\n📊 테이블 목록 ({len(tables)}개):")
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"  - {table_name}: {count:,}행")
            
            # 2. company_info 테이블 존재 여부 확인
            table_names = [table[0] for table in tables]
            
            if 'company_info' in table_names:
                print(f"\n✅ company_info 테이블 존재")
                
                # 현재 --all_stocks으로 선택되는 종목들
                cursor.execute("""
                    SELECT stock_code, company_name, market_cap
                    FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                    ORDER BY market_cap DESC 
                    LIMIT 20
                """)
                
                top_stocks = cursor.fetchall()
                print(f"\n🏆 현재 --all_stocks으로 선택되는 상위 20개 종목:")
                for i, (code, name, cap) in enumerate(top_stocks, 1):
                    print(f"  {i:2d}. {name}({code}) - {cap:,}")
                
                # 아모레퍼시픽 포함 여부 확인
                cursor.execute("SELECT * FROM company_info WHERE stock_code = '090430'")
                amore_info = cursor.fetchone()
                if amore_info:
                    print(f"\n📋 아모레퍼시픽 정보: {amore_info}")
                else:
                    print(f"\n❌ company_info에 아모레퍼시픽(090430) 정보가 없습니다.")
                
            else:
                print(f"\n❌ company_info 테이블이 없습니다!")
                print(f"   현재 --all_stocks 옵션이 작동하지 않습니다.")
            
            # 3. 실제 주가 데이터 테이블 확인
            price_tables = ['daily_prices', 'stock_prices', 'prices']
            found_price_table = None
            
            for table in price_tables:
                if table in table_names:
                    found_price_table = table
                    break
            
            if found_price_table:
                print(f"\n📈 주가 데이터 테이블: {found_price_table}")
                
                # 테이블 구조
                cursor.execute(f"PRAGMA table_info({found_price_table})")
                columns = cursor.fetchall()
                column_names = [col[1] for col in columns]
                print(f"   컬럼: {column_names}")
                
                # 고유 종목 수 확인
                symbol_columns = ['symbol', 'stock_code', 'code']
                symbol_column = None
                
                for col in symbol_columns:
                    if col in column_names:
                        symbol_column = col
                        break
                
                if symbol_column:
                    cursor.execute(f"SELECT COUNT(DISTINCT {symbol_column}) FROM {found_price_table}")
                    unique_stocks = cursor.fetchone()[0]
                    print(f"   고유 종목 수: {unique_stocks:,}개")
                    
                    # 샘플 종목들
                    cursor.execute(f"SELECT DISTINCT {symbol_column} FROM {found_price_table} LIMIT 10")
                    sample_symbols = cursor.fetchall()
                    print(f"   샘플 종목: {[s[0] for s in sample_symbols]}")
                    
                    # 아모레퍼시픽 데이터 확인
                    cursor.execute(f"SELECT COUNT(*) FROM {found_price_table} WHERE {symbol_column} LIKE '%090430%' OR {symbol_column} = '090430'")
                    amore_count = cursor.fetchone()[0]
                    print(f"   아모레퍼시픽(090430) 주가 데이터: {amore_count}개")
                    
                    if amore_count > 0:
                        cursor.execute(f"SELECT MIN(date), MAX(date) FROM {found_price_table} WHERE {symbol_column} LIKE '%090430%' OR {symbol_column} = '090430'")
                        date_range = cursor.fetchone()
                        print(f"   아모레퍼시픽 데이터 기간: {date_range[0]} ~ {date_range[1]}")
            
            print(f"\n" + "=" * 60)
            print(f"🔧 결론 및 권장사항")
            print(f"=" * 60)
            
            if 'company_info' not in table_names:
                print("❌ 현재 --all_stocks 옵션은 작동하지 않습니다!")
                print("   company_info 테이블이 존재하지 않습니다.")
                print("\n📋 해결 방안:")
                print("1. company_info 테이블 생성 필요")
                print("2. 또는 daily_prices 테이블 기반으로 코드 수정")
                print("3. 임시로 개별 종목 수집 사용")
            else:
                cursor.execute("SELECT COUNT(*) FROM company_info WHERE market_cap IS NOT NULL AND market_cap > 0")
                valid_stocks = cursor.fetchone()[0]
                print(f"✅ --all_stocks 옵션으로 {valid_stocks}개 종목 처리 가능")
                print(f"   (기본 LIMIT 50으로 인해 실제로는 최대 50개)")
                
                if '090430' not in [stock[0] for stock in top_stocks]:
                    print("⚠️  아모레퍼시픽은 상위 50개에 포함되지 않음")
                    print("   --limit 옵션을 늘리거나 개별 수집 필요")

    except Exception as e:
        print(f"❌ 데이터베이스 분석 실패: {e}")

if __name__ == "__main__":
    main()
