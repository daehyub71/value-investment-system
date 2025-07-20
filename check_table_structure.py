#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
테이블 구조 확인 후 즉시 실행 가능한 수정 스크립트
"""

import sqlite3
import os

def check_and_fix():
    """테이블 구조 확인 및 즉시 수정"""
    
    os.chdir('C:/data_analysis/value-investment-system/value-investment-system')
    stock_db = 'data/databases/stock_data.db'
    
    print("🔍 테이블 구조 확인")
    print("=" * 40)
    
    try:
        with sqlite3.connect(stock_db) as conn:
            cursor = conn.cursor()
            
            # daily_prices 테이블 구조 확인
            cursor.execute("PRAGMA table_info(daily_prices)")
            columns = cursor.fetchall()
            
            print(f"📋 daily_prices 컬럼:")
            column_names = []
            for col in columns:
                print(f"   {col[1]} ({col[2]})")
                column_names.append(col[1])
            
            # 종목 컬럼 찾기
            possible_stock_columns = ['symbol', 'stock_code', 'code', 'ticker', 'Symbol', 'Code']
            stock_column = None
            
            for col in possible_stock_columns:
                if col in column_names:
                    stock_column = col
                    break
            
            if stock_column:
                print(f"✅ 종목 컬럼 발견: {stock_column}")
                
                # 샘플 데이터 확인
                cursor.execute(f"SELECT DISTINCT {stock_column} FROM daily_prices WHERE {stock_column} IS NOT NULL LIMIT 10")
                samples = [row[0] for row in cursor.fetchall()]
                print(f"📊 샘플 종목: {samples}")
                
                # 전체 종목 수 확인
                cursor.execute(f"SELECT COUNT(DISTINCT {stock_column}) FROM daily_prices WHERE {stock_column} IS NOT NULL AND LENGTH({stock_column}) = 6")
                total_count = cursor.fetchone()[0]
                print(f"📈 총 종목 수: {total_count}개")
                
                # 수정된 쿼리 생성
                fixed_query = f"""
                    SELECT DISTINCT {stock_column}, COUNT(*) as data_count
                    FROM daily_prices 
                    WHERE {stock_column} IS NOT NULL 
                      AND {stock_column} != ''
                      AND LENGTH({stock_column}) = 6
                    GROUP BY {stock_column}
                    HAVING data_count >= 5
                    ORDER BY data_count DESC
                """
                
                print(f"\n🔧 수정된 쿼리:")
                print(fixed_query)
                
                # 실제 실행 테스트
                cursor.execute(fixed_query)
                results = cursor.fetchall()
                
                # 숫자로만 된 종목코드 필터링
                valid_stocks = []
                for stock_code, count in results:
                    if isinstance(stock_code, str) and stock_code.isdigit() and len(stock_code) == 6:
                        valid_stocks.append(stock_code)
                
                print(f"✅ 유효한 종목: {len(valid_stocks)}개")
                print(f"📋 상위 20개: {valid_stocks[:20]}")
                
                return stock_column, valid_stocks
                
            else:
                print(f"❌ 종목 컬럼을 찾을 수 없음")
                print(f"사용 가능한 컬럼: {column_names}")
                return None, []
                
    except Exception as e:
        print(f"❌ 확인 실패: {e}")
        return None, []

if __name__ == "__main__":
    check_and_fix()
