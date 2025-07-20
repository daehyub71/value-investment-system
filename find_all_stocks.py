#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
모든 테이블에서 종목 찾기 - 전체 종목 확보
"""

import sqlite3
import os

def comprehensive_stock_search():
    """모든 테이블에서 종목 데이터 종합 분석"""
    
    os.chdir('C:/data_analysis/value-investment-system/value-investment-system')
    stock_db = 'data/databases/stock_data.db'
    
    print("🔍 전체 테이블 종목 데이터 종합 분석")
    print("=" * 60)
    
    all_stocks = set()
    table_results = {}
    
    try:
        with sqlite3.connect(stock_db) as conn:
            cursor = conn.cursor()
            
            # 모든 테이블 확인
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [table[0] for table in cursor.fetchall()]
            
            for table_name in tables:
                print(f"\n📊 {table_name} 테이블 분석:")
                
                try:
                    # 컬럼 정보
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = cursor.fetchall()
                    column_names = [col[1] for col in columns]
                    
                    # 종목 관련 컬럼 찾기
                    stock_columns = [col for col in column_names 
                                   if any(keyword in col.lower() for keyword in ['code', 'symbol', 'ticker'])]
                    
                    if stock_columns:
                        print(f"   종목 컬럼: {stock_columns}")
                        
                        for col in stock_columns:
                            try:
                                # 6자리 숫자 종목코드 찾기
                                cursor.execute(f"""
                                    SELECT DISTINCT {col} 
                                    FROM {table_name} 
                                    WHERE {col} IS NOT NULL 
                                      AND LENGTH({col}) = 6
                                    ORDER BY {col}
                                """)
                                
                                stocks = [str(row[0]) for row in cursor.fetchall()]
                                valid_stocks = [s for s in stocks if s.isdigit()]
                                
                                if valid_stocks:
                                    print(f"   ✅ {col}: {len(valid_stocks)}개 종목")
                                    print(f"      샘플: {valid_stocks[:10]}")
                                    
                                    table_results[f"{table_name}.{col}"] = valid_stocks
                                    all_stocks.update(valid_stocks)
                                else:
                                    print(f"   ❌ {col}: 유효한 종목코드 없음")
                                    
                            except Exception as e:
                                print(f"   ❌ {col}: 오류 - {e}")
                    else:
                        print(f"   ⚠️ 종목 관련 컬럼 없음")
                        
                except Exception as e:
                    print(f"   ❌ 테이블 분석 실패: {e}")
            
            # 결과 요약
            print(f"\n" + "=" * 60)
            print(f"📊 종합 결과")
            print(f"=" * 60)
            
            print(f"🎯 총 고유 종목 수: {len(all_stocks)}개")
            
            if table_results:
                print(f"\n📋 테이블별 종목 수:")
                for table_col, stocks in table_results.items():
                    print(f"   {table_col}: {len(stocks)}개")
                
                # 가장 많은 종목을 가진 테이블 추천
                best_source = max(table_results.items(), key=lambda x: len(x[1]))
                print(f"\n🏆 권장 소스: {best_source[0]} ({len(best_source[1])}개 종목)")
                
                # 전체 종목 목록 생성
                sorted_stocks = sorted(list(all_stocks))
                print(f"\n📋 전체 종목 목록 (상위 50개):")
                for i, stock in enumerate(sorted_stocks[:50]):
                    if i % 10 == 0:
                        print(f"\n   ", end="")
                    print(f"{stock} ", end="")
                
                if len(sorted_stocks) > 50:
                    print(f"\n   ... 외 {len(sorted_stocks)-50}개")
                
                return best_source[0], best_source[1]
            else:
                print("❌ 어떤 테이블에서도 종목 데이터를 찾을 수 없음")
                return None, []
                
    except Exception as e:
        print(f"❌ 전체 분석 실패: {e}")
        return None, []

if __name__ == "__main__":
    best_table, stock_list = comprehensive_stock_search()
    
    if stock_list:
        print(f"\n🔧 수정된 스크립트 생성 가이드:")
        table_name, column_name = best_table.split('.')
        print(f"테이블: {table_name}")
        print(f"컬럼: {column_name}")
        print(f"종목 수: {len(stock_list)}개")
