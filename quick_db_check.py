#!/usr/bin/env python3
"""
데이터베이스 전체 현황 빠른 체크 도구
모든 financial_ratios 관련 테이블의 데이터 확인

실행 방법:
python quick_db_check.py
"""

import sqlite3
import pandas as pd
from pathlib import Path

def check_all_tables():
    """모든 테이블 현황 체크"""
    db_path = Path('data/databases/stock_data.db')
    
    if not db_path.exists():
        print("❌ stock_data.db 파일을 찾을 수 없습니다.")
        return
    
    try:
        with sqlite3.connect(db_path) as conn:
            # 1. 모든 테이블 목록
            tables = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """).fetchall()
            
            print("📋 데이터베이스 내 모든 테이블:")
            print("=" * 60)
            
            for table in tables:
                table_name = table[0]
                
                # 테이블 레코드 수
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"   📊 {table_name}: {count:,}개 레코드")
                    
                    # financial_ratios 관련 테이블이면 상세 정보
                    if 'financial_ratio' in table_name.lower():
                        print(f"      🔍 상세 분석:")
                        
                        # 고유 종목 수
                        try:
                            unique_stocks = conn.execute(f"SELECT COUNT(DISTINCT stock_code) FROM {table_name}").fetchone()[0]
                            print(f"         고유 종목: {unique_stocks:,}개")
                        except:
                            pass
                        
                        # 삼성전자 데이터 확인
                        try:
                            samsung_data = conn.execute(f"""
                                SELECT stock_code, company_name, per, pbr, current_price, updated_at
                                FROM {table_name} 
                                WHERE stock_code = '005930'
                                LIMIT 3
                            """).fetchall()
                            
                            if samsung_data:
                                print(f"         ✅ 삼성전자 데이터 있음:")
                                for row in samsung_data:
                                    print(f"            {row}")
                            else:
                                print(f"         ❌ 삼성전자 데이터 없음")
                        except Exception as e:
                            print(f"         ⚠️ 삼성전자 조회 실패: {e}")
                        
                        # 최근 데이터 3개
                        try:
                            recent_data = conn.execute(f"""
                                SELECT stock_code, company_name, per, pbr 
                                FROM {table_name} 
                                ORDER BY updated_at DESC 
                                LIMIT 3
                            """).fetchall()
                            
                            if recent_data:
                                print(f"         📅 최근 데이터:")
                                for row in recent_data:
                                    print(f"            {row}")
                        except Exception as e:
                            print(f"         ⚠️ 최근 데이터 조회 실패: {e}")
                        
                        print()
                
                except Exception as e:
                    print(f"   ❌ {table_name}: 조회 실패 ({e})")
            
            print("\n" + "=" * 60)
            print("📊 요약:")
            
            # financial_ratios 관련 테이블만 요약
            financial_tables = [t[0] for t in tables if 'financial_ratio' in t[0].lower()]
            
            if financial_tables:
                print(f"💼 Financial Ratios 테이블: {len(financial_tables)}개")
                for table_name in financial_tables:
                    try:
                        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                        unique_stocks = conn.execute(f"SELECT COUNT(DISTINCT stock_code) FROM {table_name}").fetchone()[0]
                        print(f"   • {table_name}: {count:,}개 레코드, {unique_stocks:,}개 종목")
                    except:
                        pass
            else:
                print("❌ Financial Ratios 테이블이 없습니다.")
            
            # stock_prices 테이블 확인
            if any('stock_prices' in t[0] for t in tables):
                try:
                    stock_count = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_prices").fetchone()[0]
                    total_records = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
                    print(f"📈 Stock Prices: {total_records:,}개 레코드, {stock_count:,}개 종목")
                    
                    # 삼성전자 주가 데이터 확인
                    samsung_price = conn.execute("""
                        SELECT stock_code, date, close_price 
                        FROM stock_prices 
                        WHERE stock_code = '005930' 
                        ORDER BY date DESC 
                        LIMIT 1
                    """).fetchone()
                    
                    if samsung_price:
                        print(f"   ✅ 삼성전자 최신 주가: {samsung_price}")
                    else:
                        print(f"   ❌ 삼성전자 주가 데이터 없음")
                        
                except Exception as e:
                    print(f"   ❌ Stock Prices 조회 실패: {e}")
            
    except Exception as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")

if __name__ == "__main__":
    print("🔍 데이터베이스 전체 현황 빠른 체크")
    print("=" * 60)
    check_all_tables()
