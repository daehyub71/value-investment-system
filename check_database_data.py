#!/usr/bin/env python3
"""
데이터베이스 데이터 확인 스크립트
주가 데이터 존재 여부 및 구조 확인

실행 방법:
python check_database_data.py --stock_code=000660
python check_database_data.py --show_all_stocks
"""

import os
import sys
import sqlite3
import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime, timedelta

# 프로젝트 루트 경로
project_root = Path(__file__).parent
if not (project_root / 'config').exists():
    project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

class DatabaseChecker:
    """데이터베이스 확인 클래스"""
    
    def __init__(self):
        self.db_path = project_root / 'data' / 'databases' / 'stock_data.db'
        print(f"📁 데이터베이스 경로: {self.db_path}")
        
        if not self.db_path.exists():
            print(f"❌ 데이터베이스 파일이 없습니다: {self.db_path}")
            sys.exit(1)
        
        # 파일 크기 확인
        file_size = self.db_path.stat().st_size
        print(f"📊 데이터베이스 크기: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
    
    def get_connection(self):
        """데이터베이스 연결"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn
    
    def check_tables(self):
        """테이블 구조 확인"""
        print("\n🔍 테이블 구조 확인")
        print("=" * 50)
        
        with self.get_connection() as conn:
            # 모든 테이블 조회
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            print(f"📋 총 테이블 수: {len(tables)}")
            
            for table in tables:
                # 테이블별 레코드 수 확인
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  📊 {table}: {count:,} 레코드")
                
                # 테이블 구조 확인 (처음 5개 컬럼만)
                cursor = conn.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                col_names = [col[1] for col in columns[:5]]
                if len(columns) > 5:
                    col_names.append("...")
                print(f"     컬럼: {', '.join(col_names)}")
    
    def check_stock_data(self, stock_code: str):
        """특정 주식의 데이터 확인"""
        print(f"\n🎯 {stock_code} 데이터 확인")
        print("=" * 50)
        
        with self.get_connection() as conn:
            # stock_prices 테이블에서 해당 주식 데이터 조회
            query = """
                SELECT COUNT(*) as count,
                       MIN(date) as start_date,
                       MAX(date) as end_date
                FROM stock_prices 
                WHERE stock_code = ?
            """
            cursor = conn.execute(query, (stock_code,))
            result = cursor.fetchone()
            
            if result and result[0] > 0:
                print(f"✅ 데이터 존재: {result[0]:,} 레코드")
                print(f"📅 기간: {result[1]} ~ {result[2]}")
                
                # 최근 5일 데이터 확인
                query = """
                    SELECT date, open_price, high_price, low_price, close_price, volume
                    FROM stock_prices 
                    WHERE stock_code = ? 
                    ORDER BY date DESC 
                    LIMIT 5
                """
                cursor = conn.execute(query, (stock_code,))
                recent_data = cursor.fetchall()
                
                print(f"\n📈 최근 5일 데이터:")
                print("날짜        | 시가      | 고가      | 저가      | 종가      | 거래량")
                print("-" * 70)
                for row in recent_data:
                    date, open_p, high, low, close, volume = row
                    print(f"{date} | {open_p:>8.0f} | {high:>8.0f} | {low:>8.0f} | {close:>8.0f} | {volume:>10,}")
                
                # 기술적 지표 확인
                query = """
                    SELECT date, sma_5, sma_20, sma_200, rsi, macd, macd_signal
                    FROM stock_prices 
                    WHERE stock_code = ? 
                        AND sma_5 IS NOT NULL 
                    ORDER BY date DESC 
                    LIMIT 3
                """
                cursor = conn.execute(query, (stock_code,))
                indicator_data = cursor.fetchall()
                
                if indicator_data:
                    print(f"\n📊 기술적 지표 (최근 3일):")
                    print("날짜        | SMA5   | SMA20  | SMA200 | RSI   | MACD")
                    print("-" * 55)
                    for row in indicator_data:
                        date, sma5, sma20, sma200, rsi, macd, signal = row
                        sma5_str = f"{sma5:>6.0f}" if sma5 else "  N/A "
                        sma20_str = f"{sma20:>6.0f}" if sma20 else "  N/A "
                        sma200_str = f"{sma200:>6.0f}" if sma200 else "  N/A "
                        rsi_str = f"{rsi:>5.1f}" if rsi else " N/A "
                        macd_str = f"{macd:>6.2f}" if macd else "  N/A "
                        print(f"{date} | {sma5_str} | {sma20_str} | {sma200_str} | {rsi_str} | {macd_str}")
                else:
                    print(f"\n⚠️ 기술적 지표 데이터가 없습니다.")
                    
            else:
                print(f"❌ {stock_code} 데이터가 없습니다.")
                
                # 비슷한 주식 코드 찾기
                query = """
                    SELECT DISTINCT stock_code 
                    FROM stock_prices 
                    WHERE stock_code LIKE ? 
                    LIMIT 5
                """
                cursor = conn.execute(query, (f"%{stock_code[-3:]}%",))
                similar_codes = [row[0] for row in cursor.fetchall()]
                
                if similar_codes:
                    print(f"💡 비슷한 주식 코드들: {', '.join(similar_codes)}")
    
    def check_company_info(self, stock_code: str):
        """회사 정보 확인"""
        print(f"\n🏢 {stock_code} 회사 정보")
        print("=" * 30)
        
        with self.get_connection() as conn:
            query = """
                SELECT company_name, market_type, sector, industry
                FROM company_info 
                WHERE stock_code = ?
            """
            cursor = conn.execute(query, (stock_code,))
            result = cursor.fetchone()
            
            if result:
                print(f"회사명: {result[0]}")
                print(f"시장: {result[1] or 'N/A'}")
                print(f"섹터: {result[2] or 'N/A'}")
                print(f"업종: {result[3] or 'N/A'}")
            else:
                print(f"❌ {stock_code} 회사 정보가 없습니다.")
    
    def show_all_stocks(self, limit: int = 20):
        """저장된 모든 주식 목록 확인"""
        print(f"\n📈 저장된 주식 목록 (상위 {limit}개)")
        print("=" * 50)
        
        with self.get_connection() as conn:
            query = """
                SELECT sp.stock_code, ci.company_name, COUNT(*) as data_count,
                       MIN(sp.date) as start_date, MAX(sp.date) as end_date
                FROM stock_prices sp
                LEFT JOIN company_info ci ON sp.stock_code = ci.stock_code
                GROUP BY sp.stock_code
                ORDER BY data_count DESC
                LIMIT ?
            """
            cursor = conn.execute(query, (limit,))
            results = cursor.fetchall()
            
            print("코드   | 회사명           | 데이터수 | 시작일     | 종료일")
            print("-" * 65)
            for row in results:
                code, name, count, start, end = row
                name_display = (name or 'N/A')[:15].ljust(15)
                print(f"{code} | {name_display} | {count:>6,} | {start} | {end}")
    
    def diagnose_problem(self, stock_code: str):
        """문제 진단"""
        print(f"\n🔧 {stock_code} 문제 진단")
        print("=" * 30)
        
        issues = []
        
        with self.get_connection() as conn:
            # 1. 기본 데이터 존재 여부
            cursor = conn.execute("SELECT COUNT(*) FROM stock_prices WHERE stock_code = ?", (stock_code,))
            if cursor.fetchone()[0] == 0:
                issues.append("❌ 주가 데이터가 전혀 없음")
            
            # 2. 최근 데이터 여부 (30일 이내)
            recent_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            cursor = conn.execute(
                "SELECT COUNT(*) FROM stock_prices WHERE stock_code = ? AND date >= ?", 
                (stock_code, recent_date)
            )
            if cursor.fetchone()[0] == 0:
                issues.append("⚠️ 최근 30일 데이터가 없음")
            
            # 3. 기술적 지표 계산 여부
            cursor = conn.execute(
                "SELECT COUNT(*) FROM stock_prices WHERE stock_code = ? AND sma_20 IS NOT NULL", 
                (stock_code,)
            )
            if cursor.fetchone()[0] == 0:
                issues.append("⚠️ 기술적 지표가 계산되지 않음")
            
            # 4. 회사 정보 존재 여부
            cursor = conn.execute("SELECT COUNT(*) FROM company_info WHERE stock_code = ?", (stock_code,))
            if cursor.fetchone()[0] == 0:
                issues.append("⚠️ 회사 정보가 없음")
        
        if issues:
            print("발견된 문제들:")
            for issue in issues:
                print(f"  {issue}")
            
            print(f"\n💡 해결 방법:")
            if "주가 데이터가 전혀 없음" in str(issues):
                print(f"  1. 데이터 수집 실행: python scripts/data_collection/collect_stock_data.py --stock_code={stock_code}")
            if "기술적 지표가 계산되지 않음" in str(issues):
                print(f"  2. 기술적 지표 계산: python scripts/analysis/calculate_technical_indicators.py --stock_code={stock_code}")
            if "회사 정보가 없음" in str(issues):
                print(f"  3. 회사 정보 수집: python scripts/data_collection/collect_stock_info.py --stock_code={stock_code}")
        else:
            print("✅ 문제가 발견되지 않았습니다.")


def main():
    parser = argparse.ArgumentParser(description='데이터베이스 데이터 확인')
    parser.add_argument('--stock_code', type=str, help='확인할 주식 코드 (예: 000660)')
    parser.add_argument('--show_all_stocks', action='store_true', help='모든 주식 목록 표시')
    parser.add_argument('--tables_only', action='store_true', help='테이블 구조만 확인')
    parser.add_argument('--limit', type=int, default=20, help='표시할 주식 수 (기본: 20)')
    
    args = parser.parse_args()
    
    checker = DatabaseChecker()
    
    # 테이블 구조 확인
    checker.check_tables()
    
    if args.tables_only:
        return
    
    if args.show_all_stocks:
        checker.show_all_stocks(args.limit)
    
    if args.stock_code:
        checker.check_company_info(args.stock_code)
        checker.check_stock_data(args.stock_code)
        checker.diagnose_problem(args.stock_code)
    
    if not args.stock_code and not args.show_all_stocks:
        print("\n💡 사용법:")
        print("  특정 주식 확인: python check_database_data.py --stock_code=000660")
        print("  전체 목록 확인: python check_database_data.py --show_all_stocks")


if __name__ == "__main__":
    main()