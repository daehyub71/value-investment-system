#!/usr/bin/env python3
"""
데이터베이스 스키마 및 상태 확인 도구
Enhanced 워런 버핏 스코어카드 문제 진단
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging

class DatabaseDiagnostics:
    """데이터베이스 진단 클래스"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.databases = {
            'dart_data': Path('data/databases/dart_data.db'),
            'stock_data': Path('data/databases/stock_data.db'),
            'yahoo_finance': Path('data/databases/yahoo_finance_data.db'),
            'forecast_data': Path('data/databases/forecast_data.db')
        }
    
    def check_all_databases(self):
        """모든 데이터베이스 상태 확인"""
        print("🔍 데이터베이스 진단 보고서")
        print("=" * 60)
        
        for db_name, db_path in self.databases.items():
            print(f"\n📊 {db_name.upper()} 데이터베이스")
            print("-" * 40)
            
            if not db_path.exists():
                print(f"❌ 파일 없음: {db_path}")
                continue
            
            try:
                with sqlite3.connect(db_path) as conn:
                    # 테이블 목록
                    tables = self._get_tables(conn)
                    print(f"📋 테이블 수: {len(tables)}")
                    
                    for table_name in tables:
                        row_count = self._get_row_count(conn, table_name)
                        columns = self._get_columns(conn, table_name)
                        print(f"  🗂️ {table_name}: {row_count}행, {len(columns)}컬럼")
                        print(f"     컬럼: {', '.join(columns[:5])}" + ("..." if len(columns) > 5 else ""))
                        
                        # 샘플 데이터 확인
                        if row_count > 0:
                            sample = self._get_sample_data(conn, table_name)
                            if sample:
                                print(f"     샘플: {sample}")
            
            except Exception as e:
                print(f"❌ 오류: {e}")
    
    def _get_tables(self, conn):
        """테이블 목록 조회"""
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]
    
    def _get_row_count(self, conn, table_name):
        """테이블 행 수 조회"""
        try:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cursor.fetchone()[0]
        except:
            return 0
    
    def _get_columns(self, conn, table_name):
        """테이블 컬럼 목록 조회"""
        try:
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            return [row[1] for row in cursor.fetchall()]
        except:
            return []
    
    def _get_sample_data(self, conn, table_name):
        """샘플 데이터 조회"""
        try:
            cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 1")
            row = cursor.fetchone()
            if row:
                # 첫 번째 몇 개 컬럼만 표시
                return str(row[:3]) + "..."
            return None
        except:
            return None
    
    def fix_enhanced_scorecard_queries(self):
        """Enhanced 스코어카드를 위한 데이터베이스 수정"""
        print("\n🔧 Enhanced 스코어카드 데이터베이스 수정")
        print("=" * 60)
        
        # 1. stock_data.db에 daily_prices 테이블이 없는 경우 생성
        stock_db = self.databases['stock_data']
        if stock_db.exists():
            try:
                with sqlite3.connect(stock_db) as conn:
                    # 기존 테이블 확인
                    tables = self._get_tables(conn)
                    print(f"📊 stock_data.db 테이블: {tables}")
                    
                    if 'daily_prices' not in tables:
                        print("🔨 daily_prices 테이블 생성 중...")
                        conn.execute('''
                            CREATE TABLE daily_prices (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                stock_code TEXT NOT NULL,
                                date TEXT NOT NULL,
                                open_price REAL,
                                high_price REAL,
                                low_price REAL,
                                close_price REAL,
                                volume INTEGER,
                                change_price REAL,
                                change_rate REAL,
                                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                UNIQUE(stock_code, date)
                            )
                        ''')
                        conn.commit()
                        print("✅ daily_prices 테이블 생성 완료")
                    
                    # company_info 테이블에서 샘플 데이터 확인
                    if 'company_info' in tables:
                        cursor = conn.execute("SELECT stock_code, company_name FROM company_info LIMIT 5")
                        companies = cursor.fetchall()
                        print("📋 주요 종목:")
                        for stock_code, company_name in companies:
                            print(f"  {stock_code}: {company_name}")
                    
            except Exception as e:
                print(f"❌ stock_data.db 수정 실패: {e}")
        
        # 2. 삼성전자 기본 데이터 추가 (테스트용)
        self._add_sample_data()
    
    def _add_sample_data(self):
        """테스트용 샘플 데이터 추가"""
        print("\n📝 테스트용 샘플 데이터 추가")
        
        stock_db = self.databases['stock_data']
        if not stock_db.exists():
            return
        
        try:
            with sqlite3.connect(stock_db) as conn:
                # company_info 테이블에 삼성전자 정보가 있는지 확인
                cursor = conn.execute("SELECT COUNT(*) FROM company_info WHERE stock_code = '005930'")
                exists = cursor.fetchone()[0] > 0
                
                if not exists:
                    # company_info 테이블에 삼성전자 정보 추가
                    conn.execute('''
                        INSERT INTO company_info 
                        (stock_code, company_name, market_cap, sector, listing_date)
                        VALUES (?, ?, ?, ?, ?)
                    ''', ('005930', '삼성전자', 500000000000000, '전자', '1975-06-11'))
                    print("✅ 삼성전자 기업정보 추가")
                
                # daily_prices에 최근 주가 데이터 추가 (샘플)
                conn.execute('''
                    INSERT OR REPLACE INTO daily_prices
                    (stock_code, date, open_price, high_price, low_price, close_price, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', ('005930', '2025-07-13', 71000, 72000, 70500, 71500, 15000000))
                
                conn.commit()
                print("✅ 삼성전자 주가 데이터 추가 완료")
                
        except Exception as e:
            print(f"❌ 샘플 데이터 추가 실패: {e}")


def main():
    """메인 실행 함수"""
    logging.basicConfig(level=logging.INFO)
    
    diagnostics = DatabaseDiagnostics()
    
    # 1. 전체 데이터베이스 상태 확인
    diagnostics.check_all_databases()
    
    # 2. Enhanced 스코어카드를 위한 수정
    diagnostics.fix_enhanced_scorecard_queries()
    
    print("\n🎯 다음 단계:")
    print("1. python scripts/analysis/run_enhanced_buffett_scorecard.py --stock_code=005930")
    print("2. 더 많은 실제 데이터가 필요하면 데이터 수집 스크립트 실행")
    print("3. python scripts/data_collection/collect_alternative_data.py --stock_code=005930")


if __name__ == "__main__":
    main()
