#!/usr/bin/env python3
"""
KIS 데이터베이스 스키마 수정 스크립트
기존 테이블 구조 확인 및 올바른 스키마로 업데이트
"""

import sqlite3
import os
from pathlib import Path

def check_database_schema(db_path):
    """데이터베이스 스키마 확인"""
    print("🔍 현재 데이터베이스 스키마 확인")
    print("=" * 50)
    
    if not Path(db_path).exists():
        print(f"❌ 데이터베이스 파일이 존재하지 않습니다: {db_path}")
        return False
    
    try:
        with sqlite3.connect(db_path) as conn:
            # 테이블 목록 조회
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            print(f"📊 테이블 목록: {tables}")
            
            # 각 테이블의 스키마 확인
            for table in tables:
                print(f"\n📋 {table} 테이블 스키마:")
                cursor = conn.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                
                for col in columns:
                    cid, name, dtype, notnull, default, pk = col
                    print(f"  - {name} ({dtype})")
            
            # market_indicators 테이블 특별 확인
            if 'market_indicators' in tables:
                print(f"\n🎯 market_indicators 테이블 상세:")
                cursor = conn.execute("PRAGMA table_info(market_indicators)")
                columns = [row[1] for row in cursor.fetchall()]
                print(f"  컬럼들: {columns}")
                
                if 'index_name' not in columns:
                    print("  ❌ index_name 컬럼이 없습니다!")
                    return False
                else:
                    print("  ✅ index_name 컬럼이 있습니다!")
                    return True
            
            return True
            
    except Exception as e:
        print(f"❌ 데이터베이스 확인 실패: {e}")
        return False

def create_correct_kis_tables(db_path):
    """올바른 KIS 테이블 생성"""
    print("\n🔧 올바른 KIS 테이블 생성")
    print("=" * 50)
    
    # 올바른 스키마 정의
    table_schemas = {
        'realtime_quotes': '''
            CREATE TABLE IF NOT EXISTS realtime_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                current_price REAL,
                change_price REAL,
                change_rate REAL,
                volume INTEGER,
                high_price REAL,
                low_price REAL,
                open_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, timestamp)
            )
        ''',
        
        'market_indicators': '''
            CREATE TABLE IF NOT EXISTS market_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                index_name TEXT NOT NULL,
                index_code TEXT NOT NULL,
                close_price REAL,
                change_price REAL,
                change_rate REAL,
                volume INTEGER,
                high_price REAL,
                low_price REAL,
                open_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, index_name)
            )
        ''',
        
        'account_balance': '''
            CREATE TABLE IF NOT EXISTS account_balance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_no TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT,
                holding_qty INTEGER,
                avg_price REAL,
                current_price REAL,
                evaluation_amount REAL,
                profit_loss REAL,
                profit_loss_rate REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_no, stock_code)
            )
        ''',
        
        'order_history': '''
            CREATE TABLE IF NOT EXISTS order_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_no TEXT UNIQUE NOT NULL,
                account_no TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                order_type TEXT,
                order_qty INTEGER,
                order_price REAL,
                executed_qty INTEGER,
                executed_price REAL,
                order_status TEXT,
                order_time TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
    }
    
    try:
        with sqlite3.connect(db_path) as conn:
            # 기존 market_indicators 테이블 삭제 (스키마가 다르면)
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='market_indicators'")
            if cursor.fetchone():
                cursor = conn.execute("PRAGMA table_info(market_indicators)")
                columns = [row[1] for row in cursor.fetchall()]
                
                if 'index_name' not in columns:
                    print("🗑️  기존 market_indicators 테이블 삭제 (잘못된 스키마)")
                    conn.execute("DROP TABLE market_indicators")
            
            # 모든 테이블 생성
            for table_name, schema in table_schemas.items():
                print(f"📝 {table_name} 테이블 생성/업데이트")
                conn.execute(schema)
            
            # 인덱스 생성
            index_queries = [
                'CREATE INDEX IF NOT EXISTS idx_realtime_quotes_stock_time ON realtime_quotes(stock_code, timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_market_indicators_date_name ON market_indicators(date, index_name)',
                'CREATE INDEX IF NOT EXISTS idx_account_balance_account ON account_balance(account_no)',
                'CREATE INDEX IF NOT EXISTS idx_order_history_stock ON order_history(stock_code)'
            ]
            
            for query in index_queries:
                conn.execute(query)
            
            conn.commit()
            print("✅ KIS 데이터베이스 테이블 생성 완료")
            return True
            
    except Exception as e:
        print(f"❌ 테이블 생성 실패: {e}")
        return False

def test_table_access(db_path):
    """테이블 접근 테스트"""
    print("\n🧪 테이블 접근 테스트")
    print("=" * 50)
    
    test_data = {
        'date': '2025-07-12',
        'index_name': 'KOSPI',
        'index_code': '0001',
        'close_price': 2800.0,
        'change_price': 10.0,
        'change_rate': 0.36,
        'volume': 1000000,
        'high_price': 2810.0,
        'low_price': 2790.0,
        'open_price': 2795.0,
        'created_at': '2025-07-12 19:45:00'
    }
    
    try:
        with sqlite3.connect(db_path) as conn:
            # 테스트 데이터 삽입
            conn.execute('''
                INSERT OR REPLACE INTO market_indicators
                (date, index_name, index_code, close_price, change_price, change_rate,
                 volume, high_price, low_price, open_price, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                test_data['date'], test_data['index_name'], test_data['index_code'],
                test_data['close_price'], test_data['change_price'], test_data['change_rate'],
                test_data['volume'], test_data['high_price'], test_data['low_price'],
                test_data['open_price'], test_data['created_at']
            ))
            
            # 데이터 조회 테스트
            cursor = conn.execute("SELECT * FROM market_indicators WHERE index_name = ?", (test_data['index_name'],))
            result = cursor.fetchone()
            
            if result:
                print("✅ 테이블 접근 테스트 성공")
                print(f"   저장된 데이터: {test_data['index_name']} - {test_data['close_price']}")
                return True
            else:
                print("❌ 데이터 조회 실패")
                return False
                
    except Exception as e:
        print(f"❌ 테이블 접근 테스트 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    db_path = 'data/databases/kis_data.db'
    
    print("🚀 KIS 데이터베이스 스키마 수정 시작")
    print("=" * 60)
    
    # 1단계: 현재 스키마 확인
    schema_ok = check_database_schema(db_path)
    
    if not schema_ok:
        # 2단계: 올바른 테이블 생성
        if create_correct_kis_tables(db_path):
            # 3단계: 테스트
            if test_table_access(db_path):
                print("\n🎉 KIS 데이터베이스 수정 완료!")
                print("이제 다음 명령어를 실행할 수 있습니다:")
                print("python scripts/data_collection/collect_kis_data.py --market_indicators")
            else:
                print("\n❌ 테스트 실패")
        else:
            print("\n❌ 테이블 생성 실패")
    else:
        print("\n✅ 데이터베이스 스키마가 이미 올바릅니다!")

if __name__ == "__main__":
    main()
