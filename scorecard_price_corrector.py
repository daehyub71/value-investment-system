#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
스코어카드 테이블 구조 디버깅 스크립트
==================================

버핏 스코어카드 데이터베이스의 테이블 구조와 데이터를 상세히 확인합니다.
"""

import sqlite3
from pathlib import Path

def debug_buffett_scorecard(stock_code="090430"):
    """버핏 스코어카드 데이터베이스 디버깅"""
    
    # 데이터베이스 경로 찾기
    db_path = None
    current_dir = Path(__file__).parent
    
    for _ in range(5):
        test_path = current_dir / "data" / "databases" / "buffett_scorecard.db"
        if test_path.exists():
            db_path = test_path
            break
        current_dir = current_dir.parent
    
    if not db_path:
        print("❌ buffett_scorecard.db 파일을 찾을 수 없습니다.")
        return
    
    print(f"🔍 데이터베이스 위치: {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    try:
        # 1. 모든 테이블 목록 조회
        print("\n📊 테이블 목록:")
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            print(f"   - {table}")
        
        # 2. 각 테이블의 구조와 데이터 확인
        target_tables = ['buffett_scorecard', 'buffett_top50_scores', 'buffett_all_stocks_final']
        
        for table_name in target_tables:
            if table_name not in tables:
                print(f"\n❌ {table_name} 테이블이 존재하지 않습니다.")
                continue
            
            print(f"\n📋 {table_name} 테이블 분석:")
            print("-" * 50)
            
            # 테이블 구조 확인
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print("   컬럼 구조:")
            for col in columns:
                print(f"      {col[1]} ({col[2]})")
            
            # 해당 종목 데이터 확인
            try:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE stock_code = ?", (stock_code,))
                count = cursor.fetchone()[0]
                print(f"   {stock_code} 데이터 개수: {count}건")
                
                if count > 0:
                    # 실제 데이터 조회
                    cursor = conn.execute(f"SELECT * FROM {table_name} WHERE stock_code = ? LIMIT 1", (stock_code,))
                    result = cursor.fetchone()
                    
                    if result:
                        print("   샘플 데이터:")
                        result_dict = dict(result)
                        
                        # 주요 컬럼만 표시
                        key_columns = ['current_price', 'target_price', 'target_price_high', 'target_price_low', 
                                     'total_score', 'analysis_date', 'created_at', 'calculation_date']
                        
                        for col in key_columns:
                            if col in result_dict:
                                print(f"      {col}: {result_dict[col]}")
                
            except Exception as e:
                print(f"   ❌ 데이터 조회 실패: {e}")
    
    finally:
        conn.close()

def check_stock_data_price(stock_code="090430"):
    """stock_data.db에서 실시간 가격 확인"""
    
    # 데이터베이스 경로 찾기
    db_path = None
    current_dir = Path(__file__).parent
    
    for _ in range(5):
        test_path = current_dir / "data" / "databases" / "stock_data.db"
        if test_path.exists():
            db_path = test_path
            break
        current_dir = current_dir.parent
    
    if not db_path:
        print("❌ stock_data.db 파일을 찾을 수 없습니다.")
        return
    
    print(f"\n📈 실시간 가격 데이터 확인: {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    try:
        # financial_ratios_real 테이블 확인
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%ratio%'")
        ratio_tables = [row[0] for row in cursor.fetchall()]
        
        print(f"   재무비율 관련 테이블: {ratio_tables}")
        
        for table in ratio_tables:
            try:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE stock_code = ?", (stock_code,))
                count = cursor.fetchone()[0]
                
                if count > 0:
                    cursor = conn.execute(f"SELECT current_price, updated_at FROM {table} WHERE stock_code = ? ORDER BY updated_at DESC LIMIT 1", (stock_code,))
                    result = cursor.fetchone()
                    
                    if result:
                        print(f"   {table}: {result['current_price']:,.0f}원 ({result['updated_at']})")
                        
            except Exception as e:
                print(f"   {table}: 조회 실패 ({e})")
    
    finally:
        conn.close()

if __name__ == "__main__":
    print("🔧 스코어카드 테이블 구조 디버깅")
    print("=" * 50)
    
    stock_code = "090430"
    
    # 1. 실시간 가격 데이터 확인
    check_stock_data_price(stock_code)
    
    # 2. 버핏 스코어카드 구조 확인
    debug_buffett_scorecard(stock_code)
    
    print("\n✅ 디버깅 완료!")