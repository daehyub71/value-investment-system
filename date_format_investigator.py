#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
날짜 형식 문제 조사 도구
각 테이블의 날짜 컬럼 형식을 분석하고 파싱 문제 해결
"""

import sqlite3
from pathlib import Path
from datetime import datetime

def investigate_date_formats():
    """각 데이터베이스의 날짜 형식 조사"""
    
    databases = {
        'stock_data.db': {
            'company_info': ['created_at', 'updated_at'],
            'stock_prices': ['date', 'created_at'],
        },
        'news_data.db': {
            'news_articles': ['pubDate', 'created_at'],
            'sentiment_scores': ['created_at'],
        },
        'kis_data.db': {
            'realtime_quotes': ['created_at'],
            'market_indicators': ['created_at'],
        }
    }
    
    print("🔍 날짜 형식 문제 조사")
    print("=" * 60)
    
    db_base_path = Path("data/databases")
    
    for db_file, tables in databases.items():
        db_path = db_base_path / db_file
        
        if not db_path.exists():
            print(f"\n❌ {db_file} 파일을 찾을 수 없습니다")
            continue
            
        print(f"\n📊 {db_file}")
        print("-" * 40)
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            for table, date_columns in tables.items():
                print(f"\n📋 테이블: {table}")
                
                # 테이블 존재 확인
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name = ?
                """, (table,))
                
                if not cursor.fetchone():
                    print(f"   ❌ 테이블이 존재하지 않습니다")
                    continue
                
                for date_col in date_columns:
                    try:
                        # 컬럼 존재 확인
                        cursor.execute(f"PRAGMA table_info([{table}])")
                        columns = [col[1] for col in cursor.fetchall()]
                        
                        if date_col not in columns:
                            print(f"   ⚠️  컬럼 '{date_col}' 없음")
                            continue
                        
                        # 샘플 데이터 조회
                        cursor.execute(f"""
                            SELECT [{date_col}] 
                            FROM [{table}] 
                            WHERE [{date_col}] IS NOT NULL 
                            ORDER BY [{date_col}] DESC 
                            LIMIT 5
                        """)
                        
                        samples = cursor.fetchall()
                        
                        if not samples:
                            print(f"   📅 {date_col}: 데이터 없음")
                            continue
                            
                        print(f"   📅 {date_col} 샘플:")
                        for i, (date_value,) in enumerate(samples[:3]):
                            # 파싱 시도
                            parsed_info = try_parse_date(date_value)
                            print(f"      {i+1}. '{date_value}' → {parsed_info}")
                            
                    except Exception as e:
                        print(f"   ❌ {date_col} 조회 오류: {e}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ 데이터베이스 연결 오류: {e}")

def try_parse_date(date_value):
    """다양한 형식으로 날짜 파싱 시도"""
    if not date_value:
        return "NULL"
    
    date_str = str(date_value)
    
    # 다양한 형식 시도
    formats = [
        '%Y-%m-%d %H:%M:%S',      # 2025-07-12 20:15:38
        '%Y-%m-%d %H:%M:%S.%f',   # 2025-07-12 20:15:38.123
        '%Y-%m-%dT%H:%M:%S',      # 2025-07-12T20:15:38
        '%Y-%m-%dT%H:%M:%S.%f',   # 2025-07-12T20:15:38.123
        '%Y-%m-%d',               # 2025-07-12
        '%Y%m%d',                 # 20250712
        '%Y%m%d%H%M%S',          # 20250712201538
    ]
    
    for fmt in formats:
        try:
            # 문자열 길이에 맞게 조정
            if '.%f' in fmt and '.' not in date_str:
                continue
            if 'T' in fmt and 'T' not in date_str:
                continue
            if '%H' in fmt and len(date_str) < 10:
                continue
                
            parsed_dt = datetime.strptime(date_str[:len(fmt.replace('%f', '000000').replace('%', ''))], fmt)
            return f"✅ {fmt} → {parsed_dt}"
            
        except:
            continue
    
    # Unix timestamp 시도
    try:
        if date_str.isdigit() and len(date_str) in [10, 13]:
            timestamp = int(date_str)
            if len(date_str) == 13:  # 밀리초
                timestamp = timestamp / 1000
            parsed_dt = datetime.fromtimestamp(timestamp)
            return f"✅ Unix timestamp → {parsed_dt}"
    except:
        pass
    
    return f"❌ 파싱 실패 (길이: {len(date_str)})"

def suggest_fixes():
    """날짜 형식 문제 해결 방안 제시"""
    print(f"\n💡 날짜 형식 문제 해결 방안:")
    print("=" * 50)
    
    print("1. 📅 표준 형식으로 통일:")
    print("   - 권장: 'YYYY-MM-DD HH:MM:SS' (ISO 8601)")
    print("   - 예시: '2025-07-13 15:30:45'")
    
    print("\n2. 🔧 데이터베이스 업데이트:")
    print("   - 기존 데이터 형식 변환")
    print("   - 새로운 데이터 입력 시 표준 형식 사용")
    
    print("\n3. 🚀 자동 수정 스크립트:")
    print("   - 모든 날짜 컬럼을 표준 형식으로 변환")
    print("   - 백업 후 일괄 업데이트")

if __name__ == "__main__":
    investigate_date_formats()
    suggest_fixes()