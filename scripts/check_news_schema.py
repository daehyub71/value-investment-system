#!/usr/bin/env python3
"""
news_data.db 테이블 구조 확인 스크립트
"""

import sqlite3
from pathlib import Path

def check_news_database_schema():
    """뉴스 데이터베이스 스키마 확인"""
    
    db_path = Path('data/databases/news_data.db')
    
    if not db_path.exists():
        print("❌ news_data.db 파일이 없습니다.")
        return
    
    with sqlite3.connect(db_path) as conn:
        # 모든 테이블 조회
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        print("📊 news_data.db 테이블 목록:")
        print("-" * 50)
        for table in tables:
            print(f"  📋 {table}")
        
        # 각 테이블 구조 확인
        for table_name in tables:
            if table_name == 'sqlite_sequence':
                continue
                
            print(f"\n📋 {table_name} 테이블 구조:")
            print("-" * 50)
            
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            for col in columns:
                print(f"  {col[1]} ({col[2]}) - NULL: {bool(col[3])}")
            
            # 샘플 데이터
            cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 3")
            rows = cursor.fetchall()
            
            if rows:
                print(f"\n  📈 샘플 데이터 (3개):")
                column_names = [description[0] for description in cursor.description]
                print("    " + " | ".join(column_names))
                print("    " + "-" * (len(" | ".join(column_names))))
                
                for row in rows:
                    print("    " + " | ".join(str(cell)[:20] if cell else "NULL" for cell in row))

if __name__ == "__main__":
    check_news_database_schema()
