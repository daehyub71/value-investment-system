#!/usr/bin/env python3
"""
간단한 데이터베이스 확인 스크립트 (의존성 최소화)
"""

import sqlite3
import os
from pathlib import Path

def check_database_simple():
    """데이터베이스 간단 확인"""
    
    # 프로젝트 루트에서 data/databases 경로 찾기
    current_dir = Path(__file__).parent.parent.parent
    db_dir = current_dir / 'data' / 'databases'
    
    print("🔍 데이터베이스 확인 중...")
    print(f"📂 데이터베이스 경로: {db_dir}")
    print("=" * 80)
    
    if not db_dir.exists():
        print("❌ 데이터베이스 디렉토리를 찾을 수 없습니다.")
        print(f"   경로: {db_dir}")
        return
    
    # DB 파일들 확인
    db_files = {
        'DART 데이터': 'dart_data.db',
        '주가 데이터': 'stock_data.db', 
        '뉴스 데이터': 'news_data.db',
        'KIS 데이터': 'kis_data.db'
    }
    
    for db_name, db_file in db_files.items():
        db_path = db_dir / db_file
        
        print(f"\n🗄️  {db_name} ({db_file})")
        
        if not db_path.exists():
            print("   ❌ 파일 없음")
            continue
            
        # 파일 크기
        size_mb = db_path.stat().st_size / 1024 / 1024
        print(f"   📏 파일 크기: {size_mb:.2f} MB")
        
        # 테이블 정보
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                # 테이블 목록
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                
                if not tables:
                    print("   ⚠️  테이블 없음")
                    continue
                
                print(f"   📋 테이블 수: {len(tables)}")
                
                # 각 테이블 레코드 수
                for (table_name,) in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count = cursor.fetchone()[0]
                        print(f"      • {table_name}: {count:,}건")
                    except Exception as e:
                        print(f"      • {table_name}: 오류 ({str(e)[:50]})")
                        
        except Exception as e:
            print(f"   ❌ 연결 오류: {e}")
    
    print("\n" + "=" * 80)
    print("✅ 데이터베이스 확인 완료")

def search_companies_simple(keyword):
    """간단한 기업 검색"""
    db_path = Path(__file__).parent.parent.parent / 'data' / 'databases' / 'dart_data.db'
    
    if not db_path.exists():
        print("❌ DART 데이터베이스를 찾을 수 없습니다.")
        return
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # 테이블 존재 확인
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='corp_codes'")
            if not cursor.fetchone():
                print("❌ corp_codes 테이블을 찾을 수 없습니다.")
                return
            
            # 검색 실행
            cursor.execute("""
                SELECT corp_code, corp_name, stock_code 
                FROM corp_codes 
                WHERE corp_name LIKE ? 
                ORDER BY corp_name 
                LIMIT 10
            """, (f'%{keyword}%',))
            
            results = cursor.fetchall()
            
            print(f"\n🔍 '{keyword}' 검색 결과:")
            print("-" * 80)
            
            if not results:
                print("❌ 검색 결과가 없습니다.")
                return
            
            for i, (corp_code, corp_name, stock_code) in enumerate(results, 1):
                stock_info = stock_code if stock_code else "비상장"
                print(f"   {i:2d}. {corp_name}")
                print(f"       기업코드: {corp_code} | 주식코드: {stock_info}")
                
    except Exception as e:
        print(f"❌ 검색 중 오류: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # 검색어가 있으면 검색 실행
        keyword = sys.argv[1]
        search_companies_simple(keyword)
    else:
        # 기본적으로 데이터베이스 확인
        check_database_simple()
        
        print("\n💡 사용법:")
        print("   python scripts/analysis/simple_inspect.py        # 데이터베이스 확인")
        print("   python scripts/analysis/simple_inspect.py 삼성    # 기업 검색")
