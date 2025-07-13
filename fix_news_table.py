#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
뉴스 테이블 구조 확인 및 수정 도구
news_articles 테이블의 스키마를 확인하고 필요시 수정
"""

import sqlite3
from pathlib import Path

def check_news_table_schema():
    """뉴스 테이블 스키마 확인"""
    db_path = Path('data/databases/news_data.db')
    
    if not db_path.exists():
        print("❌ news_data.db 파일을 찾을 수 없습니다.")
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 테이블 존재 확인
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='news_articles'
        """)
        
        if not cursor.fetchone():
            print("❌ news_articles 테이블이 존재하지 않습니다.")
            conn.close()
            return None
        
        # 스키마 정보 조회
        cursor.execute("PRAGMA table_info(news_articles)")
        columns = cursor.fetchall()
        
        print("📋 현재 news_articles 테이블 스키마:")
        print("=" * 60)
        print(f"{'컬럼명':<20} {'타입':<15} {'NOT NULL':<10} {'기본값':<15}")
        print("-" * 60)
        
        column_names = []
        for col in columns:
            cid, name, col_type, notnull, default_val, pk = col
            not_null = "YES" if notnull else "NO"
            default = str(default_val) if default_val else ""
            pk_mark = " (PK)" if pk else ""
            print(f"{name + pk_mark:<20} {col_type:<15} {not_null:<10} {default:<15}")
            column_names.append(name)
        
        conn.close()
        return column_names
        
    except Exception as e:
        print(f"❌ 스키마 확인 실패: {e}")
        return None

def add_company_name_column():
    """company_name 컬럼 추가"""
    db_path = Path('data/databases/news_data.db')
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # company_name 컬럼 추가
        cursor.execute("""
            ALTER TABLE news_articles 
            ADD COLUMN company_name TEXT
        """)
        
        conn.commit()
        conn.close()
        
        print("✅ company_name 컬럼 추가 완료!")
        return True
        
    except sqlite3.Error as e:
        if "duplicate column name" in str(e).lower():
            print("ℹ️ company_name 컬럼이 이미 존재합니다.")
            return True
        else:
            print(f"❌ 컬럼 추가 실패: {e}")
            return False

def get_fixed_insert_query(columns):
    """기존 컬럼에 맞는 INSERT 쿼리 생성"""
    
    # 표준 컬럼들 (우선순위 순)
    standard_columns = [
        'stock_code', 'title', 'description', 'originallink', 'link', 
        'pubDate', 'source', 'category', 'sentiment_score', 'sentiment_label', 
        'confidence_score', 'keywords', 'created_at', 'company_name'
    ]
    
    # 실제 존재하는 컬럼만 선택
    available_columns = [col for col in standard_columns if col in columns]
    
    column_str = ', '.join(available_columns)
    placeholder_str = ', '.join(['?' for _ in available_columns])
    
    query = f"""
        INSERT INTO news_articles ({column_str})
        VALUES ({placeholder_str})
    """
    
    return query, available_columns

def create_fixed_news_collector():
    """수정된 뉴스 수집 스크립트 생성"""
    
    # 현재 스키마 확인
    columns = check_news_table_schema()
    if not columns:
        return False
    
    # company_name 컬럼이 없으면 추가
    if 'company_name' not in columns:
        print("\n🔧 company_name 컬럼이 없습니다. 추가하시겠습니까? (y/n): ", end='')
        response = input().strip().lower()
        
        if response == 'y':
            if add_company_name_column():
                columns.append('company_name')
            else:
                return False
        else:
            print("⚠️ company_name 없이 진행합니다.")
    
    # 수정된 쿼리 생성
    insert_query, available_columns = get_fixed_insert_query(columns)
    
    print(f"\n✅ 수정된 INSERT 쿼리:")
    print("=" * 60)
    print(insert_query)
    
    print(f"\n📋 사용 가능한 컬럼들:")
    for i, col in enumerate(available_columns, 1):
        print(f"{i:2d}. {col}")
    
    return True

def main():
    """메인 실행 함수"""
    print("🔧 뉴스 테이블 스키마 확인 및 수정 도구")
    print("=" * 60)
    
    # 현재 스키마 확인
    columns = check_news_table_schema()
    
    if not columns:
        return
    
    # company_name 컬럼 존재 확인
    if 'company_name' in columns:
        print(f"\n✅ company_name 컬럼이 존재합니다.")
        print("스크립트 실행에 문제가 없어야 합니다.")
    else:
        print(f"\n❌ company_name 컬럼이 없습니다!")
        print("이것이 오류의 원인입니다.")
        
        print(f"\n🔧 해결 방법:")
        print("1. company_name 컬럼 추가")
        print("2. 기존 컬럼만 사용하도록 스크립트 수정")
        
        choice = input("\n선택하세요 (1/2): ").strip()
        
        if choice == '1':
            if add_company_name_column():
                print("✅ 문제 해결 완료! 뉴스 수집을 다시 실행하세요.")
            else:
                print("❌ 컬럼 추가 실패")
        elif choice == '2':
            create_fixed_news_collector()
        else:
            print("올바른 선택을 해주세요.")

if __name__ == "__main__":
    main()
