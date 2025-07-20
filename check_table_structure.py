#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
데이터베이스 테이블 구조 확인 및 수정 도구
"""

import sqlite3
import pandas as pd

def check_table_structure():
    """데이터베이스 테이블 구조 확인"""
    print("🔍 데이터베이스 테이블 구조 확인")
    print("=" * 60)
    
    db_files = [
        ("stock_data.db", "주식 데이터"),
        ("dart_data.db", "DART 데이터"),
        ("buffett_scorecard.db", "워런 버핏 스코어카드")
    ]
    
    for db_file, description in db_files:
        try:
            print(f"\n📊 {description} ({db_file}):")
            conn = sqlite3.connect(f"data/databases/{db_file}")
            
            # 테이블 목록 조회
            tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
            tables = pd.read_sql_query(tables_query, conn)
            
            print(f"   테이블 수: {len(tables)}개")
            for table_name in tables['name']:
                print(f"   - {table_name}")
                
                # 각 테이블의 컬럼 정보 확인
                try:
                    pragma_query = f"PRAGMA table_info({table_name});"
                    columns = pd.read_sql_query(pragma_query, conn)
                    print(f"     컬럼 수: {len(columns)}개")
                    for _, col in columns.iterrows():
                        print(f"       {col['name']} ({col['type']})")
                    
                    # 데이터 수 확인
                    count_query = f"SELECT COUNT(*) as count FROM {table_name};"
                    count_result = pd.read_sql_query(count_query, conn)
                    print(f"     데이터 수: {count_result.iloc[0]['count']:,}건")
                    
                except Exception as e:
                    print(f"     ❌ 테이블 정보 조회 실패: {e}")
            
            conn.close()
            
        except Exception as e:
            print(f"   ❌ 데이터베이스 연결 실패: {e}")

def check_company_info_data():
    """company_info 테이블 데이터 샘플 확인"""
    print("\n🏢 company_info 테이블 상세 확인")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect("data/databases/stock_data.db")
        
        # 테이블 구조 확인
        print("📋 테이블 구조:")
        pragma_query = "PRAGMA table_info(company_info);"
        columns = pd.read_sql_query(pragma_query, conn)
        for _, col in columns.iterrows():
            print(f"   {col['name']} ({col['type']})")
        
        # 샘플 데이터 확인
        print("\n📊 샘플 데이터 (상위 10개):")
        sample_query = "SELECT * FROM company_info LIMIT 10;"
        sample_data = pd.read_sql_query(sample_query, conn)
        print(sample_data.to_string(index=False))
        
        # 사용 가능한 종목 수 확인
        count_query = """
        SELECT COUNT(*) as total_count,
               COUNT(CASE WHEN stock_code IS NOT NULL AND stock_code != '' THEN 1 END) as valid_code_count
        FROM company_info
        """
        count_result = pd.read_sql_query(count_query, conn)
        print(f"\n📈 종목 통계:")
        print(f"   전체 레코드 수: {count_result.iloc[0]['total_count']:,}개")
        print(f"   유효한 종목코드: {count_result.iloc[0]['valid_code_count']:,}개")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ company_info 확인 실패: {e}")

def get_available_stocks():
    """분석 가능한 종목 리스트 조회 (수정된 쿼리)"""
    print("\n🎯 분석 가능한 종목 리스트 (수정된 쿼리)")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect("data/databases/stock_data.db")
        
        # 컬럼 존재 여부에 따라 다른 쿼리 사용
        pragma_query = "PRAGMA table_info(company_info);"
        columns = pd.read_sql_query(pragma_query, conn)
        column_names = columns['name'].tolist()
        
        print(f"📋 사용 가능한 컬럼: {', '.join(column_names)}")
        
        # 기본 쿼리 (필수 컬럼만)
        if 'market' in column_names and 'sector' in column_names:
            # 모든 컬럼이 있는 경우
            query = """
            SELECT 
                stock_code,
                company_name,
                market,
                sector,
                industry
            FROM company_info 
            WHERE stock_code IS NOT NULL 
                AND stock_code != ''
                AND LENGTH(stock_code) = 6
            ORDER BY company_name
            LIMIT 20
            """
        else:
            # 기본 컬럼만 있는 경우
            query = """
            SELECT 
                stock_code,
                company_name
            FROM company_info 
            WHERE stock_code IS NOT NULL 
                AND stock_code != ''
                AND LENGTH(stock_code) = 6
            ORDER BY company_name
            LIMIT 20
            """
        
        result = pd.read_sql_query(query, conn)
        print(f"\n📊 분석 가능한 종목 (상위 20개):")
        print(result.to_string(index=False))
        
        # 전체 개수 확인
        count_query = """
        SELECT COUNT(*) as count
        FROM company_info 
        WHERE stock_code IS NOT NULL 
            AND stock_code != ''
            AND LENGTH(stock_code) = 6
        """
        count_result = pd.read_sql_query(count_query, conn)
        print(f"\n📈 총 분석 가능한 종목 수: {count_result.iloc[0]['count']:,}개")
        
        conn.close()
        
        return result
        
    except Exception as e:
        print(f"❌ 종목 리스트 조회 실패: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    print("🔧 데이터베이스 테이블 구조 확인 및 수정")
    print("=" * 70)
    
    # 1. 전체 테이블 구조 확인
    check_table_structure()
    
    # 2. company_info 테이블 상세 확인
    check_company_info_data()
    
    # 3. 분석 가능한 종목 확인
    available_stocks = get_available_stocks()
    
    if len(available_stocks) > 0:
        print("\n✅ 워런 버핏 분석 시스템 준비 완료!")
        print("📊 수정된 쿼리로 분석 프로그램을 업데이트합니다.")
    else:
        print("\n❌ 분석 가능한 종목 데이터가 없습니다.")
        print("💡 데이터 수집 프로그램을 먼저 실행해주세요.")
