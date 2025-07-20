#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DART 데이터베이스 테이블 구조 확인 및 쿼리 수정
"""

import sqlite3
import pandas as pd

def check_dart_database_structure():
    """DART 데이터베이스의 실제 테이블 구조 확인"""
    print("🔍 DART 데이터베이스 구조 확인")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect("data/databases/dart_data.db")
        
        # 1. 테이블 목록 확인
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql_query(tables_query, conn)
        
        print(f"📊 테이블 수: {len(tables)}개")
        for table_name in tables['name']:
            print(f"   - {table_name}")
        
        # 2. financial_statements 테이블 구조 확인
        if 'financial_statements' in tables['name'].values:
            print(f"\n📋 financial_statements 테이블 구조:")
            pragma_query = "PRAGMA table_info(financial_statements);"
            columns = pd.read_sql_query(pragma_query, conn)
            
            print("컬럼명 | 타입 | Null 허용 | 기본값")
            print("-" * 50)
            for _, col in columns.iterrows():
                print(f"{col['name']:<20} | {col['type']:<10} | {col['notnull']:<8} | {col['dflt_value']}")
            
            # 3. 샘플 데이터 확인
            print(f"\n📊 샘플 데이터 (상위 5개):")
            sample_query = "SELECT * FROM financial_statements LIMIT 5;"
            sample_data = pd.read_sql_query(sample_query, conn)
            print(sample_data.to_string(index=False))
            
            # 4. 데이터 수 확인
            count_query = "SELECT COUNT(*) as count FROM financial_statements;"
            count_result = pd.read_sql_query(count_query, conn)
            print(f"\n📈 총 데이터 수: {count_result.iloc[0]['count']:,}건")
            
            # 5. 종목별 데이터 확인
            stock_count_query = """
            SELECT stock_code, COUNT(*) as count 
            FROM financial_statements 
            WHERE stock_code IS NOT NULL AND stock_code != ''
            GROUP BY stock_code 
            ORDER BY count DESC 
            LIMIT 10;
            """
            stock_counts = pd.read_sql_query(stock_count_query, conn)
            print(f"\n📊 종목별 데이터 수 (상위 10개):")
            print(stock_counts.to_string(index=False))
            
        else:
            print("❌ financial_statements 테이블이 존재하지 않습니다.")
        
        # 6. 다른 주요 테이블들도 확인
        important_tables = ['corp_codes', 'company_info', 'dart_reports']
        for table in important_tables:
            if table in tables['name'].values:
                count_query = f"SELECT COUNT(*) as count FROM {table};"
                count_result = pd.read_sql_query(count_query, conn)
                print(f"\n📊 {table} 테이블: {count_result.iloc[0]['count']:,}건")
                
                # 컬럼 정보
                pragma_query = f"PRAGMA table_info({table});"
                columns = pd.read_sql_query(pragma_query, conn)
                column_names = columns['name'].tolist()
                print(f"   컬럼: {', '.join(column_names)}")
        
        conn.close()
        return columns['name'].tolist() if 'financial_statements' in tables['name'].values else []
        
    except Exception as e:
        print(f"❌ DART 데이터베이스 확인 실패: {e}")
        return []

def create_safe_financial_query(available_columns):
    """사용 가능한 컬럼만으로 안전한 쿼리 생성"""
    print(f"\n🔧 안전한 재무 데이터 쿼리 생성")
    print("=" * 60)
    
    # 필수 컬럼들
    required_columns = ['stock_code', 'account_nm']
    
    # 금액 관련 컬럼들 (우선순위순)
    amount_columns = [
        'thstrm_amount',     # 당기 금액
        'frmtrm_amount',     # 전기 금액  
        'bfefrmtrm_amount',  # 전전기 금액
        'amount',            # 일반 금액
        'value',             # 값
    ]
    
    # 사용 가능한 컬럼 확인
    safe_columns = []
    for col in required_columns:
        if col in available_columns:
            safe_columns.append(col)
        else:
            print(f"❌ 필수 컬럼 누락: {col}")
    
    # 금액 컬럼 추가
    for col in amount_columns:
        if col in available_columns:
            safe_columns.append(col)
            print(f"✅ 금액 컬럼 발견: {col}")
        else:
            print(f"⚠️ 금액 컬럼 없음: {col}")
    
    # 기타 유용한 컬럼들
    useful_columns = ['fs_div', 'sj_div', 'rcept_no', 'ord', 'fs_nm', 'sj_nm']
    for col in useful_columns:
        if col in available_columns:
            safe_columns.append(col)
    
    # 안전한 쿼리 생성
    if len(safe_columns) >= 2:  # 최소 stock_code, account_nm
        select_part = ', '.join(safe_columns)
        
        safe_query = f"""
        SELECT {select_part}
        FROM financial_statements 
        WHERE stock_code = ?
        """
        
        # 조건 추가 (사용 가능한 컬럼이 있는 경우만)
        if 'fs_div' in available_columns:
            safe_query += " AND fs_div = '1'"
        if 'sj_div' in available_columns:
            safe_query += " AND sj_div = '1'"
        
        safe_query += " ORDER BY rcept_no DESC LIMIT 50" if 'rcept_no' in available_columns else " LIMIT 50"
        
        print(f"\n✅ 생성된 안전한 쿼리:")
        print(safe_query)
        
        return safe_query
    else:
        print(f"❌ 충분한 컬럼이 없습니다. 사용 가능: {safe_columns}")
        return None

def test_safe_query(query):
    """안전한 쿼리 테스트"""
    print(f"\n🧪 쿼리 테스트")
    print("=" * 60)
    
    if not query:
        print("❌ 테스트할 쿼리가 없습니다.")
        return False
    
    try:
        conn = sqlite3.connect("data/databases/dart_data.db")
        
        # 삼성전자로 테스트
        test_stock = '005930'
        print(f"📊 테스트 종목: {test_stock} (삼성전자)")
        
        result = pd.read_sql_query(query, conn, params=(test_stock,))
        
        print(f"✅ 쿼리 성공! 결과: {len(result)}건")
        
        if len(result) > 0:
            print(f"📋 결과 컬럼: {list(result.columns)}")
            print(f"📊 샘플 데이터:")
            print(result.head().to_string(index=False))
        else:
            print("⚠️ 결과 데이터가 없습니다.")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 쿼리 테스트 실패: {e}")
        return False

def main():
    """메인 실행"""
    print("🔧 DART 데이터베이스 구조 분석 및 쿼리 수정")
    print("=" * 70)
    
    # 1단계: 데이터베이스 구조 확인
    available_columns = check_dart_database_structure()
    
    if available_columns:
        print(f"\n📋 financial_statements 사용 가능한 컬럼: {len(available_columns)}개")
        
        # 2단계: 안전한 쿼리 생성
        safe_query = create_safe_financial_query(available_columns)
        
        # 3단계: 쿼리 테스트
        if safe_query:
            success = test_safe_query(safe_query)
            
            if success:
                print(f"\n🎉 안전한 재무 데이터 쿼리 생성 완료!")
                print(f"💡 이 쿼리를 분석 시스템에 적용하세요.")
                
                # 수정된 쿼리를 파일로 저장
                with open("safe_financial_query.sql", "w", encoding="utf-8") as f:
                    f.write(safe_query)
                print(f"📁 쿼리 파일 저장: safe_financial_query.sql")
            else:
                print(f"\n❌ 쿼리 테스트 실패")
        else:
            print(f"\n❌ 안전한 쿼리 생성 실패")
    else:
        print(f"\n❌ financial_statements 테이블 정보를 가져올 수 없습니다.")

if __name__ == "__main__":
    main()
