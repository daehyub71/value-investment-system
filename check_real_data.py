#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
실제 데이터베이스에서 정확한 삼성전자 데이터 확인
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys
import os

def check_real_samsung_data():
    """실제 DB에서 삼성전자 데이터 확인"""
    
    print("🔍 실제 데이터베이스에서 삼성전자 정확한 데이터 확인")
    print("=" * 60)
    
    # 데이터베이스 경로
    db_path = Path("data/databases")
    
    if not db_path.exists():
        print(f"❌ 데이터베이스 디렉토리가 없습니다: {db_path}")
        return None
    
    results = {}
    
    # 1. DART 데이터 확인
    print("\n📊 1. DART 공시 데이터베이스 확인")
    print("-" * 40)
    
    try:
        dart_db_path = db_path / "dart_data.db"
        if not dart_db_path.exists():
            print(f"❌ DART DB 파일이 없습니다: {dart_db_path}")
        else:
            dart_db = sqlite3.connect(dart_db_path)
            
            # 테이블 목록 확인
            tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
            tables = pd.read_sql(tables_query, dart_db)
            
            print(f"📋 DART DB 테이블 목록 ({len(tables)}개):")
            for table_name in tables['name']:
                try:
                    count_query = f"SELECT COUNT(*) as count FROM [{table_name}]"
                    count = pd.read_sql(count_query, dart_db)['count'][0]
                    print(f"  ✅ {table_name}: {count:,}건")
                    
                    # 테이블 구조 확인
                    if count > 0:
                        structure_query = f"PRAGMA table_info([{table_name}])"
                        structure = pd.read_sql(structure_query, dart_db)
                        columns = structure['name'].tolist()
                        print(f"     컬럼: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                        
                        # 샘플 데이터 확인
                        sample_query = f"SELECT * FROM [{table_name}] LIMIT 3"
                        sample_data = pd.read_sql(sample_query, dart_db)
                        
                        # 삼성전자 관련 데이터 검색
                        samsung_found = False
                        for col in columns:
                            if any(keyword in col.lower() for keyword in ['code', 'corp', 'name']):
                                try:
                                    if 'code' in col.lower():
                                        search_query = f"SELECT * FROM [{table_name}] WHERE [{col}] LIKE '%005930%' OR [{col}] LIKE '%00593%' LIMIT 5"
                                    else:
                                        search_query = f"SELECT * FROM [{table_name}] WHERE [{col}] LIKE '%삼성전자%' OR [{col}] LIKE '%Samsung%' LIMIT 5"
                                    
                                    samsung_data = pd.read_sql(search_query, dart_db)
                                    if not samsung_data.empty:
                                        print(f"     🎯 삼성전자 데이터 발견! ({len(samsung_data)}건)")
                                        print(f"        {samsung_data.iloc[0].to_dict()}")
                                        samsung_found = True
                                        results[f'dart_{table_name}'] = samsung_data
                                        break
                                except Exception as e:
                                    continue
                        
                        if not samsung_found and count < 100:  # 작은 테이블만 전체 검색
                            print(f"     📋 샘플 데이터: {sample_data.iloc[0].to_dict() if not sample_data.empty else 'None'}")
                        
                except Exception as e:
                    print(f"  ❌ {table_name} 테이블 조회 실패: {e}")
            
            dart_db.close()
            
    except Exception as e:
        print(f"❌ DART DB 접근 실패: {e}")
    
    # 2. 주식 데이터 확인
    print("\n📊 2. 주식 데이터베이스 확인")
    print("-" * 40)
    
    try:
        stock_db_path = db_path / "stock_data.db"
        if not stock_db_path.exists():
            print(f"❌ 주식 DB 파일이 없습니다: {stock_db_path}")
        else:
            stock_db = sqlite3.connect(stock_db_path)
            
            # 테이블 목록 확인
            tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
            tables = pd.read_sql(tables_query, stock_db)
            
            print(f"📋 주식 DB 테이블 목록 ({len(tables)}개):")
            for table_name in tables['name']:
                try:
                    count_query = f"SELECT COUNT(*) as count FROM [{table_name}]"
                    count = pd.read_sql(count_query, stock_db)['count'][0]
                    print(f"  ✅ {table_name}: {count:,}건")
                    
                    if count > 0:
                        # 테이블 구조 확인
                        structure_query = f"PRAGMA table_info([{table_name}])"
                        structure = pd.read_sql(structure_query, stock_db)
                        columns = structure['name'].tolist()
                        print(f"     컬럼: {', '.join(columns[:7])}{'...' if len(columns) > 7 else ''}")
                        
                        # 삼성전자 데이터 검색
                        samsung_found = False
                        for col in columns:
                            if any(keyword in col.lower() for keyword in ['code', 'symbol', 'name', 'corp']):
                                try:
                                    if 'code' in col.lower() or 'symbol' in col.lower():
                                        search_query = f"SELECT * FROM [{table_name}] WHERE [{col}] = '005930' OR [{col}] = 'KS005930' OR [{col}] LIKE '%005930%' LIMIT 5"
                                    else:
                                        search_query = f"SELECT * FROM [{table_name}] WHERE [{col}] LIKE '%삼성전자%' OR [{col}] LIKE '%Samsung%' LIMIT 5"
                                    
                                    samsung_data = pd.read_sql(search_query, stock_db)
                                    if not samsung_data.empty:
                                        print(f"     🎯 삼성전자 데이터 발견! ({len(samsung_data)}건)")
                                        
                                        # 최신 데이터 확인
                                        if 'date' in ' '.join(columns).lower():
                                            date_cols = [c for c in columns if 'date' in c.lower() or 'time' in c.lower()]
                                            if date_cols:
                                                latest_query = f"SELECT * FROM [{table_name}] WHERE [{col}] = '005930' ORDER BY [{date_cols[0]}] DESC LIMIT 3"
                                                latest_data = pd.read_sql(latest_query, stock_db)
                                                print(f"     📅 최신 데이터:")
                                                for _, row in latest_data.iterrows():
                                                    print(f"        {row.to_dict()}")
                                        else:
                                            print(f"        {samsung_data.iloc[0].to_dict()}")
                                        
                                        samsung_found = True
                                        results[f'stock_{table_name}'] = samsung_data
                                        break
                                except Exception as e:
                                    continue
                        
                        if not samsung_found and count < 50:  # 작은 테이블만 샘플 조회
                            sample_query = f"SELECT * FROM [{table_name}] LIMIT 2"
                            sample_data = pd.read_sql(sample_query, stock_db)
                            print(f"     📋 샘플 데이터: {sample_data.iloc[0].to_dict() if not sample_data.empty else 'None'}")
                        
                except Exception as e:
                    print(f"  ❌ {table_name} 테이블 조회 실패: {e}")
            
            stock_db.close()
            
    except Exception as e:
        print(f"❌ 주식 DB 접근 실패: {e}")
    
    # 3. 결과 요약
    print("\n🎯 3. 삼성전자 실제 데이터 발견 결과")
    print("-" * 40)
    
    if results:
        print(f"✅ 총 {len(results)}개 테이블에서 삼성전자 데이터 발견:")
        for key, data in results.items():
            print(f"  📊 {key}: {len(data)}건")
            
            # 재무 관련 컬럼이 있는지 확인
            financial_keywords = ['revenue', 'income', 'asset', 'equity', 'debt', 'profit', 'sales', 'earning']
            financial_cols = [col for col in data.columns if any(keyword in col.lower() for keyword in financial_keywords)]
            
            if financial_cols:
                print(f"     💰 재무 관련 컬럼: {', '.join(financial_cols[:5])}")
                
                # 실제 재무 데이터 표시
                if not data.empty:
                    latest_row = data.iloc[0]
                    financial_data = {}
                    for col in financial_cols[:10]:  # 최대 10개 컬럼
                        value = latest_row[col]
                        if pd.notna(value) and str(value).strip():
                            financial_data[col] = value
                    
                    if financial_data:
                        print(f"     📈 실제 재무 데이터:")
                        for key, value in financial_data.items():
                            print(f"        {key}: {value}")
        
        return results
    else:
        print("❌ 삼성전자 데이터를 찾을 수 없습니다.")
        print("💡 가능한 원인:")
        print("  1. 데이터가 아직 수집되지 않음")
        print("  2. 다른 종목코드나 형식으로 저장됨")
        print("  3. 테이블 구조가 예상과 다름")
        return None

if __name__ == "__main__":
    try:
        results = check_real_samsung_data()
        
        if results:
            print(f"\n🎉 실제 데이터 확인 완료!")
            print(f"📊 이제 정확한 데이터로 워런 버핏 스코어카드를 다시 계산할 수 있습니다.")
        else:
            print(f"\n⚠️ 실제 데이터를 찾지 못했습니다.")
            print(f"🔧 DART API 데이터 수집이 먼저 필요할 수 있습니다.")
            
    except Exception as e:
        print(f"❌ 스크립트 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()
