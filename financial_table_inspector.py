#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
재무 테이블 구조 검사 프로그램
=========================

데이터베이스에서 재무 관련 테이블의 실제 구조를 확인하고 분석합니다.
DART 재무데이터와 관련된 모든 테이블을 검색하고 스키마를 출력합니다.

Author: Finance Data Vibe Team
Created: 2025-07-20
"""

import sqlite3
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd

class FinancialTableInspector:
    """재무 테이블 구조 검사 클래스"""
    
    def __init__(self, db_path: str = None):
        """
        초기화
        
        Args:
            db_path: 데이터베이스 파일들이 위치한 경로
        """
        if db_path is None:
            # 프로젝트 루트에서 data/databases 경로 자동 탐지
            current_dir = Path(__file__).parent
            for _ in range(5):  # 최대 5단계 상위 폴더까지 탐색
                db_path = current_dir / "data" / "databases"
                if db_path.exists():
                    break
                current_dir = current_dir.parent
            else:
                db_path = Path("data/databases")  # 기본 경로
        
        self.db_path = Path(db_path)
        
        # 재무 관련 키워드 정의
        self.financial_keywords = [
            'financial', 'dart', 'corp', 'company', 'samsung',
            'statements', 'balance', 'income', 'cash', 'ratios',
            'scorecard', 'buffett', 'fundamental'
        ]
        
        # 알려진 데이터베이스 파일들
        self.database_files = [
            'stock_data.db',
            'dart_data.db', 
            'buffett_scorecard.db',
            'news_data.db',
            'kis_data.db',
            'forecast_data.db',
            'yahoo_finance_data.db'
        ]
    
    def get_connection(self, db_file: str) -> Optional[sqlite3.Connection]:
        """데이터베이스 연결"""
        db_full_path = self.db_path / db_file
        if not db_full_path.exists():
            return None
        
        try:
            conn = sqlite3.connect(str(db_full_path))
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            print(f"❌ 데이터베이스 연결 실패 ({db_file}): {e}")
            return None
    
    def get_table_schema(self, conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
        """테이블 스키마 정보 조회"""
        try:
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'cid': row[0],
                    'name': row[1],
                    'type': row[2],
                    'notnull': bool(row[3]),
                    'default_value': row[4],
                    'pk': bool(row[5])
                })
            return columns
        except Exception as e:
            print(f"❌ 스키마 조회 실패 ({table_name}): {e}")
            return []
    
    def get_table_indexes(self, conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
        """테이블 인덱스 정보 조회"""
        try:
            cursor = conn.execute(f"PRAGMA index_list({table_name})")
            indexes = []
            for row in cursor.fetchall():
                index_name = row[1]
                # 인덱스 상세 정보 조회
                cursor2 = conn.execute(f"PRAGMA index_info({index_name})")
                columns = [col[2] for col in cursor2.fetchall()]
                
                indexes.append({
                    'name': index_name,
                    'unique': bool(row[2]),
                    'columns': columns
                })
            return indexes
        except Exception as e:
            return []
    
    def get_sample_data(self, conn: sqlite3.Connection, table_name: str, limit: int = 3) -> List[Dict[str, Any]]:
        """테이블 샘플 데이터 조회"""
        try:
            cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            return []
    
    def get_table_stats(self, conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
        """테이블 통계 정보 조회"""
        stats = {
            'row_count': 0,
            'has_data': False,
            'date_range': None,
            'unique_stock_codes': 0
        }
        
        try:
            # 레코드 수 조회
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            stats['row_count'] = cursor.fetchone()[0]
            stats['has_data'] = stats['row_count'] > 0
            
            if stats['has_data']:
                # 테이블 스키마 확인
                columns_info = self.get_table_schema(conn, table_name)
                column_names = [col['name'] for col in columns_info]
                
                # 날짜 범위 확인
                date_columns = [col for col in column_names if 'date' in col.lower() or col in ['year', 'bsns_year']]
                if date_columns:
                    date_col = date_columns[0]
                    cursor = conn.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table_name}")
                    min_date, max_date = cursor.fetchone()
                    if min_date:
                        stats['date_range'] = {'start': min_date, 'end': max_date}
                
                # 종목 코드 개수 확인
                code_columns = [col for col in column_names if 'stock_code' in col or 'corp_code' in col]
                if code_columns:
                    code_col = code_columns[0]
                    cursor = conn.execute(f"SELECT COUNT(DISTINCT {code_col}) FROM {table_name}")
                    stats['unique_stock_codes'] = cursor.fetchone()[0]
        
        except Exception as e:
            stats['error'] = str(e)
        
        return stats
    
    def is_financial_table(self, table_name: str) -> bool:
        """재무 관련 테이블인지 확인"""
        table_lower = table_name.lower()
        return any(keyword in table_lower for keyword in self.financial_keywords)
    
    def format_schema_display(self, columns: List[Dict[str, Any]]) -> str:
        """스키마를 보기 좋게 포맷팅"""
        if not columns:
            return "스키마 정보 없음"
        
        lines = []
        lines.append("┌─────┬──────────────────────┬──────────────┬─────────┬──────────┬────┐")
        lines.append("│ CID │ Column Name          │ Type         │ NotNull │ Default  │ PK │")
        lines.append("├─────┼──────────────────────┼──────────────┼─────────┼──────────┼────┤")
        
        for col in columns:
            cid = str(col['cid']).ljust(3)
            name = col['name'][:20].ljust(20)
            col_type = col['type'][:12].ljust(12)
            notnull = "✓" if col['notnull'] else " "
            default = str(col['default_value'] or "")[:8].ljust(8)
            pk = "✓" if col['pk'] else " "
            
            lines.append(f"│ {cid} │ {name} │ {col_type} │    {notnull}    │ {default} │ {pk}  │")
        
        lines.append("└─────┴──────────────────────┴──────────────┴─────────┴──────────┴────┘")
        return "\n".join(lines)
    
    def analyze_all_financial_tables(self) -> Dict[str, Any]:
        """모든 재무 관련 테이블 분석"""
        print("🔍 재무 관련 테이블 구조 분석 시작...")
        print("=" * 100)
        
        all_results = {}
        total_financial_tables = 0
        total_records = 0
        
        for db_file in self.database_files:
            print(f"\n📊 데이터베이스: {db_file}")
            print("-" * 80)
            
            conn = self.get_connection(db_file)
            if not conn:
                print(f"   ❌ 연결 실패: {db_file}")
                continue
            
            try:
                # 모든 테이블 목록 조회
                cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                all_tables = [row[0] for row in cursor.fetchall()]
                
                # 재무 관련 테이블 필터링
                financial_tables = [table for table in all_tables if self.is_financial_table(table)]
                
                if not financial_tables:
                    print("   ⭕ 재무 관련 테이블 없음")
                    all_results[db_file] = {'tables': {}, 'financial_table_count': 0}
                    continue
                
                print(f"   📋 발견된 재무 테이블: {', '.join(financial_tables)}")
                
                db_results = {'tables': {}, 'financial_table_count': len(financial_tables)}
                total_financial_tables += len(financial_tables)
                
                for table_name in financial_tables:
                    print(f"\n   📊 테이블 분석: {table_name}")
                    print("   " + "─" * 60)
                    
                    # 스키마 정보
                    schema = self.get_table_schema(conn, table_name)
                    
                    # 통계 정보
                    stats = self.get_table_stats(conn, table_name)
                    total_records += stats['row_count']
                    
                    # 인덱스 정보
                    indexes = self.get_table_indexes(conn, table_name)
                    
                    # 샘플 데이터
                    sample_data = self.get_sample_data(conn, table_name)
                    
                    # 결과 저장
                    table_result = {
                        'schema': schema,
                        'stats': stats,
                        'indexes': indexes,
                        'sample_data': sample_data
                    }
                    db_results['tables'][table_name] = table_result
                    
                    # 출력
                    print(f"   📈 레코드 수: {stats['row_count']:,}건")
                    
                    if stats['date_range']:
                        print(f"   📅 날짜 범위: {stats['date_range']['start']} ~ {stats['date_range']['end']}")
                    
                    if stats['unique_stock_codes'] > 0:
                        print(f"   🏢 고유 종목/기업: {stats['unique_stock_codes']}개")
                    
                    print("\n   📋 테이블 스키마:")
                    print("   " + self.format_schema_display(schema).replace('\n', '\n   '))
                    
                    if indexes:
                        print(f"\n   🔍 인덱스 ({len(indexes)}개):")
                        for idx in indexes:
                            unique_str = "(UNIQUE)" if idx['unique'] else ""
                            print(f"      - {idx['name']}: {', '.join(idx['columns'])} {unique_str}")
                    
                    if sample_data:
                        print(f"\n   📄 샘플 데이터 (최대 3건):")
                        for i, row in enumerate(sample_data, 1):
                            # 주요 컬럼만 표시
                            key_data = {}
                            for key, value in row.items():
                                if key in ['id', 'stock_code', 'corp_code', 'date', 'year', 'bsns_year', 'account_nm', 'corp_name']:
                                    key_data[key] = value
                                if len(key_data) >= 5:  # 최대 5개 컬럼만
                                    break
                            
                            print(f"      {i}. {key_data}")
                
                all_results[db_file] = db_results
                
            finally:
                conn.close()
        
        # 종합 요약
        print(f"\n📋 재무 테이블 분석 요약")
        print("=" * 100)
        print(f"🗄️  분석된 데이터베이스: {len([db for db in all_results if all_results[db]['financial_table_count'] > 0])}개")
        print(f"📊 발견된 재무 테이블: {total_financial_tables}개")
        print(f"📈 총 재무 레코드: {total_records:,}건")
        
        # 테이블별 상세 요약
        print(f"\n📊 테이블별 상세 현황:")
        for db_file, db_result in all_results.items():
            if db_result['financial_table_count'] > 0:
                print(f"\n   📁 {db_file}:")
                for table_name, table_data in db_result['tables'].items():
                    row_count = table_data['stats']['row_count']
                    unique_codes = table_data['stats']['unique_stock_codes']
                    print(f"      📊 {table_name}: {row_count:,}건 ({unique_codes}개 종목/기업)")
        
        return all_results
    
    def search_specific_tables(self, table_names: List[str]) -> Dict[str, Any]:
        """특정 테이블 이름들을 검색"""
        print(f"🔍 특정 테이블 검색: {', '.join(table_names)}")
        print("=" * 100)
        
        found_tables = {}
        
        for db_file in self.database_files:
            conn = self.get_connection(db_file)
            if not conn:
                continue
            
            try:
                cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                existing_tables = [row[0] for row in cursor.fetchall()]
                
                # 검색하는 테이블이 있는지 확인
                found_in_db = []
                for target_table in table_names:
                    if target_table in existing_tables:
                        found_in_db.append(target_table)
                
                if found_in_db:
                    print(f"\n📊 {db_file}에서 발견:")
                    for table_name in found_in_db:
                        schema = self.get_table_schema(conn, table_name)
                        stats = self.get_table_stats(conn, table_name)
                        
                        print(f"   ✅ {table_name}: {stats['row_count']:,}건")
                        print(f"      📋 컬럼: {', '.join([col['name'] for col in schema])}")
                        
                        found_tables[f"{db_file}.{table_name}"] = {
                            'database': db_file,
                            'table': table_name,
                            'schema': schema,
                            'stats': stats
                        }
            finally:
                conn.close()
        
        if not found_tables:
            print("❌ 검색된 테이블이 없습니다.")
        
        return found_tables

def main():
    """메인 함수"""
    print("🏦 Finance Data Vibe - 재무 테이블 구조 검사 프로그램")
    print("=" * 100)
    
    inspector = FinancialTableInspector()
    
    try:
        # 모든 재무 관련 테이블 분석
        print("1️⃣ 모든 재무 관련 테이블 분석 중...")
        all_results = inspector.analyze_all_financial_tables()
        
        # 특정 테이블 검색
        print("\n" + "=" * 100)
        print("2️⃣ 특정 재무 테이블 검색 중...")
        
        specific_tables = [
            'samsung_financial_statements',
            'financial_statements', 
            'dart_financial_data',
            'corp_financial_data',
            'multi_stock_financial_statements',
            'financial_ratios',
            'corp_codes',
            'company_outlines'
        ]
        
        found_specific = inspector.search_specific_tables(specific_tables)
        
        # 결과 요약
        print(f"\n✅ 재무 테이블 구조 분석 완료!")
        print(f"📊 총 {len([table for db_result in all_results.values() for table in db_result['tables']])}개 재무 테이블 발견")
        print(f"🔍 특정 검색 테이블 {len(found_specific)}개 발견")
        
        # 권장사항
        if len(found_specific) < len(specific_tables):
            print(f"\n💡 권장사항:")
            print(f"   - 일부 재무 테이블이 누락되었습니다.")
            print(f"   - DART 데이터 수집 스크립트를 실행하여 데이터를 보완하세요.")
            print(f"   - python scripts/data_collection/collect_dart_data.py")
        
    except KeyboardInterrupt:
        print("\n⏹️  사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()