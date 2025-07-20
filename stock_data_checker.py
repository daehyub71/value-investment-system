#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
종목 데이터베이스 정보 조회 프로그램
=================================

종목코드를 입력받아 모든 데이터베이스에서 해당 종목의 정보를 조회하고 출력합니다.
아모레퍼시픽(090430) 등 특정 종목의 데이터 수집 현황을 확인할 수 있습니다.

Author: Finance Data Vibe Team
Created: 2025-07-20
"""

import sqlite3
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd

class StockDataChecker:
    """종목 데이터베이스 조회 클래스"""
    
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
        
        # 데이터베이스 정의
        self.databases = {
            'stock_data.db': {
                'name': '주식 기본 데이터',
                'tables': ['stock_prices', 'company_info', 'technical_indicators', 'investment_scores']
            },
            'dart_data.db': {
                'name': 'DART 공시 데이터',
                'tables': ['corp_codes', 'financial_statements', 'disclosures', 'company_outlines']
            },
            'news_data.db': {
                'name': '뉴스 및 감정분석',
                'tables': ['news_articles', 'sentiment_scores', 'market_sentiment']
            },
            'buffett_scorecard.db': {
                'name': '워런 버핏 스코어카드',
                'tables': ['financial_ratios']
            },
            'kis_data.db': {
                'name': 'KIS API 실시간 데이터',
                'tables': ['realtime_quotes', 'account_balance', 'order_history', 'market_indicators']
            },
            'forecast_data.db': {
                'name': 'AI 예측 데이터',
                'tables': []
            },
            'yahoo_finance_data.db': {
                'name': '야후 파이낸스 데이터',
                'tables': []
            }
        }
    
    def get_connection(self, db_file: str) -> Optional[sqlite3.Connection]:
        """데이터베이스 연결"""
        db_full_path = self.db_path / db_file
        if not db_full_path.exists():
            print(f"❌ 데이터베이스 파일이 존재하지 않습니다: {db_full_path}")
            return None
        
        try:
            conn = sqlite3.connect(str(db_full_path))
            conn.row_factory = sqlite3.Row  # 딕셔너리 형태로 결과 반환
            return conn
        except Exception as e:
            print(f"❌ 데이터베이스 연결 실패 ({db_file}): {e}")
            return None
    
    def get_table_list(self, conn: sqlite3.Connection) -> List[str]:
        """테이블 목록 조회"""
        try:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"❌ 테이블 목록 조회 실패: {e}")
            return []
    
    def check_stock_in_table(self, conn: sqlite3.Connection, table_name: str, stock_code: str) -> Dict[str, Any]:
        """특정 테이블에서 종목 데이터 확인"""
        result = {
            'table_name': table_name,
            'exists': False,
            'count': 0,
            'latest_data': None,
            'date_range': None,
            'sample_data': None,
            'error': None
        }
        
        try:
            # 테이블 구조 확인
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            
            # stock_code 컬럼이 있는지 확인
            if 'stock_code' not in columns:
                # corp_code나 다른 컬럼명 확인
                code_column = None
                for col in ['corp_code', 'stock_symbol', 'symbol']:
                    if col in columns:
                        code_column = col
                        break
                
                if not code_column:
                    result['error'] = f"종목코드 컬럼을 찾을 수 없습니다. 컬럼: {', '.join(columns)}"
                    return result
            else:
                code_column = 'stock_code'
            
            # 데이터 개수 확인
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {code_column} = ?", (stock_code,))
            count = cursor.fetchone()[0]
            result['count'] = count
            
            if count > 0:
                result['exists'] = True
                
                # 날짜 관련 컬럼 찾기
                date_columns = [col for col in columns if 'date' in col.lower() or col in ['year', 'pubDate', 'rcept_dt']]
                
                if date_columns:
                    date_col = date_columns[0]
                    
                    # 날짜 범위 조회
                    cursor = conn.execute(f"""
                        SELECT MIN({date_col}) as min_date, MAX({date_col}) as max_date
                        FROM {table_name} WHERE {code_column} = ?
                    """, (stock_code,))
                    date_range = cursor.fetchone()
                    if date_range and date_range[0]:
                        result['date_range'] = {
                            'start': date_range[0],
                            'end': date_range[1]
                        }
                    
                    # 최신 데이터 조회
                    cursor = conn.execute(f"""
                        SELECT * FROM {table_name} 
                        WHERE {code_column} = ? 
                        ORDER BY {date_col} DESC 
                        LIMIT 1
                    """, (stock_code,))
                else:
                    # 날짜 컬럼이 없는 경우 첫 번째 데이터 조회
                    cursor = conn.execute(f"SELECT * FROM {table_name} WHERE {code_column} = ? LIMIT 1", (stock_code,))
                
                latest_data = cursor.fetchone()
                if latest_data:
                    result['latest_data'] = dict(latest_data)
                
                # 샘플 데이터 (최대 3개) 조회
                cursor = conn.execute(f"SELECT * FROM {table_name} WHERE {code_column} = ? LIMIT 3", (stock_code,))
                sample_data = cursor.fetchall()
                result['sample_data'] = [dict(row) for row in sample_data]
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def format_data_for_display(self, data: Dict[str, Any], max_length: int = 50) -> str:
        """데이터를 출력용으로 포맷팅"""
        if not data:
            return "없음"
        
        formatted_items = []
        for key, value in data.items():
            if value is None:
                continue
            
            str_value = str(value)
            if len(str_value) > max_length:
                str_value = str_value[:max_length] + "..."
            
            formatted_items.append(f"{key}: {str_value}")
        
        return " | ".join(formatted_items[:5])  # 최대 5개 필드만 표시
    
    def check_stock_data(self, stock_code: str) -> Dict[str, Any]:
        """종목의 모든 데이터베이스 정보 조회"""
        print(f"\n🔍 {stock_code} 종목 데이터 조회 시작...")
        print("=" * 80)
        
        results = {}
        total_records = 0
        databases_found = 0
        tables_with_data = 0
        
        for db_file, db_info in self.databases.items():
            print(f"\n📊 {db_info['name']} ({db_file})")
            print("-" * 60)
            
            conn = self.get_connection(db_file)
            if not conn:
                results[db_file] = {'error': '데이터베이스 연결 실패'}
                continue
            
            databases_found += 1
            
            try:
                # 실제 테이블 목록 조회
                actual_tables = self.get_table_list(conn)
                
                if not actual_tables:
                    print("   ❌ 테이블이 없습니다.")
                    results[db_file] = {'tables': [], 'error': '테이블 없음'}
                    continue
                
                print(f"   📋 발견된 테이블: {', '.join(actual_tables)}")
                
                db_results = {'tables': {}}
                
                for table_name in actual_tables:
                    table_result = self.check_stock_in_table(conn, table_name, stock_code)
                    db_results['tables'][table_name] = table_result
                    
                    if table_result['exists']:
                        tables_with_data += 1
                        total_records += table_result['count']
                        
                        print(f"   ✅ {table_name}: {table_result['count']}건")
                        
                        if table_result['date_range']:
                            print(f"      📅 기간: {table_result['date_range']['start']} ~ {table_result['date_range']['end']}")
                        
                        if table_result['latest_data']:
                            print(f"      📄 최신데이터: {self.format_data_for_display(table_result['latest_data'])}")
                    
                    elif table_result['error']:
                        print(f"   ❌ {table_name}: {table_result['error']}")
                    else:
                        print(f"   ⭕ {table_name}: 데이터 없음")
                
                results[db_file] = db_results
                
            finally:
                conn.close()
        
        # 요약 정보 출력
        print(f"\n📋 {stock_code} 데이터 수집 현황 요약")
        print("=" * 80)
        print(f"🗄️  검색된 데이터베이스: {databases_found}개")
        print(f"📊 데이터가 있는 테이블: {tables_with_data}개")
        print(f"📈 총 레코드 수: {total_records:,}건")
        
        # 데이터 품질 평가
        if total_records == 0:
            print(f"❌ {stock_code} 종목의 데이터가 전혀 수집되지 않았습니다.")
        elif total_records < 100:
            print(f"🟡 {stock_code} 종목의 데이터가 부족합니다. (추가 수집 필요)")
        elif total_records < 1000:
            print(f"🟢 {stock_code} 종목의 데이터가 적당히 수집되었습니다.")
        else:
            print(f"✅ {stock_code} 종목의 데이터가 충분히 수집되었습니다!")
        
        return results
    
    def get_detailed_analysis(self, stock_code: str) -> None:
        """상세 분석 정보 출력"""
        print(f"\n🔬 {stock_code} 상세 분석")
        print("=" * 80)
        
        # 1. 기업 기본 정보
        conn = self.get_connection('stock_data.db')
        if conn:
            try:
                cursor = conn.execute("SELECT * FROM company_info WHERE stock_code = ?", (stock_code,))
                company_info = cursor.fetchone()
                if company_info:
                    print("🏢 기업 기본 정보:")
                    company_dict = dict(company_info)
                    for key, value in company_dict.items():
                        if value is not None:
                            print(f"   {key}: {value}")
                else:
                    print("🏢 기업 기본 정보: 없음")
            finally:
                conn.close()
        
        # 2. 최신 주가 정보
        conn = self.get_connection('stock_data.db')
        if conn:
            try:
                cursor = conn.execute("""
                    SELECT * FROM stock_prices 
                    WHERE stock_code = ? 
                    ORDER BY date DESC 
                    LIMIT 5
                """, (stock_code,))
                stock_prices = cursor.fetchall()
                if stock_prices:
                    print(f"\n📈 최신 주가 정보 (최근 5일):")
                    for price in stock_prices:
                        price_dict = dict(price)
                        print(f"   {price_dict.get('date', 'N/A')}: "
                              f"종가 {price_dict.get('close_price', 'N/A'):,}원, "
                              f"거래량 {price_dict.get('volume', 'N/A'):,}주")
                else:
                    print("\n📈 최신 주가 정보: 없음")
            finally:
                conn.close()
        
        # 3. 뉴스 정보
        conn = self.get_connection('news_data.db')
        if conn:
            try:
                cursor = conn.execute("""
                    SELECT title, pubDate, source 
                    FROM news_articles 
                    WHERE stock_code = ? 
                    ORDER BY pubDate DESC 
                    LIMIT 5
                """, (stock_code,))
                news = cursor.fetchall()
                if news:
                    print(f"\n📰 최신 뉴스 (최근 5건):")
                    for article in news:
                        article_dict = dict(article)
                        print(f"   [{article_dict.get('pubDate', 'N/A')}] {article_dict.get('title', 'N/A')[:50]}...")
                else:
                    print("\n📰 최신 뉴스: 없음")
            finally:
                conn.close()
        
        # 4. 워런 버핏 스코어
        conn = self.get_connection('buffett_scorecard.db')
        if conn:
            try:
                cursor = conn.execute("""
                    SELECT total_buffett_score, profitability_score, growth_score, 
                           stability_score, efficiency_score, valuation_score, year
                    FROM financial_ratios 
                    WHERE stock_code = ? 
                    ORDER BY year DESC 
                    LIMIT 1
                """, (stock_code,))
                buffett_score = cursor.fetchone()
                if buffett_score:
                    score_dict = dict(buffett_score)
                    print(f"\n🏆 워런 버핏 스코어카드 ({score_dict.get('year', 'N/A')}년):")
                    print(f"   총점: {score_dict.get('total_buffett_score', 'N/A')}/110점")
                    print(f"   수익성: {score_dict.get('profitability_score', 'N/A')}/30점")
                    print(f"   성장성: {score_dict.get('growth_score', 'N/A')}/25점")
                    print(f"   안정성: {score_dict.get('stability_score', 'N/A')}/25점")
                    print(f"   효율성: {score_dict.get('efficiency_score', 'N/A')}/10점")
                    print(f"   가치평가: {score_dict.get('valuation_score', 'N/A')}/20점")
                else:
                    print("\n🏆 워런 버핏 스코어카드: 없음")
            finally:
                conn.close()

def main():
    """메인 함수"""
    print("🎯 Finance Data Vibe - 종목 데이터베이스 조회 프로그램")
    print("=" * 80)
    
    # 명령행 인수로 종목코드가 제공된 경우
    if len(sys.argv) > 1:
        stock_code = sys.argv[1]
    else:
        # 사용자 입력 받기
        stock_code = input("📝 종목코드를 입력하세요 (예: 090430): ").strip()
    
    if not stock_code:
        print("❌ 종목코드가 입력되지 않았습니다.")
        return
    
    # 종목코드 형식 검증 (6자리 숫자)
    if not stock_code.isdigit() or len(stock_code) != 6:
        print("❌ 올바른 종목코드 형식이 아닙니다. (6자리 숫자)")
        return
    
    # 데이터 조회 실행
    checker = StockDataChecker()
    
    try:
        # 기본 데이터 조회
        results = checker.check_stock_data(stock_code)
        
        # 상세 분석 제공 여부 묻기
        if input("\n🔍 상세 분석을 보시겠습니까? (y/N): ").lower() == 'y':
            checker.get_detailed_analysis(stock_code)
        
        print(f"\n✅ {stock_code} 종목 데이터 조회 완료!")
        
    except KeyboardInterrupt:
        print("\n⏹️  사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()