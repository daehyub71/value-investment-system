#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
아모레퍼시픽 재무데이터 종합 분석기
==============================

종목코드를 입력받아 모든 재무 관련 테이블에서 해당 종목의 데이터를 
종합적으로 조회하고 분석합니다.

특징:
- 모든 데이터베이스의 재무 테이블 검색
- 종목별 상세 재무 데이터 표시
- 워런 버핏 스코어카드 분석
- 시계열 데이터 트렌드 분석

Author: Finance Data Vibe Team
Created: 2025-07-20
"""

import sqlite3
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from datetime import datetime
import json

class StockFinancialAnalyzer:
    """종목 재무데이터 종합 분석 클래스"""
    
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
        
        # 종목 정보 캐시
        self.stock_info_cache = {}
    
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
                    'name': row[1],
                    'type': row[2],
                    'notnull': bool(row[3]),
                    'default_value': row[4],
                    'pk': bool(row[5])
                })
            return columns
        except Exception as e:
            return []
    
    def is_financial_table(self, table_name: str) -> bool:
        """재무 관련 테이블인지 확인"""
        table_lower = table_name.lower()
        return any(keyword in table_lower for keyword in self.financial_keywords)
    
    def find_stock_code_column(self, columns: List[str]) -> Optional[str]:
        """종목코드 컬럼 찾기"""
        for col in columns:
            if col.lower() in ['stock_code', 'corp_code', 'symbol', 'ticker']:
                return col
        return None
    
    def get_company_basic_info(self, stock_code: str) -> Dict[str, Any]:
        """기업 기본 정보 조회"""
        if stock_code in self.stock_info_cache:
            return self.stock_info_cache[stock_code]
        
        company_info = {
            'stock_code': stock_code,
            'company_name': 'Unknown',
            'market_type': 'Unknown',
            'sector': 'Unknown',
            'industry': 'Unknown'
        }
        
        # stock_data.db에서 기업 정보 조회
        conn = self.get_connection('stock_data.db')
        if conn:
            try:
                cursor = conn.execute("SELECT * FROM company_info WHERE stock_code = ?", (stock_code,))
                result = cursor.fetchone()
                if result:
                    company_info.update(dict(result))
            except:
                pass
            finally:
                conn.close()
        
        # dart_data.db에서 기업 정보 조회
        conn = self.get_connection('dart_data.db')
        if conn:
            try:
                cursor = conn.execute("SELECT * FROM corp_codes WHERE stock_code = ?", (stock_code,))
                result = cursor.fetchone()
                if result:
                    company_info['corp_code'] = result['corp_code']
                    company_info['corp_name'] = result['corp_name']
                    if company_info['company_name'] == 'Unknown':
                        company_info['company_name'] = result['corp_name']
            except:
                pass
            finally:
                conn.close()
        
        self.stock_info_cache[stock_code] = company_info
        return company_info
    
    def format_financial_value(self, value: Any) -> str:
        """재무 수치를 보기 좋게 포맷팅"""
        if value is None:
            return "N/A"
        
        try:
            num_value = float(value)
            if abs(num_value) >= 1_000_000_000_000:  # 조 단위
                return f"{num_value/1_000_000_000_000:,.1f}조"
            elif abs(num_value) >= 100_000_000:  # 억 단위
                return f"{num_value/100_000_000:,.1f}억"
            elif abs(num_value) >= 10_000:  # 만 단위
                return f"{num_value/10_000:,.1f}만"
            elif abs(num_value) >= 1:
                return f"{num_value:,.1f}"
            else:
                return f"{num_value:.4f}"
        except:
            return str(value)
    
    def analyze_stock_financial_data(self, stock_code: str) -> Dict[str, Any]:
        """종목의 모든 재무데이터 종합 분석"""
        
        print(f"\n🏢 {stock_code} 종목 재무데이터 종합 분석")
        print("=" * 100)
        
        # 기업 기본 정보 조회
        company_info = self.get_company_basic_info(stock_code)
        print(f"📋 기업명: {company_info['company_name']}")
        print(f"🏷️  종목코드: {stock_code}")
        if 'corp_code' in company_info:
            print(f"🏛️  DART 기업코드: {company_info['corp_code']}")
        if company_info.get('sector') != 'Unknown':
            print(f"🏭 섹터: {company_info['sector']}")
        
        all_results = {}
        total_records = 0
        tables_with_data = 0
        
        # 모든 데이터베이스 검색
        for db_file in self.database_files:
            print(f"\n📊 데이터베이스: {db_file}")
            print("-" * 80)
            
            conn = self.get_connection(db_file)
            if not conn:
                print(f"   ❌ 연결 실패")
                continue
            
            try:
                # 모든 테이블 목록 조회
                cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                all_tables = [row[0] for row in cursor.fetchall()]
                
                # 재무 관련 테이블 필터링
                financial_tables = [table for table in all_tables if self.is_financial_table(table)]
                
                if not financial_tables:
                    print("   ⭕ 재무 관련 테이블 없음")
                    continue
                
                db_results = {}
                
                for table_name in financial_tables:
                    # 테이블 스키마 확인
                    schema = self.get_table_schema(conn, table_name)
                    column_names = [col['name'] for col in schema]
                    
                    # 종목코드 컬럼 찾기
                    code_column = self.find_stock_code_column(column_names)
                    
                    if not code_column:
                        continue
                    
                    # 해당 종목 데이터 조회
                    try:
                        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {code_column} = ?", (stock_code,))
                        count = cursor.fetchone()[0]
                        
                        if count > 0:
                            tables_with_data += 1
                            total_records += count
                            
                            print(f"   ✅ {table_name}: {count}건")
                            
                            # 전체 데이터 조회
                            cursor = conn.execute(f"SELECT * FROM {table_name} WHERE {code_column} = ? ORDER BY ROWID", (stock_code,))
                            data = [dict(row) for row in cursor.fetchall()]
                            
                            # 데이터 분석
                            table_analysis = self.analyze_table_data(table_name, data, schema)
                            
                            db_results[table_name] = {
                                'count': count,
                                'data': data,
                                'analysis': table_analysis,
                                'schema': schema
                            }
                            
                            # 상세 데이터 출력
                            self.display_table_data(table_name, data, schema)
                    
                    except Exception as e:
                        print(f"   ❌ {table_name}: 조회 실패 ({e})")
                
                if db_results:
                    all_results[db_file] = db_results
                
            finally:
                conn.close()
        
        # 종합 분석 결과
        self.display_comprehensive_analysis(stock_code, all_results, total_records, tables_with_data)
        
        return all_results
    
    def analyze_table_data(self, table_name: str, data: List[Dict], schema: List[Dict]) -> Dict[str, Any]:
        """테이블 데이터 분석"""
        if not data:
            return {'empty': True}
        
        analysis = {
            'record_count': len(data),
            'date_range': None,
            'key_metrics': {},
            'data_quality': {}
        }
        
        # 날짜 범위 분석
        date_columns = [col['name'] for col in schema if 'date' in col['name'].lower() or col['name'] in ['year', 'bsns_year']]
        if date_columns and data:
            date_col = date_columns[0]
            dates = [row.get(date_col) for row in data if row.get(date_col)]
            if dates:
                analysis['date_range'] = {
                    'start': min(dates),
                    'end': max(dates),
                    'column': date_col
                }
        
        # 숫자형 컬럼 통계
        numeric_columns = [col['name'] for col in schema if col['type'].upper() in ['REAL', 'INTEGER']]
        for col in numeric_columns:
            values = [row.get(col) for row in data if row.get(col) is not None]
            if values:
                try:
                    numeric_values = [float(v) for v in values]
                    analysis['key_metrics'][col] = {
                        'count': len(numeric_values),
                        'min': min(numeric_values),
                        'max': max(numeric_values),
                        'avg': sum(numeric_values) / len(numeric_values) if numeric_values else 0
                    }
                except:
                    pass
        
        return analysis
    
    def display_table_data(self, table_name: str, data: List[Dict], schema: List[Dict]):
        """테이블 데이터 상세 출력"""
        if not data:
            print(f"      📋 {table_name}: 데이터 없음")
            return
        
        print(f"\n      📊 {table_name} 상세 데이터 ({len(data)}건)")
        print("      " + "─" * 70)
        
        # 테이블별 맞춤 출력
        if 'financial_statements' in table_name.lower():
            self.display_financial_statements(data)
        elif 'ratios' in table_name.lower() or 'scorecard' in table_name.lower():
            self.display_financial_ratios(data)
        elif 'corp_codes' in table_name.lower():
            self.display_corp_codes(data)
        elif 'company_info' in table_name.lower():
            self.display_company_info(data)
        elif 'stock_prices' in table_name.lower():
            self.display_stock_prices(data)
        else:
            self.display_generic_data(data)
    
    def display_financial_statements(self, data: List[Dict]):
        """재무제표 데이터 출력"""
        print("      📈 재무제표 데이터:")
        
        # 연도별로 그룹화
        by_year = {}
        for row in data:
            year = row.get('bsns_year', row.get('year', 'Unknown'))
            if year not in by_year:
                by_year[year] = []
            by_year[year].append(row)
        
        for year in sorted(by_year.keys(), reverse=True):
            print(f"\n         📅 {year}년:")
            year_data = by_year[year]
            
            # 주요 계정과목만 필터링
            important_accounts = [
                '매출액', '매출총이익', '영업이익', '당기순이익', '총자산', '자기자본', '부채총계',
                'Revenue', 'Operating Income', 'Net Income', 'Total Assets', 'Total Equity'
            ]
            
            important_data = []
            other_data = []
            
            for row in year_data:
                account_name = row.get('account_nm', '')
                if any(important in account_name for important in important_accounts):
                    important_data.append(row)
                else:
                    other_data.append(row)
            
            # 주요 계정과목 먼저 출력
            for row in important_data:
                account_name = row.get('account_nm', 'N/A')
                current_amount = self.format_financial_value(row.get('thstrm_amount'))
                print(f"            💰 {account_name}: {current_amount}")
            
            # 기타 계정과목 (처음 5개만)
            if other_data:
                print(f"            📝 기타 {len(other_data)}개 항목 (상위 5개):")
                for row in other_data[:5]:
                    account_name = row.get('account_nm', 'N/A')
                    current_amount = self.format_financial_value(row.get('thstrm_amount'))
                    print(f"               - {account_name}: {current_amount}")
                
                if len(other_data) > 5:
                    print(f"               ... 외 {len(other_data) - 5}개 항목")
    
    def display_financial_ratios(self, data: List[Dict]):
        """재무비율 데이터 출력"""
        print("      🏆 재무비율 및 스코어:")
        
        for row in data:
            year = row.get('year', 'N/A')
            quarter = row.get('quarter', '')
            period_str = f"{year}년" + (f" {quarter}분기" if quarter else "")
            
            print(f"\n         📅 {period_str}:")
            
            # 워런 버핏 스코어카드
            if 'total_buffett_score' in row:
                total_score = row.get('total_buffett_score')
                if total_score:
                    print(f"            🎯 워런 버핏 총점: {total_score:.1f}/110점")
                    
                    # 세부 점수
                    scores = [
                        ('수익성', row.get('profitability_score'), 30),
                        ('성장성', row.get('growth_score'), 25),
                        ('안정성', row.get('stability_score'), 25),
                        ('효율성', row.get('efficiency_score'), 10),
                        ('가치평가', row.get('valuation_score'), 20)
                    ]
                    
                    for name, score, max_score in scores:
                        if score is not None:
                            percentage = (score / max_score * 100) if max_score > 0 else 0
                            print(f"               - {name}: {score:.1f}/{max_score}점 ({percentage:.1f}%)")
            
            # 주요 재무비율
            key_ratios = [
                ('ROE', row.get('roe'), '%'),
                ('ROA', row.get('roa'), '%'),
                ('부채비율', row.get('debt_ratio'), '%'),
                ('유동비율', row.get('current_ratio'), '배'),
                ('PER', row.get('per'), '배'),
                ('PBR', row.get('pbr'), '배'),
                ('배당수익률', row.get('dividend_yield'), '%')
            ]
            
            print(f"            📊 주요 재무비율:")
            for name, value, unit in key_ratios:
                if value is not None:
                    if unit == '%':
                        print(f"               - {name}: {value:.2f}%")
                    else:
                        print(f"               - {name}: {value:.2f}{unit}")
    
    def display_corp_codes(self, data: List[Dict]):
        """기업코드 데이터 출력"""
        print("      🏛️ DART 기업 정보:")
        for row in data:
            corp_code = row.get('corp_code', 'N/A')
            corp_name = row.get('corp_name', 'N/A')
            stock_code = row.get('stock_code', 'N/A')
            print(f"         - 기업코드: {corp_code}")
            print(f"         - 기업명: {corp_name}")
            print(f"         - 종목코드: {stock_code}")
    
    def display_company_info(self, data: List[Dict]):
        """기업정보 데이터 출력"""
        print("      🏢 기업 기본 정보:")
        for row in data:
            company_name = row.get('company_name', 'N/A')
            market_type = row.get('market_type', 'N/A')
            sector = row.get('sector', 'N/A')
            industry = row.get('industry', 'N/A')
            market_cap = row.get('market_cap')
            
            print(f"         - 회사명: {company_name}")
            print(f"         - 시장구분: {market_type}")
            print(f"         - 섹터: {sector}")
            print(f"         - 업종: {industry}")
            if market_cap:
                print(f"         - 시가총액: {self.format_financial_value(market_cap)}원")
    
    def display_stock_prices(self, data: List[Dict]):
        """주가 데이터 출력"""
        print("      📈 주가 데이터 (최근 10일):")
        
        # 날짜 기준 정렬
        sorted_data = sorted(data, key=lambda x: x.get('date', ''), reverse=True)
        
        for row in sorted_data[:10]:
            date = row.get('date', 'N/A')
            close_price = row.get('close_price')
            volume = row.get('volume')
            change = ""
            
            if close_price:
                close_str = f"{close_price:,.0f}원"
            else:
                close_str = "N/A"
            
            if volume:
                volume_str = f"거래량 {volume:,}주"
            else:
                volume_str = ""
            
            print(f"         - {date}: {close_str} {volume_str}")
    
    def display_generic_data(self, data: List[Dict]):
        """일반 데이터 출력"""
        print("      📄 데이터 내용:")
        for i, row in enumerate(data[:5], 1):
            # 주요 컬럼만 표시
            key_data = {}
            for key, value in row.items():
                if key.lower() in ['id', 'date', 'year', 'amount', 'value', 'score', 'name']:
                    key_data[key] = value
                if len(key_data) >= 4:
                    break
            
            print(f"         {i}. {key_data}")
        
        if len(data) > 5:
            print(f"         ... 외 {len(data) - 5}건")
    
    def display_comprehensive_analysis(self, stock_code: str, all_results: Dict, total_records: int, tables_with_data: int):
        """종합 분석 결과 출력"""
        
        print(f"\n📋 {stock_code} 재무데이터 종합 분석 결과")
        print("=" * 100)
        
        print(f"📊 데이터 수집 현황:")
        print(f"   - 데이터가 있는 테이블: {tables_with_data}개")
        print(f"   - 총 레코드 수: {total_records:,}건")
        
        # 데이터 완전성 평가
        completeness_score = 0
        if any('financial_statements' in str(results) for results in all_results.values()):
            completeness_score += 30
        if any('ratios' in str(results) or 'scorecard' in str(results) for results in all_results.values()):
            completeness_score += 25
        if any('company_info' in str(results) for results in all_results.values()):
            completeness_score += 20
        if any('stock_prices' in str(results) for results in all_results.values()):
            completeness_score += 25
        
        print(f"\n📈 데이터 완전성 평가: {completeness_score}/100점")
        
        if completeness_score >= 80:
            print(f"   ✅ 우수 - 종합적인 재무분석이 가능합니다")
        elif completeness_score >= 60:
            print(f"   🟡 보통 - 기본적인 재무분석이 가능합니다")
        elif completeness_score >= 40:
            print(f"   🟠 부족 - 일부 재무분석만 가능합니다")
        else:
            print(f"   🔴 미흡 - 추가 데이터 수집이 필요합니다")
        
        # 권장사항
        print(f"\n💡 분석 결과 및 권장사항:")
        
        missing_data = []
        if not any('financial_statements' in str(results) for results in all_results.values()):
            missing_data.append("DART 재무제표 데이터")
        if not any('ratios' in str(results) or 'scorecard' in str(results) for results in all_results.values()):
            missing_data.append("재무비율 및 워런 버핏 스코어")
        if not any('stock_prices' in str(results) for results in all_results.values()):
            missing_data.append("주가 데이터")
        
        if missing_data:
            print(f"   🔴 부족한 데이터: {', '.join(missing_data)}")
            print(f"   📝 권장 액션:")
            if "DART 재무제표 데이터" in missing_data:
                print(f"      - python scripts/data_collection/collect_dart_data.py")
            if "재무비율 및 워런 버핏 스코어" in missing_data:
                print(f"      - python scripts/analysis/calculate_buffett_score.py --stock_code={stock_code}")
            if "주가 데이터" in missing_data:
                print(f"      - python scripts/data_collection/collect_stock_data.py --stock_code={stock_code}")
        else:
            print(f"   ✅ 모든 핵심 데이터가 수집되었습니다!")
            print(f"   🚀 다음 단계: 웹 인터페이스에서 종합 분석 결과 확인")

def main():
    """메인 함수"""
    print("🏦 Finance Data Vibe - 종목 재무데이터 종합 분석기")
    print("=" * 100)
    
    # 명령행 인수로 종목코드가 제공된 경우
    if len(sys.argv) > 1:
        stock_code = sys.argv[1]
    else:
        # 사용자 입력 받기 (기본값: 아모레퍼시픽)
        default_code = "090430"
        user_input = input(f"📝 종목코드를 입력하세요 (기본값: {default_code} - 아모레퍼시픽): ").strip()
        stock_code = user_input if user_input else default_code
    
    if not stock_code:
        print("❌ 종목코드가 입력되지 않았습니다.")
        return
    
    # 종목코드 형식 검증 (6자리 숫자)
    if not stock_code.isdigit() or len(stock_code) != 6:
        print("❌ 올바른 종목코드 형식이 아닙니다. (6자리 숫자)")
        return
    
    # 아모레퍼시픽 정보 출력
    if stock_code == "090430":
        print("🎯 분석 대상: 아모레퍼시픽 (090430)")
        print("   - 화장품 및 생활용품 제조업")
        print("   - KOSPI 상장")
        print("   - 대표 브랜드: 설화수, 라네즈, 마몽드, 이니스프리 등")
    
    # 재무데이터 분석 실행
    analyzer = StockFinancialAnalyzer()
    
    try:
        results = analyzer.analyze_stock_financial_data(stock_code)
        
        # 결과 저장 옵션
        save_option = input(f"\n💾 분석 결과를 JSON 파일로 저장하시겠습니까? (y/N): ").lower()
        if save_option == 'y':
            output_file = f"{stock_code}_financial_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            # JSON 직렬화를 위한 데이터 변환
            json_results = {}
            for db_name, db_data in results.items():
                json_results[db_name] = {}
                for table_name, table_data in db_data.items():
                    json_results[db_name][table_name] = {
                        'count': table_data['count'],
                        'data': table_data['data'],
                        'analysis': table_data['analysis']
                    }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(json_results, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"✅ 분석 결과가 {output_file}에 저장되었습니다.")
        
        print(f"\n✅ {stock_code} 종목 재무데이터 분석 완료!")
        
    except KeyboardInterrupt:
        print("\n⏹️  사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()