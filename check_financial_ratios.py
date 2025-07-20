#!/usr/bin/env python3
"""
Financial Ratios 테이블 데이터 수집 상태 확인 도구

주요 기능:
- 전체 데이터 수집 현황 요약
- 종목별 상세 정보 확인
- 누락 데이터 분석
- 데이터 품질 검증
- 수집 완료도 리포트

실행 방법:
python check_financial_ratios.py --mode summary        # 전체 현황
python check_financial_ratios.py --mode detail         # 상세 정보
python check_financial_ratios.py --mode missing        # 누락 데이터
python check_financial_ratios.py --stock_code 005930   # 특정 종목
"""

import sys
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse
import logging
from typing import Dict, List, Any, Optional

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinancialRatiosChecker:
    """Financial Ratios 테이블 상태 확인 클래스"""
    
    def __init__(self):
        self.stock_db_path = Path('data/databases/stock_data.db')
        
        # 사용 가능한 테이블들 확인
        self.available_tables = self._get_available_tables()
        self.financial_table = self._select_financial_table()
        
        if not self.financial_table:
            raise Exception("❌ financial_ratios 관련 테이블을 찾을 수 없습니다.")
        
        logger.info(f"사용할 테이블: {self.financial_table}")
    
    def _get_available_tables(self) -> List[str]:
        """사용 가능한 테이블 목록 조회"""
        try:
            with sqlite3.connect(self.stock_db_path) as conn:
                cursor = conn.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE '%financial_ratio%'
                    ORDER BY name
                """)
                tables = [row[0] for row in cursor.fetchall()]
                return tables
        except Exception as e:
            logger.error(f"테이블 조회 실패: {e}")
            return []
    
    def _select_financial_table(self) -> Optional[str]:
        """사용할 financial_ratios 테이블 선택"""
        if not self.available_tables:
            return None
        
        # 우선순위: financial_ratios_fdr > financial_ratios
        for table in ['financial_ratios_fdr', 'financial_ratios']:
            if table in self.available_tables:
                return table
        
        # 첫 번째 테이블 사용
        return self.available_tables[0]
    
    def get_table_info(self) -> Dict[str, Any]:
        """테이블 기본 정보 조회"""
        try:
            with sqlite3.connect(self.stock_db_path) as conn:
                # 테이블 구조
                cursor = conn.execute(f"PRAGMA table_info({self.financial_table})")
                columns = [{'name': row[1], 'type': row[2], 'notnull': row[3]} for row in cursor.fetchall()]
                
                # 전체 레코드 수
                total_count = conn.execute(f"SELECT COUNT(*) FROM {self.financial_table}").fetchone()[0]
                
                # 고유 종목 수
                unique_stocks = conn.execute(f"SELECT COUNT(DISTINCT stock_code) FROM {self.financial_table}").fetchone()[0]
                
                # 최신 업데이트 시간
                try:
                    latest_update = conn.execute(f"SELECT MAX(updated_at) FROM {self.financial_table}").fetchone()[0]
                except:
                    latest_update = "N/A"
                
                # 연도별 분포
                year_dist = conn.execute(f"""
                    SELECT year, COUNT(*) as count 
                    FROM {self.financial_table} 
                    GROUP BY year 
                    ORDER BY year DESC
                """).fetchall()
                
                return {
                    'table_name': self.financial_table,
                    'columns': columns,
                    'total_records': total_count,
                    'unique_stocks': unique_stocks,
                    'latest_update': latest_update,
                    'year_distribution': year_dist
                }
                
        except Exception as e:
            logger.error(f"테이블 정보 조회 실패: {e}")
            return {}
    
    def get_data_summary(self) -> Dict[str, Any]:
        """데이터 수집 현황 요약"""
        try:
            with sqlite3.connect(self.stock_db_path) as conn:
                # 1. 데이터 완전성 체크
                completeness = {}
                
                # 핵심 컬럼별 데이터 존재 비율
                key_columns = ['current_price', 'per', 'pbr', 'market_cap', 'eps', 'bps']
                
                for col in key_columns:
                    try:
                        non_null_count = conn.execute(f"""
                            SELECT COUNT(*) FROM {self.financial_table} 
                            WHERE {col} IS NOT NULL AND {col} > 0
                        """).fetchone()[0]
                        
                        total_count = conn.execute(f"SELECT COUNT(*) FROM {self.financial_table}").fetchone()[0]
                        
                        completeness[col] = {
                            'non_null': non_null_count,
                            'total': total_count,
                            'percentage': (non_null_count / total_count * 100) if total_count > 0 else 0
                        }
                    except sqlite3.OperationalError:
                        # 컬럼이 존재하지 않는 경우
                        completeness[col] = {'non_null': 0, 'total': 0, 'percentage': 0}
                
                # 2. PER/PBR 분포 분석
                per_stats = self._get_ratio_stats('per')
                pbr_stats = self._get_ratio_stats('pbr')
                
                # 3. 시가총액별 분포
                market_cap_dist = conn.execute(f"""
                    SELECT 
                        CASE 
                            WHEN market_cap >= 10000000000000 THEN '10조원 이상'
                            WHEN market_cap >= 1000000000000 THEN '1-10조원'
                            WHEN market_cap >= 100000000000 THEN '1000억-1조원'
                            WHEN market_cap >= 10000000000 THEN '100-1000억원'
                            WHEN market_cap > 0 THEN '100억원 미만'
                            ELSE '데이터 없음'
                        END as market_cap_range,
                        COUNT(*) as count
                    FROM {self.financial_table}
                    GROUP BY market_cap_range
                    ORDER BY 
                        CASE 
                            WHEN market_cap >= 10000000000000 THEN 1
                            WHEN market_cap >= 1000000000000 THEN 2
                            WHEN market_cap >= 100000000000 THEN 3
                            WHEN market_cap >= 10000000000 THEN 4
                            WHEN market_cap > 0 THEN 5
                            ELSE 6
                        END
                """).fetchall()
                
                # 4. 최근 업데이트된 종목들
                recent_updates = conn.execute(f"""
                    SELECT stock_code, company_name, current_price, per, pbr, updated_at
                    FROM {self.financial_table}
                    ORDER BY updated_at DESC
                    LIMIT 10
                """).fetchall()
                
                return {
                    'completeness': completeness,
                    'per_stats': per_stats,
                    'pbr_stats': pbr_stats,
                    'market_cap_distribution': market_cap_dist,
                    'recent_updates': recent_updates
                }
                
        except Exception as e:
            logger.error(f"데이터 요약 조회 실패: {e}")
            return {}
    
    def _get_ratio_stats(self, column: str) -> Dict[str, Any]:
        """특정 비율 컬럼의 통계 조회"""
        try:
            with sqlite3.connect(self.stock_db_path) as conn:
                stats = conn.execute(f"""
                    SELECT 
                        COUNT(*) as count,
                        AVG({column}) as avg_val,
                        MIN({column}) as min_val,
                        MAX({column}) as max_val,
                        COUNT(CASE WHEN {column} BETWEEN 5 AND 30 THEN 1 END) as reasonable_range
                    FROM {self.financial_table}
                    WHERE {column} IS NOT NULL AND {column} > 0 AND {column} < 1000
                """).fetchone()
                
                if stats[0] > 0:
                    return {
                        'count': stats[0],
                        'average': round(stats[1], 2),
                        'min': round(stats[2], 2),
                        'max': round(stats[3], 2),
                        'reasonable_count': stats[4],
                        'reasonable_ratio': round((stats[4] / stats[0]) * 100, 1)
                    }
                else:
                    return {'count': 0}
                    
        except Exception as e:
            logger.debug(f"{column} 통계 조회 실패: {e}")
            return {'count': 0}
    
    def get_stock_detail(self, stock_code: str) -> Dict[str, Any]:
        """특정 종목의 상세 정보 조회"""
        try:
            with sqlite3.connect(self.stock_db_path) as conn:
                # 기본 정보
                basic_info = conn.execute(f"""
                    SELECT * FROM {self.financial_table}
                    WHERE stock_code = ?
                    ORDER BY year DESC, quarter DESC
                """, (stock_code,)).fetchall()
                
                if not basic_info:
                    return {'error': f'종목 {stock_code}의 데이터를 찾을 수 없습니다.'}
                
                # 컬럼 이름 조회
                cursor = conn.execute(f"PRAGMA table_info({self.financial_table})")
                column_names = [row[1] for row in cursor.fetchall()]
                
                # 데이터를 딕셔너리로 변환
                stock_data = []
                for row in basic_info:
                    stock_record = dict(zip(column_names, row))
                    stock_data.append(stock_record)
                
                # stock_prices에서 최신 주가 정보 확인
                try:
                    latest_price_info = conn.execute("""
                        SELECT date, close_price, volume
                        FROM stock_prices 
                        WHERE stock_code = ?
                        ORDER BY date DESC
                        LIMIT 1
                    """, (stock_code,)).fetchone()
                except:
                    latest_price_info = None
                
                return {
                    'stock_code': stock_code,
                    'records_count': len(stock_data),
                    'financial_data': stock_data,
                    'latest_price_info': latest_price_info
                }
                
        except Exception as e:
            logger.error(f"종목 상세 정보 조회 실패 ({stock_code}): {e}")
            return {'error': str(e)}
    
    def get_missing_data_analysis(self) -> Dict[str, Any]:
        """누락 데이터 분석"""
        try:
            with sqlite3.connect(self.stock_db_path) as conn:
                # 1. stock_prices에는 있지만 financial_ratios에는 없는 종목들
                missing_in_financial = conn.execute(f"""
                    SELECT DISTINCT sp.stock_code
                    FROM stock_prices sp
                    LEFT JOIN {self.financial_table} fr ON sp.stock_code = fr.stock_code
                    WHERE fr.stock_code IS NULL
                    AND sp.stock_code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
                    ORDER BY sp.stock_code
                """).fetchall()
                
                # 2. 핵심 데이터가 누락된 종목들
                incomplete_stocks = conn.execute(f"""
                    SELECT stock_code, company_name,
                           CASE WHEN current_price IS NULL OR current_price = 0 THEN 'X' ELSE 'O' END as has_price,
                           CASE WHEN per IS NULL OR per = 0 THEN 'X' ELSE 'O' END as has_per,
                           CASE WHEN pbr IS NULL OR pbr = 0 THEN 'X' ELSE 'O' END as has_pbr,
                           CASE WHEN market_cap IS NULL OR market_cap = 0 THEN 'X' ELSE 'O' END as has_market_cap
                    FROM {self.financial_table}
                    WHERE (current_price IS NULL OR current_price = 0)
                       OR (per IS NULL OR per = 0)
                       OR (pbr IS NULL OR pbr = 0)
                       OR (market_cap IS NULL OR market_cap = 0)
                    ORDER BY stock_code
                """).fetchall()
                
                # 3. 비정상적인 값을 가진 종목들
                abnormal_stocks = conn.execute(f"""
                    SELECT stock_code, company_name, per, pbr, current_price
                    FROM {self.financial_table}
                    WHERE (per > 100 OR per < 0)
                       OR (pbr > 10 OR pbr < 0)
                       OR (current_price > 1000000 OR current_price < 100)
                    ORDER BY stock_code
                """).fetchall()
                
                return {
                    'missing_in_financial': [row[0] for row in missing_in_financial],
                    'incomplete_stocks': incomplete_stocks,
                    'abnormal_stocks': abnormal_stocks,
                    'summary': {
                        'missing_count': len(missing_in_financial),
                        'incomplete_count': len(incomplete_stocks),
                        'abnormal_count': len(abnormal_stocks)
                    }
                }
                
        except Exception as e:
            logger.error(f"누락 데이터 분석 실패: {e}")
            return {}
    
    def get_top_stocks_by_market_cap(self, limit: int = 20) -> List[Dict[str, Any]]:
        """시가총액 상위 종목들 조회"""
        try:
            with sqlite3.connect(self.stock_db_path) as conn:
                top_stocks = conn.execute(f"""
                    SELECT stock_code, company_name, market_cap, current_price, per, pbr, 
                           updated_at
                    FROM {self.financial_table}
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                    ORDER BY market_cap DESC
                    LIMIT ?
                """, (limit,)).fetchall()
                
                columns = ['stock_code', 'company_name', 'market_cap', 'current_price', 
                          'per', 'pbr', 'updated_at']
                
                return [dict(zip(columns, row)) for row in top_stocks]
                
        except Exception as e:
            logger.error(f"상위 종목 조회 실패: {e}")
            return []


def print_summary_report(checker: FinancialRatiosChecker):
    """전체 현황 요약 리포트 출력"""
    print("=" * 80)
    print("📊 Financial Ratios 테이블 수집 현황 리포트")
    print("=" * 80)
    
    # 기본 정보
    table_info = checker.get_table_info()
    if table_info:
        print(f"\n🗃️ 테이블 정보:")
        print(f"   테이블명: {table_info['table_name']}")
        print(f"   총 레코드: {table_info['total_records']:,}개")
        print(f"   고유 종목: {table_info['unique_stocks']:,}개")
        print(f"   마지막 업데이트: {table_info['latest_update']}")
        
        if table_info['year_distribution']:
            print(f"\n📅 연도별 분포:")
            for year, count in table_info['year_distribution']:
                print(f"   {year}년: {count:,}개")
    
    # 데이터 요약
    summary = checker.get_data_summary()
    if summary:
        print(f"\n📈 데이터 완전성:")
        for col, stats in summary['completeness'].items():
            if stats['total'] > 0:
                print(f"   {col}: {stats['non_null']:,}/{stats['total']:,} ({stats['percentage']:.1f}%)")
        
        if summary['per_stats']['count'] > 0:
            per_stats = summary['per_stats']
            print(f"\n📊 PER 통계 ({per_stats['count']:,}개 종목):")
            print(f"   평균: {per_stats['average']}")
            print(f"   범위: {per_stats['min']} ~ {per_stats['max']}")
            print(f"   적정범위(5-30): {per_stats['reasonable_count']:,}개 ({per_stats['reasonable_ratio']:.1f}%)")
        
        if summary['market_cap_distribution']:
            print(f"\n💰 시가총액 분포:")
            for range_name, count in summary['market_cap_distribution']:
                print(f"   {range_name}: {count:,}개")
        
        if summary['recent_updates']:
            print(f"\n🔄 최근 업데이트 종목:")
            for stock_code, name, price, per, pbr, updated in summary['recent_updates'][:5]:
                price_str = f"{price:,}원" if price else "N/A"
                per_str = f"{per:.1f}" if per else "N/A"
                pbr_str = f"{pbr:.1f}" if pbr else "N/A"
                print(f"   {name}({stock_code}): {price_str}, PER {per_str}, PBR {pbr_str}")


def print_missing_analysis(checker: FinancialRatiosChecker):
    """누락 데이터 분석 리포트 출력"""
    print("=" * 80)
    print("🔍 누락 데이터 분석 리포트")
    print("=" * 80)
    
    missing_analysis = checker.get_missing_data_analysis()
    if not missing_analysis:
        print("❌ 누락 데이터 분석을 수행할 수 없습니다.")
        return
    
    summary = missing_analysis['summary']
    
    print(f"\n📋 누락 데이터 요약:")
    print(f"   Financial Ratios에 없는 종목: {summary['missing_count']:,}개")
    print(f"   불완전한 데이터 종목: {summary['incomplete_count']:,}개")  
    print(f"   비정상적인 값 종목: {summary['abnormal_count']:,}개")
    
    # 누락 종목 일부 출력
    if missing_analysis['missing_in_financial']:
        print(f"\n❌ Financial Ratios에 없는 종목 (처음 20개):")
        for stock_code in missing_analysis['missing_in_financial'][:20]:
            print(f"   {stock_code}")
        
        if len(missing_analysis['missing_in_financial']) > 20:
            remaining = len(missing_analysis['missing_in_financial']) - 20
            print(f"   ... 외 {remaining}개 더")
    
    # 불완전한 데이터 종목
    if missing_analysis['incomplete_stocks']:
        print(f"\n⚠️ 불완전한 데이터 종목 (처음 10개):")
        print("   종목코드    회사명           가격  PER  PBR  시총")
        print("   " + "-" * 50)
        for stock_code, name, price, per, pbr, cap in missing_analysis['incomplete_stocks'][:10]:
            name_short = (name[:8] + "..") if name and len(name) > 10 else (name or "Unknown")
            print(f"   {stock_code}  {name_short:12} {price:4} {per:4} {pbr:4} {cap:4}")


def print_stock_detail(checker: FinancialRatiosChecker, stock_code: str):
    """특정 종목 상세 정보 출력"""
    print("=" * 80)
    print(f"🔍 종목 상세 정보: {stock_code}")
    print("=" * 80)
    
    detail = checker.get_stock_detail(stock_code)
    
    if 'error' in detail:
        print(f"❌ {detail['error']}")
        return
    
    print(f"\n📊 수집된 레코드: {detail['records_count']}개")
    
    if detail['latest_price_info']:
        date, price, volume = detail['latest_price_info']
        print(f"📈 최신 주가 정보: {price:,}원 ({date}), 거래량: {volume:,}")
    
    # 재무비율 데이터 출력
    if detail['financial_data']:
        print(f"\n💼 Financial Ratios 데이터:")
        
        for i, record in enumerate(detail['financial_data']):
            print(f"\n   📅 {record.get('year', 'N/A')}년 {record.get('quarter', 'N/A')}분기:")
            print(f"      회사명: {record.get('company_name', 'N/A')}")
            print(f"      현재가: {record.get('current_price', 0):,}원" if record.get('current_price') else "      현재가: N/A")
            print(f"      시가총액: {(record.get('market_cap', 0)/1000000000000):.1f}조원" if record.get('market_cap') else "      시가총액: N/A")
            print(f"      PER: {record.get('per', 0):.1f}" if record.get('per') else "      PER: N/A")
            print(f"      PBR: {record.get('pbr', 0):.1f}" if record.get('pbr') else "      PBR: N/A")
            print(f"      EPS: {record.get('eps', 0):,}원" if record.get('eps') else "      EPS: N/A")
            print(f"      BPS: {record.get('bps', 0):,}원" if record.get('bps') else "      BPS: N/A")
            
            # 52주 고저점 정보
            if record.get('week52_high') and record.get('week52_low'):
                print(f"      52주 고점: {record.get('week52_high', 0):,}원")
                print(f"      52주 저점: {record.get('week52_low', 0):,}원")
                print(f"      고점대비: {record.get('week52_high_ratio', 0):.1%}")
            
            print(f"      업데이트: {record.get('updated_at', 'N/A')}")
            
            if i >= 2:  # 최대 3개 레코드만 출력
                break


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='Financial Ratios 테이블 데이터 수집 상태 확인')
    parser.add_argument('--mode', choices=['summary', 'detail', 'missing', 'top'], 
                       default='summary', help='확인 모드')
    parser.add_argument('--stock_code', type=str, help='특정 종목 코드 (detail 모드)')
    parser.add_argument('--limit', type=int, default=20, help='상위 종목 수 (top 모드)')
    
    args = parser.parse_args()
    
    try:
        checker = FinancialRatiosChecker()
        
        if args.mode == 'summary':
            print_summary_report(checker)
        
        elif args.mode == 'detail':
            if not args.stock_code:
                print("❌ --stock_code 옵션이 필요합니다.")
                return False
            print_stock_detail(checker, args.stock_code)
        
        elif args.mode == 'missing':
            print_missing_analysis(checker)
        
        elif args.mode == 'top':
            print("=" * 80)
            print(f"💰 시가총액 상위 {args.limit}개 종목")
            print("=" * 80)
            
            top_stocks = checker.get_top_stocks_by_market_cap(args.limit)
            if top_stocks:
                print(f"\n순위  종목코드  회사명                시가총액      현재가     PER   PBR")
                print("-" * 80)
                for i, stock in enumerate(top_stocks, 1):
                    name = (stock['company_name'][:12] + "..") if len(stock.get('company_name', '')) > 15 else stock.get('company_name', 'Unknown')
                    market_cap_t = stock['market_cap'] / 1000000000000 if stock['market_cap'] else 0
                    price = stock['current_price'] if stock['current_price'] else 0
                    per = stock['per'] if stock['per'] else 0
                    pbr = stock['pbr'] if stock['pbr'] else 0
                    
                    print(f"{i:3d}  {stock['stock_code']}  {name:20} {market_cap_t:8.1f}조  {price:8,}원  {per:5.1f} {pbr:5.1f}")
        
        return True
        
    except Exception as e:
        logger.error(f"실행 실패: {e}")
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
