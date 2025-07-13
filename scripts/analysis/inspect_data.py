#!/usr/bin/env python3
"""
수집된 데이터 확인 및 분석 스크립트
DART, 주가, 뉴스 데이터 현황을 종합적으로 분석합니다.

사용법:
    python scripts/analysis/inspect_data.py --summary
    python scripts/analysis/inspect_data.py --detail --corp_code=00126380
    python scripts/analysis/inspect_data.py --export --table=corp_codes
"""

import sqlite3
import argparse
import pandas as pd
from pathlib import Path
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config import ConfigManager

class DataInspector:
    """데이터 수집 현황 분석기"""
    
    def __init__(self):
        """초기화"""
        self.config = ConfigManager()
        self.db_path = self.config.database_config.base_path
        
        print("🔍 데이터 검사기 초기화 완료")
        print(f"📂 데이터베이스 경로: {self.db_path}")
    
    def get_database_summary(self) -> Dict[str, Dict]:
        """모든 데이터베이스 요약 정보"""
        summary = {}
        
        db_files = {
            'dart_data': 'dart_data.db',
            'stock_data': 'stock_data.db', 
            'news_data': 'news_data.db',
            'kis_data': 'kis_data.db'
        }
        
        for db_name, db_file in db_files.items():
            db_path = self.db_path / db_file
            
            if not db_path.exists():
                summary[db_name] = {'status': 'NOT_FOUND', 'tables': {}}
                continue
            
            try:
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()
                    
                    # 테이블 목록 조회
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = cursor.fetchall()
                    
                    table_info = {}
                    for (table_name,) in tables:
                        # 각 테이블의 레코드 수 조회
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count = cursor.fetchone()[0]
                        
                        # 최신 데이터 조회 (created_at이 있는 경우)
                        try:
                            cursor.execute(f"SELECT MAX(created_at) FROM {table_name}")
                            latest = cursor.fetchone()[0]
                        except:
                            latest = None
                        
                        table_info[table_name] = {
                            'count': count,
                            'latest': latest
                        }
                    
                    summary[db_name] = {
                        'status': 'CONNECTED',
                        'tables': table_info,
                        'file_size': f"{db_path.stat().st_size / 1024 / 1024:.2f} MB"
                    }
                    
            except Exception as e:
                summary[db_name] = {
                    'status': 'ERROR',
                    'error': str(e),
                    'tables': {}
                }
        
        return summary
    
    def print_summary(self):
        """데이터베이스 요약 정보 출력"""
        print("\n" + "="*80)
        print("📊 데이터베이스 요약 현황")
        print("="*80)
        
        summary = self.get_database_summary()
        
        for db_name, info in summary.items():
            print(f"\n🗄️  {db_name.upper()}")
            print(f"   상태: {info['status']}")
            
            if info['status'] == 'CONNECTED':
                print(f"   파일 크기: {info['file_size']}")
                print(f"   테이블 수: {len(info['tables'])}")
                
                for table_name, table_info in info['tables'].items():
                    latest_str = table_info['latest'][:19] if table_info['latest'] else "N/A"
                    print(f"     📋 {table_name}: {table_info['count']:,}건 (최신: {latest_str})")
            
            elif info['status'] == 'ERROR':
                print(f"   ❌ 오류: {info['error']}")
            
            elif info['status'] == 'NOT_FOUND':
                print(f"   ❌ 파일을 찾을 수 없음")
    
    def get_dart_statistics(self) -> Dict:
        """DART 데이터 상세 통계"""
        dart_db = self.db_path / 'dart_data.db'
        
        if not dart_db.exists():
            return {'error': 'DART 데이터베이스를 찾을 수 없습니다.'}
        
        try:
            with sqlite3.connect(dart_db) as conn:
                stats = {}
                
                # 기업코드 통계
                df_corps = pd.read_sql("SELECT * FROM corp_codes", conn)
                stats['corp_codes'] = {
                    'total': len(df_corps),
                    'with_stock_code': len(df_corps[df_corps['stock_code'].notna()]),
                    'kospi_kosdaq': len(df_corps[df_corps['stock_code'].str.len() == 6])
                }
                
                # 재무제표 통계 (테이블이 존재하는 경우)
                try:
                    df_financials = pd.read_sql("SELECT corp_code, bsns_year, COUNT(*) as item_count FROM financial_statements GROUP BY corp_code, bsns_year", conn)
                    stats['financial_statements'] = {
                        'total_records': len(pd.read_sql("SELECT * FROM financial_statements", conn)),
                        'unique_companies': df_financials['corp_code'].nunique(),
                        'years_covered': sorted(df_financials['bsns_year'].unique().tolist()),
                        'avg_items_per_company': df_financials['item_count'].mean()
                    }
                except:
                    stats['financial_statements'] = {'error': '재무제표 테이블이 없습니다.'}
                
                # 공시정보 통계 (테이블이 존재하는 경우)
                try:
                    df_disclosures = pd.read_sql("SELECT * FROM disclosures", conn)
                    stats['disclosures'] = {
                        'total': len(df_disclosures),
                        'unique_companies': df_disclosures['corp_code'].nunique() if len(df_disclosures) > 0 else 0,
                        'recent_30days': len(df_disclosures[df_disclosures['rcept_dt'] >= (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')]) if len(df_disclosures) > 0 else 0
                    }
                except:
                    stats['disclosures'] = {'error': '공시정보 테이블이 없습니다.'}
                
                return stats
                
        except Exception as e:
            return {'error': f'데이터베이스 연결 오류: {e}'}
    
    def print_dart_statistics(self):
        """DART 데이터 상세 통계 출력"""
        print("\n" + "="*80)
        print("📈 DART 데이터 상세 분석")
        print("="*80)
        
        stats = self.get_dart_statistics()
        
        if 'error' in stats:
            print(f"❌ 오류: {stats['error']}")
            return
        
        # 기업코드 통계
        corp_stats = stats['corp_codes']
        print(f"\n🏢 기업코드 현황:")
        print(f"   총 기업 수: {corp_stats['total']:,}개")
        print(f"   주식코드 보유: {corp_stats['with_stock_code']:,}개")
        print(f"   상장기업(추정): {corp_stats['kospi_kosdaq']:,}개")
        
        # 재무제표 통계
        if 'error' not in stats['financial_statements']:
            fin_stats = stats['financial_statements']
            print(f"\n📊 재무제표 현황:")
            print(f"   총 레코드: {fin_stats['total_records']:,}건")
            print(f"   데이터 보유 기업: {fin_stats['unique_companies']:,}개")
            print(f"   수집 연도: {fin_stats['years_covered']}")
            print(f"   기업당 평균 항목: {fin_stats['avg_items_per_company']:.1f}개")
        else:
            print(f"\n📊 재무제표: {stats['financial_statements']['error']}")
        
        # 공시정보 통계
        if 'error' not in stats['disclosures']:
            disc_stats = stats['disclosures']
            print(f"\n📋 공시정보 현황:")
            print(f"   총 공시: {disc_stats['total']:,}건")
            print(f"   공시 기업: {disc_stats['unique_companies']:,}개")
            print(f"   최근 30일: {disc_stats['recent_30days']:,}건")
        else:
            print(f"\n📋 공시정보: {stats['disclosures']['error']}")
    
    def get_company_detail(self, corp_code: str) -> Dict:
        """특정 기업의 상세 데이터 조회"""
        dart_db = self.db_path / 'dart_data.db'
        
        if not dart_db.exists():
            return {'error': 'DART 데이터베이스를 찾을 수 없습니다.'}
        
        try:
            with sqlite3.connect(dart_db) as conn:
                result = {}
                
                # 기업 기본정보
                corp_info = pd.read_sql(
                    "SELECT * FROM corp_codes WHERE corp_code = ?", 
                    conn, params=[corp_code]
                )
                
                if len(corp_info) == 0:
                    return {'error': f'기업코드 {corp_code}를 찾을 수 없습니다.'}
                
                result['corp_info'] = corp_info.iloc[0].to_dict()
                
                # 재무제표 데이터
                try:
                    financials = pd.read_sql(
                        "SELECT * FROM financial_statements WHERE corp_code = ? ORDER BY bsns_year DESC, account_nm", 
                        conn, params=[corp_code]
                    )
                    result['financials'] = {
                        'count': len(financials),
                        'years': sorted(financials['bsns_year'].unique().tolist(), reverse=True),
                        'sample': financials.head(10).to_dict('records') if len(financials) > 0 else []
                    }
                except:
                    result['financials'] = {'error': '재무제표 데이터 없음'}
                
                # 공시정보
                try:
                    disclosures = pd.read_sql(
                        "SELECT * FROM disclosures WHERE corp_code = ? ORDER BY rcept_dt DESC", 
                        conn, params=[corp_code]
                    )
                    result['disclosures'] = {
                        'count': len(disclosures),
                        'recent': disclosures.head(5).to_dict('records') if len(disclosures) > 0 else []
                    }
                except:
                    result['disclosures'] = {'error': '공시정보 데이터 없음'}
                
                return result
                
        except Exception as e:
            return {'error': f'데이터베이스 연결 오류: {e}'}
    
    def print_company_detail(self, corp_code: str):
        """특정 기업 상세 정보 출력"""
        print(f"\n" + "="*80)
        print(f"🏢 기업 상세 정보: {corp_code}")
        print("="*80)
        
        detail = self.get_company_detail(corp_code)
        
        if 'error' in detail:
            print(f"❌ 오류: {detail['error']}")
            return
        
        # 기업 기본정보
        corp = detail['corp_info']
        print(f"\n📋 기업 기본정보:")
        print(f"   기업명: {corp.get('corp_name', 'N/A')}")
        print(f"   기업코드: {corp.get('corp_code', 'N/A')}")
        print(f"   주식코드: {corp.get('stock_code', 'N/A')}")
        print(f"   수정일: {corp.get('modify_date', 'N/A')}")
        
        # 재무제표 정보
        if 'error' not in detail['financials']:
            fin = detail['financials']
            print(f"\n📊 재무제표 데이터:")
            print(f"   총 항목: {fin['count']:,}개")
            print(f"   수집 연도: {fin['years']}")
            
            if fin['sample']:
                print(f"\n   최근 재무항목 (상위 10개):")
                for item in fin['sample'][:5]:
                    print(f"     • {item.get('account_nm', 'N/A')}: {item.get('thstrm_amount', 'N/A'):,}")
        else:
            print(f"\n📊 재무제표: {detail['financials']['error']}")
        
        # 공시정보
        if 'error' not in detail['disclosures']:
            disc = detail['disclosures']
            print(f"\n📋 공시정보:")
            print(f"   총 공시: {disc['count']:,}건")
            
            if disc['recent']:
                print(f"\n   최근 공시 (상위 5개):")
                for item in disc['recent']:
                    print(f"     • [{item.get('rcept_dt', 'N/A')}] {item.get('report_nm', 'N/A')}")
        else:
            print(f"\n📋 공시정보: {detail['disclosures']['error']}")
    
    def export_to_csv(self, table_name: str, output_dir: str = "exports"):
        """데이터를 CSV로 내보내기"""
        print(f"\n📤 {table_name} 테이블 CSV 내보내기 시작...")
        
        # 내보내기 디렉토리 생성
        export_path = Path(output_dir)
        export_path.mkdir(exist_ok=True)
        
        # 데이터베이스 매핑
        db_mapping = {
            'corp_codes': 'dart_data.db',
            'financial_statements': 'dart_data.db',
            'disclosures': 'dart_data.db',
            'stock_prices': 'stock_data.db',
            'news_articles': 'news_data.db'
        }
        
        if table_name not in db_mapping:
            print(f"❌ 지원하지 않는 테이블: {table_name}")
            print(f"지원 테이블: {list(db_mapping.keys())}")
            return
        
        db_file = db_mapping[table_name]
        db_path = self.db_path / db_file
        
        if not db_path.exists():
            print(f"❌ 데이터베이스 파일을 찾을 수 없음: {db_file}")
            return
        
        try:
            with sqlite3.connect(db_path) as conn:
                # 테이블 존재 확인
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                if not cursor.fetchone():
                    print(f"❌ 테이블을 찾을 수 없음: {table_name}")
                    return
                
                # 데이터 조회 및 CSV 저장
                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                
                if len(df) == 0:
                    print(f"⚠️ 테이블 {table_name}에 데이터가 없습니다.")
                    return
                
                # 파일명 생성
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{table_name}_{timestamp}.csv"
                filepath = export_path / filename
                
                # CSV 저장
                df.to_csv(filepath, index=False, encoding='utf-8-sig')
                
                print(f"✅ 내보내기 완료!")
                print(f"   파일: {filepath}")
                print(f"   레코드 수: {len(df):,}건")
                print(f"   파일 크기: {filepath.stat().st_size / 1024 / 1024:.2f} MB")
                
        except Exception as e:
            print(f"❌ 내보내기 실패: {e}")
    
    def search_companies(self, keyword: str, limit: int = 10) -> List[Dict]:
        """기업명으로 검색"""
        dart_db = self.db_path / 'dart_data.db'
        
        if not dart_db.exists():
            print("❌ DART 데이터베이스를 찾을 수 없습니다.")
            return []
        
        try:
            with sqlite3.connect(dart_db) as conn:
                query = """
                SELECT corp_code, corp_name, stock_code, modify_date 
                FROM corp_codes 
                WHERE corp_name LIKE ? 
                ORDER BY corp_name 
                LIMIT ?
                """
                df = pd.read_sql(query, conn, params=[f'%{keyword}%', limit])
                return df.to_dict('records')
                
        except Exception as e:
            print(f"❌ 검색 실패: {e}")
            return []
    
    def print_search_results(self, keyword: str, limit: int = 10):
        """기업 검색 결과 출력"""
        print(f"\n🔍 기업 검색: '{keyword}'")
        print("-" * 80)
        
        results = self.search_companies(keyword, limit)
        
        if not results:
            print("❌ 검색 결과가 없습니다.")
            return
        
        print(f"📋 검색 결과 ({len(results)}개):")
        
        for i, company in enumerate(results, 1):
            stock_code = company.get('stock_code') or 'N/A'
            print(f"   {i:2d}. {company['corp_name']}")
            print(f"       기업코드: {company['corp_code']} | 주식코드: {stock_code}")
        
        print(f"\n💡 상세 조회: python scripts/analysis/inspect_data.py --detail --corp_code=<기업코드>")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='수집된 데이터 확인 및 분석')
    
    parser.add_argument('--summary', action='store_true', help='전체 데이터베이스 요약 출력')
    parser.add_argument('--dart', action='store_true', help='DART 데이터 상세 통계')
    parser.add_argument('--detail', action='store_true', help='특정 기업 상세 정보')
    parser.add_argument('--corp_code', type=str, help='조회할 기업코드 (8자리)')
    parser.add_argument('--search', type=str, help='기업명 검색 키워드')
    parser.add_argument('--export', action='store_true', help='데이터 CSV 내보내기')
    parser.add_argument('--table', type=str, help='내보낼 테이블명')
    parser.add_argument('--output', type=str, default='exports', help='내보내기 디렉토리 (기본값: exports)')
    
    args = parser.parse_args()
    
    # 인스펙터 초기화
    inspector = DataInspector()
    
    # 옵션에 따른 실행
    if args.summary:
        inspector.print_summary()
    
    if args.dart:
        inspector.print_dart_statistics()
    
    if args.detail and args.corp_code:
        inspector.print_company_detail(args.corp_code)
    elif args.detail and not args.corp_code:
        print("❌ --detail 옵션을 사용할 때는 --corp_code를 함께 지정해야 합니다.")
        print("예시: --detail --corp_code=00126380")
    
    if args.search:
        inspector.print_search_results(args.search)
    
    if args.export and args.table:
        inspector.export_to_csv(args.table, args.output)
    elif args.export and not args.table:
        print("❌ --export 옵션을 사용할 때는 --table을 함께 지정해야 합니다.")
        print("지원 테이블: corp_codes, financial_statements, disclosures, stock_prices, news_articles")
    
    # 기본 동작: 아무 옵션이 없으면 요약 출력
    if not any([args.summary, args.dart, args.detail, args.search, args.export]):
        inspector.print_summary()
        
        # 기본 통계도 함께 출력
        print("\n💡 상세 정보를 보려면:")
        print("   python scripts/analysis/inspect_data.py --dart")
        print("   python scripts/analysis/inspect_data.py --search 삼성")
        print("   python scripts/analysis/inspect_data.py --detail --corp_code=00126380")

if __name__ == "__main__":
    main()
