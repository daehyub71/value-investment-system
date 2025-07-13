#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Company Info 테이블 확인 프로그램 (수정 버전)
올바른 데이터베이스 경로를 사용하여 회사 정보 확인
"""

import sqlite3
import pandas as pd
from datetime import datetime
import os
from pathlib import Path

class CompanyInfoChecker:
    def __init__(self, db_path=None):
        """
        CompanyInfoChecker 초기화
        
        Args:
            db_path (str): SQLite 데이터베이스 파일 경로
        """
        # 데이터베이스 경로 자동 탐지
        if db_path:
            self.db_path = db_path
        else:
            # 가능한 데이터베이스 경로들
            possible_paths = [
                "data/databases/stock_data.db",
                "stock_data.db",
                "finance_data.db",
                "../data/databases/stock_data.db"
            ]
            
            self.db_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    self.db_path = path
                    break
            
            if not self.db_path:
                print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
                print("가능한 위치:")
                for path in possible_paths:
                    print(f"  • {path}")
                
                # 사용자 입력으로 경로 지정
                custom_path = input("\n데이터베이스 파일 경로를 직접 입력하세요: ").strip()
                if custom_path and os.path.exists(custom_path):
                    self.db_path = custom_path
                else:
                    raise FileNotFoundError("데이터베이스 파일을 찾을 수 없습니다.")
        
        self.conn = None
        print(f"📍 사용할 데이터베이스: {self.db_path}")
    
    def connect_db(self):
        """데이터베이스 연결"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            print(f"✅ 데이터베이스 연결 성공: {self.db_path}")
            return True
        except sqlite3.Error as e:
            print(f"❌ 데이터베이스 연결 실패: {e}")
            return False
    
    def close_db(self):
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            print("✅ 데이터베이스 연결 종료")
    
    def check_table_exists(self):
        """테이블 존재 여부 확인"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='company_info'
            """)
            result = cursor.fetchone()
            
            if result:
                print("✅ company_info 테이블이 존재합니다.")
                return True
            else:
                print("❌ company_info 테이블이 존재하지 않습니다.")
                # 사용 가능한 테이블 목록 표시
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table'
                """)
                tables = cursor.fetchall()
                if tables:
                    print("사용 가능한 테이블:")
                    for table in tables:
                        print(f"  • {table[0]}")
                return False
        except sqlite3.Error as e:
            print(f"❌ 테이블 확인 중 오류: {e}")
            return False
    
    def get_table_schema(self):
        """테이블 스키마 정보 조회"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA table_info(company_info)")
            columns = cursor.fetchall()
            
            print("\n" + "="*60)
            print("📋 COMPANY_INFO 테이블 스키마")
            print("="*60)
            print(f"{'컬럼명':<20} {'타입':<12} {'NOT NULL':<10} {'기본값':<15}")
            print("-" * 60)
            
            for col in columns:
                cid, name, col_type, notnull, default_val, pk = col
                not_null = "YES" if notnull else "NO"
                default = str(default_val) if default_val else ""
                pk_mark = " (PK)" if pk else ""
                print(f"{name + pk_mark:<20} {col_type:<12} {not_null:<10} {default:<15}")
            
            return columns
        except sqlite3.Error as e:
            print(f"❌ 스키마 조회 중 오류: {e}")
            return None
    
    def get_basic_stats(self):
        """기본 통계 정보 조회"""
        try:
            cursor = self.conn.cursor()
            
            # 총 레코드 수
            cursor.execute("SELECT COUNT(*) FROM company_info")
            total_count = cursor.fetchone()[0]
            
            # 시장별 분포
            cursor.execute("""
                SELECT market_type, COUNT(*) as count 
                FROM company_info 
                GROUP BY market_type 
                ORDER BY count DESC
            """)
            market_dist = cursor.fetchall()
            
            # 섹터별 분포 (상위 10개)
            cursor.execute("""
                SELECT sector, COUNT(*) as count 
                FROM company_info 
                WHERE sector IS NOT NULL AND sector != ''
                GROUP BY sector 
                ORDER BY count DESC 
                LIMIT 10
            """)
            sector_dist = cursor.fetchall()
            
            # 데이터 완성도 통계
            cursor.execute("SELECT COUNT(*) FROM company_info WHERE sector IS NOT NULL AND sector != ''")
            sector_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM company_info WHERE market_cap IS NOT NULL AND market_cap > 0")
            market_cap_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM company_info WHERE industry IS NOT NULL AND industry != ''")
            industry_count = cursor.fetchone()[0]
            
            print("\n" + "="*60)
            print("📊 COMPANY_INFO 테이블 통계")
            print("="*60)
            print(f"총 등록 기업 수: {total_count:,}개")
            
            print(f"\n📈 데이터 완성도:")
            if total_count > 0:
                print(f"  • 섹터 정보: {sector_count:,}개 ({sector_count/total_count*100:.1f}%)")
                print(f"  • 시총 정보: {market_cap_count:,}개 ({market_cap_count/total_count*100:.1f}%)")
                print(f"  • 업종 정보: {industry_count:,}개 ({industry_count/total_count*100:.1f}%)")
            
            print(f"\n📈 시장별 분포:")
            for market, count in market_dist:
                market_name = market if market else "미분류"
                print(f"  • {market_name}: {count:,}개")
            
            print(f"\n🏭 주요 섹터 분포 (상위 10개):")
            for sector, count in sector_dist:
                sector_name = sector if sector else "미분류"
                print(f"  • {sector_name}: {count:,}개")
            
            return {
                'total_count': total_count,
                'market_dist': market_dist,
                'sector_dist': sector_dist,
                'data_completeness': {
                    'sector': (sector_count, total_count),
                    'market_cap': (market_cap_count, total_count),
                    'industry': (industry_count, total_count)
                }
            }
        except sqlite3.Error as e:
            print(f"❌ 통계 조회 중 오류: {e}")
            return None
    
    def show_sample_data(self, limit=10):
        """샘플 데이터 조회"""
        try:
            query = f"""
                SELECT stock_code, company_name, market_type, sector, industry,
                       market_cap, listing_date
                FROM company_info 
                ORDER BY 
                    CASE WHEN market_cap IS NOT NULL THEN market_cap ELSE 0 END DESC,
                    company_name
                LIMIT {limit}
            """
            
            df = pd.read_sql_query(query, self.conn)
            
            print("\n" + "="*80)
            print(f"📋 샘플 데이터 (상위 {limit}개 기업)")
            print("="*80)
            
            if len(df) > 0:
                # 시가총액을 억원 단위로 변환
                df['시가총액(억원)'] = df['market_cap'].apply(
                    lambda x: f"{x//100000000:,}" if x and x > 0 else "N/A"
                )
                
                # 컬럼명 한글화
                df_display = df.rename(columns={
                    'stock_code': '종목코드',
                    'company_name': '회사명',
                    'market_type': '시장',
                    'sector': '섹터',
                    'industry': '업종',
                    'listing_date': '상장일'
                })
                
                # 표시할 컬럼 선택
                display_cols = ['종목코드', '회사명', '시장', '섹터', '업종', '시가총액(억원)']
                
                # NULL 값을 "N/A"로 표시
                for col in ['섹터', '업종', '시장']:
                    df_display[col] = df_display[col].fillna('N/A')
                    df_display[col] = df_display[col].replace('', 'N/A')
                
                print(df_display[display_cols].to_string(index=False))
            else:
                print("❌ 데이터가 없습니다.")
            
            return df
        except Exception as e:
            print(f"❌ 샘플 데이터 조회 중 오류: {e}")
            return None
    
    def search_company(self, keyword):
        """회사명으로 검색 (개선된 버전)"""
        try:
            query = """
                SELECT stock_code, company_name, market_type, sector, industry,
                       market_cap, listing_date
                FROM company_info 
                WHERE company_name LIKE ? OR stock_code LIKE ?
                ORDER BY 
                    CASE WHEN market_cap IS NOT NULL THEN market_cap ELSE 0 END DESC,
                    LENGTH(company_name),
                    company_name
            """
            
            search_term = f"%{keyword}%"
            df = pd.read_sql_query(query, self.conn, params=[search_term, search_term])
            
            print(f"\n🔍 '{keyword}' 검색 결과 ({len(df)}개)")
            print("-" * 70)
            
            if len(df) > 0:
                for _, row in df.iterrows():
                    # 시가총액 포맷팅
                    market_cap = f"{row['market_cap']//100000000:,}억원" if row['market_cap'] and row['market_cap'] > 0 else "N/A"
                    
                    # 섹터, 업종 정보 포맷팅
                    sector = row['sector'] if row['sector'] and row['sector'] != '' else "N/A"
                    industry = row['industry'] if row['industry'] and row['industry'] != '' else "N/A"
                    market_type = row['market_type'] if row['market_type'] and row['market_type'] != '' else "N/A"
                    
                    print(f"📈 {row['stock_code']} | {row['company_name']}")
                    print(f"   시장: {market_type} | 섹터: {sector} | 시총: {market_cap}")
                    if industry != "N/A":
                        print(f"   업종: {industry}")
                    print()
            else:
                print("❌ 검색 결과가 없습니다.")
                
                # 유사한 결과 찾기
                similar_query = """
                    SELECT stock_code, company_name 
                    FROM company_info 
                    WHERE company_name LIKE ? OR company_name LIKE ?
                    LIMIT 5
                """
                similar_df = pd.read_sql_query(similar_query, self.conn, 
                                             params=[f"%{keyword[0]}%", f"%{keyword[-1]}%"])
                
                if len(similar_df) > 0:
                    print("💡 비슷한 회사들:")
                    for _, row in similar_df.iterrows():
                        print(f"   • {row['stock_code']} {row['company_name']}")
            
            return df
        except Exception as e:
            print(f"❌ 검색 중 오류: {e}")
            return None
    
    def diagnose_data_issues(self):
        """데이터 문제점 진단"""
        print("\n🔍 데이터 품질 진단")
        print("=" * 50)
        
        try:
            cursor = self.conn.cursor()
            
            # 1. 섹터 정보 누락
            cursor.execute("SELECT COUNT(*) FROM company_info WHERE sector IS NULL OR sector = ''")
            missing_sector = cursor.fetchone()[0]
            
            # 2. 시총 정보 누락
            cursor.execute("SELECT COUNT(*) FROM company_info WHERE market_cap IS NULL OR market_cap <= 0")
            missing_market_cap = cursor.fetchone()[0]
            
            # 3. 전체 데이터 수
            cursor.execute("SELECT COUNT(*) FROM company_info")
            total_count = cursor.fetchone()[0]
            
            print(f"📊 데이터 누락 현황:")
            print(f"  • 섹터 정보 누락: {missing_sector:,}개 ({missing_sector/total_count*100:.1f}%)")
            print(f"  • 시총 정보 누락: {missing_market_cap:,}개 ({missing_market_cap/total_count*100:.1f}%)")
            
            # 4. 누락 데이터가 많은 회사들 (예시)
            cursor.execute("""
                SELECT stock_code, company_name, sector, market_cap
                FROM company_info 
                WHERE (sector IS NULL OR sector = '') 
                   OR (market_cap IS NULL OR market_cap <= 0)
                ORDER BY stock_code
                LIMIT 10
            """)
            
            incomplete_data = cursor.fetchall()
            
            if incomplete_data:
                print(f"\n❌ 정보가 누락된 회사들 (예시 10개):")
                for stock_code, company_name, sector, market_cap in incomplete_data:
                    issues = []
                    if not sector:
                        issues.append("섹터 누락")
                    if not market_cap or market_cap <= 0:
                        issues.append("시총 누락")
                    
                    print(f"  • {stock_code} {company_name}: {', '.join(issues)}")
            
            # 해결 방안 제시
            if missing_sector > 0 or missing_market_cap > 0:
                print(f"\n💡 해결 방안:")
                print(f"  1. DART API를 사용하여 누락된 정보 수집")
                print(f"  2. FinanceDataReader로 시가총액 정보 보완")
                print(f"  3. 수동으로 주요 종목 정보 입력")
                print(f"\n🔧 실행 명령:")
                print(f"  python company_data_fix.py  # 데이터 수집 스크립트 실행")
            
        except Exception as e:
            print(f"❌ 진단 중 오류: {e}")
    
    def run_full_check(self):
        """전체 점검 실행"""
        print("🚀 Company Info 테이블 전체 점검 시작")
        print("=" * 60)
        
        # 데이터베이스 연결
        if not self.connect_db():
            return
        
        # 테이블 존재 확인
        if not self.check_table_exists():
            self.close_db()
            return
        
        # 스키마 정보 조회
        self.get_table_schema()
        
        # 기본 통계 조회
        self.get_basic_stats()
        
        # 샘플 데이터 조회
        self.show_sample_data()
        
        # 데이터 품질 진단
        self.diagnose_data_issues()
        
        # 대화형 검색
        print(f"\n{'='*60}")
        print("🔍 대화형 검색 (종료하려면 'quit' 입력)")
        print("="*60)
        
        while True:
            keyword = input("\n검색할 회사명 또는 종목코드를 입력하세요: ").strip()
            if keyword.lower() in ['quit', 'exit', 'q']:
                break
            if keyword:
                self.search_company(keyword)
        
        # 연결 종료
        self.close_db()
        print("\n✅ 점검 완료!")


def main():
    """메인 함수"""
    print("💼 Finance Data Vibe - Company Info 테이블 점검 도구 (개선 버전)")
    print("=" * 70)
    
    try:
        # 점검 실행
        checker = CompanyInfoChecker()
        checker.run_full_check()
        
    except FileNotFoundError as e:
        print(f"❌ {e}")
        print("\n💡 해결 방법:")
        print("1. 데이터베이스가 생성되었는지 확인")
        print("2. 올바른 경로에서 스크립트 실행")
        print("3. 먼저 데이터 수집 스크립트 실행:")
        print("   python company_data_fix.py")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")


if __name__ == "__main__":
    main()