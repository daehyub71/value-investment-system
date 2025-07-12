#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Company Info 테이블 확인 프로그램
Finance Data Vibe 프로젝트용 SQLite 데이터베이스 점검 도구
"""

import sqlite3
import pandas as pd
from datetime import datetime
import os

class CompanyInfoChecker:
    def __init__(self, db_path="finance_data.db"):
        """
        CompanyInfoChecker 초기화
        
        Args:
            db_path (str): SQLite 데이터베이스 파일 경로
        """
        self.db_path = db_path
        self.conn = None
    
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
            
            # 섹터별 분포 (상위 5개)
            cursor.execute("""
                SELECT sector, COUNT(*) as count 
                FROM company_info 
                WHERE sector IS NOT NULL
                GROUP BY sector 
                ORDER BY count DESC 
                LIMIT 5
            """)
            sector_dist = cursor.fetchall()
            
            print("\n" + "="*60)
            print("📊 COMPANY_INFO 테이블 통계")
            print("="*60)
            print(f"총 등록 기업 수: {total_count:,}개")
            
            print(f"\n📈 시장별 분포:")
            for market, count in market_dist:
                market_name = market if market else "미분류"
                print(f"  • {market_name}: {count:,}개")
            
            print(f"\n🏭 주요 섹터 분포 (상위 5개):")
            for sector, count in sector_dist:
                sector_name = sector if sector else "미분류"
                print(f"  • {sector_name}: {count:,}개")
            
            return {
                'total_count': total_count,
                'market_dist': market_dist,
                'sector_dist': sector_dist
            }
        except sqlite3.Error as e:
            print(f"❌ 통계 조회 중 오류: {e}")
            return None
    
    def show_sample_data(self, limit=10):
        """샘플 데이터 조회"""
        try:
            query = f"""
                SELECT stock_code, company_name, market_type, sector, 
                       market_cap, listing_date
                FROM company_info 
                ORDER BY market_cap DESC 
                LIMIT {limit}
            """
            
            df = pd.read_sql_query(query, self.conn)
            
            print("\n" + "="*80)
            print(f"📋 샘플 데이터 (시가총액 상위 {limit}개 기업)")
            print("="*80)
            
            if len(df) > 0:
                # 시가총액을 억원 단위로 변환
                df['시가총액(억원)'] = df['market_cap'].apply(
                    lambda x: f"{x//100000000:,}" if x else "N/A"
                )
                
                # 컬럼명 한글화
                df_display = df.rename(columns={
                    'stock_code': '종목코드',
                    'company_name': '회사명',
                    'market_type': '시장',
                    'sector': '섹터',
                    'listing_date': '상장일'
                })
                
                # 표시할 컬럼 선택
                display_cols = ['종목코드', '회사명', '시장', '섹터', '시가총액(억원)', '상장일']
                print(df_display[display_cols].to_string(index=False))
            else:
                print("❌ 데이터가 없습니다.")
            
            return df
        except Exception as e:
            print(f"❌ 샘플 데이터 조회 중 오류: {e}")
            return None
    
    def search_company(self, keyword):
        """회사명으로 검색"""
        try:
            query = """
                SELECT stock_code, company_name, market_type, sector, 
                       market_cap, listing_date
                FROM company_info 
                WHERE company_name LIKE ? OR stock_code LIKE ?
                ORDER BY market_cap DESC
            """
            
            search_term = f"%{keyword}%"
            df = pd.read_sql_query(query, self.conn, params=[search_term, search_term])
            
            print(f"\n🔍 '{keyword}' 검색 결과 ({len(df)}개)")
            print("-" * 60)
            
            if len(df) > 0:
                for _, row in df.iterrows():
                    market_cap = f"{row['market_cap']//100000000:,}억원" if row['market_cap'] else "N/A"
                    print(f"📈 {row['stock_code']} | {row['company_name']}")
                    print(f"   시장: {row['market_type']} | 섹터: {row['sector']} | 시총: {market_cap}")
                    print()
            else:
                print("❌ 검색 결과가 없습니다.")
            
            return df
        except Exception as e:
            print(f"❌ 검색 중 오류: {e}")
            return None
    
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
    print("💼 Finance Data Vibe - Company Info 테이블 점검 도구")
    print("=" * 60)
    
    # 데이터베이스 파일 경로 확인
    db_files = ["finance_data.db", "data/finance_data.db", "../data/finance_data.db"]
    db_path = None
    
    for file_path in db_files:
        if os.path.exists(file_path):
            db_path = file_path
            break
    
    if not db_path:
        print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
        print("다음 위치에 있는지 확인하세요:")
        for file_path in db_files:
            print(f"  • {file_path}")
        
        # 사용자 입력으로 경로 지정
        custom_path = input("\n데이터베이스 파일 경로를 직접 입력하세요 (엔터: 종료): ").strip()
        if custom_path and os.path.exists(custom_path):
            db_path = custom_path
        else:
            print("❌ 프로그램을 종료합니다.")
            return
    
    # 점검 실행
    checker = CompanyInfoChecker(db_path)
    checker.run_full_check()


if __name__ == "__main__":
    main()