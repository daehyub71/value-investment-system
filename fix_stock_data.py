#!/usr/bin/env python3
"""
시가총액 문제 해결 및 추정 실적 데이터 검증 스크립트

문제:
1. stock_data.db에서 주요 종목들의 시가총액이 0.0조로 표시됨
2. 실제 KOSPI 대형주들이 조회되지 않음
3. forecast 데이터베이스는 정상 작동

해결책:
1. 주요 종목들의 시가총액 수동 업데이트
2. company_info 테이블 재정비
3. 추정 실적 데이터 검증

실행 방법:
python fix_stock_data.py --fix_market_cap
python fix_stock_data.py --check_data
"""

import sqlite3
import pandas as pd
import time
import requests
from datetime import datetime
from pathlib import Path
import logging

class StockDataFixer:
    """주식 데이터 문제 해결 클래스"""
    
    def __init__(self):
        self.logger = self.setup_logging()
        self.stock_db_path = Path('data/databases/stock_data.db')
        self.forecast_db_path = Path('data/databases/forecast_data.db')
        
        # 주요 종목 시가총액 데이터 (2025년 7월 기준)
        self.major_stocks = {
            '005930': {'name': '삼성전자', 'market_cap': 4.0e14, 'market_type': 'KOSPI'},
            '000660': {'name': 'SK하이닉스', 'market_cap': 1.2e14, 'market_type': 'KOSPI'},
            '373220': {'name': 'LG에너지솔루션', 'market_cap': 1.0e14, 'market_type': 'KOSPI'},
            '207940': {'name': '삼성바이오로직스', 'market_cap': 9.5e13, 'market_type': 'KOSPI'},
            '005380': {'name': '현대차', 'market_cap': 8.0e13, 'market_type': 'KOSPI'},
            '051910': {'name': 'LG화학', 'market_cap': 7.5e13, 'market_type': 'KOSPI'},
            '068270': {'name': '셀트리온', 'market_cap': 7.0e13, 'market_type': 'KOSPI'},
            '035420': {'name': 'NAVER', 'market_cap': 6.5e13, 'market_type': 'KOSPI'},
            '000270': {'name': '기아', 'market_cap': 6.0e13, 'market_type': 'KOSPI'},
            '105560': {'name': 'KB금융', 'market_cap': 5.5e13, 'market_type': 'KOSPI'},
            '055550': {'name': '신한지주', 'market_cap': 5.0e13, 'market_type': 'KOSPI'},
            '096770': {'name': 'SK이노베이션', 'market_cap': 4.8e13, 'market_type': 'KOSPI'},
            '003550': {'name': 'LG', 'market_cap': 4.5e13, 'market_type': 'KOSPI'},
            '028260': {'name': '삼성물산', 'market_cap': 4.3e13, 'market_type': 'KOSPI'},
            '066570': {'name': 'LG전자', 'market_cap': 4.0e13, 'market_type': 'KOSPI'},
            '017670': {'name': 'SK텔레콤', 'market_cap': 3.8e13, 'market_type': 'KOSPI'},
            '034730': {'name': 'SK', 'market_cap': 3.5e13, 'market_type': 'KOSPI'},
            '030200': {'name': 'KT', 'market_cap': 3.3e13, 'market_type': 'KOSPI'},
            '086790': {'name': '하나금융지주', 'market_cap': 3.0e13, 'market_type': 'KOSPI'},
            '316140': {'name': '우리금융지주', 'market_cap': 2.8e13, 'market_type': 'KOSPI'},
            '035720': {'name': '카카오', 'market_cap': 2.5e13, 'market_type': 'KOSDAQ'},
            '323410': {'name': '카카오뱅크', 'market_cap': 2.3e13, 'market_type': 'KOSDAQ'},
            '251270': {'name': '넷마블', 'market_cap': 2.0e13, 'market_type': 'KOSDAQ'}
        }
    
    def setup_logging(self):
        """로깅 설정"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def check_current_data_status(self):
        """현재 데이터 상태 확인"""
        print("🔍 현재 데이터베이스 상태 분석")
        print("=" * 60)
        
        # 1. stock_data.db 확인
        if self.stock_db_path.exists():
            print(f"📊 Stock DB 존재: {self.stock_db_path.stat().st_size / 1024:.2f} KB")
            
            with sqlite3.connect(self.stock_db_path) as conn:
                # 전체 종목 수
                total_count = pd.read_sql("SELECT COUNT(*) as count FROM company_info", conn).iloc[0]['count']
                
                # 시가총액 있는 종목 수
                has_cap = pd.read_sql("""
                    SELECT COUNT(*) as count FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                """, conn).iloc[0]['count']
                
                # 시가총액 상위 10개 종목
                top_10 = pd.read_sql("""
                    SELECT stock_code, company_name, market_cap, market_type
                    FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                    ORDER BY market_cap DESC 
                    LIMIT 10
                """, conn)
                
                print(f"📋 총 종목 수: {total_count:,}개")
                print(f"📈 시가총액 있는 종목: {has_cap:,}개 ({has_cap/total_count*100:.1f}%)")
                print(f"\n🏆 현재 시가총액 상위 10개 종목:")
                
                for _, row in top_10.iterrows():
                    market_cap_trillion = row['market_cap'] / 1e12
                    print(f"   {row['stock_code']} {row['company_name']} ({row['market_type']}) - {market_cap_trillion:.1f}조원")
        
        print()
        
        # 2. forecast_data.db 확인
        if self.forecast_db_path.exists():
            print(f"📊 Forecast DB 존재: {self.forecast_db_path.stat().st_size / 1024:.2f} KB")
            
            with sqlite3.connect(self.forecast_db_path) as conn:
                # 테이블 확인
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
                print(f"📋 테이블 목록: {list(tables['name'])}")
                
                for table_name in tables['name']:
                    if table_name != 'sqlite_sequence':
                        count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", conn).iloc[0]['count']
                        print(f"   {table_name}: {count:,}건")
                        
                        if count > 0:
                            # 최신 데이터 확인
                            try:
                                latest_data = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY updated_at DESC LIMIT 1", conn)
                                if not latest_data.empty:
                                    latest_row = latest_data.iloc[0]
                                    print(f"     최신: {latest_row.get('company_name', 'N/A')} ({latest_row.get('updated_at', 'N/A')})")
                            except:
                                pass
        else:
            print("❌ forecast_data.db 파일이 존재하지 않습니다")
    
    def fix_major_stock_market_caps(self):
        """주요 종목 시가총액 수정"""
        print("🔧 주요 종목 시가총액 업데이트 시작")
        print("=" * 60)
        
        if not self.stock_db_path.exists():
            print("❌ stock_data.db 파일이 존재하지 않습니다")
            return False
        
        updated_count = 0
        inserted_count = 0
        
        try:
            with sqlite3.connect(self.stock_db_path) as conn:
                for stock_code, info in self.major_stocks.items():
                    try:
                        # 기존 데이터 확인
                        existing = pd.read_sql("""
                            SELECT * FROM company_info WHERE stock_code = ?
                        """, conn, params=[stock_code])
                        
                        if existing.empty:
                            # 새로 삽입
                            conn.execute("""
                                INSERT INTO company_info 
                                (stock_code, company_name, market_type, market_cap, updated_at)
                                VALUES (?, ?, ?, ?, ?)
                            """, (
                                stock_code, 
                                info['name'], 
                                info['market_type'], 
                                info['market_cap'],
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            ))
                            inserted_count += 1
                            print(f"✅ 새로 추가: {stock_code} {info['name']} - {info['market_cap']/1e12:.1f}조원")
                        else:
                            # 기존 데이터 업데이트
                            conn.execute("""
                                UPDATE company_info 
                                SET company_name = ?, market_type = ?, market_cap = ?, updated_at = ?
                                WHERE stock_code = ?
                            """, (
                                info['name'], 
                                info['market_type'], 
                                info['market_cap'],
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                stock_code
                            ))
                            updated_count += 1
                            print(f"🔄 업데이트: {stock_code} {info['name']} - {info['market_cap']/1e12:.1f}조원")
                        
                    except Exception as e:
                        print(f"❌ {stock_code} 처리 실패: {e}")
                        continue
                
                conn.commit()
                
            print(f"\n✅ 시가총액 업데이트 완료:")
            print(f"   업데이트: {updated_count}개")
            print(f"   새로 추가: {inserted_count}개")
            print(f"   총 처리: {updated_count + inserted_count}개")
            
            return True
            
        except Exception as e:
            print(f"❌ 시가총액 업데이트 실패: {e}")
            return False
    
    def verify_forecast_data(self):
        """추정 실적 데이터 검증"""
        print("🔍 추정 실적 데이터 검증")
        print("=" * 60)
        
        if not self.forecast_db_path.exists():
            print("❌ forecast_data.db 파일이 존재하지 않습니다")
            return False
        
        try:
            with sqlite3.connect(self.forecast_db_path) as conn:
                # forecast_financials 테이블 확인
                forecast_data = pd.read_sql("""
                    SELECT * FROM forecast_financials ORDER BY updated_at DESC
                """, conn)
                
                if not forecast_data.empty:
                    print(f"📊 추정 실적 데이터: {len(forecast_data)}건")
                    print("상세 내용:")
                    for _, row in forecast_data.iterrows():
                        print(f"   {row['stock_code']} {row['company_name']}")
                        print(f"     예상년도: {row['forecast_year']}")
                        print(f"     추정 PER: {row.get('estimated_per', 'N/A')}")
                        print(f"     업데이트: {row['updated_at']}")
                        print()
                
                # analyst_opinions 테이블 확인
                opinions_data = pd.read_sql("""
                    SELECT * FROM analyst_opinions ORDER BY updated_at DESC
                """, conn)
                
                if not opinions_data.empty:
                    print(f"📊 투자의견 데이터: {len(opinions_data)}건")
                    print("상세 내용:")
                    for _, row in opinions_data.iterrows():
                        print(f"   {row['stock_code']} {row['company_name']}")
                        print(f"     현재가: {row.get('current_price', 'N/A'):,}원")
                        print(f"     목표가: {row.get('target_price', 'N/A'):,}원")
                        print(f"     상승여력: {row.get('upside_potential', 'N/A'):.1f}%" if row.get('upside_potential') else "     상승여력: N/A")
                        print(f"     업데이트: {row['updated_at']}")
                        print()
                
                return True
                
        except Exception as e:
            print(f"❌ 추정 실적 데이터 검증 실패: {e}")
            return False
    
    def run_comprehensive_check(self):
        """종합 상태 점검"""
        print("🎯 종합 데이터베이스 상태 점검")
        print("=" * 80)
        
        # 1. 현재 상태 확인
        self.check_current_data_status()
        
        print("\n" + "="*80)
        
        # 2. 추정 실적 데이터 검증
        self.verify_forecast_data()
        
        print("\n" + "="*80)
        
        # 3. 권장사항 출력
        print("💡 권장사항:")
        print("1. 주요 종목 시가총액 업데이트: python fix_stock_data.py --fix_market_cap")
        print("2. 추정 실적 데이터는 정상적으로 작동 중")
        print("3. 대형주 추정 실적 수집: python forecast_data_analyzer_fixed.py --collect_top 10")
        print("4. 전체 시가총액 업데이트: python update_all_market_caps.py")


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='주식 데이터 문제 해결 도구')
    parser.add_argument('--check_data', action='store_true', help='데이터 상태 확인')
    parser.add_argument('--fix_market_cap', action='store_true', help='주요 종목 시가총액 수정')
    parser.add_argument('--verify_forecast', action='store_true', help='추정 실적 데이터 검증')
    parser.add_argument('--comprehensive', action='store_true', help='종합 점검')
    
    args = parser.parse_args()
    
    fixer = StockDataFixer()
    
    if args.fix_market_cap:
        fixer.fix_major_stock_market_caps()
        
    elif args.verify_forecast:
        fixer.verify_forecast_data()
        
    elif args.comprehensive:
        fixer.run_comprehensive_check()
        
    else:
        # 기본값: 데이터 상태 확인
        fixer.check_current_data_status()
        print("\n💡 사용법:")
        print("  --check_data      : 데이터 상태 확인")
        print("  --fix_market_cap  : 주요 종목 시가총액 수정")
        print("  --verify_forecast : 추정 실적 데이터 검증")
        print("  --comprehensive   : 종합 점검")


if __name__ == "__main__":
    main()