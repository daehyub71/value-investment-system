#!/usr/bin/env python3
"""
즉시 실행 가능한 워런 버핏 스코어카드 테스트
기존 데이터베이스 활용 버전
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

def check_existing_databases():
    """기존 데이터베이스 확인"""
    print("🔍 기존 데이터베이스 확인")
    print("=" * 50)
    
    db_dir = Path('data/databases')
    if not db_dir.exists():
        print("❌ 데이터베이스 디렉토리가 없습니다.")
        return False
    
    databases = {
        'dart_data.db': '13.33 MB',
        'stock_data.db': '250.62 MB', 
        'news_data.db': '186.55 MB',
        'buffett_scorecard.db': '1.11 MB'
    }
    
    available_dbs = []
    
    for db_name, size in databases.items():
        db_path = db_dir / db_name
        if db_path.exists():
            actual_size = db_path.stat().st_size / (1024*1024)
            print(f"✅ {db_name}: {actual_size:.2f} MB")
            available_dbs.append(db_name)
        else:
            print(f"❌ {db_name}: 없음")
    
    return len(available_dbs) > 0

def check_database_contents():
    """데이터베이스 내용 확인"""
    print("\n📊 데이터베이스 내용 확인")
    print("-" * 40)
    
    # 1. DART 데이터 확인
    dart_db = Path('data/databases/dart_data.db')
    if dart_db.exists():
        try:
            with sqlite3.connect(dart_db) as conn:
                # 테이블 목록
                tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
                tables = pd.read_sql_query(tables_query, conn)
                print(f"DART DB 테이블: {list(tables['name'])}")
                
                # 각 테이블 데이터 확인
                for table in tables['name']:
                    try:
                        count_query = f"SELECT COUNT(*) as count FROM {table}"
                        count = pd.read_sql_query(count_query, conn)
                        print(f"  • {table}: {count.iloc[0]['count']}건")
                        
                        # 삼성전자 데이터 있는지 확인
                        if 'samsung' in table.lower() or 'financial' in table.lower():
                            sample_query = f"SELECT * FROM {table} LIMIT 3"
                            sample = pd.read_sql_query(sample_query, conn)
                            if not sample.empty and 'corp_name' in sample.columns:
                                samsung_data = sample[sample['corp_name'].str.contains('삼성', na=False)]
                                if not samsung_data.empty:
                                    print(f"    ✅ 삼성 관련 데이터 발견!")
                    except:
                        continue
                        
        except Exception as e:
            print(f"DART DB 확인 실패: {e}")
    
    # 2. 주식 데이터 확인
    stock_db = Path('data/databases/stock_data.db')
    if stock_db.exists():
        try:
            with sqlite3.connect(stock_db) as conn:
                tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
                tables = pd.read_sql_query(tables_query, conn)
                print(f"\n주식 DB 테이블: {list(tables['name'])}")
                
                # 삼성전자 데이터 확인
                for table in tables['name']:
                    try:
                        if 'company' in table.lower():
                            samsung_query = f"SELECT * FROM {table} WHERE stock_code = '005930' OR corp_name LIKE '%삼성전자%'"
                            samsung_data = pd.read_sql_query(samsung_query, conn)
                            if not samsung_data.empty:
                                print(f"  ✅ {table}에서 삼성전자 데이터 발견: {len(samsung_data)}건")
                        
                        elif 'price' in table.lower():
                            samsung_query = f"SELECT COUNT(*) as count FROM {table} WHERE stock_code = '005930'"
                            count = pd.read_sql_query(samsung_query, conn)
                            if count.iloc[0]['count'] > 0:
                                print(f"  ✅ {table}에서 삼성전자 주가 데이터: {count.iloc[0]['count']}건")
                    except:
                        continue
                        
        except Exception as e:
            print(f"주식 DB 확인 실패: {e}")

def run_immediate_scorecard_test():
    """즉시 실행 가능한 스코어카드 테스트"""
    print("\n🚀 즉시 실행 가능한 워런 버핏 스코어카드 테스트")
    print("=" * 60)
    
    try:
        # ConfigManager 없이 직접 실행
        from buffett_scorecard_calculator_fixed import FixedBuffettScorecard
        
        print("✅ 수정된 BuffettScorecard 임포트 성공")
        
        scorecard = FixedBuffettScorecard()
        print("✅ BuffettScorecard 인스턴스 생성 성공")
        
        # 실제 데이터 기반 분석 실행
        result = scorecard.calculate_total_score_real_data()
        
        if result:
            print("\n🎉 워런 버핏 스코어카드 계산 성공!")
            print(f"📊 총점: {result['total_score']:.1f}점")
            print(f"📈 데이터 소스: {result.get('data_source', 'database')}")
            
            return True
        else:
            print("❌ 스코어카드 계산 실패")
            return False
            
    except ImportError as e:
        print(f"❌ 모듈 임포트 실패: {e}")
        return False
    except Exception as e:
        print(f"❌ 스코어카드 실행 실패: {e}")
        return False

def suggest_next_steps(scorecard_success):
    """다음 단계 제안"""
    print("\n💡 다음 단계 제안:")
    print("=" * 50)
    
    if scorecard_success:
        print("✅ 기본 시스템이 작동합니다!")
        print("\n🎯 개선 방안:")
        print("1. python scripts/data_collection/collect_dart_data_improved.py")
        print("   (개선된 DART 데이터 수집 시도)")
        print("2. streamlit run src/web/app.py")
        print("   (웹 인터페이스에서 결과 확인)")
        print("3. 기술분석 및 감정분석 모듈 개발")
        
    else:
        print("⚠️ 기본 문제 해결이 필요합니다.")
        print("\n🔧 문제 해결:")
        print("1. python test_fixed_config.py")
        print("   (ConfigManager 설정 확인)")
        print("2. 데이터베이스 파일 확인")
        print("3. Python 의존성 재설치:")
        print("   pip install pandas numpy sqlite3 python-dotenv")

def main():
    """메인 실행"""
    print("🎯 즉시 실행 가능한 워런 버핏 스코어카드 시스템 테스트")
    print("=" * 70)
    
    # 1. 기존 데이터베이스 확인
    db_available = check_existing_databases()
    
    if not db_available:
        print("\n❌ 사용 가능한 데이터베이스가 없습니다.")
        print("먼저 데이터 수집을 실행하세요.")
        return False
    
    # 2. 데이터베이스 내용 확인
    check_database_contents()
    
    # 3. 스코어카드 테스트
    scorecard_success = run_immediate_scorecard_test()
    
    # 4. 다음 단계 제안
    suggest_next_steps(scorecard_success)
    
    return scorecard_success

if __name__ == "__main__":
    success = main()
    
    if success:
        print("\n🎉 즉시 실행 테스트 성공!")
        print("Value Investment System의 핵심 기능이 작동합니다!")
    else:
        print("\n⚠️ 일부 문제가 있지만 기본 구조는 완성되었습니다.")
