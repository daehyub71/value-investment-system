#!/usr/bin/env python3
"""
주가 데이터 정제 스크립트
상장 전/거래정지 등으로 인한 잘못된 데이터를 정제하여 품질 개선

실행 방법:
python stock_data_cleaner.py --analyze  # 분석만
python stock_data_cleaner.py --clean    # 실제 정제
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse

class StockDataCleaner:
    """주가 데이터 정제 클래스"""
    
    def __init__(self):
        self.db_path = Path('data/databases/stock_data.db')
        
        if not self.db_path.exists():
            raise FileNotFoundError("stock_data.db 파일을 찾을 수 없습니다.")
    
    def analyze_problematic_data(self):
        """문제가 있는 데이터 분석"""
        print("🔍 문제 데이터 상세 분석")
        print("=" * 60)
        
        with sqlite3.connect(self.db_path) as conn:
            # 1. 가격이 0인 데이터 분석
            print("1️⃣ 가격이 0인 데이터 분석")
            zero_price_analysis = """
                SELECT 
                    stock_code,
                    COUNT(*) as zero_count,
                    MIN(date) as first_zero_date,
                    MAX(date) as last_zero_date,
                    (SELECT MIN(date) FROM stock_prices sp2 
                     WHERE sp2.stock_code = sp1.stock_code 
                     AND sp2.close_price > 0) as first_valid_date
                FROM stock_prices sp1
                WHERE open_price = 0 AND high_price = 0 AND low_price = 0
                GROUP BY stock_code
                ORDER BY zero_count DESC
                LIMIT 10
            """
            
            zero_analysis = pd.read_sql(zero_price_analysis, conn)
            
            print(f"   영향받는 종목 수: {len(zero_analysis)}개")
            print("   상위 10개 종목:")
            for _, row in zero_analysis.iterrows():
                print(f"     {row['stock_code']}: {row['zero_count']}건 "
                      f"({row['first_zero_date']} ~ {row['last_zero_date']})")
                if pd.notna(row['first_valid_date']):
                    print(f"       → 첫 유효 거래일: {row['first_valid_date']}")
            print()
            
            # 2. 상장일 정보와 대조
            print("2️⃣ 상장일 정보 대조")
            listing_comparison = """
                SELECT 
                    ci.stock_code,
                    ci.company_name,
                    ci.listing_date,
                    COUNT(sp.date) as zero_price_days,
                    MIN(sp.date) as first_zero,
                    MAX(sp.date) as last_zero
                FROM company_info ci
                JOIN stock_prices sp ON ci.stock_code = sp.stock_code
                WHERE sp.open_price = 0 AND sp.high_price = 0 AND sp.low_price = 0
                AND ci.listing_date IS NOT NULL
                GROUP BY ci.stock_code, ci.company_name, ci.listing_date
                ORDER BY zero_price_days DESC
                LIMIT 10
            """
            
            listing_comparison_result = pd.read_sql(listing_comparison, conn)
            
            print("   상장일 vs 0가격 데이터 비교:")
            for _, row in listing_comparison_result.iterrows():
                print(f"     {row['company_name']}({row['stock_code']})")
                print(f"       상장일: {row['listing_date']}")
                print(f"       0가격 기간: {row['first_zero']} ~ {row['last_zero']} ({row['zero_price_days']}일)")
                
                # 상장일과 0가격 데이터 관계 분석
                if pd.notna(row['listing_date']):
                    if row['first_zero'] < row['listing_date']:
                        print(f"       ✅ 상장 전 데이터로 추정")
                    else:
                        print(f"       ⚠️ 상장 후에도 0가격 존재")
                print()
            
            # 3. 정제 대상 데이터 통계
            print("3️⃣ 정제 대상 데이터 통계")
            cleanup_stats = """
                SELECT 
                    COUNT(*) as total_zero_records,
                    COUNT(DISTINCT stock_code) as affected_stocks,
                    COUNT(DISTINCT date) as affected_dates
                FROM stock_prices 
                WHERE open_price = 0 AND high_price = 0 AND low_price = 0
            """
            
            stats = pd.read_sql(cleanup_stats, conn).iloc[0]
            
            print(f"   정제 대상 레코드: {stats['total_zero_records']:,}건")
            print(f"   영향받는 종목: {stats['affected_stocks']:,}개")
            print(f"   영향받는 날짜: {stats['affected_dates']:,}일")
            print()
            
            # 4. 정제 후 예상 품질 점수
            total_records_query = "SELECT COUNT(*) as total FROM stock_prices"
            total_records = pd.read_sql(total_records_query, conn).iloc[0]['total']
            
            remaining_records = total_records - stats['total_zero_records']
            quality_improvement = (stats['total_zero_records'] / total_records) * 100
            
            print("4️⃣ 정제 후 예상 품질 개선")
            print(f"   현재 총 레코드: {total_records:,}건")
            print(f"   정제 후 레코드: {remaining_records:,}건")
            print(f"   제거 비율: {quality_improvement:.2f}%")
            
            # 예상 품질 점수 (가격 양수 검증이 100점이 되고, OHLC 논리도 개선)
            current_score = 98.84
            price_positive_improvement = (quality_improvement * 0.25)  # 25% 가중치
            ohlc_logic_improvement = (quality_improvement * 0.20)      # 20% 가중치
            
            expected_new_score = current_score + price_positive_improvement + ohlc_logic_improvement
            expected_new_score = min(100, expected_new_score)  # 100점 초과 방지
            
            print(f"   예상 품질 점수: {current_score:.2f} → {expected_new_score:.2f}")
            print()
            
            return stats
    
    def clean_invalid_data(self, dry_run=True):
        """잘못된 데이터 정제"""
        print("🧹 데이터 정제 작업")
        print("=" * 60)
        
        with sqlite3.connect(self.db_path) as conn:
            if dry_run:
                print("⚠️ DRY RUN 모드: 실제 삭제하지 않고 시뮬레이션만 수행")
                print()
            
            # 1. 정제할 데이터 식별
            problematic_query = """
                SELECT COUNT(*) as count
                FROM stock_prices 
                WHERE (open_price <= 0 OR high_price <= 0 OR low_price <= 0)
                AND close_price > 0
            """
            
            problematic_count = pd.read_sql(problematic_query, conn).iloc[0]['count']
            
            print(f"정제 대상: {problematic_count:,}건")
            
            # 2. 상장 전 데이터 vs 거래정지 데이터 구분
            pre_listing_query = """
                SELECT 
                    sp.stock_code,
                    COUNT(*) as records_to_delete
                FROM stock_prices sp
                LEFT JOIN company_info ci ON sp.stock_code = ci.stock_code
                WHERE (sp.open_price <= 0 OR sp.high_price <= 0 OR sp.low_price <= 0)
                AND sp.close_price > 0
                AND (ci.listing_date IS NULL OR sp.date < ci.listing_date)
                GROUP BY sp.stock_code
                ORDER BY records_to_delete DESC
            """
            
            pre_listing_data = pd.read_sql(pre_listing_query, conn)
            pre_listing_total = pre_listing_data['records_to_delete'].sum()
            
            print(f"  - 상장 전 데이터: {pre_listing_total:,}건 ({len(pre_listing_data)}개 종목)")
            
            # 3. 실제 정제 수행 (dry_run이 False인 경우)
            if not dry_run:
                print("\n🔥 실제 데이터 정제 시작...")
                
                # 백업 테이블 생성
                backup_query = """
                    CREATE TABLE IF NOT EXISTS stock_prices_backup AS 
                    SELECT * FROM stock_prices 
                    WHERE (open_price <= 0 OR high_price <= 0 OR low_price <= 0)
                    AND close_price > 0
                """
                
                conn.execute(backup_query)
                backup_count = conn.execute("SELECT COUNT(*) FROM stock_prices_backup").fetchone()[0]
                print(f"✅ 백업 테이블 생성: {backup_count:,}건 백업")
                
                # 문제 데이터 삭제
                delete_query = """
                    DELETE FROM stock_prices 
                    WHERE (open_price <= 0 OR high_price <= 0 OR low_price <= 0)
                    AND close_price > 0
                """
                
                cursor = conn.execute(delete_query)
                deleted_count = cursor.rowcount
                
                conn.commit()
                
                print(f"✅ 문제 데이터 삭제: {deleted_count:,}건")
                print("✅ 데이터 정제 완료!")
                
                # 정제 후 통계
                total_remaining = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
                print(f"📊 정제 후 총 레코드: {total_remaining:,}건")
                
            else:
                print("\n💡 실제 정제를 수행하려면 --clean 옵션을 사용하세요.")
                print("   예: python stock_data_cleaner.py --clean")
            
            return problematic_count
    
    def restore_backup(self):
        """백업에서 데이터 복원"""
        print("🔄 백업 데이터 복원")
        print("=" * 60)
        
        with sqlite3.connect(self.db_path) as conn:
            # 백업 테이블 존재 확인
            backup_exists = conn.execute("""
                SELECT COUNT(*) FROM sqlite_master 
                WHERE type='table' AND name='stock_prices_backup'
            """).fetchone()[0]
            
            if not backup_exists:
                print("❌ 백업 테이블이 존재하지 않습니다.")
                return False
            
            backup_count = conn.execute("SELECT COUNT(*) FROM stock_prices_backup").fetchone()[0]
            
            if backup_count == 0:
                print("❌ 백업 데이터가 없습니다.")
                return False
            
            print(f"📦 백업 데이터: {backup_count:,}건")
            
            # 백업 데이터 복원
            restore_query = "INSERT INTO stock_prices SELECT * FROM stock_prices_backup"
            conn.execute(restore_query)
            conn.commit()
            
            print(f"✅ {backup_count:,}건 복원 완료")
            
            # 백업 테이블 삭제
            conn.execute("DROP TABLE stock_prices_backup")
            conn.commit()
            
            print("✅ 백업 테이블 정리 완료")
            return True

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='주가 데이터 정제 스크립트')
    parser.add_argument('--analyze', action='store_true', help='문제 데이터 분석만 수행')
    parser.add_argument('--clean', action='store_true', help='실제 데이터 정제 수행')
    parser.add_argument('--restore', action='store_true', help='백업에서 데이터 복원')
    
    args = parser.parse_args()
    
    try:
        cleaner = StockDataCleaner()
        
        if args.restore:
            cleaner.restore_backup()
        elif args.clean:
            cleaner.analyze_problematic_data()
            cleaner.clean_invalid_data(dry_run=False)
        elif args.analyze:
            cleaner.analyze_problematic_data()
        else:
            # 기본값: 분석 + dry run
            cleaner.analyze_problematic_data()
            cleaner.clean_invalid_data(dry_run=True)
        
        print("\n💡 다음 단계:")
        print("1. 분석 결과 검토")
        print("2. 필요시 실제 정제: python stock_data_cleaner.py --clean")
        print("3. 정제 후 품질 재검증: python detailed_quality_analyzer.py")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()
