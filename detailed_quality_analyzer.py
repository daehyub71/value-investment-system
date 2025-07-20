#!/usr/bin/env python3
"""
주가 데이터 품질 상세 분석 스크립트
98.84% 점수의 원인을 자세히 분석

실행 방법:
python detailed_quality_analyzer.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

def analyze_stock_quality_issues():
    """주가 데이터 품질 문제 상세 분석"""
    
    db_path = Path('data/databases/stock_data.db')
    
    if not db_path.exists():
        print("❌ stock_data.db 파일을 찾을 수 없습니다.")
        return
    
    print("🔍 주가 데이터 품질 문제 상세 분석")
    print("=" * 60)
    
    with sqlite3.connect(db_path) as conn:
        # 전체 데이터 통계
        total_query = "SELECT COUNT(*) as total FROM stock_prices"
        total_records = pd.read_sql(total_query, conn).iloc[0]['total']
        
        print(f"📊 총 데이터: {total_records:,}건")
        print()
        
        # 1. 가격 양수 검증
        print("1️⃣ 가격 양수 검증 (가중치: 25%)")
        negative_price_query = """
            SELECT 
                COUNT(*) as total_errors,
                COUNT(CASE WHEN open_price <= 0 THEN 1 END) as negative_open,
                COUNT(CASE WHEN high_price <= 0 THEN 1 END) as negative_high,
                COUNT(CASE WHEN low_price <= 0 THEN 1 END) as negative_low,
                COUNT(CASE WHEN close_price <= 0 THEN 1 END) as negative_close
            FROM stock_prices 
            WHERE open_price <= 0 OR high_price <= 0 OR low_price <= 0 OR close_price <= 0
        """
        
        price_result = pd.read_sql(negative_price_query, conn).iloc[0]
        error_rate = (price_result['total_errors'] / total_records) * 100
        score = max(0, 100 - error_rate)
        
        print(f"   오류 건수: {price_result['total_errors']:,}건 ({error_rate:.4f}%)")
        print(f"   - 시가 ≤ 0: {price_result['negative_open']:,}건")
        print(f"   - 고가 ≤ 0: {price_result['negative_high']:,}건") 
        print(f"   - 저가 ≤ 0: {price_result['negative_low']:,}건")
        print(f"   - 종가 ≤ 0: {price_result['negative_close']:,}건")
        print(f"   점수: {score:.2f}/100")
        print()
        
        # 2. 고가/저가 순서 검증
        print("2️⃣ 고가/저가 순서 검증 (가중치: 20%)")
        high_low_query = """
            SELECT COUNT(*) as errors FROM stock_prices WHERE high_price < low_price
        """
        
        high_low_errors = pd.read_sql(high_low_query, conn).iloc[0]['errors']
        error_rate = (high_low_errors / total_records) * 100
        score = max(0, 100 - error_rate)
        
        print(f"   오류 건수: {high_low_errors:,}건 ({error_rate:.4f}%)")
        print(f"   점수: {score:.2f}/100")
        print()
        
        # 3. OHLC 논리 검증
        print("3️⃣ OHLC 논리 검증 (가중치: 20%)")
        ohlc_query = """
            SELECT 
                COUNT(*) as total_errors,
                COUNT(CASE WHEN open_price NOT BETWEEN low_price AND high_price THEN 1 END) as invalid_open,
                COUNT(CASE WHEN close_price NOT BETWEEN low_price AND high_price THEN 1 END) as invalid_close
            FROM stock_prices 
            WHERE open_price NOT BETWEEN low_price AND high_price 
               OR close_price NOT BETWEEN low_price AND high_price
        """
        
        ohlc_result = pd.read_sql(ohlc_query, conn).iloc[0]
        error_rate = (ohlc_result['total_errors'] / total_records) * 100
        score = max(0, 100 - error_rate)
        
        print(f"   오류 건수: {ohlc_result['total_errors']:,}건 ({error_rate:.4f}%)")
        print(f"   - 시가 범위 오류: {ohlc_result['invalid_open']:,}건")
        print(f"   - 종가 범위 오류: {ohlc_result['invalid_close']:,}건")
        print(f"   점수: {score:.2f}/100")
        print()
        
        # 4. 거래량 검증
        print("4️⃣ 거래량 검증 (가중치: 15%)")
        volume_query = """
            SELECT 
                COUNT(CASE WHEN volume < 0 THEN 1 END) as negative_volume,
                COUNT(CASE WHEN volume = 0 THEN 1 END) as zero_volume,
                AVG(volume) as avg_volume,
                MAX(volume) as max_volume
            FROM stock_prices
        """
        
        volume_result = pd.read_sql(volume_query, conn).iloc[0]
        error_rate = (volume_result['negative_volume'] / total_records) * 100
        score = max(0, 100 - error_rate)
        
        print(f"   음수 거래량: {volume_result['negative_volume']:,}건 ({error_rate:.4f}%)")
        print(f"   거래량 0: {volume_result['zero_volume']:,}건")
        print(f"   평균 거래량: {int(volume_result['avg_volume']):,}주")
        print(f"   최대 거래량: {int(volume_result['max_volume']):,}주")
        print(f"   점수: {score:.2f}/100")
        print()
        
        # 5. 가격 변동성 검증  
        print("5️⃣ 가격 변동성 검증 (가중치: 15%)")
        variation_query = """
            WITH daily_changes AS (
                SELECT 
                    stock_code, date, close_price,
                    LAG(close_price) OVER (PARTITION BY stock_code ORDER BY date) as prev_close
                FROM stock_prices 
                ORDER BY stock_code, date
            ),
            change_rates AS (
                SELECT 
                    *,
                    CASE 
                        WHEN prev_close > 0 THEN 
                            ABS((close_price - prev_close) / prev_close) * 100 
                        ELSE NULL 
                    END as daily_change_pct
                FROM daily_changes
            )
            SELECT 
                COUNT(CASE WHEN daily_change_pct > 30 THEN 1 END) as extreme_changes,
                COUNT(CASE WHEN daily_change_pct IS NOT NULL THEN 1 END) as valid_comparisons,
                ROUND(AVG(daily_change_pct), 2) as avg_change,
                ROUND(MAX(daily_change_pct), 2) as max_change
            FROM change_rates
        """
        
        variation_result = pd.read_sql(variation_query, conn).iloc[0]
        valid_comparisons = variation_result['valid_comparisons']
        extreme_changes = variation_result['extreme_changes']
        
        if valid_comparisons > 0:
            error_rate = (extreme_changes / valid_comparisons) * 100
            score = max(0, 100 - error_rate) if error_rate > 1 else 100  # 1% 미만은 허용
        else:
            score = 100
        
        print(f"   극심한 변동(30%+): {extreme_changes:,}건 / {valid_comparisons:,}건")
        print(f"   평균 일일 변동률: {variation_result['avg_change']:.2f}%")
        print(f"   최대 일일 변동률: {variation_result['max_change']:.2f}%")
        print(f"   점수: {score:.2f}/100")
        print()
        
        # 6. 데이터 연속성 검증
        print("6️⃣ 데이터 연속성 검증 (가중치: 5%)")
        
        # 최근 30 영업일 체크
        end_date = datetime.now()
        start_date = end_date - timedelta(days=50)
        business_days = pd.date_range(start=start_date, end=end_date, freq='B')
        expected_dates = [d.strftime('%Y-%m-%d') for d in business_days[-30:]]
        
        missing_dates = []
        for date in expected_dates:
            count_query = "SELECT COUNT(DISTINCT stock_code) as count FROM stock_prices WHERE date = ?"
            count = pd.read_sql(count_query, conn, params=[date]).iloc[0]['count']
            if count == 0:
                missing_dates.append(date)
        
        continuity_score = (len(expected_dates) - len(missing_dates)) / len(expected_dates) * 100
        
        print(f"   누락 날짜: {len(missing_dates)}개 / {len(expected_dates)}개")
        if missing_dates:
            print(f"   누락된 날짜들: {missing_dates[:5]}")
        print(f"   연속성 점수: {continuity_score:.2f}%")
        print()
        
        # 종합 점수 계산
        print("🎯 종합 품질 점수 계산")
        print("=" * 60)
        
        weights = {
            'price_positive': 25,
            'high_low_order': 20, 
            'ohlc_logic': 20,
            'volume_check': 15,
            'price_variation': 15,
            'data_continuity': 5
        }
        
        # 각 점수를 다시 계산하여 표시
        weighted_scores = []
        
        # 실제 계산된 점수들 (위에서 계산한 것들을 재사용)
        scores = {
            'price_positive': max(0, 100 - (price_result['total_errors'] / total_records) * 100),
            'high_low_order': max(0, 100 - (high_low_errors / total_records) * 100),
            'ohlc_logic': max(0, 100 - (ohlc_result['total_errors'] / total_records) * 100),
            'volume_check': max(0, 100 - (volume_result['negative_volume'] / total_records) * 100),
            'price_variation': score,  # 위에서 계산된 변동성 점수
            'data_continuity': continuity_score
        }
        
        total_weighted_score = 0
        for rule, weight in weights.items():
            rule_score = scores[rule]
            weighted_score = rule_score * (weight / 100)
            weighted_scores.append(weighted_score)
            total_weighted_score += weighted_score
            
            print(f"{rule}: {rule_score:.2f}점 × {weight}% = {weighted_score:.2f}점")
        
        print("-" * 40)
        print(f"최종 품질 점수: {total_weighted_score:.2f}/100")
        
        # 품질 등급
        if total_weighted_score >= 95:
            grade = 'Excellent'
        elif total_weighted_score >= 90:
            grade = 'Very Good'
        elif total_weighted_score >= 80:
            grade = 'Good'
        else:
            grade = 'Fair'
        
        print(f"품질 등급: {grade}")

if __name__ == "__main__":
    analyze_stock_quality_issues()
