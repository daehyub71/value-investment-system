#!/usr/bin/env python3
"""
OHLC 종가 범위 이상치 분석 스크립트
종가가 고가-저가 범위를 벗어나는 353건의 데이터를 상세 분석

실행 방법:
python ohlc_anomaly_analyzer.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np

class OHLCAnomalyAnalyzer:
    """OHLC 이상치 분석 클래스"""
    
    def __init__(self):
        self.db_path = Path('data/databases/stock_data.db')
        
        if not self.db_path.exists():
            raise FileNotFoundError("stock_data.db 파일을 찾을 수 없습니다.")
    
    def analyze_close_price_anomalies(self):
        """종가 범위 이상치 상세 분석"""
        print("🔍 종가 범위 이상치 상세 분석")
        print("=" * 60)
        
        with sqlite3.connect(self.db_path) as conn:
            # 1. 종가 범위 오류 데이터 조회
            anomaly_query = """
                SELECT 
                    stock_code,
                    date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    CASE 
                        WHEN close_price > high_price THEN 'ABOVE_HIGH'
                        WHEN close_price < low_price THEN 'BELOW_LOW'
                        ELSE 'UNKNOWN'
                    END as anomaly_type,
                    CASE 
                        WHEN close_price > high_price THEN 
                            ROUND(((close_price - high_price) / high_price) * 100, 4)
                        WHEN close_price < low_price THEN 
                            ROUND(((low_price - close_price) / low_price) * 100, 4)
                        ELSE 0
                    END as deviation_pct
                FROM stock_prices 
                WHERE close_price NOT BETWEEN low_price AND high_price
                AND open_price > 0 AND high_price > 0 AND low_price > 0 AND close_price > 0
                ORDER BY deviation_pct DESC
            """
            
            anomalies = pd.read_sql(anomaly_query, conn)
            
            print(f"📊 총 이상치: {len(anomalies)}건")
            print()
            
            # 2. 이상치 유형별 분류
            print("1️⃣ 이상치 유형별 분류")
            type_distribution = anomalies['anomaly_type'].value_counts()
            
            for anomaly_type, count in type_distribution.items():
                percentage = (count / len(anomalies)) * 100
                print(f"   {anomaly_type}: {count}건 ({percentage:.1f}%)")
            print()
            
            # 3. 편차 크기별 분석
            print("2️⃣ 편차 크기별 분석")
            deviation_ranges = [
                (0, 0.1, "미세한 편차 (0.1% 이하)"),
                (0.1, 0.5, "작은 편차 (0.1~0.5%)"),
                (0.5, 1.0, "중간 편차 (0.5~1.0%)"),
                (1.0, 5.0, "큰 편차 (1.0~5.0%)"),
                (5.0, float('inf'), "극심한 편차 (5.0% 이상)")
            ]
            
            for min_dev, max_dev, description in deviation_ranges:
                if max_dev == float('inf'):
                    count = len(anomalies[anomalies['deviation_pct'] >= min_dev])
                else:
                    count = len(anomalies[(anomalies['deviation_pct'] >= min_dev) & 
                                        (anomalies['deviation_pct'] < max_dev)])
                
                if count > 0:
                    percentage = (count / len(anomalies)) * 100
                    print(f"   {description}: {count}건 ({percentage:.1f}%)")
            print()
            
            # 4. 상위 10개 극심한 케이스
            print("3️⃣ 극심한 편차 상위 10개 케이스")
            top_anomalies = anomalies.head(10)
            
            for idx, row in top_anomalies.iterrows():
                print(f"   #{idx+1}. {row['stock_code']} ({row['date']})")
                print(f"       시가: {row['open_price']:,}원")
                print(f"       고가: {row['high_price']:,}원")
                print(f"       저가: {row['low_price']:,}원")
                print(f"       종가: {row['close_price']:,}원")
                print(f"       거래량: {row['volume']:,}주")
                print(f"       유형: {row['anomaly_type']}")
                print(f"       편차: {row['deviation_pct']}%")
                print()
            
            # 5. 종목별 이상치 빈도
            print("4️⃣ 종목별 이상치 빈도 (상위 10개)")
            stock_frequency = anomalies['stock_code'].value_counts().head(10)
            
            for stock_code, count in stock_frequency.items():
                # 해당 종목의 회사명 조회
                company_query = "SELECT company_name FROM company_info WHERE stock_code = ?"
                company_result = pd.read_sql(company_query, conn, params=[stock_code])
                
                company_name = company_result.iloc[0]['company_name'] if not company_result.empty else "Unknown"
                
                print(f"   {stock_code} ({company_name}): {count}건")
            print()
            
            # 6. 날짜별 이상치 분포
            print("5️⃣ 최근 이상치 발생 날짜")
            recent_anomalies = anomalies.sort_values('date', ascending=False).head(10)
            
            for _, row in recent_anomalies.iterrows():
                print(f"   {row['date']}: {row['stock_code']} (편차: {row['deviation_pct']}%)")
            print()
            
            # 7. 통계적 분석
            print("6️⃣ 통계적 분석")
            print(f"   평균 편차: {anomalies['deviation_pct'].mean():.4f}%")
            print(f"   중간값 편차: {anomalies['deviation_pct'].median():.4f}%")
            print(f"   최대 편차: {anomalies['deviation_pct'].max():.4f}%")
            print(f"   표준편차: {anomalies['deviation_pct'].std():.4f}%")
            print()
            
            # 8. 거래량과의 상관관계
            print("7️⃣ 거래량 분석")
            
            # 이상치가 있는 날의 평균 거래량
            anomaly_avg_volume = anomalies['volume'].mean()
            
            # 전체 평균 거래량 (비교용)
            total_avg_query = "SELECT AVG(volume) as avg_vol FROM stock_prices WHERE volume > 0"
            total_avg_volume = pd.read_sql(total_avg_query, conn).iloc[0]['avg_vol']
            
            print(f"   이상치 발생일 평균 거래량: {anomaly_avg_volume:,.0f}주")
            print(f"   전체 평균 거래량: {total_avg_volume:,.0f}주")
            
            volume_ratio = anomaly_avg_volume / total_avg_volume
            print(f"   거래량 비율: {volume_ratio:.2f}배")
            
            if volume_ratio > 1.5:
                print("   → 이상치는 거래량이 많은 날에 주로 발생")
            elif volume_ratio < 0.7:
                print("   → 이상치는 거래량이 적은 날에 주로 발생")
            else:
                print("   → 이상치와 거래량 간 특별한 상관관계 없음")
            print()
            
            # 9. 결론 및 권장사항
            print("8️⃣ 분석 결론")
            print("=" * 40)
            
            # 미세한 편차 비율 계산
            minor_anomalies = len(anomalies[anomalies['deviation_pct'] <= 0.5])
            minor_percentage = (minor_anomalies / len(anomalies)) * 100
            
            if minor_percentage > 80:
                print("✅ 대부분(80%+)이 미세한 편차(0.5% 이하)")
                print("   → 시스템적 오류보다는 정상적인 시장 상황으로 추정")
                print("   → 시간외 거래, 단일가 매매 등이 원인일 가능성")
                recommendation = "데이터 유지 권장"
            elif minor_percentage > 50:
                print("⚠️ 절반 이상이 미세한 편차, 일부 큰 편차 존재")
                print("   → 대부분은 정상, 일부 검토 필요")
                recommendation = "큰 편차만 선별적 검토"
            else:
                print("❌ 상당수가 큰 편차를 보임")
                print("   → 데이터 품질 문제 가능성")
                recommendation = "전면적 데이터 검토 필요"
            
            print(f"\n💡 권장사항: {recommendation}")
            
            return anomalies
    
    def check_specific_anomaly(self, stock_code, date):
        """특정 이상치 케이스 상세 분석"""
        print(f"🔍 특정 이상치 상세 분석: {stock_code} ({date})")
        print("=" * 60)
        
        with sqlite3.connect(self.db_path) as conn:
            # 해당 종목의 전후 데이터 조회
            context_query = """
                SELECT 
                    date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    CASE 
                        WHEN close_price NOT BETWEEN low_price AND high_price THEN '⚠️'
                        ELSE '✅'
                    END as status
                FROM stock_prices 
                WHERE stock_code = ?
                AND date BETWEEN date(?, '-5 days') AND date(?, '+5 days')
                ORDER BY date
            """
            
            context_data = pd.read_sql(context_query, conn, params=[stock_code, date, date])
            
            print("전후 10일 데이터:")
            for _, row in context_data.iterrows():
                status_icon = "🎯" if row['date'] == date else row['status']
                print(f"   {status_icon} {row['date']}: "
                      f"시{row['open_price']:,} 고{row['high_price']:,} "
                      f"저{row['low_price']:,} 종{row['close_price']:,} "
                      f"거래량{row['volume']:,}")

def main():
    """메인 실행 함수"""
    try:
        analyzer = OHLCAnomalyAnalyzer()
        
        # 전체 이상치 분석
        anomalies = analyzer.analyze_close_price_anomalies()
        
        # 사용자가 특정 케이스를 보고 싶어할 경우를 대비한 예시
        if len(anomalies) > 0:
            print("\n" + "="*60)
            print("💡 특정 케이스 상세 분석 예시:")
            
            # 가장 큰 편차를 보인 케이스
            top_case = anomalies.iloc[0]
            print(f"   python -c \"")
            print(f"from ohlc_anomaly_analyzer import OHLCAnomalyAnalyzer;")
            print(f"analyzer = OHLCAnomalyAnalyzer();")
            print(f"analyzer.check_specific_anomaly('{top_case['stock_code']}', '{top_case['date']}')\"")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()
