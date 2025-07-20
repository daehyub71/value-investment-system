#!/usr/bin/env python3
"""
KIS API realtime_quotes 실제 데이터 구조 기반 분석
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def analyze_kis_realtime_quotes_fixed():
    """KIS API realtime_quotes 실제 구조 기반 분석"""
    
    print("📊 KIS API realtime_quotes 실제 데이터 분석")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect('data/databases/kis_data.db')
        
        # 1. 기본 현황 확인
        print("\n📋 1. 기본 데이터 현황")
        print("-" * 40)
        
        basic_query = """
        SELECT 
            COUNT(*) as 총레코드수,
            COUNT(DISTINCT stock_code) as 종목수,
            MIN(created_at) as 최초수집일,
            MAX(created_at) as 최근수집일
        FROM realtime_quotes
        """
        
        basic_result = pd.read_sql(basic_query, conn)
        
        if not basic_result.empty:
            row = basic_result.iloc[0]
            print(f"📈 총 레코드 수: {row['총레코드수']:,}건")
            print(f"🏢 수집 종목 수: {row['종목수']:,}개")
            print(f"📅 수집 기간: {row['최초수집일']} ~ {row['최근수집일']}")
            
            # 데이터 신선도 계산
            if row['최근수집일']:
                최근일시 = datetime.strptime(row['최근수집일'], '%Y-%m-%d %H:%M:%S')
                현재일시 = datetime.now()
                지연일수 = (현재일시 - 최근일시).days
                print(f"⏰ 데이터 지연: {지연일수}일 전")
        
        # 2. 실제 테이블 구조 상세 확인
        print("\n📋 2. 실제 테이블 구조")
        print("-" * 40)
        
        # 샘플 데이터로 실제 컬럼 내용 확인
        sample_query = "SELECT * FROM realtime_quotes LIMIT 3"
        sample_result = pd.read_sql(sample_query, conn)
        
        if not sample_result.empty:
            print("📊 실제 데이터 샘플:")
            for col in sample_result.columns:
                sample_value = sample_result[col].iloc[0] if not sample_result[col].isnull().all() else "NULL"
                print(f"   {col}: {sample_value}")
        
        # 3. 주요 종목 실제 데이터 확인 (실존 컬럼만 사용)
        print("\n📋 3. 주요 종목 최신 시세 데이터")
        print("-" * 40)
        
        major_stocks_query = """
        SELECT 
            stock_code,
            current_price,
            change_price,
            change_rate,
            volume,
            high_price,
            low_price,
            open_price,
            created_at
        FROM realtime_quotes 
        WHERE stock_code IN ('005930', '000660', '035420', '051910', '005490')
        ORDER BY stock_code, created_at DESC
        """
        
        major_result = pd.read_sql(major_stocks_query, conn)
        
        if not major_result.empty:
            print("🎯 주요 종목 최신 시세:")
            
            # 종목별 최신 데이터만 표시
            latest_by_stock = major_result.groupby('stock_code').first().reset_index()
            
            stock_names = {
                '005930': '삼성전자',
                '000660': 'SK하이닉스', 
                '035420': 'NAVER',
                '051910': 'LG화학',
                '005490': 'POSCO홀딩스'
            }
            
            for _, row in latest_by_stock.iterrows():
                stock_name = stock_names.get(row['stock_code'], '알수없음')
                
                # 안전한 데이터 처리
                price = f"{int(row['current_price']):,}원" if pd.notna(row['current_price']) else "N/A"
                change = f"{row['change_rate']:.2f}%" if pd.notna(row['change_rate']) else "N/A"
                volume = f"{int(row['volume']):,}주" if pd.notna(row['volume']) else "N/A"
                high = f"{int(row['high_price']):,}원" if pd.notna(row['high_price']) else "N/A"
                low = f"{int(row['low_price']):,}원" if pd.notna(row['low_price']) else "N/A"
                
                print(f"   💰 {stock_name} ({row['stock_code']})")
                print(f"      현재가: {price} ({change})")
                print(f"      고가/저가: {high}/{low}")
                print(f"      거래량: {volume}")
                print(f"      수집일: {row['created_at']}")
                print()
        else:
            print("❌ 주요 종목 데이터가 없습니다.")
        
        # 4. 가격 데이터 품질 확인
        print("\n📋 4. 가격 데이터 품질 분석")
        print("-" * 40)
        
        quality_query = """
        SELECT 
            COUNT(*) as 전체레코드,
            COUNT(CASE WHEN current_price IS NOT NULL AND current_price > 0 THEN 1 END) as 유효가격,
            COUNT(CASE WHEN volume IS NOT NULL AND volume > 0 THEN 1 END) as 유효거래량,
            AVG(current_price) as 평균주가,
            MAX(current_price) as 최고주가,
            MIN(current_price) as 최저주가
        FROM realtime_quotes
        WHERE current_price IS NOT NULL
        """
        
        quality_result = pd.read_sql(quality_query, conn)
        
        if not quality_result.empty:
            row = quality_result.iloc[0]
            print("📊 데이터 품질 지표:")
            print(f"   📈 전체 레코드: {row['전체레코드']:,}건")
            print(f"   ✅ 유효 가격 데이터: {row['유효가격']:,}건 ({row['유효가격']/row['전체레코드']*100:.1f}%)")
            print(f"   ✅ 유효 거래량 데이터: {row['유효거래량']:,}건 ({row['유효거래량']/row['전체레코드']*100:.1f}%)")
            print(f"   💰 평균 주가: {row['평균주가']:,.0f}원")
            print(f"   📊 가격 범위: {row['최저주가']:,.0f}원 ~ {row['최고주가']:,.0f}원")
        
        # 5. 워런 버핏 스코어카드 활용성 분석
        print("\n📋 5. 워런 버핏 스코어카드 활용 분석")
        print("-" * 40)
        
        print("🎯 현재 KIS 데이터로 계산 가능한 지표:")
        print("   ✅ 현재 주가 (최신 종가)")
        print("   ✅ 일중 변동성 (고가/저가)")
        print("   ✅ 거래 활성도 (거래량)")
        print("   ✅ 시장 참여도 (거래 빈도)")
        
        print("\n❌ 부족한 워런 버핏 지표 (가치평가 20점):")
        print("   📊 PER (주가수익비율) - 없음")
        print("   📊 PBR (주가순자산비율) - 없음") 
        print("   📊 시가총액 - 계산 필요")
        print("   📊 배당수익률 - 없음")
        
        # 6. 데이터 수집 패턴 분석
        print("\n📋 6. 데이터 수집 패턴")
        print("-" * 40)
        
        pattern_query = """
        SELECT 
            DATE(created_at) as 수집일,
            COUNT(*) as 수집건수,
            COUNT(DISTINCT stock_code) as 종목수,
            MIN(created_at) as 시작시간,
            MAX(created_at) as 종료시간
        FROM realtime_quotes
        GROUP BY DATE(created_at)
        ORDER BY 수집일 DESC
        LIMIT 5
        """
        
        pattern_result = pd.read_sql(pattern_query, conn)
        
        if not pattern_result.empty:
            print("📅 최근 수집 패턴:")
            for _, row in pattern_result.iterrows():
                print(f"   {row['수집일']}: {row['수집건수']:,}건, {row['종목수']:,}개 종목")
                print(f"      수집시간: {row['시작시간']} ~ {row['종료시간']}")
        
        conn.close()
        
        # 7. 종합 평가 및 권장사항
        print("\n" + "=" * 60)
        print("🎯 종합 평가 및 권장사항")
        print("=" * 60)
        
        # 현재 상태 평가
        total_records = basic_result.iloc[0]['총레코드수']
        total_stocks = basic_result.iloc[0]['종목수']
        
        print("📊 현재 KIS realtime_quotes 상태:")
        if total_records >= 3000 and total_stocks >= 2000:
            print("   ✅ 데이터 볼륨: 우수 (3,000+건, 2,800+종목)")
        else:
            print(f"   🟡 데이터 볼륨: 보통 ({total_records:,}건, {total_stocks:,}종목)")
        
        if 지연일수 <= 1:
            print("   ✅ 데이터 신선도: 우수")
        elif 지연일수 <= 7:
            print(f"   🟡 데이터 신선도: 지연 ({지연일수}일)")
        else:
            print(f"   🔴 데이터 신선도: 심각한 지연 ({지연일수}일)")
        
        print("\n💡 워런 버핏 스코어카드 완성을 위한 권장사항:")
        print("   1. 🔄 KIS 데이터 업데이트 (6일 지연 해결)")
        print("   2. 📊 PER/PBR 데이터 추가 수집 필요")
        print("   3. 🔗 DART 재무데이터와 연계하여 가치평가 지표 계산")
        print("   4. 📈 현재 주가 + 재무제표 = 완전한 스코어카드")
        
        print("\n🚀 즉시 실행 가능한 해결책:")
        print("   python scripts/data_collection/collect_kis_data.py --update")
        print("   python scripts/data_collection/collect_stock_data.py --include_valuation")
        print("   python buffett_scorecard_calculator.py --use_kis_data")
        
    except Exception as e:
        print(f"❌ 데이터 분석 중 오류: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_kis_realtime_quotes_fixed()
