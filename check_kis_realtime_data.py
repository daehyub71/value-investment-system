#!/usr/bin/env python3
"""
KIS API realtime_quotes 데이터 상세 분석
실시간 시세 데이터 현황 및 활용 가능성 확인
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def analyze_kis_realtime_quotes():
    """KIS API realtime_quotes 테이블 상세 분석"""
    
    print("📊 KIS API realtime_quotes 데이터 분석")
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
        
        # 2. 테이블 구조 확인
        print("\n📋 2. 테이블 구조 분석")
        print("-" * 40)
        
        structure_query = "PRAGMA table_info(realtime_quotes)"
        structure_result = pd.read_sql(structure_query, conn)
        
        print("📊 컬럼 구조:")
        for _, col in structure_result.iterrows():
            print(f"   {col['name']} ({col['type']})")
        
        # 3. 주요 종목 데이터 확인
        print("\n📋 3. 주요 종목 최신 데이터")
        print("-" * 40)
        
        major_stocks_query = """
        SELECT 
            stock_code,
            current_price,
            change_price,
            change_rate,
            volume,
            market_cap,
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
                price = f"{int(row['current_price']):,}" if pd.notna(row['current_price']) else "N/A"
                change = f"{row['change_rate']:.2f}%" if pd.notna(row['change_rate']) else "N/A"
                print(f"   💰 {stock_name} ({row['stock_code']}): {price}원 ({change})")
                print(f"      📅 수집일: {row['created_at']}")
        else:
            print("❌ 주요 종목 데이터가 없습니다.")
        
        # 4. 시가총액 데이터 확인
        print("\n📋 4. 시가총액 데이터 활용성")
        print("-" * 40)
        
        market_cap_query = """
        SELECT 
            stock_code,
            market_cap,
            per,
            pbr,
            created_at
        FROM realtime_quotes 
        WHERE market_cap IS NOT NULL 
        ORDER BY market_cap DESC
        LIMIT 10
        """
        
        market_cap_result = pd.read_sql(market_cap_query, conn)
        
        if not market_cap_result.empty:
            print("🏆 시가총액 상위 종목:")
            for _, row in market_cap_result.iterrows():
                market_cap = f"{int(row['market_cap']/100000000):,}억" if pd.notna(row['market_cap']) else "N/A"
                per = f"{row['per']:.1f}배" if pd.notna(row['per']) else "N/A"
                pbr = f"{row['pbr']:.1f}배" if pd.notna(row['pbr']) else "N/A"
                print(f"   📊 {row['stock_code']}: 시총 {market_cap}, PER {per}, PBR {pbr}")
        else:
            print("❌ 시가총액 데이터가 없습니다.")
        
        # 5. 워런 버핏 스코어카드 활용 가능성
        print("\n📋 5. 워런 버핏 스코어카드 활용 분석")
        print("-" * 40)
        
        valuation_query = """
        SELECT 
            COUNT(*) as 전체종목,
            COUNT(CASE WHEN per IS NOT NULL THEN 1 END) as PER보유종목,
            COUNT(CASE WHEN pbr IS NOT NULL THEN 1 END) as PBR보유종목,
            COUNT(CASE WHEN market_cap IS NOT NULL THEN 1 END) as 시총보유종목,
            COUNT(CASE WHEN dividend_yield IS NOT NULL THEN 1 END) as 배당수익률보유종목
        FROM realtime_quotes
        """
        
        valuation_result = pd.read_sql(valuation_query, conn)
        
        if not valuation_result.empty:
            row = valuation_result.iloc[0]
            print("💰 가치평가 지표 보유 현황:")
            print(f"   📊 전체 종목: {row['전체종목']:,}개")
            print(f"   📈 PER 보유: {row['PER보유종목']:,}개 ({row['PER보유종목']/row['전체종목']*100:.1f}%)")
            print(f"   📈 PBR 보유: {row['PBR보유종목']:,}개 ({row['PBR보유종목']/row['전체종목']*100:.1f}%)")
            print(f"   📈 시가총액: {row['시총보유종목']:,}개 ({row['시총보유종목']/row['전체종목']*100:.1f}%)")
            print(f"   📈 배당수익률: {row['배당수익률보유종목']:,}개")
            
            # 워런 버핏 스코어카드에서 활용 가능한 지표 (20점 중)
            per_coverage = row['PER보유종목'] / row['전체종목'] * 100
            pbr_coverage = row['PBR보유종목'] / row['전체종목'] * 100
            
            if per_coverage > 80 and pbr_coverage > 80:
                print("\n✅ 가치평가 지표 (20점) 계산 가능!")
                print("   🎯 PER ≤ 15배, PBR 1-3배 조건 확인 가능")
            else:
                print(f"\n🟡 가치평가 지표 부분 활용 가능 (PER {per_coverage:.1f}%, PBR {pbr_coverage:.1f}%)")
        
        # 6. 데이터 업데이트 필요성
        print("\n📋 6. 데이터 업데이트 현황")
        print("-" * 40)
        
        if basic_result.iloc[0]['최근수집일']:
            최근일시 = datetime.strptime(basic_result.iloc[0]['최근수집일'], '%Y-%m-%d %H:%M:%S')
            현재일시 = datetime.now()
            지연일수 = (현재일시 - 최근일시).days
            
            if 지연일수 == 0:
                print("✅ 데이터가 최신 상태입니다!")
            elif 지연일수 <= 1:
                print("🟡 데이터가 1일 지연되었습니다. 업데이트 권장.")
            elif 지연일수 <= 7:
                print(f"🟠 데이터가 {지연일수}일 지연되었습니다. 업데이트 필요.")
            else:
                print(f"🔴 데이터가 {지연일수}일 지연되었습니다. 즉시 업데이트 필요!")
            
            print(f"\n💡 권장 조치:")
            if 지연일수 > 1:
                print("   1. KIS API 실시간 시세 업데이트 실행")
                print("   2. python scripts/data_collection/collect_kis_data.py --realtime")
                print("   3. 주가 데이터와 KIS 데이터 동기화")
        
        conn.close()
        
        # 7. 종합 결론
        print("\n" + "=" * 60)
        print("🎯 종합 결론")
        print("=" * 60)
        
        realtime_quotes_3k = basic_result.iloc[0]['총레코드수'] >= 3000
        has_valuation_data = valuation_result.iloc[0]['PER보유종목'] > 100
        is_recent = 지연일수 <= 7
        
        if realtime_quotes_3k and has_valuation_data and is_recent:
            print("✅ KIS realtime_quotes 데이터 활용 가능!")
            print("✅ 워런 버핏 스코어카드 가치평가 지표 (20점) 계산 지원")
            print("🚀 현재 데이터로 스코어카드 계산 가능")
        elif realtime_quotes_3k and has_valuation_data:
            print("🟡 KIS 데이터는 좋으나 업데이트 필요")
            print("🔄 최신 데이터 수집 후 활용 권장")
        else:
            print("🔴 KIS 데이터 보완 필요")
            print("📥 데이터 수집 및 품질 개선 필요")
        
        # realtime_quotes의 역할
        print(f"\n💡 realtime_quotes 데이터의 역할:")
        print(f"   📊 실시간 주가: 최신 종가, 변동률")
        print(f"   💰 가치평가: PER, PBR (워런 버핏 스코어 20점)")
        print(f"   📈 시가총액: 대형주/중소형주 구분")
        print(f"   🎯 보완 관계: DART 재무데이터 + KIS 시세데이터 = 완전한 분석")
        
    except Exception as e:
        print(f"❌ 데이터 분석 중 오류: {e}")

if __name__ == "__main__":
    analyze_kis_realtime_quotes()
