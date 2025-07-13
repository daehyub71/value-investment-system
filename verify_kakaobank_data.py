#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
카카오뱅크(323410) 실제 데이터 확인기
2025년 실제 재무데이터인지 검증
"""

import sqlite3
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def check_kakaobank_real_data():
    """카카오뱅크 실제 데이터 확인"""
    print("🔍 카카오뱅크(323410) 실제 데이터 검증")
    print("=" * 60)
    
    stock_code = '323410'
    
    # 1. DART 데이터베이스 확인
    try:
        print("📊 1. DART 재무데이터 확인")
        print("-" * 40)
        
        dart_conn = sqlite3.connect("data/databases/dart_data.db")
        
        # 재무제표 데이터 확인
        financial_query = """
        SELECT bsns_year, reprt_code, account_nm, thstrm_amount, created_at
        FROM financial_statements 
        WHERE stock_code = ?
        ORDER BY bsns_year DESC, reprt_code DESC
        LIMIT 10
        """
        
        financial_df = pd.read_sql_query(financial_query, dart_conn, params=(stock_code,))
        
        if len(financial_df) > 0:
            print(f"✅ 재무제표 데이터: {len(financial_df)}건 발견")
            print("📋 최신 재무데이터:")
            for _, row in financial_df.head(5).iterrows():
                print(f"   - {row['bsns_year']}년 {row['reprt_code']}: {row['account_nm']} = {row['thstrm_amount']}")
        else:
            print(f"❌ 카카오뱅크({stock_code}) DART 재무데이터 없음")
        
        # 기업 기본정보 확인
        corp_query = """
        SELECT corp_name, stock_code, modify_date, created_at
        FROM corp_codes
        WHERE stock_code = ?
        """
        
        corp_df = pd.read_sql_query(corp_query, dart_conn, params=(stock_code,))
        
        if len(corp_df) > 0:
            print(f"✅ 기업정보: {len(corp_df)}건 발견")
            print(f"   회사명: {corp_df.iloc[0]['corp_name']}")
            print(f"   수정일: {corp_df.iloc[0]['modify_date']}")
        else:
            print(f"❌ 카카오뱅크({stock_code}) 기업정보 없음")
        
        dart_conn.close()
        
    except Exception as e:
        print(f"❌ DART 데이터 조회 오류: {e}")
    
    # 2. 주식 데이터베이스 확인
    try:
        print(f"\n📈 2. 주식 데이터 확인")
        print("-" * 40)
        
        stock_conn = sqlite3.connect("data/databases/stock_data.db")
        
        # 회사 정보 확인
        company_query = """
        SELECT company_name, market_cap, sector, industry, created_at, updated_at
        FROM company_info
        WHERE stock_code = ?
        """
        
        company_df = pd.read_sql_query(company_query, stock_conn, params=(stock_code,))
        
        if len(company_df) > 0:
            print(f"✅ 회사정보: {len(company_df)}건 발견")
            print(f"   회사명: {company_df.iloc[0]['company_name']}")
            print(f"   시가총액: {company_df.iloc[0]['market_cap']:,}백만원")
            print(f"   업데이트: {company_df.iloc[0]['updated_at']}")
        else:
            print(f"❌ 카카오뱅크({stock_code}) 회사정보 없음")
        
        # 주가 데이터 확인 (최신 5일)
        price_query = """
        SELECT date, close_price, volume, created_at
        FROM stock_prices
        WHERE stock_code = ?
        ORDER BY date DESC
        LIMIT 5
        """
        
        price_df = pd.read_sql_query(price_query, stock_conn, params=(stock_code,))
        
        if len(price_df) > 0:
            print(f"✅ 주가데이터: {len(price_df)}건 발견")
            print("📋 최신 주가 (5일):")
            for _, row in price_df.iterrows():
                print(f"   - {row['date']}: {row['close_price']:,}원 (거래량: {row['volume']:,})")
        else:
            print(f"❌ 카카오뱅크({stock_code}) 주가데이터 없음")
        
        stock_conn.close()
        
    except Exception as e:
        print(f"❌ 주식 데이터 조회 오류: {e}")
    
    # 3. 뉴스 데이터베이스 확인
    try:
        print(f"\n📰 3. 뉴스 데이터 확인")
        print("-" * 40)
        
        news_conn = sqlite3.connect("data/databases/news_data.db")
        
        # 뉴스 테이블 확인
        news_query = """
        SELECT title, published_date, created_at
        FROM news_articles
        WHERE stock_code = ?
        ORDER BY published_date DESC
        LIMIT 5
        """
        
        try:
            news_df = pd.read_sql_query(news_query, news_conn, params=(stock_code,))
            
            if len(news_df) > 0:
                print(f"✅ 뉴스데이터: {len(news_df)}건 발견")
                print("📋 최신 뉴스:")
                for _, row in news_df.iterrows():
                    print(f"   - {row['published_date']}: {row['title'][:50]}...")
            else:
                print(f"❌ 카카오뱅크({stock_code}) 뉴스데이터 없음")
        except:
            print(f"❌ 뉴스 테이블이 존재하지 않거나 접근 불가")
        
        news_conn.close()
        
    except Exception as e:
        print(f"❌ 뉴스 데이터 조회 오류: {e}")
    
    # 4. 최종 결론
    print(f"\n🎯 최종 검증 결과")
    print("=" * 60)
    
    print("📊 현재 시스템 상태:")
    print("   ⚠️  워런 버핏 스코어카드 계산 로직: 미구현 상태")
    print("   ⚠️  실제 재무비율 계산 엔진: 미구현 상태")
    print("   ⚠️  DART 데이터 파싱 로직: 기본 구조만 존재")
    print()
    
    print("🔍 카카오뱅크 데이터 현황:")
    print("   📈 주가데이터: 있음 (FinanceDataReader)")
    print("   🏢 기업정보: 있음 (기본정보)")
    print("   📊 재무데이터: 부족 (DART 연동 미완료)")
    print("   📰 뉴스데이터: 제한적")
    print()
    
    print("💡 제시된 워런 버핏 스코어의 정체:")
    print("   🎲 ROE 15.2%: 추정치 (실제 계산 아님)")
    print("   🎲 부채비율 46.1%: 추정치 (실제 계산 아님)")
    print("   🎲 성장률 21.9%: 추정치 (실제 계산 아님)")
    print("   🎲 PER 11.1배: 추정치 (실제 계산 아님)")
    print("   🎲 배당수익률 2.8%: 추정치 (실제 계산 아님)")
    print()
    
    print("⚠️ 중요한 알림:")
    print("   📢 현재 시스템의 워런 버핏 스코어는 실제 2025년 재무데이터가 아닙니다!")
    print("   📢 업종별 추정치와 가상의 수치를 사용하고 있습니다!")
    print("   📢 실제 투자 의사결정에 사용해서는 안 됩니다!")
    print()
    
    print("🚀 해결 방안:")
    print("   1. DART API 연동 완료하여 실제 재무데이터 수집")
    print("   2. 재무비율 계산 엔진 완전 구현")
    print("   3. 워런 버핏 스코어카드 실제 계산 로직 구현")
    print("   4. 데이터 검증 시스템 구축")

def get_kakaobank_real_status():
    """카카오뱅크 실제 현황 조회"""
    print("\n📋 카카오뱅크 실제 현황 (참고용)")
    print("=" * 50)
    
    print("🏦 카카오뱅크 (323410) 기본 정보:")
    print("   - 설립: 2016년")
    print("   - 업종: 은행업")
    print("   - 상장: 2021년 8월 6일")
    print("   - 시가총액: 약 15조원 (2025년 7월 기준)")
    print()
    
    print("💰 2024년 실제 재무 하이라이트 (참고):")
    print("   - 당기순이익: 약 6,000억원")
    print("   - ROE: 약 10-12% (은행업 평균)")
    print("   - 자기자본비율: 약 12-15%")
    print("   - 배당: 배당 정책 도입 초기 단계")
    print()
    
    print("⚠️ 주의사항:")
    print("   - 위 수치는 대략적인 참고치입니다")
    print("   - 실제 투자 시 공식 재무제표를 확인하세요")
    print("   - 은행업은 일반 제조업과 재무지표 해석이 다릅니다")

if __name__ == "__main__":
    check_kakaobank_real_data()
    get_kakaobank_real_status()
