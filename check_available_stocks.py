#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
데이터베이스 종목 목록 확인 및 워런 버핏 스코어카드 범용 시스템 구축
"""

import sqlite3
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def check_available_stocks():
    """데이터베이스에서 분석 가능한 종목 목록 조회"""
    print("🔍 데이터베이스 종목 현황 확인")
    print("=" * 60)
    
    try:
        # DART 데이터베이스에서 재무 데이터가 있는 종목들
        dart_conn = sqlite3.connect("data/databases/dart_data.db")
        
        # 재무 데이터가 있는 종목 수 확인
        financial_count_query = """
        SELECT COUNT(DISTINCT stock_code) as count
        FROM financial_statements
        WHERE stock_code IS NOT NULL AND stock_code != ''
        """
        financial_count = pd.read_sql_query(financial_count_query, dart_conn)
        
        # 회사 기본정보가 있는 종목들
        corp_query = """
        SELECT stock_code, corp_name
        FROM corp_codes 
        WHERE stock_code IS NOT NULL AND stock_code != ''
        ORDER BY corp_name
        LIMIT 20
        """
        corp_data = pd.read_sql_query(corp_query, dart_conn)
        dart_conn.close()
        
        # 주식 데이터베이스에서 주가 데이터가 있는 종목들
        stock_conn = sqlite3.connect("data/databases/stock_data.db")
        
        price_count_query = """
        SELECT COUNT(DISTINCT stock_code) as count
        FROM stock_prices
        WHERE stock_code IS NOT NULL AND stock_code != ''
        """
        price_count = pd.read_sql_query(price_count_query, stock_conn)
        
        # 회사 정보가 있는 종목들
        company_query = """
        SELECT stock_code, company_name
        FROM company_info
        WHERE stock_code IS NOT NULL AND stock_code != ''
        ORDER BY company_name
        LIMIT 20
        """
        company_data = pd.read_sql_query(company_query, stock_conn)
        stock_conn.close()
        
        print(f"📊 데이터 현황:")
        print(f"   - 재무데이터 보유 종목: {financial_count.iloc[0]['count']}개")
        print(f"   - 주가데이터 보유 종목: {price_count.iloc[0]['count']}개")
        print()
        
        print(f"📋 DART 기업 정보 샘플 (상위 20개):")
        for _, row in corp_data.head(10).iterrows():
            print(f"   {row['stock_code']}: {row['corp_name']}")
        print("   ...")
        print()
        
        print(f"📈 주식 데이터 샘플 (상위 20개):")
        for _, row in company_data.head(10).iterrows():
            print(f"   {row['stock_code']}: {row['company_name']}")
        print("   ...")
        
        return {
            'financial_stocks': financial_count.iloc[0]['count'],
            'price_stocks': price_count.iloc[0]['count'],
            'corp_sample': corp_data,
            'company_sample': company_data
        }
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return None

def get_major_stocks():
    """주요 대형주 종목 코드 리스트"""
    major_stocks = {
        '005930': '삼성전자',
        '000660': 'SK하이닉스',
        '035420': 'NAVER',
        '005380': '현대차',
        '051910': 'LG화학',
        '006400': '삼성SDI',
        '035720': '카카오',
        '068270': '셀트리온',
        '000270': '기아',
        '105560': 'KB금융',
        '055550': '신한지주',
        '096770': 'SK이노베이션',
        '017670': 'SK텔레콤',
        '030200': 'KT',
        '003670': '포스코홀딩스',
        '012330': '현대모비스',
        '323410': 'KAKAOPAY',
        '377300': '카카오페이',
        '207940': '삼성바이오로직스',
        '086790': '하나금융지주'
    }
    return major_stocks

if __name__ == "__main__":
    print("🚀 워런 버핏 스코어카드 범용 시스템 준비")
    print("=" * 60)
    
    # 1. 데이터베이스 종목 현황 확인
    stock_info = check_available_stocks()
    
    if stock_info:
        print(f"\n✅ 분석 준비 완료!")
        print(f"📊 총 {stock_info['financial_stocks']}개 종목의 재무데이터 보유")
        print(f"📈 총 {stock_info['price_stocks']}개 종목의 주가데이터 보유")
        
        # 2. 주요 종목 리스트 표시
        major_stocks = get_major_stocks()
        print(f"\n🏆 주요 대형주 워런 버핏 스코어카드 분석 가능 종목:")
        print("=" * 60)
        
        for code, name in major_stocks.items():
            print(f"   {code}: {name}")
        
        print(f"\n💡 다음 단계:")
        print("1. 특정 종목 분석: python buffett_universal_calculator.py --stock_code=종목코드")
        print("2. 여러 종목 일괄 분석: python buffett_batch_analyzer.py")
        print("3. 우량주 스크리닝: python buffett_screening_system.py")
        print("4. 순위표 생성: python buffett_ranking_system.py")
        
    else:
        print("❌ 데이터베이스 연결 실패")
