#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
워런 버핏 분석 결과에서 실제 거래 가능한 종목 필터링
상장폐지/거래정지 종목 제외하고 실제 투자 가능한 종목만 추출
"""

import sqlite3
import pandas as pd
import requests
import time
from datetime import datetime

def check_stock_status_naver(stock_code):
    """네이버 증권 API로 종목 상태 확인"""
    try:
        # 네이버 증권 API (실제 거래되는 종목만 응답)
        url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{stock_code}"
        
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            data = response.json()
            if data and 'datas' in data and len(data['datas']) > 0:
                return True  # 정상 거래 종목
        return False  # 상장폐지 또는 거래정지
        
    except Exception as e:
        return None  # 확인 불가

def check_major_stocks_realtime():
    """주요 종목들의 실시간 거래 상태 확인"""
    print("🔍 주요 종목 실시간 거래 상태 확인")
    print("=" * 60)
    
    # 테스트할 주요 종목들
    test_stocks = [
        ('005930', '삼성전자'),
        ('000660', 'SK하이닉스'),
        ('035420', 'NAVER'),
        ('035720', '카카오'),
        ('045880', '유티엑스'),  # 의심 종목
        ('014900', '에스컴'),    # 의심 종목
        ('012205', '계양전기우'),
        ('098460', '고영'),
    ]
    
    active_stocks = []
    inactive_stocks = []
    
    for stock_code, company_name in test_stocks:
        print(f"📊 확인 중: {company_name} ({stock_code})")
        
        status = check_stock_status_naver(stock_code)
        
        if status == True:
            active_stocks.append((stock_code, company_name, "✅ 정상거래"))
            print(f"   ✅ 정상거래 중")
        elif status == False:
            inactive_stocks.append((stock_code, company_name, "❌ 거래중단"))
            print(f"   ❌ 거래중단/상장폐지")
        else:
            inactive_stocks.append((stock_code, company_name, "❓ 확인불가"))
            print(f"   ❓ 상태 확인 불가")
        
        time.sleep(0.5)  # API 호출 간격
    
    print(f"\n📈 정상 거래 종목: {len(active_stocks)}개")
    for stock_code, name, status in active_stocks:
        print(f"   {stock_code}: {name}")
    
    print(f"\n❌ 거래 중단 종목: {len(inactive_stocks)}개")  
    for stock_code, name, status in inactive_stocks:
        print(f"   {stock_code}: {name} ({status})")
    
    return active_stocks, inactive_stocks

def filter_active_stocks_from_results():
    """분석 결과에서 실제 거래 가능한 종목만 필터링"""
    print("\n🔍 워런 버핏 분석 결과에서 활성 종목 필터링")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect("data/databases/buffett_scorecard.db")
        
        # Top 50 종목 조회
        query = """
        SELECT stock_code, company_name, total_score, grade, investment_grade
        FROM buffett_all_stocks_final 
        ORDER BY total_score DESC 
        LIMIT 50
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"📊 Top 50 종목 중 실제 거래 가능 종목 확인...")
        
        active_top_stocks = []
        inactive_count = 0
        
        for idx, row in df.iterrows():
            stock_code = row['stock_code']
            company_name = row['company_name']
            
            print(f"   ({idx+1}/50) {company_name} ({stock_code}) 확인 중...")
            
            status = check_stock_status_naver(stock_code)
            
            if status == True:
                active_top_stocks.append({
                    'rank': len(active_top_stocks) + 1,
                    'stock_code': stock_code,
                    'company_name': company_name,
                    'total_score': row['total_score'],
                    'grade': row['grade'],
                    'investment_grade': row['investment_grade']
                })
                print(f"      ✅ 정상거래")
            else:
                inactive_count += 1
                print(f"      ❌ 거래중단")
            
            time.sleep(0.3)  # API 호출 간격
            
            # 활성 종목 20개 찾으면 중단
            if len(active_top_stocks) >= 20:
                break
        
        print(f"\n🏆 실제 투자 가능한 Top 20 종목:")
        print("=" * 80)
        for stock in active_top_stocks:
            print(f"   {stock['rank']:2d}. {stock['company_name']} ({stock['stock_code']}): "
                  f"{stock['total_score']:.1f}점, {stock['grade']}, {stock['investment_grade']}")
        
        print(f"\n📊 필터링 결과:")
        print(f"   - 활성 종목: {len(active_top_stocks)}개")
        print(f"   - 비활성 종목: {inactive_count}개")
        
        # 결과 저장
        if active_top_stocks:
            active_df = pd.DataFrame(active_top_stocks)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            output_file = f"results/buffett_analysis/buffett_active_top20_{timestamp}.csv"
            active_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"💾 활성 Top 20 종목 저장: {output_file}")
        
        return active_top_stocks
        
    except Exception as e:
        print(f"❌ 필터링 실패: {e}")
        return []

def get_major_market_stocks():
    """주요 대형주 중 확실히 거래되는 종목들"""
    major_stocks = [
        ('005930', '삼성전자'),
        ('000660', 'SK하이닉스'), 
        ('035420', 'NAVER'),
        ('035720', '카카오'),
        ('005380', '현대차'),
        ('051910', 'LG화학'),
        ('006400', '삼성SDI'),
        ('068270', '셀트리온'),
        ('000270', '기아'),
        ('105560', 'KB금융'),
        ('055550', '신한지주'),
        ('096770', 'SK이노베이션'),
        ('017670', 'SK텔레콤'),
        ('030200', 'KT'),
        ('003670', '포스코홀딩스'),
        ('012330', '현대모비스'),
        ('207940', '삼성바이오로직스'),
        ('086790', '하나금융지주'),
        ('028260', '삼성물산'),
        ('066570', 'LG전자')
    ]
    
    print("\n🏢 주요 대형주의 워런 버핏 점수 확인")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect("data/databases/buffett_scorecard.db")
        
        major_results = []
        
        for stock_code, company_name in major_stocks:
            query = """
            SELECT stock_code, company_name, total_score, grade, investment_grade,
                   profitability_score, growth_score, stability_score
            FROM buffett_all_stocks_final 
            WHERE stock_code = ?
            """
            
            result = pd.read_sql_query(query, conn, params=(stock_code,))
            
            if not result.empty:
                row = result.iloc[0]
                major_results.append({
                    'stock_code': stock_code,
                    'company_name': company_name,
                    'total_score': row['total_score'],
                    'grade': row['grade'],
                    'investment_grade': row['investment_grade'],
                    'profitability_score': row['profitability_score'],
                    'growth_score': row['growth_score'],
                    'stability_score': row['stability_score']
                })
        
        conn.close()
        
        # 점수순 정렬
        major_results.sort(key=lambda x: x['total_score'], reverse=True)
        
        print(f"📊 주요 대형주 워런 버핏 스코어 (Top 15):")
        for i, stock in enumerate(major_results[:15], 1):
            print(f"   {i:2d}. {stock['company_name']} ({stock['stock_code']}): "
                  f"{stock['total_score']:.1f}점, {stock['grade']}, {stock['investment_grade']}")
            print(f"       수익성: {stock['profitability_score']:.1f}, "
                  f"성장성: {stock['growth_score']:.1f}, "
                  f"안정성: {stock['stability_score']:.1f}")
        
        # 결과 저장
        if major_results:
            major_df = pd.DataFrame(major_results)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            output_file = f"results/buffett_analysis/buffett_major_stocks_{timestamp}.csv"
            major_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"\n💾 주요 대형주 분석 결과 저장: {output_file}")
        
        return major_results
        
    except Exception as e:
        print(f"❌ 주요 종목 조회 실패: {e}")
        return []

def main():
    """메인 실행"""
    print("🔍 워런 버핏 분석 결과 실제 투자 가능 종목 필터링")
    print("=" * 80)
    
    # 1. 주요 종목 거래 상태 확인
    active_stocks, inactive_stocks = check_major_stocks_realtime()
    
    # 2. Top 50에서 활성 종목 필터링
    active_top_stocks = filter_active_stocks_from_results()
    
    # 3. 주요 대형주 점수 확인
    major_stock_scores = get_major_market_stocks()
    
    print("\n" + "=" * 80)
    print("🎯 결론 및 추천")
    print("=" * 80)
    
    if inactive_stocks:
        print(f"❌ 상장폐지/거래정지 의심 종목: {len(inactive_stocks)}개")
        print("   → 실제 투자 시 반드시 거래 가능 여부 확인 필요")
    
    if active_top_stocks:
        print(f"\n✅ 실제 투자 가능한 우수 종목: {len(active_top_stocks)}개")
        print(f"   → buffett_active_top20_*.csv 파일 참조")
    
    if major_stock_scores:
        top_major = major_stock_scores[0]
        print(f"\n🏆 주요 대형주 중 최고 점수: {top_major['company_name']} "
              f"({top_major['total_score']:.1f}점, {top_major['grade']})")
        
        # Buy 등급 대형주 추천
        buy_majors = [s for s in major_stock_scores if s['investment_grade'] in ['Strong Buy', 'Buy']]
        if buy_majors:
            print(f"\n💰 투자 추천 대형주: {len(buy_majors)}개")
            for stock in buy_majors[:5]:
                print(f"   - {stock['company_name']}: {stock['total_score']:.1f}점, {stock['investment_grade']}")

if __name__ == "__main__":
    main()
