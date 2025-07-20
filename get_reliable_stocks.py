#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
워런 버핏 분석 결과에서 주요 대형주 중심의 실용적 추천 종목 추출
"""

import sqlite3
import pandas as pd
from datetime import datetime

def get_reliable_top_stocks():
    """신뢰할 수 있는 대형주 중심의 Top 종목 추출"""
    print("🏢 신뢰할 수 있는 주요 종목 워런 버핏 분석 결과")
    print("=" * 70)
    
    try:
        conn = sqlite3.connect("data/databases/buffett_scorecard.db")
        
        # 주요 대형주 및 중견주 리스트 (확실히 거래되는 종목들)
        reliable_stocks = [
            # 대형주
            '005930', '000660', '035420', '035720', '005380', '051910', '006400', 
            '068270', '000270', '105560', '055550', '096770', '017670', '030200',
            '003670', '012330', '207940', '086790', '028260', '066570', '003550',
            '033780', '015760', '009150', '011200', '032830', '018260', '010950',
            '051900', '024110', '000810', '161390', '097950', '078930', '010130',
            '036570', '302440', '011070', '090430', '047050', '000720', '034730',
            
            # 중견주 (코스피 상위)
            '011780', '005420', '180640', '139480', '004020', '006800', '081660',
            '000120', '004170', '251270', '009540', '005490', '002790', '138930',
            '000880', '185750', '032640', '047810', '001230', '004990', '021240',
            '069960', '267250', '010620', '036460', '005870', '293490', '000990',
            
            # 코스닥 대형주
            '091990', '196170', '042700', '263750', '041510', '214150', '112040',
            '079550', '357780', '095570', '122870', '145020', '039030', '086900',
            '067310', '328130', '018290', '108860', '047560', '036540'
        ]
        
        # 신뢰할 수 있는 종목들의 분석 결과 조회
        placeholders = ','.join('?' * len(reliable_stocks))
        query = f"""
        SELECT stock_code, company_name, total_score, grade, investment_grade,
               profitability_score, growth_score, stability_score, 
               efficiency_score, valuation_score
        FROM buffett_all_stocks_final 
        WHERE stock_code IN ({placeholders})
        ORDER BY total_score DESC
        """
        
        df = pd.read_sql_query(query, conn, params=reliable_stocks)
        conn.close()
        
        if df.empty:
            print("❌ 신뢰할 수 있는 종목 데이터를 찾을 수 없습니다.")
            return []
        
        print(f"📊 신뢰할 수 있는 종목 분석 결과: {len(df)}개")
        print("\n🏆 Top 30 신뢰할 수 있는 종목:")
        print("=" * 100)
        
        top30 = df.head(30)
        
        for idx, row in top30.iterrows():
            rank = idx + 1
            print(f"{rank:2d}. {row['company_name']:<15} ({row['stock_code']}): "
                  f"{row['total_score']:5.1f}점, {row['grade']:<2}, {row['investment_grade']:<10}")
            print(f"    수익성:{row['profitability_score']:4.1f} 성장성:{row['growth_score']:4.1f} "
                  f"안정성:{row['stability_score']:4.1f} 효율성:{row['efficiency_score']:4.1f} "
                  f"가치평가:{row['valuation_score']:4.1f}")
            print()
        
        # 등급별 분포
        print("\n📊 등급별 분포:")
        grade_dist = df['grade'].value_counts().sort_index()
        for grade, count in grade_dist.items():
            print(f"   {grade}: {count}개")
        
        # 투자 등급별 분포
        print("\n💰 투자 등급별 분포:")
        investment_dist = df['investment_grade'].value_counts()
        for grade, count in investment_dist.items():
            print(f"   {grade}: {count}개")
        
        # Strong Buy 종목들
        strong_buy = df[df['investment_grade'] == 'Strong Buy']
        if len(strong_buy) > 0:
            print(f"\n🌟 Strong Buy 등급 신뢰 종목: {len(strong_buy)}개")
            for _, row in strong_buy.iterrows():
                print(f"   - {row['company_name']} ({row['stock_code']}): {row['total_score']:.1f}점")
        
        # Buy 등급 종목들 (상위 15개)
        buy_stocks = df[df['investment_grade'] == 'Buy'].head(15)
        if len(buy_stocks) > 0:
            print(f"\n💎 Buy 등급 신뢰 종목 (상위 15개):")
            for _, row in buy_stocks.iterrows():
                print(f"   - {row['company_name']} ({row['stock_code']}): {row['total_score']:.1f}점")
        
        # 안정성 우수 종목 (B+ 이상, 안정성 20점 이상)
        stable_stocks = df[(df['total_score'] >= 75) & (df['stability_score'] >= 20)].head(10)
        if len(stable_stocks) > 0:
            print(f"\n🛡️ 안정성 우수 종목 (상위 10개):")
            for _, row in stable_stocks.iterrows():
                print(f"   - {row['company_name']} ({row['stock_code']}): "
                      f"총점 {row['total_score']:.1f}, 안정성 {row['stability_score']:.1f}")
        
        # 결과 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 전체 신뢰 종목
        output_file = f"results/buffett_analysis/buffett_reliable_stocks_{timestamp}.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        # Top 30만 별도 저장
        top30_file = f"results/buffett_analysis/buffett_reliable_top30_{timestamp}.csv"
        top30.to_csv(top30_file, index=False, encoding='utf-8-sig')
        
        # 투자 추천 종목만 별도 저장
        buy_recommendations = df[df['investment_grade'].isin(['Strong Buy', 'Buy'])]
        if len(buy_recommendations) > 0:
            buy_file = f"results/buffett_analysis/buffett_reliable_buy_{timestamp}.csv"
            buy_recommendations.to_csv(buy_file, index=False, encoding='utf-8-sig')
            print(f"\n💾 투자 추천 신뢰 종목: {len(buy_recommendations)}개 → {buy_file}")
        
        print(f"\n📁 파일 저장 완료:")
        print(f"   - 전체 신뢰 종목: {output_file}")
        print(f"   - Top 30: {top30_file}")
        
        return df.to_dict('records')
        
    except Exception as e:
        print(f"❌ 신뢰 종목 조회 실패: {e}")
        return []

def get_sector_analysis():
    """업종별 대표 종목 분석"""
    print("\n📊 업종별 대표 종목 워런 버핏 분석")
    print("=" * 70)
    
    # 업종별 대표 종목들
    sector_stocks = {
        '반도체': [('005930', '삼성전자'), ('000660', 'SK하이닉스')],
        'IT서비스': [('035420', 'NAVER'), ('035720', '카카오')],
        '자동차': [('005380', '현대차'), ('000270', '기아')],
        '화학': [('051910', 'LG화학'), ('096770', 'SK이노베이션')],
        '금융': [('105560', 'KB금융'), ('055550', '신한지주'), ('086790', '하나금융지주')],
        '통신': [('017670', 'SK텔레콤'), ('030200', 'KT')],
        '철강': [('003670', '포스코홀딩스')],
        '바이오': [('068270', '셀트리온'), ('207940', '삼성바이오로직스')],
        '전자': [('066570', 'LG전자'), ('009150', '삼성전기')],
        '건설': [('000720', '현대건설'), ('012330', '현대모비스')]
    }
    
    try:
        conn = sqlite3.connect("data/databases/buffett_scorecard.db")
        
        sector_results = {}
        
        for sector, stocks in sector_stocks.items():
            sector_data = []
            
            for stock_code, company_name in stocks:
                query = """
                SELECT stock_code, company_name, total_score, grade, investment_grade
                FROM buffett_all_stocks_final 
                WHERE stock_code = ?
                """
                
                result = pd.read_sql_query(query, conn, params=(stock_code,))
                
                if not result.empty:
                    row = result.iloc[0]
                    sector_data.append({
                        'stock_code': stock_code,
                        'company_name': company_name,
                        'total_score': row['total_score'],
                        'grade': row['grade'],
                        'investment_grade': row['investment_grade']
                    })
            
            if sector_data:
                # 업종 내 점수순 정렬
                sector_data.sort(key=lambda x: x['total_score'], reverse=True)
                sector_results[sector] = sector_data
        
        conn.close()
        
        # 업종별 결과 출력
        for sector, stocks in sector_results.items():
            if stocks:
                print(f"\n🏭 {sector}:")
                for i, stock in enumerate(stocks, 1):
                    print(f"   {i}. {stock['company_name']} ({stock['stock_code']}): "
                          f"{stock['total_score']:.1f}점, {stock['grade']}, {stock['investment_grade']}")
        
        return sector_results
        
    except Exception as e:
        print(f"❌ 업종별 분석 실패: {e}")
        return {}

def main():
    """메인 실행"""
    print("🎯 실용적 워런 버핏 투자 추천 종목 분석")
    print("=" * 80)
    
    # 1. 신뢰할 수 있는 종목들의 분석 결과
    reliable_results = get_reliable_top_stocks()
    
    # 2. 업종별 대표 종목 분석
    sector_results = get_sector_analysis()
    
    print("\n" + "=" * 80)
    print("🎯 투자 추천 요약")
    print("=" * 80)
    
    if reliable_results:
        # 상위 5개 추천
        print("🥇 최고 추천 종목 (Top 5):")
        for i in range(min(5, len(reliable_results))):
            stock = reliable_results[i]
            print(f"   {i+1}. {stock['company_name']} ({stock['stock_code']}): "
                  f"{stock['total_score']:.1f}점, {stock['grade']}, {stock['investment_grade']}")
        
        # Strong Buy 종목 추천
        strong_buy_count = len([s for s in reliable_results if s['investment_grade'] == 'Strong Buy'])
        buy_count = len([s for s in reliable_results if s['investment_grade'] == 'Buy'])
        
        print(f"\n💰 투자 등급 요약:")
        print(f"   - Strong Buy: {strong_buy_count}개 (최우선 투자)")
        print(f"   - Buy: {buy_count}개 (적극 투자)")
        print(f"   - 총 투자 추천: {strong_buy_count + buy_count}개")
        
        print(f"\n📊 분석 완료된 신뢰 종목: {len(reliable_results)}개")
        print(f"📁 상세 결과: results/buffett_analysis/ 폴더 확인")

if __name__ == "__main__":
    main()
