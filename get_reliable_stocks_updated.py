#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
투자 가능 여부를 고려한 워런 버핏 분석 결과에서 주요 대형주 중심의 실용적 추천 종목 추출
"""

import sqlite3
import pandas as pd
from datetime import datetime
import os

def get_reliable_top_stocks_with_investment_status():
    """투자 가능 여부를 고려한 신뢰할 수 있는 대형주 중심의 Top 종목 추출"""
    print("🏢 투자 가능한 신뢰할 수 있는 주요 종목 워런 버핏 분석 결과")
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
        
        # 투자 가능 여부를 고려한 신뢰할 수 있는 종목들의 분석 결과 조회
        placeholders = ','.join('?' * len(reliable_stocks))
        query = f"""
        SELECT 
            b.stock_code, 
            b.company_name, 
            b.total_score, 
            b.grade, 
            b.investment_grade,
            b.profitability_score, 
            b.growth_score, 
            b.stability_score, 
            b.efficiency_score, 
            b.valuation_score,
            COALESCE(i.is_investable, 1) as is_investable,
            COALESCE(i.listing_status, 'LISTED') as listing_status,
            COALESCE(i.investment_warning, 'NONE') as investment_warning,
            i.market_type,
            i.notes as investment_notes
        FROM buffett_all_stocks_final b
        LEFT JOIN investment_status i ON b.stock_code = i.stock_code
        WHERE b.stock_code IN ({placeholders})
        ORDER BY 
            COALESCE(i.is_investable, 1) DESC,  -- 투자 가능한 종목 우선
            b.total_score DESC
        """
        
        df = pd.read_sql_query(query, conn, params=reliable_stocks)
        conn.close()
        
        if df.empty:
            print("❌ 신뢰할 수 있는 종목 데이터를 찾을 수 없습니다.")
            return []
        
        # 투자 가능/불가 종목 분리
        investable_df = df[df['is_investable'] == 1]
        non_investable_df = df[df['is_investable'] == 0]
        
        print(f"📊 신뢰할 수 있는 종목 분석 결과: {len(df)}개")
        print(f"   💎 투자 가능: {len(investable_df)}개")
        print(f"   ❌ 투자 불가: {len(non_investable_df)}개")
        
        # 투자 가능한 Top 30 종목
        print("\n🏆 투자 가능한 Top 30 신뢰할 수 있는 종목:")
        print("=" * 120)
        
        top30_investable = investable_df.head(30)
        
        for idx, row in top30_investable.iterrows():
            rank = list(top30_investable.index).index(idx) + 1
            market_info = f"({row['market_type']})" if pd.notna(row['market_type']) else ""
            warning_info = f"[{row['investment_warning']}]" if row['investment_warning'] != 'NONE' else ""
            
            print(f"{rank:2d}. {row['company_name']:<15} ({row['stock_code']}) {market_info:<8} {warning_info:<10}: "
                  f"{row['total_score']:5.1f}점, {row['grade']:<2}, {row['investment_grade']:<10}")
            print(f"    수익성:{row['profitability_score']:4.1f} 성장성:{row['growth_score']:4.1f} "
                  f"안정성:{row['stability_score']:4.1f} 효율성:{row['efficiency_score']:4.1f} "
                  f"가치평가:{row['valuation_score']:4.1f}")
            print()
        
        # 투자 불가 종목이 있는 경우 표시
        if len(non_investable_df) > 0:
            print(f"\n❌ 투자 불가 신뢰 종목: {len(non_investable_df)}개")
            print("=" * 80)
            for _, row in non_investable_df.iterrows():
                status_info = f"{row['listing_status']}"
                if row['investment_warning'] != 'NONE':
                    status_info += f" / {row['investment_warning']}"
                
                print(f"   - {row['company_name']} ({row['stock_code']}): {row['total_score']:5.1f}점")
                print(f"     상태: {status_info}")
                if pd.notna(row['investment_notes']):
                    print(f"     사유: {row['investment_notes']}")
                print()
        
        # 투자 가능한 종목들의 등급별 분포
        print("\n📊 투자 가능 종목 등급별 분포:")
        grade_dist = investable_df['grade'].value_counts().sort_index()
        for grade, count in grade_dist.items():
            print(f"   {grade}: {count}개")
        
        # 투자 가능한 종목들의 투자 등급별 분포
        print("\n💰 투자 가능 종목 투자 등급별 분포:")
        investment_dist = investable_df['investment_grade'].value_counts()
        for grade, count in investment_dist.items():
            print(f"   {grade}: {count}개")
        
        # Strong Buy 투자 가능 종목들
        strong_buy_investable = investable_df[investable_df['investment_grade'] == 'Strong Buy']
        if len(strong_buy_investable) > 0:
            print(f"\n🌟 Strong Buy 등급 투자 가능 신뢰 종목: {len(strong_buy_investable)}개")
            for _, row in strong_buy_investable.iterrows():
                print(f"   - {row['company_name']} ({row['stock_code']}): {row['total_score']:.1f}점")
        
        # Buy 등급 투자 가능 종목들 (상위 15개)
        buy_investable = investable_df[investable_df['investment_grade'] == 'Buy'].head(15)
        if len(buy_investable) > 0:
            print(f"\n💎 Buy 등급 투자 가능 신뢰 종목 (상위 15개):")
            for _, row in buy_investable.iterrows():
                print(f"   - {row['company_name']} ({row['stock_code']}): {row['total_score']:.1f}점")
        
        # 안정성 우수 투자 가능 종목 (B+ 이상, 안정성 20점 이상)
        stable_investable = investable_df[
            (investable_df['total_score'] >= 75) & 
            (investable_df['stability_score'] >= 20)
        ].head(10)
        if len(stable_investable) > 0:
            print(f"\n🛡️ 안정성 우수 투자 가능 종목 (상위 10개):")
            for _, row in stable_investable.iterrows():
                print(f"   - {row['company_name']} ({row['stock_code']}): "
                      f"총점 {row['total_score']:.1f}, 안정성 {row['stability_score']:.1f}")
        
        # 결과 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 결과 디렉토리 생성
        os.makedirs("results/buffett_analysis", exist_ok=True)
        
        # 전체 신뢰 종목 (투자 가능 여부 포함)
        output_file = f"results/buffett_analysis/buffett_reliable_stocks_with_status_{timestamp}.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        # 투자 가능한 Top 30만 별도 저장
        if len(top30_investable) > 0:
            top30_file = f"results/buffett_analysis/buffett_investable_top30_{timestamp}.csv"
            top30_investable.to_csv(top30_file, index=False, encoding='utf-8-sig')
        
        # 투자 가능한 추천 종목만 별도 저장
        investable_recommendations = investable_df[
            investable_df['investment_grade'].isin(['Strong Buy', 'Buy'])
        ]
        if len(investable_recommendations) > 0:
            buy_file = f"results/buffett_analysis/buffett_investable_recommendations_{timestamp}.csv"
            investable_recommendations.to_csv(buy_file, index=False, encoding='utf-8-sig')
            print(f"\n💾 투자 가능한 추천 신뢰 종목: {len(investable_recommendations)}개 → {buy_file}")
        
        # 투자 불가 종목 별도 저장
        if len(non_investable_df) > 0:
            non_investable_file = f"results/buffett_analysis/buffett_non_investable_{timestamp}.csv"
            non_investable_df.to_csv(non_investable_file, index=False, encoding='utf-8-sig')
            print(f"💾 투자 불가 신뢰 종목: {len(non_investable_df)}개 → {non_investable_file}")
        
        print(f"\n📁 파일 저장 완료:")
        print(f"   - 전체 신뢰 종목 (상태 포함): {output_file}")
        if len(top30_investable) > 0:
            print(f"   - 투자 가능 Top 30: {top30_file}")
        
        return investable_df.to_dict('records')
        
    except Exception as e:
        print(f"❌ 신뢰 종목 조회 실패: {e}")
        return []

def get_sector_analysis_with_investment_status():
    """투자 가능 여부를 고려한 업종별 대표 종목 분석"""
    print("\n📊 투자 가능 여부를 고려한 업종별 대표 종목 워런 버핏 분석")
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
                SELECT 
                    b.stock_code, 
                    b.company_name, 
                    b.total_score, 
                    b.grade, 
                    b.investment_grade,
                    COALESCE(i.is_investable, 1) as is_investable,
                    COALESCE(i.listing_status, 'LISTED') as listing_status,
                    COALESCE(i.investment_warning, 'NONE') as investment_warning
                FROM buffett_all_stocks_final b
                LEFT JOIN investment_status i ON b.stock_code = i.stock_code
                WHERE b.stock_code = ?
                """
                
                result = pd.read_sql_query(query, conn, params=(stock_code,))
                
                if not result.empty:
                    row = result.iloc[0]
                    sector_data.append({
                        'stock_code': stock_code,
                        'company_name': company_name,
                        'total_score': row['total_score'],
                        'grade': row['grade'],
                        'investment_grade': row['investment_grade'],
                        'is_investable': row['is_investable'],
                        'listing_status': row['listing_status'],
                        'investment_warning': row['investment_warning']
                    })
            
            if sector_data:
                # 업종 내 점수순 정렬 (투자 가능한 종목 우선)
                sector_data.sort(key=lambda x: (x['is_investable'], x['total_score']), reverse=True)
                sector_results[sector] = sector_data
        
        conn.close()
        
        # 업종별 결과 출력
        for sector, stocks in sector_results.items():
            if stocks:
                print(f"\n🏭 {sector}:")
                for i, stock in enumerate(stocks, 1):
                    status_icon = "💎" if stock['is_investable'] else "❌"
                    warning_info = f"[{stock['investment_warning']}]" if stock['investment_warning'] != 'NONE' else ""
                    
                    print(f"   {i}. {status_icon} {stock['company_name']} ({stock['stock_code']}): "
                          f"{stock['total_score']:.1f}점, {stock['grade']}, {stock['investment_grade']} {warning_info}")
                    
                    if not stock['is_investable']:
                        print(f"      ⚠️ 투자 불가: {stock['listing_status']}")
        
        return sector_results
        
    except Exception as e:
        print(f"❌ 업종별 분석 실패: {e}")
        return {}

def main():
    """메인 실행"""
    print("🎯 투자 가능 여부를 고려한 실용적 워런 버핏 투자 추천 종목 분석")
    print("=" * 80)
    
    # 1. 투자 가능 여부를 고려한 신뢰할 수 있는 종목들의 분석 결과
    reliable_results = get_reliable_top_stocks_with_investment_status()
    
    # 2. 투자 가능 여부를 고려한 업종별 대표 종목 분석
    sector_results = get_sector_analysis_with_investment_status()
    
    print("\n" + "=" * 80)
    print("🎯 투자 추천 요약 (투자 가능 종목만)")
    print("=" * 80)
    
    if reliable_results:
        # 투자 가능한 종목만 필터링
        investable_stocks = [s for s in reliable_results if s['is_investable'] == 1]
        
        if investable_stocks:
            # 상위 5개 추천
            print("🥇 최고 추천 투자 가능 종목 (Top 5):")
            for i in range(min(5, len(investable_stocks))):
                stock = investable_stocks[i]
                warning_info = f"[{stock['investment_warning']}]" if stock['investment_warning'] != 'NONE' else ""
                print(f"   {i+1}. {stock['company_name']} ({stock['stock_code']}) {warning_info}: "
                      f"{stock['total_score']:.1f}점, {stock['grade']}, {stock['investment_grade']}")
            
            # Strong Buy 종목 추천
            strong_buy_count = len([s for s in investable_stocks if s['investment_grade'] == 'Strong Buy'])
            buy_count = len([s for s in investable_stocks if s['investment_grade'] == 'Buy'])
            
            print(f"\n💰 투자 가능 종목 등급 요약:")
            print(f"   - Strong Buy: {strong_buy_count}개 (최우선 투자)")
            print(f"   - Buy: {buy_count}개 (적극 투자)")
            print(f"   - 총 투자 추천: {strong_buy_count + buy_count}개")
            
            print(f"\n📊 분석 완료된 투자 가능 신뢰 종목: {len(investable_stocks)}개")
            
        else:
            print("❌ 투자 가능한 신뢰 종목이 없습니다.")
        
        # 투자 불가 종목 요약
        non_investable_count = len([s for s in reliable_results if s['is_investable'] == 0])
        if non_investable_count > 0:
            print(f"\n⚠️ 투자 불가 신뢰 종목: {non_investable_count}개")
            print("   (상장폐지, 관리종목, 투자주의 등)")
        
        print(f"\n📁 상세 결과: results/buffett_analysis/ 폴더 확인")

if __name__ == "__main__":
    main()
