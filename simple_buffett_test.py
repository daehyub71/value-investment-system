#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
간단한 워런 버핏 스코어카드 테스트
모듈 import 없이 직접 계산 로직 테스트
"""

import math
from datetime import date

def safe_divide(num, den, default=0.0):
    """안전한 나눗셈"""
    return default if den == 0 else num / den

def calculate_cagr(initial, final, years):
    """연평균 성장률 계산"""
    if initial <= 0 or years <= 0:
        return 0
    return (final / initial) ** (1 / years) - 1

class SimpleBuffettScorecard:
    """간단한 워런 버핏 스코어카드"""
    
    def __init__(self):
        self.criteria = {
            'roe_excellent': 0.15,
            'roe_good': 0.10,
            'debt_ratio_excellent': 0.30,
            'debt_ratio_good': 0.50,
            'current_ratio_min': 1.5,
            'per_reasonable': 15,
            'pbr_undervalued': 1.0
        }
    
    def calculate_profitability_score(self, data):
        """수익성 지표 점수 계산 (30점)"""
        scores = {}
        
        # ROE (7점)
        roe = safe_divide(data['net_income'], data['shareholders_equity'])
        if roe >= self.criteria['roe_excellent']:
            scores['roe'] = 7.0
        elif roe >= self.criteria['roe_good']:
            scores['roe'] = 5.0
        elif roe >= 0.05:
            scores['roe'] = 3.0
        elif roe > 0:
            scores['roe'] = 1.0
        else:
            scores['roe'] = 0.0
        
        # ROA (5점)
        roa = safe_divide(data['net_income'], data['total_assets'])
        if roa >= 0.05:
            scores['roa'] = 5.0
        elif roa >= 0.03:
            scores['roa'] = 3.5
        elif roa >= 0.01:
            scores['roa'] = 2.0
        elif roa > 0:
            scores['roa'] = 0.5
        else:
            scores['roa'] = 0.0
        
        # 영업이익률 (4점)
        operating_margin = safe_divide(data['operating_income'], data['revenue'])
        if operating_margin >= 0.15:
            scores['operating_margin'] = 4.0
        elif operating_margin >= 0.10:
            scores['operating_margin'] = 3.0
        elif operating_margin >= 0.05:
            scores['operating_margin'] = 2.0
        elif operating_margin > 0:
            scores['operating_margin'] = 1.0
        else:
            scores['operating_margin'] = 0.0
        
        # 순이익률 (4점)
        net_margin = safe_divide(data['net_income'], data['revenue'])
        if net_margin >= 0.10:
            scores['net_margin'] = 4.0
        elif net_margin >= 0.07:
            scores['net_margin'] = 3.0
        elif net_margin >= 0.03:
            scores['net_margin'] = 2.0
        elif net_margin > 0:
            scores['net_margin'] = 1.0
        else:
            scores['net_margin'] = 0.0
        
        # EBITDA 마진 (3점)
        ebitda_margin = safe_divide(data['ebitda'], data['revenue'])
        if ebitda_margin >= 0.20:
            scores['ebitda_margin'] = 3.0
        elif ebitda_margin >= 0.15:
            scores['ebitda_margin'] = 2.0
        elif ebitda_margin >= 0.10:
            scores['ebitda_margin'] = 1.0
        else:
            scores['ebitda_margin'] = 0.0
        
        # ROIC (2점)
        roic = safe_divide(data.get('nopat', data['operating_income'] * 0.75), 
                          data.get('invested_capital', data['total_assets']))
        if roic >= 0.15:
            scores['roic'] = 2.0
        elif roic >= 0.10:
            scores['roic'] = 1.5
        elif roic >= 0.05:
            scores['roic'] = 1.0
        elif roic > 0:
            scores['roic'] = 0.5
        else:
            scores['roic'] = 0.0
        
        # 마진 일관성 (5점) - 간단화
        margins_history = data.get('margins_history', [])
        if len(margins_history) >= 3:
            avg_margin = sum(margins_history) / len(margins_history)
            if avg_margin > 0:
                std_dev = (sum([(m - avg_margin) ** 2 for m in margins_history]) / len(margins_history)) ** 0.5
                cv = std_dev / avg_margin
                if cv <= 0.1:
                    scores['margin_consistency'] = 5.0
                elif cv <= 0.2:
                    scores['margin_consistency'] = 4.0
                elif cv <= 0.3:
                    scores['margin_consistency'] = 3.0
                elif cv <= 0.5:
                    scores['margin_consistency'] = 2.0
                else:
                    scores['margin_consistency'] = 1.0
            else:
                scores['margin_consistency'] = 0.0
        else:
            scores['margin_consistency'] = 0.0
        
        total_score = sum(scores.values())
        details = {
            'ROE': f"{roe:.2%} ({scores['roe']}/7점)",
            'ROA': f"{roa:.2%} ({scores['roa']}/5점)",
            '영업이익률': f"{operating_margin:.2%} ({scores['operating_margin']}/4점)",
            '순이익률': f"{net_margin:.2%} ({scores['net_margin']}/4점)",
            'EBITDA마진': f"{ebitda_margin:.2%} ({scores['ebitda_margin']}/3점)",
            'ROIC': f"{roic:.2%} ({scores['roic']}/2점)",
            '마진일관성': f"{scores['margin_consistency']}/5점"
        }
        
        return {
            'category': '수익성',
            'total_score': total_score,
            'max_score': 30,
            'percentage': (total_score / 30) * 100,
            'details': details
        }
    
    def calculate_stability_score(self, data):
        """안정성 지표 점수 계산 (25점)"""
        scores = {}
        
        # 부채비율 (8점)
        debt_ratio = safe_divide(data['total_debt'], data['total_assets'])
        if debt_ratio <= self.criteria['debt_ratio_excellent']:
            scores['debt_ratio'] = 8.0
        elif debt_ratio <= self.criteria['debt_ratio_good']:
            scores['debt_ratio'] = 6.0
        elif debt_ratio <= 0.70:
            scores['debt_ratio'] = 4.0
        elif debt_ratio <= 1.0:
            scores['debt_ratio'] = 2.0
        else:
            scores['debt_ratio'] = 0.0
        
        # 유동비율 (5점)
        current_ratio = safe_divide(data['current_assets'], data['current_liabilities'], float('inf'))
        if current_ratio >= 2.0:
            scores['current_ratio'] = 5.0
        elif current_ratio >= self.criteria['current_ratio_min']:
            scores['current_ratio'] = 4.0
        elif current_ratio >= 1.2:
            scores['current_ratio'] = 3.0
        elif current_ratio >= 1.0:
            scores['current_ratio'] = 1.5
        else:
            scores['current_ratio'] = 0.0
        
        # 이자보상배율 (5점)
        interest_coverage = safe_divide(data['ebit'], data['interest_expense'], float('inf'))
        if interest_coverage >= 10:
            scores['interest_coverage'] = 5.0
        elif interest_coverage >= 5:
            scores['interest_coverage'] = 4.0
        elif interest_coverage >= 2:
            scores['interest_coverage'] = 2.5
        elif interest_coverage >= 1:
            scores['interest_coverage'] = 1.0
        else:
            scores['interest_coverage'] = 0.0
        
        # 당좌비율 (4점)
        quick_ratio = safe_divide(data['current_assets'] - data['inventory'], 
                                 data['current_liabilities'])
        if quick_ratio >= 1.5:
            scores['quick_ratio'] = 4.0
        elif quick_ratio >= 1.0:
            scores['quick_ratio'] = 3.0
        elif quick_ratio >= 0.8:
            scores['quick_ratio'] = 2.0
        elif quick_ratio >= 0.5:
            scores['quick_ratio'] = 1.0
        else:
            scores['quick_ratio'] = 0.0
        
        # 알트만 Z-Score (3점) - 간단화
        total_assets = data['total_assets']
        if total_assets > 0:
            a = (data['current_assets'] - data['current_liabilities']) / total_assets
            b = data.get('retained_earnings', 0) / total_assets
            c = data['ebit'] / total_assets
            d = safe_divide(data.get('market_cap', 0), data['total_debt'], 0)
            e = data['revenue'] / total_assets
            
            z_score = 1.2*a + 1.4*b + 3.3*c + 0.6*d + 1.0*e
            
            if z_score >= 3.0:
                scores['z_score'] = 3.0
            elif z_score >= 2.7:
                scores['z_score'] = 2.5
            elif z_score >= 1.8:
                scores['z_score'] = 1.5
            elif z_score >= 1.0:
                scores['z_score'] = 0.5
            else:
                scores['z_score'] = 0.0
        else:
            scores['z_score'] = 0.0
            z_score = 0
        
        total_score = sum(scores.values())
        details = {
            '부채비율': f"{debt_ratio:.2%} ({scores['debt_ratio']}/8점)",
            '유동비율': f"{current_ratio:.2f} ({scores['current_ratio']}/5점)" if current_ratio != float('inf') else f"무한대 ({scores['current_ratio']}/5점)",
            '이자보상배율': f"{interest_coverage:.2f} ({scores['interest_coverage']}/5점)" if interest_coverage != float('inf') else f"무한대 ({scores['interest_coverage']}/5점)",
            '당좌비율': f"{quick_ratio:.2f} ({scores['quick_ratio']}/4점)",
            '알트만Z점수': f"{z_score:.2f} ({scores['z_score']}/3점)"
        }
        
        return {
            'category': '안정성',
            'total_score': total_score,
            'max_score': 25,
            'percentage': (total_score / 25) * 100,
            'details': details
        }
    
    def calculate_total_score(self, financial_data):
        """종합 점수 계산"""
        profitability = self.calculate_profitability_score(financial_data)
        stability = self.calculate_stability_score(financial_data)
        
        # 간단화를 위해 수익성과 안정성만 계산 (55점 만점)
        total_score = profitability['total_score'] + stability['total_score']
        
        # 100점 만점으로 환산
        scaled_score = (total_score / 55) * 100
        
        # 등급 산정
        if scaled_score >= 90:
            grade = "A+"
        elif scaled_score >= 80:
            grade = "A"
        elif scaled_score >= 70:
            grade = "B+"
        elif scaled_score >= 60:
            grade = "B"
        elif scaled_score >= 50:
            grade = "C+"
        elif scaled_score >= 40:
            grade = "C"
        elif scaled_score >= 30:
            grade = "D"
        else:
            grade = "F"
        
        # 투자 추천
        if scaled_score >= 85 and stability['percentage'] >= 70 and profitability['percentage'] >= 70:
            recommendation = "Strong Buy"
        elif scaled_score >= 75:
            recommendation = "Buy"
        elif scaled_score >= 60:
            recommendation = "Hold"
        elif scaled_score >= 45:
            recommendation = "Weak Hold"
        else:
            recommendation = "Sell"
        
        return {
            'total_score': scaled_score,
            'grade': grade,
            'recommendation': recommendation,
            'profitability': profitability,
            'stability': stability
        }

def test_simple_buffett():
    """간단한 워런 버핏 스코어카드 테스트"""
    
    print("🎯 간단한 워런 버핏 스코어카드 테스트")
    print("=" * 50)
    
    # 삼성전자 테스트 데이터
    samsung_data = {
        'stock_code': '005930',
        'company_name': '삼성전자',
        'net_income': 15116000000000,  # 15.1조원
        'shareholders_equity': 305000000000000,  # 305조원
        'total_assets': 427000000000000,  # 427조원
        'revenue': 258900000000000,  # 258.9조원
        'operating_income': 26969000000000,  # 27조원
        'ebitda': 42000000000000,  # 42조원
        'current_assets': 201000000000000,  # 201조원
        'current_liabilities': 66000000000000,  # 66조원
        'total_debt': 36000000000000,  # 36조원
        'cash': 74000000000000,  # 74조원
        'inventory': 47000000000000,  # 47조원
        'ebit': 26969000000000,
        'interest_expense': 1300000000000,  # 1.3조원
        'retained_earnings': 250000000000000,
        'market_cap': 425000000000000,
        'margins_history': [0.168, 0.094, 0.058]  # 순이익률 과거 3년
    }
    
    # NAVER 테스트 데이터
    naver_data = {
        'stock_code': '035420',
        'company_name': 'NAVER',
        'net_income': 2400000000000,  # 2.4조원
        'shareholders_equity': 25000000000000,  # 25조원
        'total_assets': 35000000000000,  # 35조원
        'revenue': 8800000000000,  # 8.8조원
        'operating_income': 1300000000000,  # 1.3조원
        'ebitda': 2500000000000,  # 2.5조원
        'current_assets': 20000000000000,  # 20조원
        'current_liabilities': 5000000000000,  # 5조원
        'total_debt': 5000000000000,  # 5조원
        'cash': 8000000000000,  # 8조원
        'inventory': 100000000000,  # 0.1조원
        'ebit': 1300000000000,
        'interest_expense': 200000000000,  # 0.2조원
        'retained_earnings': 20000000000000,
        'market_cap': 32000000000000,
        'margins_history': [0.316, 0.292, 0.273]  # 순이익률 과거 3년
    }
    
    # 스코어카드 생성
    scorecard = SimpleBuffettScorecard()
    
    # 테스트 실행
    companies = [
        ('삼성전자', samsung_data),
        ('NAVER', naver_data)
    ]
    
    results = []
    
    for company_name, data in companies:
        print(f"\n📊 {company_name} 분석 결과")
        print("-" * 30)
        
        result = scorecard.calculate_total_score(data)
        results.append((company_name, result))
        
        print(f"종목코드: {data['stock_code']}")
        print(f"총점: {result['total_score']:.1f}/100점 ({result['grade']}등급)")
        print(f"투자추천: {result['recommendation']}")
        
        print(f"\n📈 카테고리별 점수:")
        print(f"  수익성: {result['profitability']['total_score']:.1f}/{result['profitability']['max_score']}점 ({result['profitability']['percentage']:.1f}%)")
        print(f"  안정성: {result['stability']['total_score']:.1f}/{result['stability']['max_score']}점 ({result['stability']['percentage']:.1f}%)")
        
        print(f"\n💰 수익성 상세:")
        for key, value in result['profitability']['details'].items():
            print(f"  {key}: {value}")
        
        print(f"\n🛡️ 안정성 상세:")
        for key, value in result['stability']['details'].items():
            print(f"  {key}: {value}")
    
    # 종합 비교
    print(f"\n🏆 종합 비교")
    print("=" * 30)
    
    sorted_results = sorted(results, key=lambda x: x[1]['total_score'], reverse=True)
    
    print("📊 워런 버핏 스코어카드 순위:")
    for i, (name, result) in enumerate(sorted_results, 1):
        print(f"  {i}위. {name}: {result['total_score']:.1f}점 ({result['grade']}) - {result['recommendation']}")
    
    # 기본 계산 검증
    print(f"\n🔧 기본 계산 검증:")
    
    # 삼성전자 ROE 계산
    samsung_roe = samsung_data['net_income'] / samsung_data['shareholders_equity']
    print(f"삼성전자 ROE: {samsung_roe:.2%}")
    
    # NAVER ROE 계산
    naver_roe = naver_data['net_income'] / naver_data['shareholders_equity']
    print(f"NAVER ROE: {naver_roe:.2%}")
    
    # 부채비율 계산
    samsung_debt_ratio = samsung_data['total_debt'] / samsung_data['total_assets']
    naver_debt_ratio = naver_data['total_debt'] / naver_data['total_assets']
    print(f"삼성전자 부채비율: {samsung_debt_ratio:.2%}")
    print(f"NAVER 부채비율: {naver_debt_ratio:.2%}")
    
    print(f"\n✅ 간단한 워런 버핏 스코어카드 테스트 완료!")
    print(f"📅 분석 일시: {date.today()}")
    
    return results

if __name__ == "__main__":
    test_simple_buffett()
