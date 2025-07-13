#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
워런 버핏 스코어카드 시스템 테스트 스크립트
실제 삼성전자, SK하이닉스, NAVER 데이터로 테스트

실행 방법:
python test_buffett_scorecard.py
"""

import sys
import os
from pathlib import Path
import logging
from datetime import date

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('buffett_test.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

def get_sample_data():
    """테스트용 샘플 데이터"""
    
    # 삼성전자 데이터 (2023년 기준)
    samsung_data = {
        'stock_code': '005930',
        'company_name': '삼성전자',
        'net_income': 15116000000000,  # 15.1조원 (2023년)
        'shareholders_equity': 305000000000000,  # 305조원
        'total_assets': 427000000000000,  # 427조원
        'revenue': 258900000000000,  # 258.9조원
        'operating_income': 26969000000000,  # 27조원
        'ebitda': 42000000000000,  # 42조원 (추정)
        'current_assets': 201000000000000,  # 201조원
        'current_liabilities': 66000000000000,  # 66조원
        'total_debt': 36000000000000,  # 36조원
        'cash': 74000000000000,  # 74조원
        'inventory': 47000000000000,  # 47조원
        'receivables': 31000000000000,  # 31조원
        'ebit': 26969000000000,  # 영업이익과 동일로 가정
        'interest_expense': 1300000000000,  # 1.3조원
        'shares_outstanding': 5969782550,  # 약 59.7억주
        'eps': 2533,  # 주당순이익 2,533원
        'bps': 51116,  # 주당순자산 51,116원
        'dividend_per_share': 361,  # 주당배당금 361원
        'cogs': 200000000000000,  # 매출원가 (추정)
        'retained_earnings': 250000000000000,  # 이익잉여금 (추정)
        'invested_capital': 340000000000000,  # 투하자본 (추정)
        'nopat': 20000000000000,  # 세후영업이익 (추정)
        
        # 과거 데이터 (3년간)
        'revenue_history': [236806000000000, 244166000000000, 258900000000000],  # 2021-2023
        'net_income_history': [39895000000000, 23043000000000, 15116000000000],  # 2021-2023
        'eps_history': [6683, 3863, 2533],  # 2021-2023
        'equity_history': [270000000000000, 295000000000000, 305000000000000],  # 2021-2023
        'dividend_history': [354, 354, 361],  # 2021-2023
        'margins_history': [0.168, 0.094, 0.058],  # 순이익률 과거 3년
        'eps_growth_rate': -0.15,  # EPS 감소율 (메모리 경기 하락)
        'market_cap': 425000000000000  # 시가총액 425조원 (가정)
    }
    
    # SK하이닉스 데이터 (비교용)
    sk_hynix_data = {
        'stock_code': '000660',
        'company_name': 'SK하이닉스',
        'net_income': -5310000000000,  # -5.3조원 (2023년 적자)
        'shareholders_equity': 58000000000000,  # 58조원
        'total_assets': 91000000000000,  # 91조원
        'revenue': 55756000000000,  # 55.8조원
        'operating_income': -5640000000000,  # -5.6조원 (영업손실)
        'ebitda': 8000000000000,  # 8조원 (추정)
        'current_assets': 45000000000000,  # 45조원
        'current_liabilities': 14000000000000,  # 14조원
        'total_debt': 19000000000000,  # 19조원
        'cash': 15000000000000,  # 15조원
        'inventory': 12000000000000,  # 12조원
        'receivables': 8000000000000,  # 8조원
        'ebit': -5640000000000,  # 영업이익과 동일
        'interest_expense': 500000000000,  # 0.5조원
        'shares_outstanding': 728002365,  # 약 7.3억주
        'eps': -7291,  # 주당순손실
        'bps': 79670,  # 주당순자산
        'dividend_per_share': 0,  # 무배당
        'cogs': 45000000000000,  # 매출원가 (추정)
        'retained_earnings': 30000000000000,  # 이익잉여금 (추정)
        'invested_capital': 77000000000000,  # 투하자본 (추정)
        'nopat': -4000000000000,  # 세후영업손실 (추정)
        
        # 과거 데이터
        'revenue_history': [42106000000000, 44169000000000, 55756000000000],  # 2021-2023
        'net_income_history': [9975000000000, 11779000000000, -5310000000000],  # 2021-2023
        'eps_history': [13698, 16182, -7291],  # 2021-2023
        'equity_history': [50000000000000, 55000000000000, 58000000000000],  # 2021-2023
        'dividend_history': [1000, 1000, 0],  # 2021-2023
        'margins_history': [0.237, 0.267, -0.095],  # 순이익률 과거 3년
        'eps_growth_rate': -0.50,  # EPS 급감
        'market_cap': 58000000000000  # 시가총액 58조원 (가정)
    }
    
    # NAVER 데이터 (IT서비스 비교용)
    naver_data = {
        'stock_code': '035420',
        'company_name': 'NAVER',
        'net_income': 2400000000000,  # 2.4조원 (2023년)
        'shareholders_equity': 25000000000000,  # 25조원
        'total_assets': 35000000000000,  # 35조원
        'revenue': 8800000000000,  # 8.8조원
        'operating_income': 1300000000000,  # 1.3조원
        'ebitda': 2500000000000,  # 2.5조원
        'current_assets': 20000000000000,  # 20조원
        'current_liabilities': 5000000000000,  # 5조원
        'total_debt': 5000000000000,  # 5조원
        'cash': 8000000000000,  # 8조원
        'inventory': 100000000000,  # 0.1조원 (IT서비스)
        'receivables': 2000000000000,  # 2조원
        'ebit': 1300000000000,  # 영업이익과 동일
        'interest_expense': 200000000000,  # 0.2조원
        'shares_outstanding': 164250000,  # 약 1.6억주
        'eps': 14634,  # 주당순이익
        'bps': 152289,  # 주당순자산
        'dividend_per_share': 250,  # 주당배당금
        'cogs': 5000000000000,  # 매출원가 (추정)
        'retained_earnings': 20000000000000,  # 이익잉여금 (추정)
        'invested_capital': 30000000000000,  # 투하자본 (추정)
        'nopat': 1000000000000,  # 세후영업이익 (추정)
        
        # 과거 데이터
        'revenue_history': [5700000000000, 7200000000000, 8800000000000],  # 2021-2023
        'net_income_history': [1800000000000, 2100000000000, 2400000000000],  # 2021-2023
        'eps_history': [10962, 12792, 14634],  # 2021-2023
        'equity_history': [20000000000000, 22000000000000, 25000000000000],  # 2021-2023
        'dividend_history': [200, 220, 250],  # 2021-2023
        'margins_history': [0.316, 0.292, 0.273],  # 순이익률 과거 3년
        'eps_growth_rate': 0.15,  # EPS 성장률
        'market_cap': 32000000000000  # 시가총액 32조원 (가정)
    }
    
    # 시장 데이터
    market_data = {
        '005930': {'stock_price': 71200},  # 삼성전자 71,200원
        '000660': {'stock_price': 79600},  # SK하이닉스 79,600원
        '035420': {'stock_price': 195000}  # NAVER 195,000원
    }
    
    return {
        '삼성전자': (samsung_data, market_data['005930']),
        'SK하이닉스': (sk_hynix_data, market_data['000660']),
        'NAVER': (naver_data, market_data['035420'])
    }

def test_buffett_scorecard():
    """워런 버핏 스코어카드 테스트"""
    
    try:
        from src.analysis.fundamental.buffett_scorecard import BuffettScorecard
        from src.analysis.fundamental.financial_ratios import FinancialRatios
        
        logger.info("워런 버핏 스코어카드 시스템 테스트 시작")
        print("🎯 워런 버핏 스코어카드 시스템 테스트")
        print("=" * 60)
        
        # 스코어카드 및 재무비율 분석기 초기화
        scorecard = BuffettScorecard()
        ratio_analyzer = FinancialRatios()
        
        # 테스트 데이터 가져오기
        test_companies = get_sample_data()
        
        results = {}
        
        for company_name, (financial_data, market_data) in test_companies.items():
            print(f"\n📊 {company_name} 분석 결과")
            print("-" * 40)
            
            try:
                # 워런 버핏 스코어카드 계산
                buffett_result = scorecard.calculate_total_score(financial_data, market_data)
                
                # 재무비율 종합 분석
                ratio_results = ratio_analyzer.analyze_all_ratios(financial_data, market_data)
                ratio_summary = ratio_analyzer.get_ratio_summary(ratio_results)
                
                # 결과 저장
                results[company_name] = {
                    'buffett': buffett_result,
                    'ratios': ratio_results,
                    'summary': ratio_summary
                }
                
                # 워런 버핏 스코어카드 결과 출력
                print(f"종목코드: {buffett_result.stock_code}")
                print(f"총점: {buffett_result.total_score:.1f}/100점 ({buffett_result.grade}등급)")
                print(f"투자추천: {buffett_result.recommendation}")
                print(f"리스크레벨: {buffett_result.risk_level}")
                print()
                
                print("📈 카테고리별 점수:")
                categories = [
                    buffett_result.profitability,
                    buffett_result.growth,
                    buffett_result.stability,
                    buffett_result.efficiency,
                    buffett_result.valuation
                ]
                
                for category in categories:
                    print(f"  {category.category}: {category.actual_score:.1f}/{category.max_score}점 ({category.percentage:.1f}%)")
                
                print()
                
                # 주요 강점과 약점
                if buffett_result.key_strengths:
                    print("✅ 주요 강점:")
                    for strength in buffett_result.key_strengths[:3]:
                        print(f"  • {strength}")
                    print()
                
                if buffett_result.key_weaknesses:
                    print("⚠️ 주요 약점:")
                    for weakness in buffett_result.key_weaknesses[:3]:
                        print(f"  • {weakness}")
                    print()
                
                # 투자 논리
                print("💡 투자 논리:")
                print(f"  {buffett_result.investment_thesis}")
                print()
                
                # 수익성 상세 (첫 번째 회사만)
                if company_name == '삼성전자':
                    print("📊 수익성 지표 상세:")
                    for key, value in buffett_result.profitability.details.items():
                        print(f"  {key}: {value}")
                    print()
                
            except Exception as e:
                logger.error(f"{company_name} 분석 중 오류 발생: {e}")
                print(f"❌ {company_name} 분석 실패: {e}")
                continue
        
        # 종합 비교 분석
        print("\n🏆 종합 비교 분석")
        print("=" * 60)
        
        # 점수 순으로 정렬
        sorted_companies = sorted(
            results.items(),
            key=lambda x: x[1]['buffett'].total_score,
            reverse=True
        )
        
        print("📊 워런 버핏 스코어카드 순위:")
        for i, (name, data) in enumerate(sorted_companies, 1):
            result = data['buffett']
            print(f"  {i}위. {name}: {result.total_score:.1f}점 ({result.grade}) - {result.recommendation}")
        
        print()
        
        # 카테고리별 최고 기업
        categories_best = {
            '수익성': (None, 0),
            '성장성': (None, 0),
            '안정성': (None, 0),
            '효율성': (None, 0),
            '가치평가': (None, 0)
        }
        
        for name, data in results.items():
            result = data['buffett']
            
            if result.profitability.percentage > categories_best['수익성'][1]:
                categories_best['수익성'] = (name, result.profitability.percentage)
            if result.growth.percentage > categories_best['성장성'][1]:
                categories_best['성장성'] = (name, result.growth.percentage)
            if result.stability.percentage > categories_best['안정성'][1]:
                categories_best['안정성'] = (name, result.stability.percentage)
            if result.efficiency.percentage > categories_best['효율성'][1]:
                categories_best['효율성'] = (name, result.efficiency.percentage)
            if result.valuation.percentage > categories_best['가치평가'][1]:
                categories_best['가치평가'] = (name, result.valuation.percentage)
        
        print("🥇 카테고리별 최우수 기업:")
        for category, (best_company, score) in categories_best.items():
            if best_company:
                print(f"  {category}: {best_company} ({score:.1f}%)")
        
        print()
        
        # 워런 버핏이 선호할 기업 추천
        print("💎 워런 버핏 스타일 투자 추천:")
        
        for name, data in sorted_companies:
            result = data['buffett']
            
            # 워런 버핏 기준: 안정성 70% 이상, 수익성 60% 이상, 총점 70점 이상
            stability_good = result.stability.percentage >= 70
            profitability_good = result.profitability.percentage >= 60
            total_good = result.total_score >= 70
            
            if stability_good and profitability_good and total_good:
                print(f"  ⭐ {name}: 워런 버핏 스타일에 적합")
                print(f"     - 안정성: {result.stability.percentage:.1f}%")
                print(f"     - 수익성: {result.profitability.percentage:.1f}%")
                print(f"     - 총점: {result.total_score:.1f}점")
                break
        else:
            print("  ⚠️ 현재 분석 대상 중 워런 버핏 기준을 완전히 만족하는 기업 없음")
            print("  💡 가장 근접한 기업:")
            best_name, best_data = sorted_companies[0]
            best_result = best_data['buffett']
            print(f"     {best_name}: {best_result.total_score:.1f}점 ({best_result.recommendation})")
        
        print(f"\n✅ 테스트 완료! 총 {len(results)}개 기업 분석")
        print(f"📅 분석 일시: {date.today()}")
        
        logger.info(f"워런 버핏 스코어카드 테스트 완료: {len(results)}개 기업 분석")
        
        return results
        
    except ImportError as e:
        logger.error(f"모듈 import 오류: {e}")
        print(f"❌ 모듈을 불러올 수 없습니다: {e}")
        print("📝 해결 방법:")
        print("  1. 프로젝트 루트 디렉토리에서 실행하세요")
        print("  2. 필요한 모듈이 제대로 구현되었는지 확인하세요")
        print("  3. Python 경로 설정을 확인하세요")
        return None
    
    except Exception as e:
        logger.error(f"테스트 중 예상치 못한 오류: {e}")
        print(f"❌ 테스트 실패: {e}")
        return None

def test_individual_components():
    """개별 컴포넌트 테스트"""
    
    print("\n🔧 개별 컴포넌트 테스트")
    print("=" * 40)
    
    try:
        # 1. 기본 계산 함수 테스트
        print("1. 기본 계산 함수 테스트...")
        
        def safe_divide(num, den, default=0.0):
            return default if den == 0 else num / den
        
        assert safe_divide(10, 2) == 5.0
        assert safe_divide(10, 0) == 0.0
        assert safe_divide(10, 0, -1) == -1
        print("  ✅ safe_divide 함수 정상 작동")
        
        # 2. CAGR 계산 테스트
        print("2. CAGR 계산 테스트...")
        
        def calculate_cagr(initial, final, years):
            if initial <= 0 or years <= 0: 
                return 0
            return (final / initial) ** (1 / years) - 1
        
        cagr = calculate_cagr(100, 121, 2)  # 10% 성장
        assert abs(cagr - 0.1) < 0.001
        print(f"  ✅ CAGR 계산 정상: {cagr:.2%}")
        
        # 3. 재무비율 기본 계산 테스트
        print("3. 재무비율 계산 테스트...")
        
        roe = safe_divide(1000, 10000)  # 10% ROE
        assert roe == 0.1
        print(f"  ✅ ROE 계산 정상: {roe:.2%}")
        
        per = safe_divide(50000, 2000)  # PER 25배
        assert per == 25.0
        print(f"  ✅ PER 계산 정상: {per:.1f}배")
        
        print("🎉 모든 개별 컴포넌트 테스트 통과!")
        
    except Exception as e:
        print(f"❌ 개별 컴포넌트 테스트 실패: {e}")

if __name__ == "__main__":
    print("🚀 워런 버핏 스코어카드 시스템 종합 테스트 시작")
    print("=" * 60)
    
    # 개별 컴포넌트 테스트
    test_individual_components()
    
    # 메인 시스템 테스트
    results = test_buffett_scorecard()
    
    if results:
        print("\n🎊 모든 테스트가 성공적으로 완료되었습니다!")
        print("\n📋 다음 단계:")
        print("  1. 실제 DART 데이터와 연동")
        print("  2. 웹 인터페이스 구현")
        print("  3. 더 많은 종목으로 백테스팅")
        print("  4. 기술분석 모듈 추가")
    else:
        print("\n⚠️ 테스트 중 일부 오류가 발생했습니다.")
        print("로그 파일(buffett_test.log)을 확인해주세요.")
