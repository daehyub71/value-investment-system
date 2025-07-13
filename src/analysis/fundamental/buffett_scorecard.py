"""
워런 버핏 스코어카드 시스템 - 완전 구현 버전
100점 만점 종합 평가 시스템

구성:
- 수익성 지표 (30점): ROE, ROA, 영업이익률, 순이익률, EBITDA 마진, ROIC
- 성장성 지표 (25점): 매출 성장률, 순이익 성장률, EPS 성장률, 자기자본 성장률, 배당 성장률
- 안정성 지표 (25점): 부채비율, 유동비율, 이자보상배율, 당좌비율, 알트만 Z-Score
- 효율성 지표 (10점): 재고회전율, 매출채권회전율, 총자산회전율
- 가치평가 지표 (20점): PER, PBR, PEG, 배당수익률, EV/EBITDA
"""

import math
import logging
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, date

# 내부 모듈 import (실제 경로에 맞게 수정)
try:
    from ..utils.calculation_utils import RatioCalculator, FinancialCalculator, safe_divide
    from ..utils.data_validation import validate_financial_data
    from ..utils.logging_utils import get_logger
except ImportError:
    # 테스트를 위한 fallback
    class RatioCalculator:
        @staticmethod
        def calculate_roe(net_income, equity): return safe_divide(net_income, equity)
        @staticmethod
        def calculate_roa(net_income, assets): return safe_divide(net_income, assets)
        @staticmethod
        def calculate_debt_ratio(debt, assets): return safe_divide(debt, assets)
        @staticmethod
        def calculate_current_ratio(current_assets, current_liabilities): 
            return safe_divide(current_assets, current_liabilities, float('inf'))
        @staticmethod
        def calculate_per(price, eps): return safe_divide(price, eps, float('inf'))
        @staticmethod
        def calculate_pbr(price, bps): return safe_divide(price, bps, float('inf'))
        @staticmethod
        def calculate_peg(per, growth): return safe_divide(per, growth * 100, float('inf'))
        @staticmethod
        def calculate_dividend_yield(dps, price): return safe_divide(dps, price)
        @staticmethod
        def calculate_interest_coverage_ratio(ebit, interest): 
            return safe_divide(ebit, interest, float('inf'))
    
    class FinancialCalculator:
        @staticmethod
        def calculate_cagr(initial, final, years): 
            if initial <= 0 or years <= 0: return 0
            return (final / initial) ** (1 / years) - 1
    
    def safe_divide(num, den, default=0.0):
        return default if den == 0 else num / den
    
    def validate_financial_data(data): return True
    def get_logger(name): return logging.getLogger(name)

logger = get_logger(__name__)

@dataclass
class ScoreBreakdown:
    """점수 세부 내역"""
    category: str
    max_score: float
    actual_score: float
    percentage: float
    details: Dict[str, float]
    
    def __post_init__(self):
        self.percentage = (self.actual_score / self.max_score) * 100 if self.max_score > 0 else 0

@dataclass
class BuffettAnalysisResult:
    """워런 버핏 분석 결과"""
    stock_code: str
    company_name: str
    analysis_date: date
    total_score: float
    grade: str
    recommendation: str
    risk_level: str
    profitability: ScoreBreakdown
    growth: ScoreBreakdown
    stability: ScoreBreakdown
    efficiency: ScoreBreakdown
    valuation: ScoreBreakdown
    key_strengths: List[str]
    key_weaknesses: List[str]
    investment_thesis: str

class BuffettScorecard:
    """워런 버핏 스코어카드 완전 구현"""
    
    def __init__(self):
        """초기화"""
        self.weights = {
            'profitability': 30,  # 수익성 지표
            'growth': 25,        # 성장성 지표  
            'stability': 25,     # 안정성 지표
            'efficiency': 10,    # 효율성 지표
            'valuation': 20      # 가치평가 지표
        }
        
        # 워런 버핏 기준점
        self.buffett_criteria = {
            'roe_excellent': 0.15,      # ROE 15% 이상 우수
            'roe_good': 0.10,           # ROE 10% 이상 양호
            'debt_ratio_excellent': 0.30, # 부채비율 30% 이하 우수
            'debt_ratio_good': 0.50,    # 부채비율 50% 이하 양호
            'current_ratio_min': 1.5,   # 유동비율 1.5 이상
            'per_reasonable': 15,       # PER 15배 이하 합리적
            'pbr_undervalued': 1.0,     # PBR 1.0 이하 저평가
            'dividend_yield_min': 0.02, # 배당수익률 2% 이상
            'interest_coverage_min': 5,  # 이자보상배율 5배 이상
            'growth_rate_good': 0.10,   # 성장률 10% 이상 양호
        }
        
        logger.info("워런 버핏 스코어카드 시스템 초기화 완료")
    
    def calculate_profitability_score(self, financial_data: Dict) -> ScoreBreakdown:
        """수익성 지표 점수 계산 (30점)"""
        details = {}
        scores = {}
        
        try:
            # 1. ROE (자기자본이익률) - 7점
            net_income = financial_data.get('net_income', 0)
            shareholders_equity = financial_data.get('shareholders_equity', 0)
            roe = RatioCalculator.calculate_roe(net_income, shareholders_equity)
            
            if roe >= self.buffett_criteria['roe_excellent']:
                scores['roe'] = 7.0
            elif roe >= self.buffett_criteria['roe_good']:
                scores['roe'] = 5.0
            elif roe >= 0.05:
                scores['roe'] = 3.0
            elif roe > 0:
                scores['roe'] = 1.0
            else:
                scores['roe'] = 0.0
            
            details['ROE'] = f"{roe:.2%} ({scores['roe']}/7점)"
            
            # 2. ROA (총자산이익률) - 5점
            total_assets = financial_data.get('total_assets', 0)
            roa = RatioCalculator.calculate_roa(net_income, total_assets)
            
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
            
            details['ROA'] = f"{roa:.2%} ({scores['roa']}/5점)"
            
            # 3. 영업이익률 - 4점
            revenue = financial_data.get('revenue', 0)
            operating_income = financial_data.get('operating_income', 0)
            operating_margin = safe_divide(operating_income, revenue)
            
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
            
            details['영업이익률'] = f"{operating_margin:.2%} ({scores['operating_margin']}/4점)"
            
            # 4. 순이익률 - 4점
            net_margin = safe_divide(net_income, revenue)
            
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
            
            details['순이익률'] = f"{net_margin:.2%} ({scores['net_margin']}/4점)"
            
            # 5. EBITDA 마진 - 3점
            ebitda = financial_data.get('ebitda', 0)
            ebitda_margin = safe_divide(ebitda, revenue)
            
            if ebitda_margin >= 0.20:
                scores['ebitda_margin'] = 3.0
            elif ebitda_margin >= 0.15:
                scores['ebitda_margin'] = 2.0
            elif ebitda_margin >= 0.10:
                scores['ebitda_margin'] = 1.0
            else:
                scores['ebitda_margin'] = 0.0
            
            details['EBITDA마진'] = f"{ebitda_margin:.2%} ({scores['ebitda_margin']}/3점)"
            
            # 6. ROIC (투하자본이익률) - 2점
            invested_capital = financial_data.get('invested_capital', total_assets)
            nopat = financial_data.get('nopat', operating_income * 0.75)  # 근사치
            roic = safe_divide(nopat, invested_capital)
            
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
            
            details['ROIC'] = f"{roic:.2%} ({scores['roic']}/2점)"
            
            # 6-1. 마진의 일관성 추가점수 - 5점
            margins_history = financial_data.get('margins_history', [])
            if margins_history and len(margins_history) >= 3:
                margin_consistency = self._calculate_margin_consistency(margins_history)
                scores['margin_consistency'] = margin_consistency
                details['마진일관성'] = f"{margin_consistency}/5점"
            else:
                scores['margin_consistency'] = 0.0
                details['마진일관성'] = "데이터 부족 (0/5점)"
            
        except Exception as e:
            logger.error(f"수익성 지표 계산 오류: {e}")
            return ScoreBreakdown("수익성", 30, 0, 0, {"오류": str(e)})
        
        total_score = sum(scores.values())
        
        return ScoreBreakdown(
            category="수익성",
            max_score=30,
            actual_score=total_score,
            percentage=(total_score / 30) * 100,
            details=details
        )
    
    def calculate_growth_score(self, financial_data: Dict) -> ScoreBreakdown:
        """성장성 지표 점수 계산 (25점)"""
        details = {}
        scores = {}
        
        try:
            # 1. 매출 성장률 (3년 CAGR) - 6점
            revenue_history = financial_data.get('revenue_history', [])
            if len(revenue_history) >= 3:
                revenue_cagr = FinancialCalculator.calculate_cagr(
                    revenue_history[0], revenue_history[-1], len(revenue_history) - 1
                )
                
                if revenue_cagr >= self.buffett_criteria['growth_rate_good']:
                    scores['revenue_growth'] = 6.0
                elif revenue_cagr >= 0.05:
                    scores['revenue_growth'] = 4.0
                elif revenue_cagr >= 0.0:
                    scores['revenue_growth'] = 2.0
                else:
                    scores['revenue_growth'] = 0.0
                
                details['매출성장률'] = f"{revenue_cagr:.2%} ({scores['revenue_growth']}/6점)"
            else:
                scores['revenue_growth'] = 0.0
                details['매출성장률'] = "데이터 부족 (0/6점)"
            
            # 2. 순이익 성장률 (3년 CAGR) - 5점
            net_income_history = financial_data.get('net_income_history', [])
            if len(net_income_history) >= 3:
                net_income_cagr = FinancialCalculator.calculate_cagr(
                    abs(net_income_history[0]) + 1, abs(net_income_history[-1]) + 1, 
                    len(net_income_history) - 1
                )
                
                if net_income_cagr >= 0.15:
                    scores['income_growth'] = 5.0
                elif net_income_cagr >= 0.10:
                    scores['income_growth'] = 3.5
                elif net_income_cagr >= 0.0:
                    scores['income_growth'] = 2.0
                else:
                    scores['income_growth'] = 0.0
                
                details['순이익성장률'] = f"{net_income_cagr:.2%} ({scores['income_growth']}/5점)"
            else:
                scores['income_growth'] = 0.0
                details['순이익성장률'] = "데이터 부족 (0/5점)"
            
            # 3. EPS 성장률 - 4점
            eps_history = financial_data.get('eps_history', [])
            if len(eps_history) >= 3:
                eps_cagr = FinancialCalculator.calculate_cagr(
                    abs(eps_history[0]) + 1, abs(eps_history[-1]) + 1,
                    len(eps_history) - 1
                )
                
                if eps_cagr >= 0.15:
                    scores['eps_growth'] = 4.0
                elif eps_cagr >= 0.10:
                    scores['eps_growth'] = 3.0
                elif eps_cagr >= 0.0:
                    scores['eps_growth'] = 1.5
                else:
                    scores['eps_growth'] = 0.0
                
                details['EPS성장률'] = f"{eps_cagr:.2%} ({scores['eps_growth']}/4점)"
            else:
                scores['eps_growth'] = 0.0
                details['EPS성장률'] = "데이터 부족 (0/4점)"
            
            # 4. 자기자본 성장률 - 3점
            equity_history = financial_data.get('equity_history', [])
            if len(equity_history) >= 3:
                equity_cagr = FinancialCalculator.calculate_cagr(
                    equity_history[0], equity_history[-1], len(equity_history) - 1
                )
                
                if equity_cagr >= 0.10:
                    scores['equity_growth'] = 3.0
                elif equity_cagr >= 0.05:
                    scores['equity_growth'] = 2.0
                elif equity_cagr >= 0.0:
                    scores['equity_growth'] = 1.0
                else:
                    scores['equity_growth'] = 0.0
                
                details['자기자본성장률'] = f"{equity_cagr:.2%} ({scores['equity_growth']}/3점)"
            else:
                scores['equity_growth'] = 0.0
                details['자기자본성장률'] = "데이터 부족 (0/3점)"
            
            # 5. 배당 성장률 - 2점
            dividend_history = financial_data.get('dividend_history', [])
            if len(dividend_history) >= 3 and all(d > 0 for d in dividend_history):
                dividend_cagr = FinancialCalculator.calculate_cagr(
                    dividend_history[0], dividend_history[-1], len(dividend_history) - 1
                )
                
                if dividend_cagr >= 0.10:
                    scores['dividend_growth'] = 2.0
                elif dividend_cagr >= 0.05:
                    scores['dividend_growth'] = 1.5
                elif dividend_cagr >= 0.0:
                    scores['dividend_growth'] = 1.0
                else:
                    scores['dividend_growth'] = 0.0
                
                details['배당성장률'] = f"{dividend_cagr:.2%} ({scores['dividend_growth']}/2점)"
            else:
                scores['dividend_growth'] = 0.0
                details['배당성장률'] = "배당 없음 (0/2점)"
            
            # 6. 성장의 지속성 - 5점
            growth_consistency = self._calculate_growth_consistency(financial_data)
            scores['growth_consistency'] = growth_consistency
            details['성장지속성'] = f"{growth_consistency}/5점"
            
        except Exception as e:
            logger.error(f"성장성 지표 계산 오류: {e}")
            return ScoreBreakdown("성장성", 25, 0, 0, {"오류": str(e)})
        
        total_score = sum(scores.values())
        
        return ScoreBreakdown(
            category="성장성",
            max_score=25,
            actual_score=total_score,
            percentage=(total_score / 25) * 100,
            details=details
        )
    
    def calculate_stability_score(self, financial_data: Dict) -> ScoreBreakdown:
        """안정성 지표 점수 계산 (25점)"""
        details = {}
        scores = {}
        
        try:
            # 1. 부채비율 - 8점
            total_debt = financial_data.get('total_debt', 0)
            total_assets = financial_data.get('total_assets', 0)
            debt_ratio = RatioCalculator.calculate_debt_ratio(total_debt, total_assets)
            
            if debt_ratio <= self.buffett_criteria['debt_ratio_excellent']:
                scores['debt_ratio'] = 8.0
            elif debt_ratio <= self.buffett_criteria['debt_ratio_good']:
                scores['debt_ratio'] = 6.0
            elif debt_ratio <= 0.70:
                scores['debt_ratio'] = 4.0
            elif debt_ratio <= 1.0:
                scores['debt_ratio'] = 2.0
            else:
                scores['debt_ratio'] = 0.0
            
            details['부채비율'] = f"{debt_ratio:.2%} ({scores['debt_ratio']}/8점)"
            
            # 2. 유동비율 - 5점
            current_assets = financial_data.get('current_assets', 0)
            current_liabilities = financial_data.get('current_liabilities', 0)
            current_ratio = RatioCalculator.calculate_current_ratio(current_assets, current_liabilities)
            
            if current_ratio >= 2.0:
                scores['current_ratio'] = 5.0
            elif current_ratio >= self.buffett_criteria['current_ratio_min']:
                scores['current_ratio'] = 4.0
            elif current_ratio >= 1.2:
                scores['current_ratio'] = 3.0
            elif current_ratio >= 1.0:
                scores['current_ratio'] = 1.5
            else:
                scores['current_ratio'] = 0.0
            
            details['유동비율'] = f"{current_ratio:.2f} ({scores['current_ratio']}/5점)"
            
            # 3. 이자보상배율 - 5점
            ebit = financial_data.get('ebit', 0)
            interest_expense = financial_data.get('interest_expense', 0)
            interest_coverage = RatioCalculator.calculate_interest_coverage_ratio(ebit, interest_expense)
            
            if interest_coverage >= 10:
                scores['interest_coverage'] = 5.0
            elif interest_coverage >= self.buffett_criteria['interest_coverage_min']:
                scores['interest_coverage'] = 4.0
            elif interest_coverage >= 2:
                scores['interest_coverage'] = 2.5
            elif interest_coverage >= 1:
                scores['interest_coverage'] = 1.0
            else:
                scores['interest_coverage'] = 0.0
            
            if interest_coverage == float('inf'):
                details['이자보상배율'] = "무부채 (5/5점)"
                scores['interest_coverage'] = 5.0
            else:
                details['이자보상배율'] = f"{interest_coverage:.2f} ({scores['interest_coverage']}/5점)"
            
            # 4. 당좌비율 - 4점
            inventory = financial_data.get('inventory', 0)
            quick_assets = current_assets - inventory
            quick_ratio = safe_divide(quick_assets, current_liabilities)
            
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
            
            details['당좌비율'] = f"{quick_ratio:.2f} ({scores['quick_ratio']}/4점)"
            
            # 5. 알트만 Z-Score - 3점
            z_score = self._calculate_altman_z_score(financial_data)
            
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
            
            details['알트만Z점수'] = f"{z_score:.2f} ({scores['z_score']}/3점)"
            
        except Exception as e:
            logger.error(f"안정성 지표 계산 오류: {e}")
            return ScoreBreakdown("안정성", 25, 0, 0, {"오류": str(e)})
        
        total_score = sum(scores.values())
        
        return ScoreBreakdown(
            category="안정성",
            max_score=25,
            actual_score=total_score,
            percentage=(total_score / 25) * 100,
            details=details
        )
    
    def calculate_efficiency_score(self, financial_data: Dict) -> ScoreBreakdown:
        """효율성 지표 점수 계산 (10점)"""
        details = {}
        scores = {}
        
        try:
            revenue = financial_data.get('revenue', 0)
            
            # 1. 재고회전율 - 4점
            inventory = financial_data.get('inventory', 0)
            cogs = financial_data.get('cogs', revenue * 0.7)  # 근사치
            inventory_turnover = safe_divide(cogs, inventory)
            
            # 업종별 평균 고려 (제조업 기준)
            if inventory_turnover >= 12:
                scores['inventory_turnover'] = 4.0
            elif inventory_turnover >= 8:
                scores['inventory_turnover'] = 3.0
            elif inventory_turnover >= 4:
                scores['inventory_turnover'] = 2.0
            elif inventory_turnover >= 1:
                scores['inventory_turnover'] = 1.0
            else:
                scores['inventory_turnover'] = 0.0
            
            details['재고회전율'] = f"{inventory_turnover:.2f} ({scores['inventory_turnover']}/4점)"
            
            # 2. 매출채권회전율 - 3점
            receivables = financial_data.get('receivables', 0)
            receivables_turnover = safe_divide(revenue, receivables)
            
            if receivables_turnover >= 12:
                scores['receivables_turnover'] = 3.0
            elif receivables_turnover >= 8:
                scores['receivables_turnover'] = 2.5
            elif receivables_turnover >= 4:
                scores['receivables_turnover'] = 1.5
            elif receivables_turnover >= 1:
                scores['receivables_turnover'] = 0.5
            else:
                scores['receivables_turnover'] = 0.0
            
            details['매출채권회전율'] = f"{receivables_turnover:.2f} ({scores['receivables_turnover']}/3점)"
            
            # 3. 총자산회전율 - 3점
            total_assets = financial_data.get('total_assets', 0)
            asset_turnover = safe_divide(revenue, total_assets)
            
            if asset_turnover >= 1.5:
                scores['asset_turnover'] = 3.0
            elif asset_turnover >= 1.0:
                scores['asset_turnover'] = 2.5
            elif asset_turnover >= 0.7:
                scores['asset_turnover'] = 2.0
            elif asset_turnover >= 0.5:
                scores['asset_turnover'] = 1.0
            else:
                scores['asset_turnover'] = 0.0
            
            details['총자산회전율'] = f"{asset_turnover:.2f} ({scores['asset_turnover']}/3점)"
            
        except Exception as e:
            logger.error(f"효율성 지표 계산 오류: {e}")
            return ScoreBreakdown("효율성", 10, 0, 0, {"오류": str(e)})
        
        total_score = sum(scores.values())
        
        return ScoreBreakdown(
            category="효율성",
            max_score=10,
            actual_score=total_score,
            percentage=(total_score / 10) * 100,
            details=details
        )
    
    def calculate_valuation_score(self, financial_data: Dict, market_data: Dict) -> ScoreBreakdown:
        """가치평가 지표 점수 계산 (20점)"""
        details = {}
        scores = {}
        
        try:
            stock_price = market_data.get('stock_price', 0)
            shares_outstanding = financial_data.get('shares_outstanding', 0)
            market_cap = stock_price * shares_outstanding
            
            # 1. PER (주가수익비율) - 6점
            eps = financial_data.get('eps', 0)
            per = RatioCalculator.calculate_per(stock_price, eps)
            
            if per <= 10:
                scores['per'] = 6.0
            elif per <= self.buffett_criteria['per_reasonable']:
                scores['per'] = 4.5
            elif per <= 20:
                scores['per'] = 3.0
            elif per <= 30:
                scores['per'] = 1.5
            else:
                scores['per'] = 0.0
            
            if per == float('inf'):
                details['PER'] = "적자 (0/6점)"
                scores['per'] = 0.0
            else:
                details['PER'] = f"{per:.2f} ({scores['per']}/6점)"
            
            # 2. PBR (주가순자산비율) - 5점
            bps = financial_data.get('bps', 0)
            pbr = RatioCalculator.calculate_pbr(stock_price, bps)
            
            if pbr <= self.buffett_criteria['pbr_undervalued']:
                scores['pbr'] = 5.0
            elif pbr <= 1.5:
                scores['pbr'] = 4.0
            elif pbr <= 2.0:
                scores['pbr'] = 3.0
            elif pbr <= 3.0:
                scores['pbr'] = 1.5
            else:
                scores['pbr'] = 0.0
            
            details['PBR'] = f"{pbr:.2f} ({scores['pbr']}/5점)"
            
            # 3. PEG (PER/성장률) - 4점
            eps_growth = financial_data.get('eps_growth_rate', 0)
            if eps_growth > 0:
                peg = RatioCalculator.calculate_peg(per, eps_growth)
                
                if peg <= 0.5:
                    scores['peg'] = 4.0
                elif peg <= 1.0:
                    scores['peg'] = 3.0
                elif peg <= 1.5:
                    scores['peg'] = 2.0
                elif peg <= 2.0:
                    scores['peg'] = 1.0
                else:
                    scores['peg'] = 0.0
                
                details['PEG'] = f"{peg:.2f} ({scores['peg']}/4점)"
            else:
                scores['peg'] = 0.0
                details['PEG'] = "성장률 음수 (0/4점)"
            
            # 4. 배당수익률 - 3점
            dividend_per_share = financial_data.get('dividend_per_share', 0)
            dividend_yield = RatioCalculator.calculate_dividend_yield(dividend_per_share, stock_price)
            
            if dividend_yield >= 0.05:
                scores['dividend_yield'] = 3.0
            elif dividend_yield >= self.buffett_criteria['dividend_yield_min']:
                scores['dividend_yield'] = 2.5
            elif dividend_yield >= 0.01:
                scores['dividend_yield'] = 1.5
            elif dividend_yield > 0:
                scores['dividend_yield'] = 0.5
            else:
                scores['dividend_yield'] = 0.0
            
            details['배당수익률'] = f"{dividend_yield:.2%} ({scores['dividend_yield']}/3점)"
            
            # 5. EV/EBITDA - 2점
            ebitda = financial_data.get('ebitda', 0)
            total_debt = financial_data.get('total_debt', 0)
            cash = financial_data.get('cash', 0)
            enterprise_value = market_cap + total_debt - cash
            ev_ebitda = safe_divide(enterprise_value, ebitda)
            
            if ev_ebitda <= 8:
                scores['ev_ebitda'] = 2.0
            elif ev_ebitda <= 12:
                scores['ev_ebitda'] = 1.5
            elif ev_ebitda <= 20:
                scores['ev_ebitda'] = 1.0
            elif ev_ebitda <= 30:
                scores['ev_ebitda'] = 0.5
            else:
                scores['ev_ebitda'] = 0.0
            
            details['EV/EBITDA'] = f"{ev_ebitda:.2f} ({scores['ev_ebitda']}/2점)"
            
        except Exception as e:
            logger.error(f"가치평가 지표 계산 오류: {e}")
            return ScoreBreakdown("가치평가", 20, 0, 0, {"오류": str(e)})
        
        total_score = sum(scores.values())
        
        return ScoreBreakdown(
            category="가치평가",
            max_score=20,
            actual_score=total_score,
            percentage=(total_score / 20) * 100,
            details=details
        )
    
    def calculate_total_score(self, financial_data: Dict, market_data: Dict = None) -> BuffettAnalysisResult:
        """종합 점수 계산 및 분석 결과 생성"""
        
        if market_data is None:
            market_data = {}
        
        # 각 카테고리별 점수 계산
        profitability = self.calculate_profitability_score(financial_data)
        growth = self.calculate_growth_score(financial_data)
        stability = self.calculate_stability_score(financial_data)
        efficiency = self.calculate_efficiency_score(financial_data)
        valuation = self.calculate_valuation_score(financial_data, market_data)
        
        # 총점 계산
        total_score = (
            profitability.actual_score + 
            growth.actual_score + 
            stability.actual_score + 
            efficiency.actual_score + 
            valuation.actual_score
        )
        
        # 등급 산정
        grade = self._calculate_grade(total_score)
        
        # 투자 추천 등급
        recommendation = self._calculate_recommendation(total_score, profitability, stability)
        
        # 리스크 레벨
        risk_level = self._calculate_risk_level(stability, valuation)
        
        # 강점과 약점 분석
        strengths, weaknesses = self._analyze_strengths_weaknesses(
            profitability, growth, stability, efficiency, valuation
        )
        
        # 투자 논리 생성
        investment_thesis = self._generate_investment_thesis(
            total_score, grade, strengths, weaknesses
        )
        
        return BuffettAnalysisResult(
            stock_code=financial_data.get('stock_code', ''),
            company_name=financial_data.get('company_name', ''),
            analysis_date=date.today(),
            total_score=total_score,
            grade=grade,
            recommendation=recommendation,
            risk_level=risk_level,
            profitability=profitability,
            growth=growth,
            stability=stability,
            efficiency=efficiency,
            valuation=valuation,
            key_strengths=strengths,
            key_weaknesses=weaknesses,
            investment_thesis=investment_thesis
        )
    
    def _calculate_margin_consistency(self, margins_history: List[float]) -> float:
        """마진 일관성 계산 (0-5점)"""
        if len(margins_history) < 3:
            return 0.0
        
        # 마진의 변동성이 낮을수록 높은 점수
        margin_std = np.std(margins_history) if len(margins_history) > 1 else 0
        avg_margin = np.mean(margins_history)
        
        if avg_margin <= 0:
            return 0.0
        
        cv = margin_std / avg_margin  # 변동계수
        
        if cv <= 0.1:
            return 5.0
        elif cv <= 0.2:
            return 4.0
        elif cv <= 0.3:
            return 3.0
        elif cv <= 0.5:
            return 2.0
        else:
            return 1.0
    
    def _calculate_growth_consistency(self, financial_data: Dict) -> float:
        """성장 지속성 계산 (0-5점)"""
        revenue_history = financial_data.get('revenue_history', [])
        income_history = financial_data.get('net_income_history', [])
        
        if len(revenue_history) < 3 or len(income_history) < 3:
            return 0.0
        
        # 매출과 순이익이 모두 증가하는 년도의 비율
        revenue_increases = sum(1 for i in range(1, len(revenue_history)) 
                              if revenue_history[i] > revenue_history[i-1])
        income_increases = sum(1 for i in range(1, len(income_history)) 
                             if income_history[i] > income_history[i-1])
        
        total_years = len(revenue_history) - 1
        growth_ratio = (revenue_increases + income_increases) / (total_years * 2)
        
        if growth_ratio >= 0.8:
            return 5.0
        elif growth_ratio >= 0.6:
            return 4.0
        elif growth_ratio >= 0.4:
            return 3.0
        elif growth_ratio >= 0.2:
            return 2.0
        else:
            return 1.0
    
    def _calculate_altman_z_score(self, financial_data: Dict) -> float:
        """알트만 Z-Score 계산"""
        try:
            # 알트만 Z-Score = 1.2A + 1.4B + 3.3C + 0.6D + 1.0E
            # A = 운전자본/총자산
            # B = 이익잉여금/총자산  
            # C = EBIT/총자산
            # D = 시가총액/부채총액
            # E = 매출/총자산
            
            total_assets = financial_data.get('total_assets', 0)
            if total_assets == 0:
                return 0.0
            
            # A: 운전자본/총자산
            current_assets = financial_data.get('current_assets', 0)
            current_liabilities = financial_data.get('current_liabilities', 0)
            working_capital = current_assets - current_liabilities
            a = working_capital / total_assets
            
            # B: 이익잉여금/총자산
            retained_earnings = financial_data.get('retained_earnings', 0)
            b = retained_earnings / total_assets
            
            # C: EBIT/총자산
            ebit = financial_data.get('ebit', 0)
            c = ebit / total_assets
            
            # D: 시가총액/부채총액
            market_cap = financial_data.get('market_cap', 0)
            total_debt = financial_data.get('total_debt', 0)
            d = safe_divide(market_cap, total_debt, 0)
            
            # E: 매출/총자산
            revenue = financial_data.get('revenue', 0)
            e = revenue / total_assets
            
            z_score = 1.2*a + 1.4*b + 3.3*c + 0.6*d + 1.0*e
            
            return max(0, z_score)
            
        except Exception:
            return 0.0
    
    def _calculate_grade(self, total_score: float) -> str:
        """등급 계산"""
        if total_score >= 90:
            return "A+"
        elif total_score >= 80:
            return "A"
        elif total_score >= 70:
            return "B+"
        elif total_score >= 60:
            return "B"
        elif total_score >= 50:
            return "C+"
        elif total_score >= 40:
            return "C"
        elif total_score >= 30:
            return "D"
        else:
            return "F"
    
    def _calculate_recommendation(self, total_score: float, 
                                profitability: ScoreBreakdown, 
                                stability: ScoreBreakdown) -> str:
        """투자 추천 등급"""
        
        # 워런 버핏 스타일: 안정성과 수익성을 중시
        stability_good = stability.percentage >= 70
        profitability_good = profitability.percentage >= 70
        
        if total_score >= 85 and stability_good and profitability_good:
            return "Strong Buy"
        elif total_score >= 75 and (stability_good or profitability_good):
            return "Buy"
        elif total_score >= 60:
            return "Hold"
        elif total_score >= 45:
            return "Weak Hold"
        else:
            return "Sell"
    
    def _calculate_risk_level(self, stability: ScoreBreakdown, 
                            valuation: ScoreBreakdown) -> str:
        """리스크 레벨 계산"""
        avg_score = (stability.percentage + valuation.percentage) / 2
        
        if avg_score >= 80:
            return "Low"
        elif avg_score >= 60:
            return "Medium"
        else:
            return "High"
    
    def _analyze_strengths_weaknesses(self, profitability: ScoreBreakdown,
                                    growth: ScoreBreakdown,
                                    stability: ScoreBreakdown,
                                    efficiency: ScoreBreakdown,
                                    valuation: ScoreBreakdown) -> Tuple[List[str], List[str]]:
        """강점과 약점 분석"""
        
        categories = [
            ("수익성", profitability.percentage),
            ("성장성", growth.percentage),
            ("안정성", stability.percentage),
            ("효율성", efficiency.percentage),
            ("가치평가", valuation.percentage)
        ]
        
        strengths = []
        weaknesses = []
        
        for name, score in categories:
            if score >= 80:
                strengths.append(f"우수한 {name} ({score:.1f}%)")
            elif score >= 60:
                strengths.append(f"양호한 {name} ({score:.1f}%)")
            elif score < 40:
                weaknesses.append(f"부족한 {name} ({score:.1f}%)")
        
        # 구체적인 강점/약점 추가
        if profitability.percentage >= 80:
            strengths.append("높은 수익성과 마진")
        if stability.percentage >= 80:
            strengths.append("탄탄한 재무구조")
        if valuation.percentage >= 80:
            strengths.append("매력적인 밸류에이션")
        
        if stability.percentage < 50:
            weaknesses.append("재무 안정성 우려")
        if growth.percentage < 30:
            weaknesses.append("성장성 부족")
        if valuation.percentage < 30:
            weaknesses.append("고평가 우려")
        
        return strengths[:5], weaknesses[:5]  # 최대 5개씩
    
    def _generate_investment_thesis(self, total_score: float, grade: str,
                                  strengths: List[str], weaknesses: List[str]) -> str:
        """투자 논리 생성"""
        
        if total_score >= 80:
            thesis = f"워런 버핏 기준으로 우수한 투자 대상입니다 ({grade}등급, {total_score:.1f}점). "
            thesis += "주요 강점: " + ", ".join(strengths[:3]) + ". "
            if weaknesses:
                thesis += "다만 " + weaknesses[0] + " 부분은 모니터링이 필요합니다."
            else:
                thesis += "전반적으로 균형잡힌 우량기업입니다."
                
        elif total_score >= 60:
            thesis = f"일부 매력적인 요소가 있는 기업입니다 ({grade}등급, {total_score:.1f}점). "
            if strengths:
                thesis += strengths[0] + "이 장점이지만, "
            if weaknesses:
                thesis += weaknesses[0] + " 등의 개선이 필요합니다."
                
        else:
            thesis = f"현재 워런 버핏 기준으로는 투자 매력도가 낮습니다 ({grade}등급, {total_score:.1f}점). "
            if weaknesses:
                thesis += "주요 우려사항: " + ", ".join(weaknesses[:2]) + ". "
            thesis += "개선 여부를 지켜본 후 재검토가 필요합니다."
        
        return thesis

# 사용 예시 및 테스트
if __name__ == "__main__":
    # 샘플 데이터로 테스트
    sample_financial_data = {
        'stock_code': '005930',
        'company_name': '삼성전자',
        'net_income': 26900000000000,
        'shareholders_equity': 286700000000000,
        'total_assets': 400000000000000,
        'revenue': 279600000000000,
        'operating_income': 37700000000000,
        'ebitda': 45000000000000,
        'current_assets': 180000000000000,
        'current_liabilities': 60000000000000,
        'total_debt': 30000000000000,
        'cash': 100000000000000,
        'inventory': 30000000000000,
        'receivables': 40000000000000,
        'ebit': 37000000000000,
        'interest_expense': 1000000000000,
        'shares_outstanding': 5900000000,
        'eps': 4500,
        'bps': 48000,
        'dividend_per_share': 361,
        'revenue_history': [200000000000000, 240000000000000, 279600000000000],
        'net_income_history': [19000000000000, 23000000000000, 26900000000000],
        'eps_history': [3200, 3900, 4500],
        'equity_history': [240000000000000, 260000000000000, 286700000000000],
        'dividend_history': [300, 330, 361],
        'eps_growth_rate': 0.12
    }
    
    sample_market_data = {
        'stock_price': 72000
    }
    
    # 워런 버핏 스코어카드 계산
    scorecard = BuffettScorecard()
    result = scorecard.calculate_total_score(sample_financial_data, sample_market_data)
    
    print("🎯 워런 버핏 스코어카드 분석 결과")
    print("=" * 50)
    print(f"종목: {result.company_name} ({result.stock_code})")
    print(f"분석일: {result.analysis_date}")
    print(f"총점: {result.total_score:.1f}/100점 ({result.grade}등급)")
    print(f"투자추천: {result.recommendation}")
    print(f"리스크: {result.risk_level}")
    print()
    
    print("📊 카테고리별 점수:")
    for category in [result.profitability, result.growth, result.stability, 
                    result.efficiency, result.valuation]:
        print(f"  {category.category}: {category.actual_score:.1f}/{category.max_score}점 ({category.percentage:.1f}%)")
    print()
    
    print("✅ 주요 강점:")
    for strength in result.key_strengths:
        print(f"  • {strength}")
    print()
    
    if result.key_weaknesses:
        print("⚠️ 주요 약점:")
        for weakness in result.key_weaknesses:
            print(f"  • {weakness}")
        print()
    
    print("💡 투자 논리:")
    print(f"  {result.investment_thesis}")
    print()
    
    print("📈 수익성 상세:")
    for key, value in result.profitability.details.items():
        print(f"  {key}: {value}")
