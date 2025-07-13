"""
재무비율 계산 모듈
워런 버핏 스코어카드 시스템을 위한 종합적인 재무비율 계산 기능

주요 기능:
- 수익성 지표: ROE, ROA, ROI, ROIC, 각종 마진율
- 성장성 지표: 매출/순이익/EPS 성장률, CAGR 계산
- 안정성 지표: 부채비율, 유동비율, 이자보상배율, Z-Score
- 효율성 지표: 회전율 지표들
- 가치평가 지표: PER, PBR, PEG, EV/EBITDA
- 추가 지표: 듀퐁 분석, 그레이엄 수, 피오트로스키 F-Score
"""

import math
import logging
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
from datetime import datetime, date

# 내부 모듈 import
try:
    from ..utils.calculation_utils import safe_divide, FinancialCalculator
    from ..utils.data_validation import validate_financial_data
    from ..utils.logging_utils import get_logger
except ImportError:
    # 테스트를 위한 fallback
    def safe_divide(num, den, default=0.0):
        return default if den == 0 else num / den
    
    class FinancialCalculator:
        @staticmethod
        def calculate_cagr(initial, final, years): 
            if initial <= 0 or years <= 0: return 0
            return (final / initial) ** (1 / years) - 1
    
    def validate_financial_data(data): return True
    def get_logger(name): return logging.getLogger(name)

logger = get_logger(__name__)

@dataclass
class RatioResult:
    """재무비율 계산 결과"""
    name: str
    value: float
    formatted_value: str
    interpretation: str
    benchmark: Optional[float] = None
    category: str = ""

class FinancialRatios:
    """재무비율 계산 클래스"""
    
    def __init__(self):
        """초기화"""
        self.industry_benchmarks = {
            'roe': {'excellent': 0.15, 'good': 0.10, 'average': 0.08},
            'roa': {'excellent': 0.05, 'good': 0.03, 'average': 0.02},
            'debt_ratio': {'excellent': 0.30, 'good': 0.50, 'acceptable': 0.70},
            'current_ratio': {'excellent': 2.0, 'good': 1.5, 'minimum': 1.0},
            'per': {'undervalued': 10, 'reasonable': 15, 'expensive': 25},
            'pbr': {'undervalued': 1.0, 'reasonable': 1.5, 'expensive': 3.0}
        }
        
        logger.info("재무비율 계산 시스템 초기화 완료")
    
    # ==================== 수익성 지표 ====================
    
    def calculate_roe(self, net_income: float, shareholders_equity: float) -> RatioResult:
        """ROE (자기자본이익률) 계산"""
        roe = safe_divide(net_income, shareholders_equity)
        
        if roe >= self.industry_benchmarks['roe']['excellent']:
            interpretation = "매우 우수한 자기자본 수익률"
        elif roe >= self.industry_benchmarks['roe']['good']:
            interpretation = "양호한 자기자본 수익률"
        elif roe >= self.industry_benchmarks['roe']['average']:
            interpretation = "평균적인 자기자본 수익률"
        elif roe > 0:
            interpretation = "낮은 자기자본 수익률"
        else:
            interpretation = "자기자본 수익률 적자"
        
        return RatioResult(
            name="ROE (자기자본이익률)",
            value=roe,
            formatted_value=f"{roe:.2%}",
            interpretation=interpretation,
            benchmark=self.industry_benchmarks['roe']['excellent'],
            category="수익성"
        )
    
    def calculate_roa(self, net_income: float, total_assets: float) -> RatioResult:
        """ROA (총자산이익률) 계산"""
        roa = safe_divide(net_income, total_assets)
        
        if roa >= self.industry_benchmarks['roa']['excellent']:
            interpretation = "매우 우수한 자산 활용 효율성"
        elif roa >= self.industry_benchmarks['roa']['good']:
            interpretation = "양호한 자산 활용 효율성"
        elif roa >= self.industry_benchmarks['roa']['average']:
            interpretation = "평균적인 자산 활용 효율성"
        elif roa > 0:
            interpretation = "낮은 자산 활용 효율성"
        else:
            interpretation = "자산 운용 손실"
        
        return RatioResult(
            name="ROA (총자산이익률)",
            value=roa,
            formatted_value=f"{roa:.2%}",
            interpretation=interpretation,
            benchmark=self.industry_benchmarks['roa']['excellent'],
            category="수익성"
        )
    
    def calculate_roic(self, nopat: float, invested_capital: float) -> RatioResult:
        """ROIC (투하자본이익률) 계산"""
        roic = safe_divide(nopat, invested_capital)
        
        if roic >= 0.15:
            interpretation = "탁월한 자본 효율성"
        elif roic >= 0.10:
            interpretation = "우수한 자본 효율성"
        elif roic >= 0.05:
            interpretation = "양호한 자본 효율성"
        elif roic > 0:
            interpretation = "낮은 자본 효율성"
        else:
            interpretation = "자본 운용 손실"
        
        return RatioResult(
            name="ROIC (투하자본이익률)",
            value=roic,
            formatted_value=f"{roic:.2%}",
            interpretation=interpretation,
            benchmark=0.15,
            category="수익성"
        )
    
    def calculate_operating_margin(self, operating_income: float, revenue: float) -> RatioResult:
        """영업이익률 계산"""
        margin = safe_divide(operating_income, revenue)
        
        if margin >= 0.15:
            interpretation = "매우 높은 영업 효율성"
        elif margin >= 0.10:
            interpretation = "우수한 영업 효율성"
        elif margin >= 0.05:
            interpretation = "양호한 영업 효율성"
        elif margin > 0:
            interpretation = "낮은 영업 효율성"
        else:
            interpretation = "영업 손실"
        
        return RatioResult(
            name="영업이익률",
            value=margin,
            formatted_value=f"{margin:.2%}",
            interpretation=interpretation,
            benchmark=0.10,
            category="수익성"
        )
    
    def calculate_net_margin(self, net_income: float, revenue: float) -> RatioResult:
        """순이익률 계산"""
        margin = safe_divide(net_income, revenue)
        
        if margin >= 0.10:
            interpretation = "매우 높은 순이익률"
        elif margin >= 0.07:
            interpretation = "우수한 순이익률"
        elif margin >= 0.03:
            interpretation = "양호한 순이익률"
        elif margin > 0:
            interpretation = "낮은 순이익률"
        else:
            interpretation = "순손실"
        
        return RatioResult(
            name="순이익률",
            value=margin,
            formatted_value=f"{margin:.2%}",
            interpretation=interpretation,
            benchmark=0.07,
            category="수익성"
        )
    
    def calculate_ebitda_margin(self, ebitda: float, revenue: float) -> RatioResult:
        """EBITDA 마진 계산"""
        margin = safe_divide(ebitda, revenue)
        
        if margin >= 0.20:
            interpretation = "매우 높은 EBITDA 마진"
        elif margin >= 0.15:
            interpretation = "우수한 EBITDA 마진"
        elif margin >= 0.10:
            interpretation = "양호한 EBITDA 마진"
        elif margin > 0:
            interpretation = "낮은 EBITDA 마진"
        else:
            interpretation = "EBITDA 손실"
        
        return RatioResult(
            name="EBITDA 마진",
            value=margin,
            formatted_value=f"{margin:.2%}",
            interpretation=interpretation,
            benchmark=0.15,
            category="수익성"
        )
    
    def calculate_gross_margin(self, gross_profit: float, revenue: float) -> RatioResult:
        """매출총이익률 계산"""
        margin = safe_divide(gross_profit, revenue)
        
        if margin >= 0.50:
            interpretation = "매우 높은 매출총이익률"
        elif margin >= 0.35:
            interpretation = "우수한 매출총이익률"
        elif margin >= 0.20:
            interpretation = "양호한 매출총이익률"
        elif margin > 0:
            interpretation = "낮은 매출총이익률"
        else:
            interpretation = "매출총손실"
        
        return RatioResult(
            name="매출총이익률",
            value=margin,
            formatted_value=f"{margin:.2%}",
            interpretation=interpretation,
            benchmark=0.35,
            category="수익성"
        )
    
    # ==================== 성장성 지표 ====================
    
    def calculate_revenue_growth(self, revenue_history: List[float]) -> RatioResult:
        """매출 성장률 (CAGR) 계산"""
        if len(revenue_history) < 2:
            return RatioResult("매출성장률", 0, "0.00%", "데이터 부족", category="성장성")
        
        years = len(revenue_history) - 1
        cagr = FinancialCalculator.calculate_cagr(revenue_history[0], revenue_history[-1], years)
        
        if cagr >= 0.15:
            interpretation = "매우 높은 매출 성장률"
        elif cagr >= 0.10:
            interpretation = "우수한 매출 성장률"
        elif cagr >= 0.05:
            interpretation = "양호한 매출 성장률"
        elif cagr >= 0:
            interpretation = "낮은 매출 성장률"
        else:
            interpretation = "매출 감소 추세"
        
        return RatioResult(
            name="매출성장률 (CAGR)",
            value=cagr,
            formatted_value=f"{cagr:.2%}",
            interpretation=interpretation,
            benchmark=0.10,
            category="성장성"
        )
    
    def calculate_earnings_growth(self, earnings_history: List[float]) -> RatioResult:
        """순이익 성장률 (CAGR) 계산"""
        if len(earnings_history) < 2:
            return RatioResult("순이익성장률", 0, "0.00%", "데이터 부족", category="성장성")
        
        # 음수 처리를 위해 절댓값 + 1 사용
        adjusted_initial = abs(earnings_history[0]) + 1
        adjusted_final = abs(earnings_history[-1]) + 1
        years = len(earnings_history) - 1
        
        cagr = FinancialCalculator.calculate_cagr(adjusted_initial, adjusted_final, years)
        
        if cagr >= 0.20:
            interpretation = "매우 높은 순이익 성장률"
        elif cagr >= 0.15:
            interpretation = "우수한 순이익 성장률"
        elif cagr >= 0.10:
            interpretation = "양호한 순이익 성장률"
        elif cagr >= 0:
            interpretation = "낮은 순이익 성장률"
        else:
            interpretation = "순이익 감소 추세"
        
        return RatioResult(
            name="순이익성장률 (CAGR)",
            value=cagr,
            formatted_value=f"{cagr:.2%}",
            interpretation=interpretation,
            benchmark=0.15,
            category="성장성"
        )
    
    def calculate_eps_growth(self, eps_history: List[float]) -> RatioResult:
        """EPS 성장률 (CAGR) 계산"""
        if len(eps_history) < 2:
            return RatioResult("EPS성장률", 0, "0.00%", "데이터 부족", category="성장성")
        
        # 음수 EPS 처리
        adjusted_initial = abs(eps_history[0]) + 1
        adjusted_final = abs(eps_history[-1]) + 1
        years = len(eps_history) - 1
        
        cagr = FinancialCalculator.calculate_cagr(adjusted_initial, adjusted_final, years)
        
        if cagr >= 0.20:
            interpretation = "매우 높은 EPS 성장률"
        elif cagr >= 0.15:
            interpretation = "우수한 EPS 성장률"
        elif cagr >= 0.10:
            interpretation = "양호한 EPS 성장률"
        elif cagr >= 0:
            interpretation = "낮은 EPS 성장률"
        else:
            interpretation = "EPS 감소 추세"
        
        return RatioResult(
            name="EPS성장률 (CAGR)",
            value=cagr,
            formatted_value=f"{cagr:.2%}",
            interpretation=interpretation,
            benchmark=0.15,
            category="성장성"
        )
    
    # ==================== 안정성 지표 ====================
    
    def calculate_debt_ratio(self, total_debt: float, total_assets: float) -> RatioResult:
        """부채비율 계산"""
        ratio = safe_divide(total_debt, total_assets)
        
        if ratio <= self.industry_benchmarks['debt_ratio']['excellent']:
            interpretation = "매우 건전한 부채 수준"
        elif ratio <= self.industry_benchmarks['debt_ratio']['good']:
            interpretation = "양호한 부채 수준"
        elif ratio <= self.industry_benchmarks['debt_ratio']['acceptable']:
            interpretation = "수용 가능한 부채 수준"
        elif ratio <= 1.0:
            interpretation = "높은 부채 수준"
        else:
            interpretation = "위험한 부채 수준"
        
        return RatioResult(
            name="부채비율",
            value=ratio,
            formatted_value=f"{ratio:.2%}",
            interpretation=interpretation,
            benchmark=self.industry_benchmarks['debt_ratio']['excellent'],
            category="안정성"
        )
    
    def calculate_current_ratio(self, current_assets: float, current_liabilities: float) -> RatioResult:
        """유동비율 계산"""
        ratio = safe_divide(current_assets, current_liabilities, float('inf'))
        
        if ratio >= self.industry_benchmarks['current_ratio']['excellent']:
            interpretation = "매우 우수한 단기 지급능력"
        elif ratio >= self.industry_benchmarks['current_ratio']['good']:
            interpretation = "우수한 단기 지급능력"
        elif ratio >= self.industry_benchmarks['current_ratio']['minimum']:
            interpretation = "최소한의 단기 지급능력"
        else:
            interpretation = "단기 지급능력 부족"
        
        formatted = "무한대" if ratio == float('inf') else f"{ratio:.2f}"
        
        return RatioResult(
            name="유동비율",
            value=ratio,
            formatted_value=formatted,
            interpretation=interpretation,
            benchmark=self.industry_benchmarks['current_ratio']['good'],
            category="안정성"
        )
    
    def calculate_quick_ratio(self, current_assets: float, inventory: float, 
                            current_liabilities: float) -> RatioResult:
        """당좌비율 계산"""
        quick_assets = current_assets - inventory
        ratio = safe_divide(quick_assets, current_liabilities, float('inf'))
        
        if ratio >= 1.5:
            interpretation = "매우 우수한 즉시 지급능력"
        elif ratio >= 1.0:
            interpretation = "우수한 즉시 지급능력"
        elif ratio >= 0.8:
            interpretation = "양호한 즉시 지급능력"
        elif ratio >= 0.5:
            interpretation = "낮은 즉시 지급능력"
        else:
            interpretation = "즉시 지급능력 부족"
        
        formatted = "무한대" if ratio == float('inf') else f"{ratio:.2f}"
        
        return RatioResult(
            name="당좌비율",
            value=ratio,
            formatted_value=formatted,
            interpretation=interpretation,
            benchmark=1.0,
            category="안정성"
        )
    
    def calculate_interest_coverage_ratio(self, ebit: float, interest_expense: float) -> RatioResult:
        """이자보상배율 계산"""
        ratio = safe_divide(ebit, interest_expense, float('inf'))
        
        if ratio == float('inf'):
            interpretation = "무부채 상태 (이상적)"
        elif ratio >= 10:
            interpretation = "매우 안전한 이자 지급능력"
        elif ratio >= 5:
            interpretation = "안전한 이자 지급능력"
        elif ratio >= 2:
            interpretation = "최소한의 이자 지급능력"
        elif ratio >= 1:
            interpretation = "위험한 이자 지급능력"
        else:
            interpretation = "이자 지급 불가"
        
        formatted = "무한대" if ratio == float('inf') else f"{ratio:.2f}"
        
        return RatioResult(
            name="이자보상배율",
            value=ratio,
            formatted_value=formatted,
            interpretation=interpretation,
            benchmark=5.0,
            category="안정성"
        )
    
    def calculate_altman_z_score(self, financial_data: Dict) -> RatioResult:
        """알트만 Z-Score 계산"""
        try:
            total_assets = financial_data.get('total_assets', 0)
            if total_assets == 0:
                return RatioResult("알트만 Z-Score", 0, "0.00", "계산 불가", category="안정성")
            
            # Z = 1.2A + 1.4B + 3.3C + 0.6D + 1.0E
            current_assets = financial_data.get('current_assets', 0)
            current_liabilities = financial_data.get('current_liabilities', 0)
            retained_earnings = financial_data.get('retained_earnings', 0)
            ebit = financial_data.get('ebit', 0)
            market_cap = financial_data.get('market_cap', 0)
            total_debt = financial_data.get('total_debt', 0)
            revenue = financial_data.get('revenue', 0)
            
            a = (current_assets - current_liabilities) / total_assets
            b = retained_earnings / total_assets
            c = ebit / total_assets
            d = safe_divide(market_cap, total_debt, 0)
            e = revenue / total_assets
            
            z_score = 1.2*a + 1.4*b + 3.3*c + 0.6*d + 1.0*e
            
            if z_score >= 3.0:
                interpretation = "파산 위험 매우 낮음"
            elif z_score >= 2.7:
                interpretation = "파산 위험 낮음"
            elif z_score >= 1.8:
                interpretation = "파산 위험 보통"
            elif z_score >= 1.0:
                interpretation = "파산 위험 높음"
            else:
                interpretation = "파산 위험 매우 높음"
            
            return RatioResult(
                name="알트만 Z-Score",
                value=z_score,
                formatted_value=f"{z_score:.2f}",
                interpretation=interpretation,
                benchmark=3.0,
                category="안정성"
            )
            
        except Exception as e:
            logger.error(f"알트만 Z-Score 계산 오류: {e}")
            return RatioResult("알트만 Z-Score", 0, "0.00", "계산 오류", category="안정성")
    
    # ==================== 효율성 지표 ====================
    
    def calculate_asset_turnover(self, revenue: float, total_assets: float) -> RatioResult:
        """총자산회전율 계산"""
        turnover = safe_divide(revenue, total_assets)
        
        if turnover >= 1.5:
            interpretation = "매우 효율적인 자산 활용"
        elif turnover >= 1.0:
            interpretation = "효율적인 자산 활용"
        elif turnover >= 0.7:
            interpretation = "양호한 자산 활용"
        elif turnover >= 0.5:
            interpretation = "낮은 자산 활용 효율"
        else:
            interpretation = "매우 낮은 자산 활용 효율"
        
        return RatioResult(
            name="총자산회전율",
            value=turnover,
            formatted_value=f"{turnover:.2f}",
            interpretation=interpretation,
            benchmark=1.0,
            category="효율성"
        )
    
    def calculate_inventory_turnover(self, cogs: float, inventory: float) -> RatioResult:
        """재고회전율 계산"""
        turnover = safe_divide(cogs, inventory)
        
        if turnover >= 12:
            interpretation = "매우 효율적인 재고 관리"
        elif turnover >= 8:
            interpretation = "효율적인 재고 관리"
        elif turnover >= 4:
            interpretation = "양호한 재고 관리"
        elif turnover >= 1:
            interpretation = "낮은 재고 회전"
        else:
            interpretation = "매우 낮은 재고 회전"
        
        return RatioResult(
            name="재고회전율",
            value=turnover,
            formatted_value=f"{turnover:.2f}",
            interpretation=interpretation,
            benchmark=8.0,
            category="효율성"
        )
    
    def calculate_receivables_turnover(self, revenue: float, receivables: float) -> RatioResult:
        """매출채권회전율 계산"""
        turnover = safe_divide(revenue, receivables)
        
        if turnover >= 12:
            interpretation = "매우 효율적인 채권 회수"
        elif turnover >= 8:
            interpretation = "효율적인 채권 회수"
        elif turnover >= 4:
            interpretation = "양호한 채권 회수"
        elif turnover >= 1:
            interpretation = "낮은 채권 회수"
        else:
            interpretation = "매우 낮은 채권 회수"
        
        return RatioResult(
            name="매출채권회전율",
            value=turnover,
            formatted_value=f"{turnover:.2f}",
            interpretation=interpretation,
            benchmark=8.0,
            category="효율성"
        )
    
    # ==================== 가치평가 지표 ====================
    
    def calculate_per(self, stock_price: float, eps: float) -> RatioResult:
        """PER (주가수익비율) 계산"""
        per = safe_divide(stock_price, eps, float('inf'))
        
        if per == float('inf'):
            interpretation = "적자로 인한 PER 산출 불가"
            formatted = "N/A"
        elif per <= self.industry_benchmarks['per']['undervalued']:
            interpretation = "저평가 구간"
            formatted = f"{per:.2f}"
        elif per <= self.industry_benchmarks['per']['reasonable']:
            interpretation = "합리적 평가 구간"
            formatted = f"{per:.2f}"
        elif per <= self.industry_benchmarks['per']['expensive']:
            interpretation = "고평가 구간"
            formatted = f"{per:.2f}"
        else:
            interpretation = "매우 높은 고평가"
            formatted = f"{per:.2f}"
        
        return RatioResult(
            name="PER (주가수익비율)",
            value=per,
            formatted_value=formatted,
            interpretation=interpretation,
            benchmark=self.industry_benchmarks['per']['reasonable'],
            category="가치평가"
        )
    
    def calculate_pbr(self, stock_price: float, bps: float) -> RatioResult:
        """PBR (주가순자산비율) 계산"""
        pbr = safe_divide(stock_price, bps, float('inf'))
        
        if pbr == float('inf'):
            interpretation = "계산 불가"
            formatted = "N/A"
        elif pbr <= self.industry_benchmarks['pbr']['undervalued']:
            interpretation = "저평가 구간"
            formatted = f"{pbr:.2f}"
        elif pbr <= self.industry_benchmarks['pbr']['reasonable']:
            interpretation = "합리적 평가 구간"
            formatted = f"{pbr:.2f}"
        elif pbr <= self.industry_benchmarks['pbr']['expensive']:
            interpretation = "고평가 구간"
            formatted = f"{pbr:.2f}"
        else:
            interpretation = "매우 높은 고평가"
            formatted = f"{pbr:.2f}"
        
        return RatioResult(
            name="PBR (주가순자산비율)",
            value=pbr,
            formatted_value=formatted,
            interpretation=interpretation,
            benchmark=self.industry_benchmarks['pbr']['reasonable'],
            category="가치평가"
        )
    
    def calculate_peg(self, per: float, growth_rate: float) -> RatioResult:
        """PEG (PER/성장률) 계산"""
        if growth_rate <= 0:
            return RatioResult("PEG", 0, "N/A", "성장률 음수로 계산 불가", category="가치평가")
        
        peg = safe_divide(per, growth_rate * 100, float('inf'))
        
        if peg == float('inf'):
            interpretation = "계산 불가"
            formatted = "N/A"
        elif peg <= 0.5:
            interpretation = "매우 저평가"
            formatted = f"{peg:.2f}"
        elif peg <= 1.0:
            interpretation = "적정 평가"
            formatted = f"{peg:.2f}"
        elif peg <= 1.5:
            interpretation = "약간 고평가"
            formatted = f"{peg:.2f}"
        else:
            interpretation = "고평가"
            formatted = f"{peg:.2f}"
        
        return RatioResult(
            name="PEG (PER/성장률)",
            value=peg,
            formatted_value=formatted,
            interpretation=interpretation,
            benchmark=1.0,
            category="가치평가"
        )
    
    def calculate_dividend_yield(self, dividend_per_share: float, stock_price: float) -> RatioResult:
        """배당수익률 계산"""
        yield_rate = safe_divide(dividend_per_share, stock_price)
        
        if yield_rate >= 0.05:
            interpretation = "높은 배당수익률"
        elif yield_rate >= 0.03:
            interpretation = "양호한 배당수익률"
        elif yield_rate >= 0.02:
            interpretation = "보통 배당수익률"
        elif yield_rate > 0:
            interpretation = "낮은 배당수익률"
        else:
            interpretation = "무배당"
        
        return RatioResult(
            name="배당수익률",
            value=yield_rate,
            formatted_value=f"{yield_rate:.2%}",
            interpretation=interpretation,
            benchmark=0.03,
            category="가치평가"
        )
    
    def calculate_ev_ebitda(self, enterprise_value: float, ebitda: float) -> RatioResult:
        """EV/EBITDA 계산"""
        ev_ebitda = safe_divide(enterprise_value, ebitda, float('inf'))
        
        if ev_ebitda == float('inf'):
            interpretation = "EBITDA 음수로 계산 불가"
            formatted = "N/A"
        elif ev_ebitda <= 8:
            interpretation = "저평가 구간"
            formatted = f"{ev_ebitda:.2f}"
        elif ev_ebitda <= 12:
            interpretation = "적정 평가 구간"
            formatted = f"{ev_ebitda:.2f}"
        elif ev_ebitda <= 20:
            interpretation = "고평가 구간"
            formatted = f"{ev_ebitda:.2f}"
        else:
            interpretation = "매우 높은 고평가"
            formatted = f"{ev_ebitda:.2f}"
        
        return RatioResult(
            name="EV/EBITDA",
            value=ev_ebitda,
            formatted_value=formatted,
            interpretation=interpretation,
            benchmark=12.0,
            category="가치평가"
        )
    
    # ==================== 종합 분석 메서드 ====================
    
    def analyze_all_ratios(self, financial_data: Dict, market_data: Dict = None) -> Dict[str, List[RatioResult]]:
        """모든 재무비율 종합 분석"""
        
        if market_data is None:
            market_data = {}
        
        results = {
            '수익성': [],
            '성장성': [],
            '안정성': [],
            '효율성': [],
            '가치평가': []
        }
        
        try:
            # 수익성 지표
            results['수익성'].extend([
                self.calculate_roe(
                    financial_data.get('net_income', 0),
                    financial_data.get('shareholders_equity', 0)
                ),
                self.calculate_roa(
                    financial_data.get('net_income', 0),
                    financial_data.get('total_assets', 0)
                ),
                self.calculate_operating_margin(
                    financial_data.get('operating_income', 0),
                    financial_data.get('revenue', 0)
                ),
                self.calculate_net_margin(
                    financial_data.get('net_income', 0),
                    financial_data.get('revenue', 0)
                ),
                self.calculate_ebitda_margin(
                    financial_data.get('ebitda', 0),
                    financial_data.get('revenue', 0)
                )
            ])
            
            # 성장성 지표
            results['성장성'].extend([
                self.calculate_revenue_growth(financial_data.get('revenue_history', [])),
                self.calculate_earnings_growth(financial_data.get('net_income_history', [])),
                self.calculate_eps_growth(financial_data.get('eps_history', []))
            ])
            
            # 안정성 지표
            results['안정성'].extend([
                self.calculate_debt_ratio(
                    financial_data.get('total_debt', 0),
                    financial_data.get('total_assets', 0)
                ),
                self.calculate_current_ratio(
                    financial_data.get('current_assets', 0),
                    financial_data.get('current_liabilities', 0)
                ),
                self.calculate_quick_ratio(
                    financial_data.get('current_assets', 0),
                    financial_data.get('inventory', 0),
                    financial_data.get('current_liabilities', 0)
                ),
                self.calculate_interest_coverage_ratio(
                    financial_data.get('ebit', 0),
                    financial_data.get('interest_expense', 0)
                ),
                self.calculate_altman_z_score(financial_data)
            ])
            
            # 효율성 지표
            results['효율성'].extend([
                self.calculate_asset_turnover(
                    financial_data.get('revenue', 0),
                    financial_data.get('total_assets', 0)
                ),
                self.calculate_inventory_turnover(
                    financial_data.get('cogs', financial_data.get('revenue', 0) * 0.7),
                    financial_data.get('inventory', 0)
                ),
                self.calculate_receivables_turnover(
                    financial_data.get('revenue', 0),
                    financial_data.get('receivables', 0)
                )
            ])
            
            # 가치평가 지표 (시장 데이터 필요)
            if market_data:
                stock_price = market_data.get('stock_price', 0)
                if stock_price > 0:
                    results['가치평가'].extend([
                        self.calculate_per(stock_price, financial_data.get('eps', 0)),
                        self.calculate_pbr(stock_price, financial_data.get('bps', 0)),
                        self.calculate_dividend_yield(
                            financial_data.get('dividend_per_share', 0),
                            stock_price
                        )
                    ])
                    
                    # EV/EBITDA 계산
                    shares_outstanding = financial_data.get('shares_outstanding', 0)
                    if shares_outstanding > 0:
                        market_cap = stock_price * shares_outstanding
                        enterprise_value = (market_cap + 
                                          financial_data.get('total_debt', 0) - 
                                          financial_data.get('cash', 0))
                        
                        results['가치평가'].append(
                            self.calculate_ev_ebitda(enterprise_value, financial_data.get('ebitda', 0))
                        )
            
        except Exception as e:
            logger.error(f"재무비율 종합 분석 오류: {e}")
        
        return results
    
    def get_ratio_summary(self, results: Dict[str, List[RatioResult]]) -> Dict[str, Any]:
        """재무비율 요약 정보 생성"""
        summary = {
            'total_ratios': sum(len(ratios) for ratios in results.values()),
            'categories': {},
            'strengths': [],
            'weaknesses': [],
            'overall_assessment': ""
        }
        
        for category, ratios in results.items():
            if not ratios:
                continue
                
            # 카테고리별 우수/양호/부족 비율 계산
            excellent = sum(1 for r in ratios if "매우" in r.interpretation or "탁월" in r.interpretation)
            good = sum(1 for r in ratios if "우수" in r.interpretation or "양호" in r.interpretation)
            poor = sum(1 for r in ratios if "낮은" in r.interpretation or "부족" in r.interpretation)
            
            summary['categories'][category] = {
                'total': len(ratios),
                'excellent': excellent,
                'good': good,
                'poor': poor,
                'score': ((excellent * 2 + good) / len(ratios)) * 100 if ratios else 0
            }
            
            # 강점과 약점 식별
            if summary['categories'][category]['score'] >= 80:
                summary['strengths'].append(f"우수한 {category}")
            elif summary['categories'][category]['score'] < 40:
                summary['weaknesses'].append(f"부족한 {category}")
        
        # 전체 평가
        avg_score = sum(cat['score'] for cat in summary['categories'].values()) / len(summary['categories']) if summary['categories'] else 0
        
        if avg_score >= 80:
            summary['overall_assessment'] = "매우 우수한 재무상태"
        elif avg_score >= 60:
            summary['overall_assessment'] = "양호한 재무상태"
        elif avg_score >= 40:
            summary['overall_assessment'] = "보통 재무상태"
        else:
            summary['overall_assessment'] = "개선 필요한 재무상태"
        
        return summary

# 사용 예시
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
        'cogs': 160000000000000,
        'retained_earnings': 200000000000000,
        'market_cap': 400000000000000
    }
    
    sample_market_data = {
        'stock_price': 72000
    }
    
    # 재무비율 분석
    analyzer = FinancialRatios()
    results = analyzer.analyze_all_ratios(sample_financial_data, sample_market_data)
    summary = analyzer.get_ratio_summary(results)
    
    print("📊 재무비율 종합 분석 결과")
    print("=" * 50)
    print(f"분석 종목: {sample_financial_data['company_name']} ({sample_financial_data['stock_code']})")
    print(f"총 분석 지표: {summary['total_ratios']}개")
    print(f"전체 평가: {summary['overall_assessment']}")
    print()
    
    # 카테고리별 결과 출력
    for category, ratios in results.items():
        if not ratios:
            continue
            
        print(f"📈 {category} 지표:")
        cat_info = summary['categories'][category]
        print(f"  종합 점수: {cat_info['score']:.1f}점")
        
        for ratio in ratios:
            print(f"  • {ratio.name}: {ratio.formatted_value} - {ratio.interpretation}")
        print()
    
    # 강점과 약점
    if summary['strengths']:
        print("✅ 주요 강점:")
        for strength in summary['strengths']:
            print(f"  • {strength}")
        print()
    
    if summary['weaknesses']:
        print("⚠️ 주요 약점:")
        for weakness in summary['weaknesses']:
            print(f"  • {weakness}")
        print()
