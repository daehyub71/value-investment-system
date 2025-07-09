"""
계산 유틸리티
재무 분석 및 투자 계산 관련 유틸리티 함수들
"""

import math
import numpy as np
import pandas as pd
from typing import Union, List, Optional, Dict, Tuple, Any
from scipy import stats
import warnings
import logging

logger = logging.getLogger(__name__)

class CalculationError(Exception):
    """계산 오류"""
    pass

class FinancialCalculator:
    """재무 계산 클래스"""
    
    @staticmethod
    def calculate_return(initial_value: float, final_value: float, 
                        periods: Optional[int] = None, annualized: bool = False) -> float:
        """수익률 계산"""
        if initial_value <= 0:
            raise CalculationError("초기값은 0보다 커야 합니다.")
        
        if periods is None:
            # 단순 수익률
            return (final_value - initial_value) / initial_value
        else:
            # 연평균 수익률
            total_return = (final_value / initial_value) - 1
            if annualized and periods > 0:
                return (1 + total_return) ** (1 / periods) - 1
            return total_return
    
    @staticmethod
    def calculate_cagr(initial_value: float, final_value: float, years: float) -> float:
        """연평균 성장률 (CAGR) 계산"""
        if initial_value <= 0:
            raise CalculationError("초기값은 0보다 커야 합니다.")
        
        if years <= 0:
            raise CalculationError("기간은 0보다 커야 합니다.")
        
        return (final_value / initial_value) ** (1 / years) - 1
    
    @staticmethod
    def calculate_volatility(returns: List[float], annualized: bool = True) -> float:
        """변동성 계산"""
        if len(returns) < 2:
            raise CalculationError("최소 2개 이상의 수익률 데이터가 필요합니다.")
        
        returns_array = np.array(returns)
        volatility = np.std(returns_array, ddof=1)
        
        if annualized:
            volatility *= np.sqrt(252)  # 연간 거래일 수
        
        return volatility
    
    @staticmethod
    def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.02) -> float:
        """샤프 비율 계산"""
        if len(returns) < 2:
            raise CalculationError("최소 2개 이상의 수익률 데이터가 필요합니다.")
        
        returns_array = np.array(returns)
        excess_returns = returns_array - risk_free_rate / 252  # 일일 무위험 수익률
        
        if np.std(excess_returns) == 0:
            return 0.0
        
        return np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)
    
    @staticmethod
    def calculate_max_drawdown(values: List[float]) -> Dict[str, float]:
        """최대 낙폭 계산"""
        if len(values) < 2:
            raise CalculationError("최소 2개 이상의 가격 데이터가 필요합니다.")
        
        values_array = np.array(values)
        peak = np.maximum.accumulate(values_array)
        drawdown = (values_array - peak) / peak
        
        max_drawdown = np.min(drawdown)
        max_drawdown_idx = np.argmin(drawdown)
        
        # 최대 낙폭이 발생한 시점의 이전 고점 찾기
        peak_before_drawdown = peak[max_drawdown_idx]
        peak_idx = np.where(values_array[:max_drawdown_idx + 1] == peak_before_drawdown)[0][-1]
        
        return {
            'max_drawdown': max_drawdown,
            'max_drawdown_percent': max_drawdown * 100,
            'peak_index': peak_idx,
            'trough_index': max_drawdown_idx,
            'peak_value': peak_before_drawdown,
            'trough_value': values_array[max_drawdown_idx]
        }
    
    @staticmethod
    def calculate_beta(stock_returns: List[float], market_returns: List[float]) -> float:
        """베타 계산"""
        if len(stock_returns) != len(market_returns):
            raise CalculationError("주식 수익률과 시장 수익률의 길이가 같아야 합니다.")
        
        if len(stock_returns) < 2:
            raise CalculationError("최소 2개 이상의 데이터가 필요합니다.")
        
        stock_array = np.array(stock_returns)
        market_array = np.array(market_returns)
        
        covariance = np.cov(stock_array, market_array)[0, 1]
        market_variance = np.var(market_array, ddof=1)
        
        if market_variance == 0:
            return 0.0
        
        return covariance / market_variance
    
    @staticmethod
    def calculate_correlation(series1: List[float], series2: List[float]) -> float:
        """상관관계 계산"""
        if len(series1) != len(series2):
            raise CalculationError("두 시리즈의 길이가 같아야 합니다.")
        
        if len(series1) < 2:
            raise CalculationError("최소 2개 이상의 데이터가 필요합니다.")
        
        return np.corrcoef(series1, series2)[0, 1]

class RatioCalculator:
    """재무비율 계산 클래스"""
    
    @staticmethod
    def calculate_roe(net_income: float, shareholders_equity: float) -> float:
        """ROE (자기자본이익률) 계산"""
        if shareholders_equity == 0:
            return 0.0
        return net_income / shareholders_equity
    
    @staticmethod
    def calculate_roa(net_income: float, total_assets: float) -> float:
        """ROA (총자산이익률) 계산"""
        if total_assets == 0:
            return 0.0
        return net_income / total_assets
    
    @staticmethod
    def calculate_roic(nopat: float, invested_capital: float) -> float:
        """ROIC (투하자본이익률) 계산"""
        if invested_capital == 0:
            return 0.0
        return nopat / invested_capital
    
    @staticmethod
    def calculate_debt_ratio(total_debt: float, total_assets: float) -> float:
        """부채비율 계산"""
        if total_assets == 0:
            return 0.0
        return total_debt / total_assets
    
    @staticmethod
    def calculate_current_ratio(current_assets: float, current_liabilities: float) -> float:
        """유동비율 계산"""
        if current_liabilities == 0:
            return float('inf')
        return current_assets / current_liabilities
    
    @staticmethod
    def calculate_quick_ratio(current_assets: float, inventory: float, 
                            current_liabilities: float) -> float:
        """당좌비율 계산"""
        if current_liabilities == 0:
            return float('inf')
        return (current_assets - inventory) / current_liabilities
    
    @staticmethod
    def calculate_per(stock_price: float, eps: float) -> float:
        """PER (주가수익비율) 계산"""
        if eps == 0:
            return float('inf')
        return stock_price / eps
    
    @staticmethod
    def calculate_pbr(stock_price: float, bps: float) -> float:
        """PBR (주가순자산비율) 계산"""
        if bps == 0:
            return float('inf')
        return stock_price / bps
    
    @staticmethod
    def calculate_peg(per: float, growth_rate: float) -> float:
        """PEG (PER/성장률) 계산"""
        if growth_rate == 0:
            return float('inf')
        return per / (growth_rate * 100)
    
    @staticmethod
    def calculate_eps(net_income: float, shares_outstanding: float) -> float:
        """EPS (주당순이익) 계산"""
        if shares_outstanding == 0:
            return 0.0
        return net_income / shares_outstanding
    
    @staticmethod
    def calculate_bps(shareholders_equity: float, shares_outstanding: float) -> float:
        """BPS (주당순자산) 계산"""
        if shares_outstanding == 0:
            return 0.0
        return shareholders_equity / shares_outstanding
    
    @staticmethod
    def calculate_dividend_yield(dividend_per_share: float, stock_price: float) -> float:
        """배당수익률 계산"""
        if stock_price == 0:
            return 0.0
        return dividend_per_share / stock_price
    
    @staticmethod
    def calculate_dividend_payout_ratio(dividend_per_share: float, eps: float) -> float:
        """배당성향 계산"""
        if eps == 0:
            return 0.0
        return dividend_per_share / eps
    
    @staticmethod
    def calculate_interest_coverage_ratio(ebit: float, interest_expense: float) -> float:
        """이자보상배율 계산"""
        if interest_expense == 0:
            return float('inf')
        return ebit / interest_expense
    
    @staticmethod
    def calculate_asset_turnover(revenue: float, total_assets: float) -> float:
        """총자산회전율 계산"""
        if total_assets == 0:
            return 0.0
        return revenue / total_assets
    
    @staticmethod
    def calculate_inventory_turnover(cogs: float, inventory: float) -> float:
        """재고회전율 계산"""
        if inventory == 0:
            return 0.0
        return cogs / inventory
    
    @staticmethod
    def calculate_receivables_turnover(revenue: float, receivables: float) -> float:
        """매출채권회전율 계산"""
        if receivables == 0:
            return 0.0
        return revenue / receivables

class ValuationCalculator:
    """기업가치 평가 계산 클래스"""
    
    @staticmethod
    def calculate_dcf(cash_flows: List[float], discount_rate: float, 
                     terminal_growth_rate: float = 0.02) -> float:
        """DCF (현금흐름할인법) 계산"""
        if not cash_flows:
            raise CalculationError("현금흐름 데이터가 필요합니다.")
        
        if discount_rate <= 0:
            raise CalculationError("할인율은 0보다 커야 합니다.")
        
        present_value = 0
        
        # 예측 기간 현금흐름 할인
        for i, cf in enumerate(cash_flows[:-1]):
            present_value += cf / (1 + discount_rate) ** (i + 1)
        
        # 터미널 가치 계산
        terminal_cf = cash_flows[-1] * (1 + terminal_growth_rate)
        terminal_value = terminal_cf / (discount_rate - terminal_growth_rate)
        terminal_pv = terminal_value / (1 + discount_rate) ** len(cash_flows)
        
        return present_value + terminal_pv
    
    @staticmethod
    def calculate_ddm(dividends: List[float], discount_rate: float,
                     growth_rate: float = 0.02) -> float:
        """DDM (배당할인모델) 계산"""
        if not dividends:
            raise CalculationError("배당 데이터가 필요합니다.")
        
        if discount_rate <= growth_rate:
            raise CalculationError("할인율은 성장률보다 커야 합니다.")
        
        # 고든 성장 모델
        next_dividend = dividends[-1] * (1 + growth_rate)
        return next_dividend / (discount_rate - growth_rate)
    
    @staticmethod
    def calculate_ev_ebitda(enterprise_value: float, ebitda: float) -> float:
        """EV/EBITDA 계산"""
        if ebitda == 0:
            return float('inf')
        return enterprise_value / ebitda
    
    @staticmethod
    def calculate_price_to_sales(market_cap: float, revenue: float) -> float:
        """PSR (주가매출비율) 계산"""
        if revenue == 0:
            return float('inf')
        return market_cap / revenue
    
    @staticmethod
    def calculate_price_to_book(market_cap: float, book_value: float) -> float:
        """Price-to-Book 계산"""
        if book_value == 0:
            return float('inf')
        return market_cap / book_value
    
    @staticmethod
    def calculate_graham_number(eps: float, bps: float) -> float:
        """그레이엄 수 계산"""
        if eps <= 0 or bps <= 0:
            return 0.0
        return math.sqrt(22.5 * eps * bps)
    
    @staticmethod
    def calculate_intrinsic_value_buffett(owner_earnings: float, 
                                        growth_rate: float = 0.05,
                                        discount_rate: float = 0.08) -> float:
        """버핏 내재가치 계산 (소유주 이익 기준)"""
        if discount_rate <= growth_rate:
            raise CalculationError("할인율은 성장률보다 커야 합니다.")
        
        # 영구 성장 모델
        return owner_earnings * (1 + growth_rate) / (discount_rate - growth_rate)

class TechnicalCalculator:
    """기술적 분석 계산 클래스"""
    
    @staticmethod
    def calculate_sma(prices: List[float], period: int) -> List[float]:
        """단순이동평균 계산"""
        if len(prices) < period:
            raise CalculationError(f"최소 {period}개의 가격 데이터가 필요합니다.")
        
        sma_values = []
        for i in range(period - 1, len(prices)):
            sma = np.mean(prices[i - period + 1:i + 1])
            sma_values.append(sma)
        
        return sma_values
    
    @staticmethod
    def calculate_ema(prices: List[float], period: int) -> List[float]:
        """지수이동평균 계산"""
        if len(prices) < period:
            raise CalculationError(f"최소 {period}개의 가격 데이터가 필요합니다.")
        
        ema_values = []
        multiplier = 2 / (period + 1)
        
        # 첫 번째 EMA는 SMA로 계산
        ema = np.mean(prices[:period])
        ema_values.append(ema)
        
        # 이후 EMA 계산
        for i in range(period, len(prices)):
            ema = (prices[i] * multiplier) + (ema * (1 - multiplier))
            ema_values.append(ema)
        
        return ema_values
    
    @staticmethod
    def calculate_rsi(prices: List[float], period: int = 14) -> List[float]:
        """RSI (상대강도지수) 계산"""
        if len(prices) < period + 1:
            raise CalculationError(f"최소 {period + 1}개의 가격 데이터가 필요합니다.")
        
        # 가격 변화 계산
        price_changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        
        gains = [change if change > 0 else 0 for change in price_changes]
        losses = [-change if change < 0 else 0 for change in price_changes]
        
        rsi_values = []
        
        # 첫 번째 RSI는 단순 평균으로 계산
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])
        
        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        
        rsi_values.append(rsi)
        
        # 이후 RSI는 지수이동평균으로 계산
        for i in range(period, len(price_changes)):
            avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
            avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
            
            if avg_loss == 0:
                rsi = 100
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            
            rsi_values.append(rsi)
        
        return rsi_values
    
    @staticmethod
    def calculate_macd(prices: List[float], fast_period: int = 12, 
                      slow_period: int = 26, signal_period: int = 9) -> Dict[str, List[float]]:
        """MACD 계산"""
        if len(prices) < slow_period:
            raise CalculationError(f"최소 {slow_period}개의 가격 데이터가 필요합니다.")
        
        # EMA 계산
        ema_fast = TechnicalCalculator.calculate_ema(prices, fast_period)
        ema_slow = TechnicalCalculator.calculate_ema(prices, slow_period)
        
        # MACD 라인 계산
        start_idx = slow_period - fast_period
        macd_line = [ema_fast[i] - ema_slow[i - start_idx] for i in range(start_idx, len(ema_fast))]
        
        # 신호선 계산
        signal_line = TechnicalCalculator.calculate_ema(macd_line, signal_period)
        
        # 히스토그램 계산
        histogram = [macd_line[i] - signal_line[i] for i in range(len(signal_line))]
        
        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        }
    
    @staticmethod
    def calculate_bollinger_bands(prices: List[float], period: int = 20, 
                                 std_dev: float = 2.0) -> Dict[str, List[float]]:
        """볼린저 밴드 계산"""
        if len(prices) < period:
            raise CalculationError(f"최소 {period}개의 가격 데이터가 필요합니다.")
        
        sma = TechnicalCalculator.calculate_sma(prices, period)
        
        upper_band = []
        lower_band = []
        
        for i in range(period - 1, len(prices)):
            std = np.std(prices[i - period + 1:i + 1])
            upper_band.append(sma[i - period + 1] + (std * std_dev))
            lower_band.append(sma[i - period + 1] - (std * std_dev))
        
        return {
            'upper': upper_band,
            'middle': sma,
            'lower': lower_band
        }
    
    @staticmethod
    def calculate_stochastic(highs: List[float], lows: List[float], 
                           closes: List[float], k_period: int = 14,
                           d_period: int = 3) -> Dict[str, List[float]]:
        """스토캐스틱 계산"""
        if len(highs) != len(lows) or len(lows) != len(closes):
            raise CalculationError("고가, 저가, 종가 데이터의 길이가 같아야 합니다.")
        
        if len(closes) < k_period:
            raise CalculationError(f"최소 {k_period}개의 데이터가 필요합니다.")
        
        k_values = []
        
        for i in range(k_period - 1, len(closes)):
            highest_high = max(highs[i - k_period + 1:i + 1])
            lowest_low = min(lows[i - k_period + 1:i + 1])
            
            if highest_high == lowest_low:
                k = 50  # 중간값으로 설정
            else:
                k = ((closes[i] - lowest_low) / (highest_high - lowest_low)) * 100
            
            k_values.append(k)
        
        # %D 계산 (K의 이동평균)
        d_values = TechnicalCalculator.calculate_sma(k_values, d_period)
        
        return {
            'k': k_values,
            'd': d_values
        }

class StatisticalCalculator:
    """통계 계산 클래스"""
    
    @staticmethod
    def calculate_percentile(data: List[float], percentile: float) -> float:
        """백분위수 계산"""
        return np.percentile(data, percentile)
    
    @staticmethod
    def calculate_z_score(value: float, mean: float, std_dev: float) -> float:
        """Z-점수 계산"""
        if std_dev == 0:
            return 0.0
        return (value - mean) / std_dev
    
    @staticmethod
    def calculate_confidence_interval(data: List[float], confidence: float = 0.95) -> Tuple[float, float]:
        """신뢰구간 계산"""
        if len(data) < 2:
            raise CalculationError("최소 2개 이상의 데이터가 필요합니다.")
        
        mean = np.mean(data)
        std_err = stats.sem(data)
        interval = stats.t.interval(confidence, len(data) - 1, loc=mean, scale=std_err)
        
        return interval
    
    @staticmethod
    def calculate_regression(x: List[float], y: List[float]) -> Dict[str, float]:
        """선형회귀 계산"""
        if len(x) != len(y):
            raise CalculationError("x와 y 데이터의 길이가 같아야 합니다.")
        
        if len(x) < 2:
            raise CalculationError("최소 2개 이상의 데이터가 필요합니다.")
        
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        
        return {
            'slope': slope,
            'intercept': intercept,
            'r_squared': r_value ** 2,
            'p_value': p_value,
            'std_error': std_err
        }

# 편의 함수들
def calculate_return(initial: float, final: float, periods: int = None) -> float:
    """수익률 계산"""
    return FinancialCalculator.calculate_return(initial, final, periods)

def calculate_cagr(initial: float, final: float, years: float) -> float:
    """연평균 성장률 계산"""
    return FinancialCalculator.calculate_cagr(initial, final, years)

def calculate_roe(net_income: float, equity: float) -> float:
    """ROE 계산"""
    return RatioCalculator.calculate_roe(net_income, equity)

def calculate_per(price: float, eps: float) -> float:
    """PER 계산"""
    return RatioCalculator.calculate_per(price, eps)

def calculate_sma(prices: List[float], period: int) -> List[float]:
    """단순이동평균 계산"""
    return TechnicalCalculator.calculate_sma(prices, period)

def calculate_rsi(prices: List[float], period: int = 14) -> List[float]:
    """RSI 계산"""
    return TechnicalCalculator.calculate_rsi(prices, period)

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """안전한 나눗셈 (0으로 나누기 방지)"""
    if denominator == 0:
        return default
    return numerator / denominator

def safe_percentage(value: float, total: float, default: float = 0.0) -> float:
    """안전한 백분율 계산"""
    if total == 0:
        return default
    return (value / total) * 100

# 사용 예시
if __name__ == "__main__":
    print("🧮 계산 유틸리티 테스트")
    print("=" * 50)
    
    # 수익률 계산 테스트
    print("📈 수익률 계산 테스트:")
    initial_price = 50000
    final_price = 60000
    return_rate = calculate_return(initial_price, final_price)
    print(f"  초기가: {initial_price:,}원")
    print(f"  최종가: {final_price:,}원")
    print(f"  수익률: {return_rate:.2%}")
    
    # CAGR 계산 테스트
    print("\n📊 CAGR 계산 테스트:")
    cagr = calculate_cagr(initial_price, final_price, 2)
    print(f"  2년간 CAGR: {cagr:.2%}")
    
    # 재무비율 계산 테스트
    print("\n💰 재무비율 계산 테스트:")
    net_income = 1000000
    equity = 8000000
    roe = calculate_roe(net_income, equity)
    print(f"  순이익: {net_income:,}원")
    print(f"  자기자본: {equity:,}원")
    print(f"  ROE: {roe:.2%}")
    
    # PER 계산 테스트
    print("\n📊 PER 계산 테스트:")
    stock_price = 55000
    eps = 3000
    per = calculate_per(stock_price, eps)
    print(f"  주가: {stock_price:,}원")
    print(f"  EPS: {eps:,}원")
    print(f"  PER: {per:.2f}")
    
    # 기술적 분석 테스트
    print("\n📈 기술적 분석 테스트:")
    sample_prices = [50000, 51000, 52000, 51500, 53000, 52500, 54000, 53500, 55000, 54500]
    
    # SMA 계산
    sma_5 = calculate_sma(sample_prices, 5)
    print(f"  5일 SMA: {sma_5[-1]:,.0f}원")
    
    # RSI 계산
    try:
        rsi_values = calculate_rsi(sample_prices)
        print(f"  RSI: {rsi_values[-1]:.2f}")
    except CalculationError as e:
        print(f"  RSI: 계산 불가 ({e})")
    
    # 변동성 계산
    print("\n📊 변동성 계산 테스트:")
    returns = [0.02, -0.01, 0.015, -0.005, 0.03, -0.02, 0.01, -0.015, 0.025, -0.01]
    volatility = FinancialCalculator.calculate_volatility(returns)
    print(f"  연간 변동성: {volatility:.2%}")
    
    # 샤프 비율 계산
    sharpe = FinancialCalculator.calculate_sharpe_ratio(returns)
    print(f"  샤프 비율: {sharpe:.2f}")
    
    # 최대 낙폭 계산
    values = [100, 105, 110, 108, 115, 120, 118, 125, 130, 125, 120, 115]
    max_drawdown = FinancialCalculator.calculate_max_drawdown(values)
    print(f"  최대 낙폭: {max_drawdown['max_drawdown_percent']:.2f}%")
    
    print("\n✅ 모든 계산 테스트 완료!")