"""
📈 기술분석 모듈 - 기본 구조
Value Investment System의 기술분석 시스템 (30% 비중)

주요 구현 내용:
1. 추세 지표: SMA, EMA, 파라볼릭 SAR, ADX
2. 모멘텀 지표: RSI, MACD, 스토캐스틱, Williams %R  
3. 변동성 지표: 볼린저 밴드, ATR, 켈트너 채널
4. 거래량 지표: OBV, VWAP, CMF
5. 통합 매매신호 생성기
"""

import numpy as np
import pandas as pd
import talib
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

class TrendIndicators:
    """추세 지표 계산 클래스"""
    
    @staticmethod
    def calculate_sma(prices: np.ndarray, periods: List[int]) -> Dict[str, np.ndarray]:
        """단순이동평균 계산 (5, 20, 60, 120, 200일)"""
        sma_data = {}
        for period in periods:
            sma_data[f'SMA_{period}'] = talib.SMA(prices, timeperiod=period)
        return sma_data
    
    @staticmethod
    def calculate_ema(prices: np.ndarray, periods: List[int] = [12, 26]) -> Dict[str, np.ndarray]:
        """지수이동평균 계산 (12, 26일)"""
        ema_data = {}
        for period in periods:
            ema_data[f'EMA_{period}'] = talib.EMA(prices, timeperiod=period)
        return ema_data
    
    @staticmethod
    def calculate_parabolic_sar(high: np.ndarray, low: np.ndarray, 
                               acceleration: float = 0.02, maximum: float = 0.2) -> np.ndarray:
        """파라볼릭 SAR 계산 - 추세 전환점 포착"""
        return talib.SAR(high, low, acceleration=acceleration, maximum=maximum)
    
    @staticmethod
    def calculate_adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, 
                     timeperiod: int = 14) -> Dict[str, np.ndarray]:
        """ADX (평균방향지수) - 추세 강도 측정"""
        adx = talib.ADX(high, low, close, timeperiod=timeperiod)
        plus_di = talib.PLUS_DI(high, low, close, timeperiod=timeperiod)
        minus_di = talib.MINUS_DI(high, low, close, timeperiod=timeperiod)
        
        return {
            'ADX': adx,
            'PLUS_DI': plus_di,
            'MINUS_DI': minus_di
        }
    
    @staticmethod
    def calculate_52week_high_low(prices: np.ndarray, dates: List[datetime]) -> Dict[str, float]:
        """52주 신고가/신저가 계산"""
        if len(prices) < 252:  # 52주 = 252 거래일
            # 데이터가 부족한 경우 전체 기간 사용
            period_high = np.max(prices)
            period_low = np.min(prices)
        else:
            # 최근 252일(52주) 데이터 사용
            recent_prices = prices[-252:]
            period_high = np.max(recent_prices)
            period_low = np.min(recent_prices)
        
        current_price = prices[-1]
        
        return {
            '52W_HIGH': period_high,
            '52W_LOW': period_low,
            'CURRENT_PRICE': current_price,
            'HIGH_RATIO': (current_price / period_high) * 100,
            'LOW_RATIO': (current_price / period_low) * 100
        }

class MomentumIndicators:
    """모멘텀 지표 계산 클래스"""
    
    @staticmethod
    def calculate_rsi(prices: np.ndarray, timeperiod: int = 14) -> np.ndarray:
        """RSI (상대강도지수) - 과매수/과매도 구간"""
        return talib.RSI(prices, timeperiod=timeperiod)
    
    @staticmethod
    def calculate_macd(prices: np.ndarray, fastperiod: int = 12, 
                      slowperiod: int = 26, signalperiod: int = 9) -> Dict[str, np.ndarray]:
        """MACD - 추세 변화 신호"""
        macd, macd_signal, macd_hist = talib.MACD(prices, 
                                                  fastperiod=fastperiod,
                                                  slowperiod=slowperiod, 
                                                  signalperiod=signalperiod)
        return {
            'MACD': macd,
            'MACD_SIGNAL': macd_signal,
            'MACD_HIST': macd_hist
        }
    
    @staticmethod
    def calculate_stochastic(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                           fastk_period: int = 14, slowk_period: int = 3, 
                           slowd_period: int = 3) -> Dict[str, np.ndarray]:
        """스토캐스틱 (%K, %D) - 단기 매매 타이밍"""
        slowk, slowd = talib.STOCH(high, low, close,
                                   fastk_period=fastk_period,
                                   slowk_period=slowk_period,
                                   slowd_period=slowd_period)
        return {
            'STOCH_K': slowk,
            'STOCH_D': slowd
        }
    
    @staticmethod
    def calculate_williams_r(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                           timeperiod: int = 14) -> np.ndarray:
        """Williams %R - 단기 반전 신호"""
        return talib.WILLR(high, low, close, timeperiod=timeperiod)
    
    @staticmethod
    def calculate_cci(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                     timeperiod: int = 14) -> np.ndarray:
        """CCI (상품채널지수) - 가격 사이클 분석"""
        return talib.CCI(high, low, close, timeperiod=timeperiod)

class VolatilityIndicators:
    """변동성 지표 계산 클래스"""
    
    @staticmethod
    def calculate_bollinger_bands(prices: np.ndarray, timeperiod: int = 20, 
                                 nbdevup: float = 2, nbdevdn: float = 2) -> Dict[str, np.ndarray]:
        """볼린저 밴드 - 가격 변동 범위"""
        upper, middle, lower = talib.BBANDS(prices, 
                                           timeperiod=timeperiod,
                                           nbdevup=nbdevup, 
                                           nbdevdn=nbdevdn)
        return {
            'BB_UPPER': upper,
            'BB_MIDDLE': middle,
            'BB_LOWER': lower,
            'BB_WIDTH': (upper - lower) / middle * 100,  # 밴드폭 %
            'BB_POSITION': (prices - lower) / (upper - lower) * 100  # 밴드 내 위치 %
        }
    
    @staticmethod
    def calculate_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                     timeperiod: int = 14) -> np.ndarray:
        """ATR (평균진실범위) - 변동성 측정"""
        return talib.ATR(high, low, close, timeperiod=timeperiod)
    
    @staticmethod
    def calculate_keltner_channel(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                                 timeperiod: int = 20, multiplier: float = 2.0) -> Dict[str, np.ndarray]:
        """켈트너 채널 - 변동성 기반 채널"""
        ema = talib.EMA(close, timeperiod=timeperiod)
        atr = talib.ATR(high, low, close, timeperiod=timeperiod)
        
        upper = ema + (multiplier * atr)
        lower = ema - (multiplier * atr)
        
        return {
            'KC_UPPER': upper,
            'KC_MIDDLE': ema,
            'KC_LOWER': lower
        }
    
    @staticmethod
    def calculate_donchian_channel(high: np.ndarray, low: np.ndarray,
                                  timeperiod: int = 20) -> Dict[str, np.ndarray]:
        """도너찬 채널 - 브레이크아웃 신호"""
        upper = talib.MAX(high, timeperiod=timeperiod)
        lower = talib.MIN(low, timeperiod=timeperiod)
        middle = (upper + lower) / 2
        
        return {
            'DC_UPPER': upper,
            'DC_MIDDLE': middle,
            'DC_LOWER': lower
        }

class VolumeIndicators:
    """거래량 지표 계산 클래스"""
    
    @staticmethod
    def calculate_obv(close: np.ndarray, volume: np.ndarray) -> np.ndarray:
        """OBV (누적거래량) - 거래량 추세"""
        return talib.OBV(close, volume)
    
    @staticmethod
    def calculate_vwap(high: np.ndarray, low: np.ndarray, close: np.ndarray, 
                      volume: np.ndarray, window: int = 20) -> np.ndarray:
        """VWAP (거래량가중평균가) - 공정가치 기준"""
        typical_price = (high + low + close) / 3
        vwap = np.zeros_like(close)
        
        for i in range(window-1, len(close)):
            start_idx = max(0, i - window + 1)
            
            tp_window = typical_price[start_idx:i+1]
            vol_window = volume[start_idx:i+1]
            
            total_tp_vol = np.sum(tp_window * vol_window)
            total_vol = np.sum(vol_window)
            
            if total_vol > 0:
                vwap[i] = total_tp_vol / total_vol
            else:
                vwap[i] = close[i]
                
        return vwap
    
    @staticmethod
    def calculate_cmf(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                     volume: np.ndarray, timeperiod: int = 20) -> np.ndarray:
        """CMF (차이킨자금흐름) - 자금 유출입"""
        cmf = np.zeros_like(close)
        
        for i in range(timeperiod-1, len(close)):
            start_idx = i - timeperiod + 1
            
            mfm_sum = 0
            volume_sum = 0
            
            for j in range(start_idx, i+1):
                if high[j] != low[j]:
                    mfm = ((close[j] - low[j]) - (high[j] - close[j])) / (high[j] - low[j])
                    mfm_sum += mfm * volume[j]
                    volume_sum += volume[j]
            
            if volume_sum > 0:
                cmf[i] = mfm_sum / volume_sum
            else:
                cmf[i] = 0
                
        return cmf

class SignalGenerator:
    """통합 매매신호 생성기"""
    
    def __init__(self):
        self.trend_indicators = TrendIndicators()
        self.momentum_indicators = MomentumIndicators()
        self.volatility_indicators = VolatilityIndicators()
        self.volume_indicators = VolumeIndicators()
    
    def calculate_all_indicators(self, ohlcv_data: pd.DataFrame) -> Dict:
        """모든 기술지표 계산"""
        high = ohlcv_data['High'].values
        low = ohlcv_data['Low'].values
        close = ohlcv_data['Close'].values
        volume = ohlcv_data['Volume'].values
        
        results = {}
        
        # 추세 지표
        results.update(self.trend_indicators.calculate_sma(close, [5, 20, 60, 120, 200]))
        results.update(self.trend_indicators.calculate_ema(close, [12, 26]))
        results['PARABOLIC_SAR'] = self.trend_indicators.calculate_parabolic_sar(high, low)
        results.update(self.trend_indicators.calculate_adx(high, low, close))
        results.update(self.trend_indicators.calculate_52week_high_low(close, ohlcv_data.index.tolist()))
        
        # 모멘텀 지표
        results['RSI'] = self.momentum_indicators.calculate_rsi(close)
        results.update(self.momentum_indicators.calculate_macd(close))
        results.update(self.momentum_indicators.calculate_stochastic(high, low, close))
        results['WILLIAMS_R'] = self.momentum_indicators.calculate_williams_r(high, low, close)
        results['CCI'] = self.momentum_indicators.calculate_cci(high, low, close)
        
        # 변동성 지표
        results.update(self.volatility_indicators.calculate_bollinger_bands(close))
        results['ATR'] = self.volatility_indicators.calculate_atr(high, low, close)
        results.update(self.volatility_indicators.calculate_keltner_channel(high, low, close))
        results.update(self.volatility_indicators.calculate_donchian_channel(high, low))
        
        # 거래량 지표
        results['OBV'] = self.volume_indicators.calculate_obv(close, volume)
        results['VWAP'] = self.volume_indicators.calculate_vwap(high, low, close, volume)
        results['CMF'] = self.volume_indicators.calculate_cmf(high, low, close, volume)
        
        return results
    
    def generate_buy_sell_signals(self, indicators: Dict, current_price: float) -> Dict[str, int]:
        """매매신호 생성 (각 지표별 점수: -2 ~ +2)"""
        signals = {}
        
        # RSI 신호 (30 이하 매수, 70 이상 매도)
        rsi_current = indicators['RSI'][-1] if not np.isnan(indicators['RSI'][-1]) else 50
        if rsi_current <= 30:
            signals['RSI'] = 2  # 강한 매수
        elif rsi_current <= 40:
            signals['RSI'] = 1  # 매수
        elif rsi_current >= 70:
            signals['RSI'] = -2  # 강한 매도
        elif rsi_current >= 60:
            signals['RSI'] = -1  # 매도
        else:
            signals['RSI'] = 0  # 중립
        
        # MACD 신호
        macd_current = indicators['MACD'][-1] if not np.isnan(indicators['MACD'][-1]) else 0
        macd_signal_current = indicators['MACD_SIGNAL'][-1] if not np.isnan(indicators['MACD_SIGNAL'][-1]) else 0
        if macd_current > macd_signal_current:
            signals['MACD'] = 1  # 매수
        elif macd_current < macd_signal_current:
            signals['MACD'] = -1  # 매도
        else:
            signals['MACD'] = 0  # 중립
        
        # 볼린저 밴드 신호
        bb_position = indicators['BB_POSITION'][-1] if not np.isnan(indicators['BB_POSITION'][-1]) else 50
        if bb_position <= 20:
            signals['BOLLINGER'] = 2  # 강한 매수 (하한선 근처)
        elif bb_position <= 40:
            signals['BOLLINGER'] = 1  # 매수
        elif bb_position >= 80:
            signals['BOLLINGER'] = -2  # 강한 매도 (상한선 근처)
        elif bb_position >= 60:
            signals['BOLLINGER'] = -1  # 매도
        else:
            signals['BOLLINGER'] = 0  # 중립
        
        # 이동평균 신호 (현재가 vs 20일 이평선)
        sma20_current = indicators['SMA_20'][-1] if not np.isnan(indicators['SMA_20'][-1]) else current_price
        price_vs_sma = (current_price / sma20_current - 1) * 100
        if price_vs_sma >= 5:
            signals['SMA'] = -1  # 매도 (이평선 대비 5% 이상 상승)
        elif price_vs_sma <= -5:
            signals['SMA'] = 1   # 매수 (이평선 대비 5% 이상 하락)
        else:
            signals['SMA'] = 0   # 중립
        
        return signals
    
    def calculate_overall_score(self, signals: Dict[str, int]) -> Dict[str, float]:
        """전체 기술분석 점수 계산 (0-100점)"""
        total_score = sum(signals.values())
        max_possible_score = len(signals) * 2  # 각 지표 최대 +2점
        min_possible_score = len(signals) * -2  # 각 지표 최소 -2점
        
        # -100 ~ +100 범위를 0 ~ 100 범위로 변환
        normalized_score = ((total_score - min_possible_score) / 
                           (max_possible_score - min_possible_score)) * 100
        
        # 투자 추천 등급
        if normalized_score >= 80:
            recommendation = "Strong Buy"
            risk_level = "Medium"
        elif normalized_score >= 65:
            recommendation = "Buy"
            risk_level = "Medium"
        elif normalized_score >= 35:
            recommendation = "Hold"
            risk_level = "Low"
        elif normalized_score >= 20:
            recommendation = "Sell"
            risk_level = "Medium"
        else:
            recommendation = "Strong Sell"
            risk_level = "High"
        
        return {
            'total_score': normalized_score,
            'recommendation': recommendation,
            'risk_level': risk_level,
            'signal_count': len(signals),
            'individual_signals': signals
        }

class TechnicalAnalysisEngine:
    """기술분석 엔진 - 메인 클래스"""
    
    def __init__(self):
        self.signal_generator = SignalGenerator()
    
    def analyze_stock(self, ohlcv_data: pd.DataFrame, stock_code: str = "") -> Dict:
        """종목 기술분석 실행"""
        try:
            # 데이터 검증
            if len(ohlcv_data) < 200:
                print(f"경고: 충분한 데이터가 없습니다. (현재: {len(ohlcv_data)}일, 권장: 200일 이상)")
            
            # 모든 기술지표 계산
            indicators = self.signal_generator.calculate_all_indicators(ohlcv_data)
            
            # 현재가
            current_price = ohlcv_data['Close'].iloc[-1]
            
            # 매매신호 생성
            signals = self.signal_generator.generate_buy_sell_signals(indicators, current_price)
            
            # 전체 점수 계산
            overall_analysis = self.signal_generator.calculate_overall_score(signals)
            
            # 결과 정리
            analysis_result = {
                'stock_code': stock_code,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'current_price': current_price,
                'data_period': f"{ohlcv_data.index[0].strftime('%Y-%m-%d')} ~ {ohlcv_data.index[-1].strftime('%Y-%m-%d')}",
                'total_days': len(ohlcv_data),
                'technical_indicators': {
                    'trend': {
                        'SMA_20': indicators['SMA_20'][-1] if not np.isnan(indicators['SMA_20'][-1]) else None,
                        'SMA_60': indicators['SMA_60'][-1] if not np.isnan(indicators['SMA_60'][-1]) else None,
                        'ADX': indicators['ADX'][-1] if not np.isnan(indicators['ADX'][-1]) else None,
                        '52W_HIGH_RATIO': indicators['HIGH_RATIO']
                    },
                    'momentum': {
                        'RSI': indicators['RSI'][-1] if not np.isnan(indicators['RSI'][-1]) else None,
                        'MACD': indicators['MACD'][-1] if not np.isnan(indicators['MACD'][-1]) else None,
                        'STOCH_K': indicators['STOCH_K'][-1] if not np.isnan(indicators['STOCH_K'][-1]) else None
                    },
                    'volatility': {
                        'BB_POSITION': indicators['BB_POSITION'][-1] if not np.isnan(indicators['BB_POSITION'][-1]) else None,
                        'ATR': indicators['ATR'][-1] if not np.isnan(indicators['ATR'][-1]) else None
                    },
                    'volume': {
                        'OBV': indicators['OBV'][-1] if not np.isnan(indicators['OBV'][-1]) else None,
                        'CMF': indicators['CMF'][-1] if not np.isnan(indicators['CMF'][-1]) else None
                    }
                },
                'trading_signals': overall_analysis,
                'all_indicators': indicators  # 전체 지표 데이터 (차트용)
            }
            
            return analysis_result
            
        except Exception as e:
            return {
                'error': f"기술분석 실행 중 오류 발생: {str(e)}",
                'stock_code': stock_code,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

def main():
    """테스트 실행 함수"""
    print("📈 기술분석 모듈 테스트")
    print("=" * 50)
    
    # 샘플 데이터 생성 (실제로는 데이터베이스에서 가져옴)
    dates = pd.date_range('2024-01-01', '2024-07-19', freq='D')
    n_days = len(dates)
    
    # 가상의 OHLCV 데이터 (삼성전자 스타일)
    np.random.seed(42)
    base_price = 70000
    price_changes = np.random.normal(0, 0.02, n_days)
    prices = [base_price]
    
    for change in price_changes[1:]:
        new_price = prices[-1] * (1 + change)
        prices.append(max(new_price, base_price * 0.7))  # 최소 30% 하락 제한
    
    # OHLC 데이터 생성
    ohlcv_data = pd.DataFrame({
        'Open': [p * np.random.uniform(0.995, 1.005) for p in prices],
        'High': [p * np.random.uniform(1.001, 1.03) for p in prices],
        'Low': [p * np.random.uniform(0.97, 0.999) for p in prices],
        'Close': prices,
        'Volume': [np.random.randint(100000, 1000000) for _ in range(n_days)]
    }, index=dates)
    
    # 기술분석 엔진 초기화 및 실행
    engine = TechnicalAnalysisEngine()
    result = engine.analyze_stock(ohlcv_data, "005930")
    
    if 'error' in result:
        print(f"❌ 오류: {result['error']}")
        return
    
    # 결과 출력
    print(f"📊 종목코드: {result['stock_code']}")
    print(f"📅 분석일시: {result['analysis_date']}")
    print(f"💰 현재가: {result['current_price']:,.0f}원")
    print(f"📈 분석기간: {result['data_period']} ({result['total_days']}일)")
    print()
    
    print("📈 주요 기술지표:")
    indicators = result['technical_indicators']
    
    print("  [추세]")
    print(f"    - SMA20: {indicators['trend']['SMA_20']:,.0f}원" if indicators['trend']['SMA_20'] else "    - SMA20: N/A")
    print(f"    - ADX: {indicators['trend']['ADX']:.1f}" if indicators['trend']['ADX'] else "    - ADX: N/A")
    print(f"    - 52주 고가 대비: {indicators['trend']['52W_HIGH_RATIO']:.1f}%")
    
    print("  [모멘텀]")
    print(f"    - RSI: {indicators['momentum']['RSI']:.1f}" if indicators['momentum']['RSI'] else "    - RSI: N/A")
    print(f"    - MACD: {indicators['momentum']['MACD']:.2f}" if indicators['momentum']['MACD'] else "    - MACD: N/A")
    
    print("  [변동성]")
    print(f"    - 볼린저밴드 위치: {indicators['volatility']['BB_POSITION']:.1f}%" if indicators['volatility']['BB_POSITION'] else "    - 볼린저밴드 위치: N/A")
    
    print()
    print("🎯 투자 분석 결과:")
    signals = result['trading_signals']
    print(f"    - 기술분석 점수: {signals['total_score']:.1f}/100점")
    print(f"    - 투자 추천: {signals['recommendation']}")
    print(f"    - 리스크 레벨: {signals['risk_level']}")
    print(f"    - 분석 지표 수: {signals['signal_count']}개")
    
    print()
    print("📊 개별 신호:")
    for indicator, signal in signals['individual_signals'].items():
        signal_text = "강한매수" if signal == 2 else "매수" if signal == 1 else "중립" if signal == 0 else "매도" if signal == -1 else "강한매도"
        print(f"    - {indicator}: {signal_text} ({signal:+d})")

if __name__ == "__main__":
    main()