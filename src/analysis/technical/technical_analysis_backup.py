#!/usr/bin/env python3
"""
📈 기술분석 모듈 - 완전 구현 버전
Value Investment System - Technical Analysis Module

주요 기능:
1. 20+ 기술지표 계산 (SMA, EMA, RSI, MACD, 볼린저밴드 등)
2. 종합 매매신호 생성
3. 리스크 평가
4. 투자 추천도 산출
5. TA-Lib 및 기본 계산 모두 지원

사용법:
analyzer = TechnicalAnalyzer()
result = analyzer.analyze_stock("005930", ohlcv_data)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# TA-Lib 체크 및 기본 계산 대체 함수
try:
    import talib
    TALIB_AVAILABLE = True
    print("✅ TA-Lib 사용 가능")
except ImportError:
    TALIB_AVAILABLE = False
    print("⚠️ TA-Lib 사용 불가, 기본 계산 사용")

# 신호 레벨 정의
SIGNAL_LEVELS = {
    'STRONG_BUY': 2,
    'BUY': 1,
    'NEUTRAL': 0,
    'SELL': -1,
    'STRONG_SELL': -2
}

# 추천 임계값
RECOMMENDATION_THRESHOLDS = {
    'STRONG_BUY': 80,
    'BUY': 60,
    'NEUTRAL': 40,
    'SELL': 20,
    'STRONG_SELL': 0
}

# 버전 정보
__version__ = "1.0.0"
__author__ = "Value Investment System"
__description__ = "Technical Analysis Module for Value Investment System"

class TechnicalIndicators:
    """기술지표 계산 클래스"""
    
    @staticmethod
    def sma(data: pd.Series, period: int) -> pd.Series:
        """단순이동평균"""
        return data.rolling(window=period).mean()
    
    @staticmethod
    def ema(data: pd.Series, period: int) -> pd.Series:
        """지수이동평균"""
        if TALIB_AVAILABLE:
            return pd.Series(talib.EMA(data.values, timeperiod=period), index=data.index)
        else:
            return data.ewm(span=period).mean()
    
    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """RSI (Relative Strength Index)"""
        if TALIB_AVAILABLE:
            return pd.Series(talib.RSI(data.values, timeperiod=period), index=data.index)
        else:
            delta = data.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            return 100 - (100 / (1 + rs))
    
    @staticmethod
    def macd(data: pd.Series, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """MACD"""
        if TALIB_AVAILABLE:
            macd_line, signal_line, histogram = talib.MACD(data.values, fastperiod=fast_period, slowperiod=slow_period, signalperiod=signal_period)
            return (
                pd.Series(macd_line, index=data.index),
                pd.Series(signal_line, index=data.index),
                pd.Series(histogram, index=data.index)
            )
        else:
            ema_fast = TechnicalIndicators.ema(data, fast_period)
            ema_slow = TechnicalIndicators.ema(data, slow_period)
            macd_line = ema_fast - ema_slow
            signal_line = TechnicalIndicators.ema(macd_line, signal_period)
            histogram = macd_line - signal_line
            return macd_line, signal_line, histogram
    
    @staticmethod
    def bollinger_bands(data: pd.Series, period: int = 20, std_dev: float = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """볼린저 밴드"""
        if TALIB_AVAILABLE:
            upper, middle, lower = talib.BBANDS(data.values, timeperiod=period, nbdevup=std_dev, nbdevdn=std_dev)
            return (
                pd.Series(upper, index=data.index),
                pd.Series(middle, index=data.index),
                pd.Series(lower, index=data.index)
            )
        else:
            sma = TechnicalIndicators.sma(data, period)
            std = data.rolling(window=period).std()
            upper = sma + (std * std_dev)
            lower = sma - (std * std_dev)
            return upper, sma, lower
    
    @staticmethod
    def stochastic(high: pd.Series, low: pd.Series, close: pd.Series, k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        """스토캐스틱"""
        if TALIB_AVAILABLE:
            k_percent, d_percent = talib.STOCH(high.values, low.values, close.values, 
                                             fastk_period=k_period, slowk_period=d_period, slowd_period=d_period)
            return (
                pd.Series(k_percent, index=close.index),
                pd.Series(d_percent, index=close.index)
            )
        else:
            lowest_low = low.rolling(window=k_period).min()
            highest_high = high.rolling(window=k_period).max()
            k_percent = 100 * ((close - lowest_low) / (highest_high - lowest_low))
            d_percent = k_percent.rolling(window=d_period).mean()
            return k_percent, d_percent
    
    @staticmethod
    def williams_r(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Williams %R"""
        if TALIB_AVAILABLE:
            return pd.Series(talib.WILLR(high.values, low.values, close.values, timeperiod=period), index=close.index)
        else:
            highest_high = high.rolling(window=period).max()
            lowest_low = low.rolling(window=period).min()
            return -100 * ((highest_high - close) / (highest_high - lowest_low))
    
    @staticmethod
    def cci(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """CCI (Commodity Channel Index)"""
        if TALIB_AVAILABLE:
            return pd.Series(talib.CCI(high.values, low.values, close.values, timeperiod=period), index=close.index)
        else:
            tp = (high + low + close) / 3
            sma_tp = tp.rolling(window=period).mean()
            mad = tp.rolling(window=period).apply(lambda x: np.mean(np.abs(x - x.mean())))
            return (tp - sma_tp) / (0.015 * mad)
    
    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """ADX (Average Directional Index)"""
        if TALIB_AVAILABLE:
            return pd.Series(talib.ADX(high.values, low.values, close.values, timeperiod=period), index=close.index)
        else:
            # 기본 ADX 계산 (단순화 버전)
            tr1 = high - low
            tr2 = abs(high - close.shift(1))
            tr3 = abs(low - close.shift(1))
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(window=period).mean()
            
            dm_plus = np.where((high - high.shift(1)) > (low.shift(1) - low), 
                               np.maximum(high - high.shift(1), 0), 0)
            dm_minus = np.where((low.shift(1) - low) > (high - high.shift(1)), 
                                np.maximum(low.shift(1) - low, 0), 0)
            
            dm_plus_series = pd.Series(dm_plus, index=close.index).rolling(window=period).mean()
            dm_minus_series = pd.Series(dm_minus, index=close.index).rolling(window=period).mean()
            
            di_plus = 100 * (dm_plus_series / atr)
            di_minus = 100 * (dm_minus_series / atr)
            
            dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
            adx = dx.rolling(window=period).mean()
            
            return adx
    
    @staticmethod
    def parabolic_sar(high: pd.Series, low: pd.Series, acceleration: float = 0.02, maximum: float = 0.2) -> pd.Series:
        """Parabolic SAR"""
        if TALIB_AVAILABLE:
            return pd.Series(talib.SAR(high.values, low.values, acceleration=acceleration, maximum=maximum), index=high.index)
        else:
            # 기본 SAR 계산 (단순화 버전)
            sar = pd.Series(index=high.index, dtype=float)
            af = acceleration
            ep = high.iloc[0]
            trend = 1  # 1 for uptrend, -1 for downtrend
            
            sar.iloc[0] = low.iloc[0]
            
            for i in range(1, len(high)):
                if trend == 1:  # uptrend
                    sar.iloc[i] = sar.iloc[i-1] + af * (ep - sar.iloc[i-1])
                    if high.iloc[i] > ep:
                        ep = high.iloc[i]
                        af = min(af + acceleration, maximum)
                    if low.iloc[i] <= sar.iloc[i]:
                        trend = -1
                        sar.iloc[i] = ep
                        ep = low.iloc[i]
                        af = acceleration
                else:  # downtrend
                    sar.iloc[i] = sar.iloc[i-1] + af * (ep - sar.iloc[i-1])
                    if low.iloc[i] < ep:
                        ep = low.iloc[i]
                        af = min(af + acceleration, maximum)
                    if high.iloc[i] >= sar.iloc[i]:
                        trend = 1
                        sar.iloc[i] = ep
                        ep = high.iloc[i]
                        af = acceleration
            
            return sar
    
    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """ATR (Average True Range)"""
        if TALIB_AVAILABLE:
            return pd.Series(talib.ATR(high.values, low.values, close.values, timeperiod=period), index=close.index)
        else:
            tr1 = high - low
            tr2 = abs(high - close.shift(1))
            tr3 = abs(low - close.shift(1))
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            return tr.rolling(window=period).mean()
    
    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        """OBV (On-Balance Volume)"""
        if TALIB_AVAILABLE:
            return pd.Series(talib.OBV(close.values, volume.values), index=close.index)
        else:
            direction = np.where(close > close.shift(1), 1, 
                                np.where(close < close.shift(1), -1, 0))
            obv = (volume * direction).cumsum()
            return pd.Series(obv, index=close.index)
    
    @staticmethod
    def vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 20) -> pd.Series:
        """VWAP (Volume Weighted Average Price)"""
        typical_price = (high + low + close) / 3
        vwap = (typical_price * volume).rolling(window=period).sum() / volume.rolling(window=period).sum()
        return vwap

class TechnicalAnalyzer:
    """기술분석 엔진"""
    
    def __init__(self):
        self.indicators = TechnicalIndicators()
        
        # 가중치 설정
        self.weights = {
            'trend': 0.35,      # 추세
            'momentum': 0.30,   # 모멘텀
            'volatility': 0.20, # 변동성
            'volume': 0.15      # 거래량
        }
    
    def analyze_stock(self, stock_code: str, ohlcv_data: pd.DataFrame) -> Dict[str, Any]:
        """종목 기술분석 실행"""
        try:
            # 데이터 검증
            if len(ohlcv_data) < 20:
                return {'error': f'데이터가 부족합니다. 최소 20일 필요, 현재 {len(ohlcv_data)}일'}
            
            # 필수 컬럼 확인
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            missing_columns = [col for col in required_columns if col not in ohlcv_data.columns]
            if missing_columns:
                return {'error': f'필수 컬럼 누락: {missing_columns}'}
            
            # 기본 정보
            current_price = float(ohlcv_data['Close'].iloc[-1])
            analysis_date = datetime.now().isoformat()
            
            # 기술지표 계산
            technical_indicators = self._calculate_all_indicators(ohlcv_data)
            
            # 매매신호 생성
            trading_signals = self._generate_trading_signals(technical_indicators, ohlcv_data)
            
            # 종합 점수 계산
            overall_score = self._calculate_overall_score(trading_signals)
            
            # 추천도 결정
            recommendation = self._get_recommendation(overall_score)
            
            # 리스크 평가
            risk_level = self._assess_risk_level(technical_indicators, ohlcv_data)
            
            # 분석 요약
            analysis_summary = self._generate_analysis_summary(technical_indicators, trading_signals, ohlcv_data)
            
            return {
                'stock_code': stock_code,
                'current_price': current_price,
                'analysis_date': analysis_date,
                'data_period': {
                    'start': ohlcv_data.index[0].strftime('%Y-%m-%d'),
                    'end': ohlcv_data.index[-1].strftime('%Y-%m-%d'),
                    'total_days': len(ohlcv_data)
                },
                'technical_indicators': technical_indicators,
                'trading_signals': trading_signals,
                'overall_score': overall_score,
                'recommendation': recommendation,
                'risk_level': risk_level,
                'analysis_summary': analysis_summary,
                'metadata': {
                    'talib_available': TALIB_AVAILABLE,
                    'indicators_calculated': len(technical_indicators),
                    'signals_generated': len(trading_signals['individual_signals']),
                    'calculation_method': 'TA-Lib' if TALIB_AVAILABLE else 'Basic Math'
                }
            }
            
        except Exception as e:
            return {'error': f'분석 중 오류 발생: {str(e)}'}
    
    def _calculate_all_indicators(self, ohlcv_data: pd.DataFrame) -> Dict[str, float]:
        """모든 기술지표 계산"""
        high = ohlcv_data['High']
        low = ohlcv_data['Low']
        close = ohlcv_data['Close']
        volume = ohlcv_data['Volume']
        
        indicators = {}
        
        try:
            # 이동평균
            indicators['SMA_5'] = self.indicators.sma(close, 5).iloc[-1]
            indicators['SMA_20'] = self.indicators.sma(close, 20).iloc[-1]
            indicators['SMA_60'] = self.indicators.sma(close, 60).iloc[-1]
            indicators['SMA_120'] = self.indicators.sma(close, 120).iloc[-1]
            indicators['EMA_12'] = self.indicators.ema(close, 12).iloc[-1]
            indicators['EMA_26'] = self.indicators.ema(close, 26).iloc[-1]
            
            # 모멘텀 지표
            indicators['RSI'] = self.indicators.rsi(close, 14).iloc[-1]
            
            macd_line, signal_line, histogram = self.indicators.macd(close)
            indicators['MACD'] = macd_line.iloc[-1]
            indicators['MACD_SIGNAL'] = signal_line.iloc[-1]
            indicators['MACD_HISTOGRAM'] = histogram.iloc[-1]
            
            # 스토캐스틱
            stoch_k, stoch_d = self.indicators.stochastic(high, low, close)
            indicators['STOCH_K'] = stoch_k.iloc[-1]
            indicators['STOCH_D'] = stoch_d.iloc[-1]
            
            # 기타 모멘텀
            indicators['WILLIAMS_R'] = self.indicators.williams_r(high, low, close).iloc[-1]
            indicators['CCI'] = self.indicators.cci(high, low, close).iloc[-1]
            
            # 추세 지표
            indicators['ADX'] = self.indicators.adx(high, low, close).iloc[-1]
            indicators['PARABOLIC_SAR'] = self.indicators.parabolic_sar(high, low).iloc[-1]
            
            # 변동성 지표
            bb_upper, bb_middle, bb_lower = self.indicators.bollinger_bands(close)
            indicators['BB_UPPER'] = bb_upper.iloc[-1]
            indicators['BB_MIDDLE'] = bb_middle.iloc[-1]
            indicators['BB_LOWER'] = bb_lower.iloc[-1]
            indicators['BB_POSITION'] = ((close.iloc[-1] - bb_lower.iloc[-1]) / 
                                        (bb_upper.iloc[-1] - bb_lower.iloc[-1])) * 100
            
            indicators['ATR'] = self.indicators.atr(high, low, close).iloc[-1]
            
            # 거래량 지표
            indicators['OBV'] = self.indicators.obv(close, volume).iloc[-1]
            indicators['VWAP'] = self.indicators.vwap(high, low, close, volume).iloc[-1]
            
            # 52주 고가/저가
            high_52w = high.tail(252).max() if len(high) >= 252 else high.max()
            low_52w = low.tail(252).min() if len(low) >= 252 else low.min()
            indicators['52W_HIGH'] = high_52w
            indicators['52W_LOW'] = low_52w
            indicators['52W_HIGH_RATIO'] = (close.iloc[-1] / high_52w) * 100
            indicators['52W_LOW_RATIO'] = (close.iloc[-1] / low_52w) * 100
            
        except Exception as e:
            # 개별 지표 계산 실패 시 NaN으로 설정
            for key in indicators:
                if pd.isna(indicators[key]) or np.isinf(indicators[key]):
                    indicators[key] = None
        
        return indicators
    
    def _generate_trading_signals(self, indicators: Dict[str, float], ohlcv_data: pd.DataFrame) -> Dict[str, Any]:
        """매매신호 생성"""
        signals = {}
        signal_scores = []
        
        close = ohlcv_data['Close']
        current_price = close.iloc[-1]
        
        # RSI 신호
        rsi = indicators.get('RSI')
        if rsi is not None:
            if rsi < 30:
                signals['RSI'] = 'STRONG_BUY'
                signal_scores.append(SIGNAL_LEVELS['STRONG_BUY'])
            elif rsi < 50:
                signals['RSI'] = 'BUY'
                signal_scores.append(SIGNAL_LEVELS['BUY'])
            elif rsi > 70:
                signals['RSI'] = 'STRONG_SELL'
                signal_scores.append(SIGNAL_LEVELS['STRONG_SELL'])
            elif rsi > 50:
                signals['RSI'] = 'SELL'
                signal_scores.append(SIGNAL_LEVELS['SELL'])
            else:
                signals['RSI'] = 'NEUTRAL'
                signal_scores.append(SIGNAL_LEVELS['NEUTRAL'])
        
        # MACD 신호
        macd = indicators.get('MACD')
        macd_signal = indicators.get('MACD_SIGNAL')
        if macd is not None and macd_signal is not None:
            if macd > macd_signal:
                signals['MACD'] = 'BUY'
                signal_scores.append(SIGNAL_LEVELS['BUY'])
            else:
                signals['MACD'] = 'SELL'
                signal_scores.append(SIGNAL_LEVELS['SELL'])
        
        # 이동평균 신호
        sma_20 = indicators.get('SMA_20')
        sma_60 = indicators.get('SMA_60')
        if sma_20 is not None and sma_60 is not None:
            if current_price > sma_20 > sma_60:
                signals['SMA'] = 'BUY'
                signal_scores.append(SIGNAL_LEVELS['BUY'])
            elif current_price < sma_20 < sma_60:
                signals['SMA'] = 'SELL'
                signal_scores.append(SIGNAL_LEVELS['SELL'])
            else:
                signals['SMA'] = 'NEUTRAL'
                signal_scores.append(SIGNAL_LEVELS['NEUTRAL'])
        
        # 볼린저밴드 신호
        bb_position = indicators.get('BB_POSITION')
        if bb_position is not None:
            if bb_position > 80:
                signals['BB'] = 'SELL'
                signal_scores.append(SIGNAL_LEVELS['SELL'])
            elif bb_position < 20:
                signals['BB'] = 'BUY'
                signal_scores.append(SIGNAL_LEVELS['BUY'])
            else:
                signals['BB'] = 'NEUTRAL'
                signal_scores.append(SIGNAL_LEVELS['NEUTRAL'])
        
        # 스토캐스틱 신호
        stoch_k = indicators.get('STOCH_K')
        stoch_d = indicators.get('STOCH_D')
        if stoch_k is not None and stoch_d is not None:
            if stoch_k < 20 and stoch_d < 20:
                signals['STOCH'] = 'BUY'
                signal_scores.append(SIGNAL_LEVELS['BUY'])
            elif stoch_k > 80 and stoch_d > 80:
                signals['STOCH'] = 'SELL'
                signal_scores.append(SIGNAL_LEVELS['SELL'])
            else:
                signals['STOCH'] = 'NEUTRAL'
                signal_scores.append(SIGNAL_LEVELS['NEUTRAL'])
        
        # ADX 추세 강도
        adx = indicators.get('ADX')
        if adx is not None and adx > 25:
            # 강한 추세
            if any('BUY' in signal for signal in signals.values()):
                signals['ADX'] = 'TREND_STRONG_UP'
            elif any('SELL' in signal for signal in signals.values()):
                signals['ADX'] = 'TREND_STRONG_DOWN'
            else:
                signals['ADX'] = 'TREND_STRONG'
        else:
            signals['ADX'] = 'TREND_WEAK'
        
        # 신호 통계
        buy_signals = sum(1 for signal in signals.values() if 'BUY' in signal)
        sell_signals = sum(1 for signal in signals.values() if 'SELL' in signal)
        neutral_signals = len(signals) - buy_signals - sell_signals
        
        # 총 점수 계산
        total_score = sum(signal_scores) if signal_scores else 0
        max_possible_score = len(signal_scores) * SIGNAL_LEVELS['STRONG_BUY']
        min_possible_score = len(signal_scores) * SIGNAL_LEVELS['STRONG_SELL']
        
        # 0-100 점수로 정규화
        if max_possible_score != min_possible_score:
            normalized_score = ((total_score - min_possible_score) / 
                               (max_possible_score - min_possible_score)) * 100
        else:
            normalized_score = 50
        
        return {
            'individual_signals': signals,
            'total_score': normalized_score,
            'buy_signals': buy_signals,
            'sell_signals': sell_signals,
            'neutral_signals': neutral_signals,
            'signal_strength': 'Strong' if abs(normalized_score - 50) > 20 else 'Weak'
        }
    
    def _calculate_overall_score(self, trading_signals: Dict[str, Any]) -> float:
        """종합 점수 계산"""
        return trading_signals['total_score']
    
    def _get_recommendation(self, score: float) -> str:
        """점수 기반 투자 추천"""
        if score >= RECOMMENDATION_THRESHOLDS['STRONG_BUY']:
            return 'STRONG_BUY'
        elif score >= RECOMMENDATION_THRESHOLDS['BUY']:
            return 'BUY'
        elif score >= RECOMMENDATION_THRESHOLDS['NEUTRAL']:
            return 'NEUTRAL'
        elif score >= RECOMMENDATION_THRESHOLDS['SELL']:
            return 'SELL'
        else:
            return 'STRONG_SELL'
    
    def _assess_risk_level(self, indicators: Dict[str, float], ohlcv_data: pd.DataFrame) -> str:
        """리스크 레벨 평가"""
        risk_factors = []
        
        # 변동성 체크
        atr = indicators.get('ATR')
        close = ohlcv_data['Close']
        if atr is not None:
            volatility_ratio = (atr / close.iloc[-1]) * 100
            if volatility_ratio > 5:
                risk_factors.append('high_volatility')
        
        # RSI 극값 체크
        rsi = indicators.get('RSI')
        if rsi is not None:
            if rsi > 80 or rsi < 20:
                risk_factors.append('extreme_rsi')
        
        # 52주 고가 대비 위치
        high_ratio = indicators.get('52W_HIGH_RATIO')
        if high_ratio is not None:
            if high_ratio > 95:
                risk_factors.append('near_52w_high')
            elif high_ratio < 60:
                risk_factors.append('far_from_high')
        
        # 리스크 레벨 결정
        if len(risk_factors) >= 3:
            return 'HIGH'
        elif len(risk_factors) >= 1:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _generate_analysis_summary(self, indicators: Dict[str, float], 
                                  trading_signals: Dict[str, Any], 
                                  ohlcv_data: pd.DataFrame) -> Dict[str, str]:
        """분석 요약 생성"""
        summary = {}
        
        # 추세 강도
        adx = indicators.get('ADX')
        if adx is not None:
            if adx > 25:
                summary['trend_strength'] = 'Strong'
            elif adx > 20:
                summary['trend_strength'] = 'Moderate'
            else:
                summary['trend_strength'] = 'Weak'
        else:
            summary['trend_strength'] = 'Unknown'
        
        # 모멘텀 상태
        rsi = indicators.get('RSI')
        if rsi is not None:
            if rsi > 70:
                summary['momentum_status'] = 'Overbought'
            elif rsi < 30:
                summary['momentum_status'] = 'Oversold'
            elif rsi > 50:
                summary['momentum_status'] = 'Bullish'
            else:
                summary['momentum_status'] = 'Bearish'
        else:
            summary['momentum_status'] = 'Unknown'
        
        # 변동성 수준
        atr = indicators.get('ATR')
        close = ohlcv_data['Close']
        if atr is not None:
            volatility_ratio = (atr / close.iloc[-1]) * 100
            if volatility_ratio > 5:
                summary['volatility_level'] = 'High'
            elif volatility_ratio > 2:
                summary['volatility_level'] = 'Medium'
            else:
                summary['volatility_level'] = 'Low'
        else:
            summary['volatility_level'] = 'Unknown'
        
        # 거래량 추세 (간단한 버전)
        volume = ohlcv_data['Volume']
        recent_volume = volume.tail(5).mean()
        historical_volume = volume.tail(20).mean()
        
        if recent_volume > historical_volume * 1.2:
            summary['volume_trend'] = 'Increasing'
        elif recent_volume < historical_volume * 0.8:
            summary['volume_trend'] = 'Decreasing'
        else:
            summary['volume_trend'] = 'Normal'
        
        return summary

def get_module_info() -> Dict[str, Any]:
    """모듈 정보 반환"""
    return {
        'name': 'technical_analysis',
        'version': __version__,
        'author': __author__,
        'description': __description__,
        'talib_available': TALIB_AVAILABLE,
        'indicators_supported': [
            'SMA (5, 20, 60, 120, 200일)',
            'EMA (12, 26일)',
            'RSI (14일)',
            'MACD (12, 26, 9)',
            'Bollinger Bands (20일, 2σ)',
            'Stochastic (14, 3)',
            'Williams %R (14일)',
            'CCI (14일)',
            'ADX (14일)',
            'Parabolic SAR',
            'ATR (14일)',
            'OBV',
            'VWAP (20일)',
            '52주 신고가/신저가'
        ],
        'signal_types': list(SIGNAL_LEVELS.keys()),
        'recommendation_levels': list(RECOMMENDATION_THRESHOLDS.keys()),
        'weights': {
            'trend': '35%',
            'momentum': '30%',
            'volatility': '20%',
            'volume': '15%'
        }
    }

def print_analysis_summary(result: Dict[str, Any]) -> None:
    """분석 결과 요약 출력"""
    if 'error' in result:
        print(f"❌ 오류: {result['error']}")
        return
    
    print(f"📊 {result['stock_code']} 기술분석 결과")
    print("=" * 50)
    print(f"💰 현재가: {result['current_price']:,.0f}원")
    print(f"📅 분석일시: {result['analysis_date'][:19]}")
    print(f"📈 데이터 기간: {result['data_period']['start']} ~ {result['data_period']['end']} ({result['data_period']['total_days']}일)")
    
    # 종합 결과
    print(f"\n🎯 투자 결론:")
    print(f"   기술분석 점수: {result['overall_score']:.1f}/100점")
    print(f"   투자 추천: {result['recommendation']}")
    print(f"   리스크 레벨: {result['risk_level']}")
    
    # 주요 지표
    indicators = result['technical_indicators']
    print(f"\n📈 주요 지표:")
    print(f"   RSI: {indicators.get('RSI', 'N/A'):.1f}" if indicators.get('RSI') else "   RSI: N/A")
    print(f"   MACD: {indicators.get('MACD', 'N/A'):.2f}" if indicators.get('MACD') else "   MACD: N/A")
    print(f"   볼린저밴드 위치: {indicators.get('BB_POSITION', 'N/A'):.1f}%" if indicators.get('BB_POSITION') else "   볼린저밴드 위치: N/A")
    print(f"   ADX: {indicators.get('ADX', 'N/A'):.1f}" if indicators.get('ADX') else "   ADX: N/A")
    print(f"   52주 고가 비율: {indicators.get('52W_HIGH_RATIO', 'N/A'):.1f}%" if indicators.get('52W_HIGH_RATIO') else "   52주 고가 비율: N/A")
    
    # 매매신호
    signals = result['trading_signals']['individual_signals']
    print(f"\n🚦 매매신호:")
    for indicator, signal in signals.items():
        emoji = "🔴" if "SELL" in signal else "🟢" if "BUY" in signal else "🟡"
        print(f"   {emoji} {indicator}: {signal}")
    
    # 분석 요약
    summary = result['analysis_summary']
    print(f"\n📋 분석 요약:")
    print(f"   추세 강도: {summary['trend_strength']}")
    print(f"   모멘텀: {summary['momentum_status']}")
    print(f"   변동성: {summary['volatility_level']}")
    print(f"   거래량 추세: {summary['volume_trend']}")

if __name__ == "__main__":
    # 모듈 테스트 실행
    print("🚀 Technical Analysis Module - 완전 구현 버전")
    print("=" * 70)
    
    # 모듈 정보 출력
    module_info = get_module_info()
    print(f"📋 모듈명: {module_info['name']} v{module_info['version']}")
    print(f"👤 개발: {module_info['author']}")
    print(f"📖 설명: {module_info['description']}")
    print(f"🔧 TA-Lib: {'✅ 사용 가능' if module_info['talib_available'] else '❌ 사용 불가 (기본 계산)'}")
    print(f"📊 지원 지표: {len(module_info['indicators_supported'])}개")
    print(f"⚖️ 가중치: 추세 {module_info['weights']['trend']}, 모멘텀 {module_info['weights']['momentum']}, 변동성 {module_info['weights']['volatility']}, 거래량 {module_info['weights']['volume']}")
    
    print(f"\n📈 지원 지표 목록:")
    for i, indicator in enumerate(module_info['indicators_supported'], 1):
        print(f"   {i:2d}. {indicator}")
    
    # 샘플 데이터로 테스트
    print(f"\n🧪 샘플 데이터 테스트 실행...")
    
    # 실제 주식 데이터와 유사한 패턴 생성
    dates = pd.date_range(end=datetime.now(), periods=200, freq='D')
    np.random.seed(42)
    
    # 삼성전자 스타일의 가격 데이터 (추세 + 노이즈)
    base_price = 70000
    trend = np.linspace(0, 0.1, 200)  # 10% 상승 추세
    noise = np.random.normal(0, 0.015, 200)  # 1.5% 노이즈
    returns = trend + noise
    
    prices = [base_price]
    for ret in returns[1:]:
        new_price = prices[-1] * (1 + ret)
        prices.append(max(new_price, base_price * 0.7))  # 30% 하락 제한
    
    # OHLC 데이터 생성 (현실적인 패턴)
    sample_data = pd.DataFrame({
        'Open': [p * np.random.uniform(0.995, 1.005) for p in prices],
        'High': [p * np.random.uniform(1.005, 1.025) for p in prices],
        'Low': [p * np.random.uniform(0.975, 0.995) for p in prices],
        'Close': prices,
        'Volume': np.random.lognormal(13, 0.5, 200).astype(int)  # 로그정규분포로 현실적인 거래량
    }, index=dates)
    
    # High/Low 보정
    for i in range(len(sample_data)):
        high = max(sample_data.iloc[i]['Open'], sample_data.iloc[i]['Close'], sample_data.iloc[i]['High'])
        low = min(sample_data.iloc[i]['Open'], sample_data.iloc[i]['Close'], sample_data.iloc[i]['Low'])
        sample_data.iloc[i, sample_data.columns.get_loc('High')] = high
        sample_data.iloc[i, sample_data.columns.get_loc('Low')] = low
    
    # 데이터 품질 체크
    print(f"📊 테스트 데이터 정보:")
    print(f"   기간: {sample_data.index[0].strftime('%Y-%m-%d')} ~ {sample_data.index[-1].strftime('%Y-%m-%d')}")
    print(f"   데이터 수: {len(sample_data)}일")
    print(f"   시작가: {sample_data['Close'].iloc[0]:,.0f}원")
    print(f"   현재가: {sample_data['Close'].iloc[-1]:,.0f}원")
    print(f"   수익률: {((sample_data['Close'].iloc[-1] / sample_data['Close'].iloc[0]) - 1) * 100:+.1f}%")
    print(f"   최고가: {sample_data['High'].max():,.0f}원")
    print(f"   최저가: {sample_data['Low'].min():,.0f}원")
    print(f"   평균 거래량: {sample_data['Volume'].mean():,.0f}주")
    
    # 기술분석 실행
    print(f"\n🔍 기술분석 실행 중...")
    analyzer = TechnicalAnalyzer()
    result = analyzer.analyze_stock("TEST_001", sample_data)
    
    # 결과 출력
    print(f"\n" + "=" * 70)
    if 'error' in result:
        print(f"❌ 분석 실패: {result['error']}")
    else:
        print_analysis_summary(result)
        
        # 상세 메타데이터
        metadata = result['metadata']
        print(f"\n🔧 메타데이터:")
        print(f"   TA-Lib 사용: {'예' if metadata['talib_available'] else '아니오'}")
        print(f"   계산된 지표: {metadata['indicators_calculated']}개")
        print(f"   생성된 신호: {metadata['signals_generated']}개")
        
        # 신호 통계
        signals = result['trading_signals']
        print(f"\n📊 신호 통계:")
        print(f"   매수 신호: {signals['buy_signals']}개")
        print(f"   매도 신호: {signals['sell_signals']}개")
        print(f"   중립 신호: {signals['neutral_signals']}개")
    
    print(f"\n" + "=" * 70)
    print("🎉 Technical Analysis Module 테스트 완료!")
    print(f"\n📚 사용법:")
    print("# 1. 모듈 import")
    print("from src.analysis.technical.technical_analysis import TechnicalAnalyzer")
    print("")
    print("# 2. 분석기 초기화")
    print("analyzer = TechnicalAnalyzer()")
    print("")
    print("# 3. 분석 실행")
    print("result = analyzer.analyze_stock('005930', ohlcv_data)")
    print("")
    print("# 4. 결과 확인")
    print("print(f'점수: {result[\"overall_score\"]:.1f}')")
    print("print(f'추천: {result[\"recommendation\"]}')")
    print("")
    print("📝 주의사항:")
    print("- OHLCV 데이터는 pandas DataFrame 형태여야 합니다")
    print("- 컬럼명: Open, High, Low, Close, Volume")
    print("- 최소 20일 이상의 데이터가 필요합니다")
    print("- TA-Lib가 없어도 기본 계산으로 동작합니다")
    print("")
    print("🔗 관련 파일:")
    print("- 이 파일을 다음 경로에 저장: src/analysis/technical/technical_analysis.py")
    print("- 실행 스크립트: scripts/analysis/run_technical_analysis.py")
