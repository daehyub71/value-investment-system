#!/usr/bin/env python3
"""
📈 기술분석 모듈 - 타입 안전 버전
Value Investment System - Technical Analysis Module

수정 사항:
- TA-Lib 데이터 타입 문제 해결
- float64 강제 변환
- 타입 안전성 강화
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# TA-Lib 체크
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

class TypeSafeTechnicalIndicators:
    """타입 안전 기술지표 계산 클래스"""
    
    @staticmethod
    def ensure_float64(data):
        """TA-Lib을 위한 float64 타입 보장"""
        if isinstance(data, pd.Series):
            return data.astype(np.float64)
        elif isinstance(data, np.ndarray):
            return data.astype(np.float64)
        else:
            return np.array(data, dtype=np.float64)
    
    @staticmethod
    def safe_calculate(func, *args, **kwargs):
        """안전한 계산 래퍼"""
        try:
            result = func(*args, **kwargs)
            if isinstance(result, pd.Series):
                last_value = result.iloc[-1] if len(result) > 0 else None
                if pd.isna(last_value) or np.isinf(last_value):
                    return None
                return float(last_value)
            elif isinstance(result, np.ndarray):
                if len(result) > 0 and not (pd.isna(result[-1]) or np.isinf(result[-1])):
                    return float(result[-1])
                return None
            return result
        except Exception as e:
            print(f"⚠️ 지표 계산 실패 ({func.__name__ if hasattr(func, '__name__') else 'calc'}): {e}")
            return None
    
    @staticmethod
    def sma(data: pd.Series, period: int) -> Optional[float]:
        """단순이동평균"""
        def calc():
            return data.rolling(window=period, min_periods=max(1, period//2)).mean()
        return TypeSafeTechnicalIndicators.safe_calculate(calc)
    
    @staticmethod
    def ema(data: pd.Series, period: int) -> Optional[float]:
        """지수이동평균"""
        def calc():
            if TALIB_AVAILABLE:
                # TA-Lib용 타입 변환
                float_data = TypeSafeTechnicalIndicators.ensure_float64(data)
                result = talib.EMA(float_data.values, timeperiod=period)
                return pd.Series(result, index=data.index)
            else:
                return data.ewm(span=period, min_periods=max(1, period//2)).mean()
        return TypeSafeTechnicalIndicators.safe_calculate(calc)
    
    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> Optional[float]:
        """RSI"""
        def calc():
            if TALIB_AVAILABLE:
                # TA-Lib용 타입 변환
                float_data = TypeSafeTechnicalIndicators.ensure_float64(data)
                result = talib.RSI(float_data.values, timeperiod=period)
                return pd.Series(result, index=data.index)
            else:
                delta = data.diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=max(1, period//2)).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=max(1, period//2)).mean()
                
                # 0으로 나누기 방지
                loss = loss.replace(0, 1e-10)
                rs = gain / loss
                return 100 - (100 / (1 + rs))
        return TypeSafeTechnicalIndicators.safe_calculate(calc)
    
    @staticmethod
    def macd(data: pd.Series) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """MACD"""
        try:
            if TALIB_AVAILABLE:
                # TA-Lib용 타입 변환
                float_data = TypeSafeTechnicalIndicators.ensure_float64(data)
                macd_line, signal_line, histogram = talib.MACD(
                    float_data.values, 
                    fastperiod=12, 
                    slowperiod=26, 
                    signalperiod=9
                )
                
                # NaN이 아닌 마지막 값 찾기
                def get_last_valid(arr):
                    valid_mask = ~np.isnan(arr)
                    if valid_mask.any():
                        return float(arr[valid_mask][-1])
                    return None
                
                macd_val = get_last_valid(macd_line)
                signal_val = get_last_valid(signal_line) 
                hist_val = get_last_valid(histogram)
            else:
                ema_12 = data.ewm(span=12, min_periods=6).mean()
                ema_26 = data.ewm(span=26, min_periods=13).mean()
                macd_series = ema_12 - ema_26
                signal_series = macd_series.ewm(span=9, min_periods=5).mean()
                hist_series = macd_series - signal_series
                
                macd_val = macd_series.iloc[-1] if not pd.isna(macd_series.iloc[-1]) else None
                signal_val = signal_series.iloc[-1] if not pd.isna(signal_series.iloc[-1]) else None
                hist_val = hist_series.iloc[-1] if not pd.isna(hist_series.iloc[-1]) else None
            
            return macd_val, signal_val, hist_val
        except Exception as e:
            print(f"⚠️ MACD 계산 실패: {e}")
            return None, None, None
    
    @staticmethod
    def bollinger_bands(data: pd.Series, period: int = 20) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """볼린저 밴드"""
        try:
            if TALIB_AVAILABLE:
                # TA-Lib용 타입 변환
                float_data = TypeSafeTechnicalIndicators.ensure_float64(data)
                upper, middle, lower = talib.BBANDS(
                    float_data.values, 
                    timeperiod=period, 
                    nbdevup=2, 
                    nbdevdn=2
                )
                
                # NaN이 아닌 마지막 값 찾기
                def get_last_valid(arr):
                    valid_mask = ~np.isnan(arr)
                    if valid_mask.any():
                        return float(arr[valid_mask][-1])
                    return None
                
                upper_val = get_last_valid(upper)
                middle_val = get_last_valid(middle)
                lower_val = get_last_valid(lower)
            else:
                middle_series = data.rolling(window=period, min_periods=max(1, period//2)).mean()
                std_series = data.rolling(window=period, min_periods=max(1, period//2)).std()
                upper_series = middle_series + (std_series * 2)
                lower_series = middle_series - (std_series * 2)
                
                upper_val = upper_series.iloc[-1] if not pd.isna(upper_series.iloc[-1]) else None
                middle_val = middle_series.iloc[-1] if not pd.isna(middle_series.iloc[-1]) else None
                lower_val = lower_series.iloc[-1] if not pd.isna(lower_series.iloc[-1]) else None
            
            # BB Position 계산
            current_price = float(data.iloc[-1])
            if upper_val and lower_val and upper_val != lower_val:
                bb_position = ((current_price - lower_val) / (upper_val - lower_val)) * 100
            else:
                bb_position = None
            
            return upper_val, middle_val, lower_val, bb_position
        except Exception as e:
            print(f"⚠️ 볼린저밴드 계산 실패: {e}")
            return None, None, None, None
    
    @staticmethod
    def stochastic(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> Tuple[Optional[float], Optional[float]]:
        """스토캐스틱"""
        try:
            if TALIB_AVAILABLE:
                # TA-Lib용 타입 변환
                high_float = TypeSafeTechnicalIndicators.ensure_float64(high)
                low_float = TypeSafeTechnicalIndicators.ensure_float64(low)
                close_float = TypeSafeTechnicalIndicators.ensure_float64(close)
                
                k_percent, d_percent = talib.STOCH(
                    high_float.values, 
                    low_float.values, 
                    close_float.values, 
                    fastk_period=period, 
                    slowk_period=3, 
                    slowd_period=3
                )
                
                # NaN이 아닌 마지막 값 찾기
                def get_last_valid(arr):
                    valid_mask = ~np.isnan(arr)
                    if valid_mask.any():
                        return float(arr[valid_mask][-1])
                    return None
                
                k_val = get_last_valid(k_percent)
                d_val = get_last_valid(d_percent)
            else:
                lowest_low = low.rolling(window=period, min_periods=max(1, period//2)).min()
                highest_high = high.rolling(window=period, min_periods=max(1, period//2)).max()
                
                # 0으로 나누기 방지
                denominator = highest_high - lowest_low
                denominator = denominator.replace(0, 1e-10)
                
                k_series = 100 * ((close - lowest_low) / denominator)
                d_series = k_series.rolling(window=3, min_periods=2).mean()
                
                k_val = k_series.iloc[-1] if not pd.isna(k_series.iloc[-1]) else None
                d_val = d_series.iloc[-1] if not pd.isna(d_series.iloc[-1]) else None
            
            return k_val, d_val
        except Exception as e:
            print(f"⚠️ 스토캐스틱 계산 실패: {e}")
            return None, None
    
    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> Optional[float]:
        """ADX"""
        try:
            if TALIB_AVAILABLE:
                # TA-Lib용 타입 변환
                high_float = TypeSafeTechnicalIndicators.ensure_float64(high)
                low_float = TypeSafeTechnicalIndicators.ensure_float64(low)
                close_float = TypeSafeTechnicalIndicators.ensure_float64(close)
                
                adx_values = talib.ADX(
                    high_float.values, 
                    low_float.values, 
                    close_float.values, 
                    timeperiod=period
                )
                
                # NaN이 아닌 마지막 값 찾기
                valid_mask = ~np.isnan(adx_values)
                if valid_mask.any():
                    return float(adx_values[valid_mask][-1])
                return None
            else:
                # 간단한 ADX 근사 계산
                tr1 = high - low
                tr2 = abs(high - close.shift(1))
                tr3 = abs(low - close.shift(1))
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                
                atr = tr.rolling(window=period, min_periods=max(1, period//2)).mean()
                
                dm_plus = np.maximum(high - high.shift(1), 0)
                dm_minus = np.maximum(low.shift(1) - low, 0)
                
                # DI 계산
                di_plus = 100 * (dm_plus.rolling(window=period, min_periods=max(1, period//2)).mean() / atr)
                di_minus = 100 * (dm_minus.rolling(window=period, min_periods=max(1, period//2)).mean() / atr)
                
                # DX 계산 (0으로 나누기 방지)
                di_sum = di_plus + di_minus
                di_sum = di_sum.replace(0, 1e-10)
                dx = 100 * abs(di_plus - di_minus) / di_sum
                
                # ADX 계산
                adx = dx.rolling(window=period, min_periods=max(1, period//2)).mean()
                return adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else None
        except Exception as e:
            print(f"⚠️ ADX 계산 실패: {e}")
            return None
    
    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> Optional[float]:
        """ATR"""
        def calc():
            if TALIB_AVAILABLE:
                # TA-Lib용 타입 변환
                high_float = TypeSafeTechnicalIndicators.ensure_float64(high)
                low_float = TypeSafeTechnicalIndicators.ensure_float64(low)
                close_float = TypeSafeTechnicalIndicators.ensure_float64(close)
                
                result = talib.ATR(high_float.values, low_float.values, close_float.values, timeperiod=period)
                return pd.Series(result, index=close.index)
            else:
                tr1 = high - low
                tr2 = abs(high - close.shift(1))
                tr3 = abs(low - close.shift(1))
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                return tr.rolling(window=period, min_periods=max(1, period//2)).mean()
        return TypeSafeTechnicalIndicators.safe_calculate(calc)

class TechnicalAnalyzer:
    """기술분석 엔진 - 타입 안전 버전"""
    
    def __init__(self):
        self.indicators = TypeSafeTechnicalIndicators()
        self.weights = {
            'trend': 0.35,
            'momentum': 0.30,
            'volatility': 0.20,
            'volume': 0.15
        }
    
    def analyze_stock(self, stock_code: str, ohlcv_data: pd.DataFrame) -> Dict[str, Any]:
        """종목 기술분석 실행"""
        try:
            # 데이터 검증
            if len(ohlcv_data) < 20:
                return {'error': f'데이터가 부족합니다. 최소 20일 필요, 현재 {len(ohlcv_data)}일'}
            
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            missing_columns = [col for col in required_columns if col not in ohlcv_data.columns]
            if missing_columns:
                return {'error': f'필수 컬럼 누락: {missing_columns}'}
            
            # 데이터 타입 보장
            print(f"🔍 데이터 타입 확인 및 변환...")
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                ohlcv_data[col] = pd.to_numeric(ohlcv_data[col], errors='coerce')
            
            # NaN 제거
            ohlcv_data = ohlcv_data.dropna()
            
            print(f"📊 정제된 데이터: {len(ohlcv_data)}일")
            
            # 기본 정보
            current_price = float(ohlcv_data['Close'].iloc[-1])
            analysis_date = datetime.now().isoformat()
            
            print(f"🔍 기술지표 계산 시작 ({len(ohlcv_data)}일 데이터)")
            
            # 기술지표 계산
            technical_indicators = self._calculate_all_indicators_safe(ohlcv_data)
            
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
                    'indicators_calculated': len([v for v in technical_indicators.values() if v is not None]),
                    'signals_generated': len(trading_signals.get('individual_signals', {})),
                    'calculation_method': 'TA-Lib' if TALIB_AVAILABLE else 'Basic Math'
                }
            }
            
        except Exception as e:
            print(f"❌ 분석 중 치명적 오류: {e}")
            import traceback
            traceback.print_exc()
            return {'error': f'분석 중 오류 발생: {str(e)}'}
    
    def _calculate_all_indicators_safe(self, ohlcv_data: pd.DataFrame) -> Dict[str, Any]:
        """안전한 기술지표 계산 - 타입 안전 버전"""
        high = ohlcv_data['High'].astype(np.float64)
        low = ohlcv_data['Low'].astype(np.float64)
        close = ohlcv_data['Close'].astype(np.float64)
        volume = ohlcv_data['Volume'].astype(np.float64)
        
        indicators = {}
        calculation_log = []
        
        print(f"🔢 데이터 타입: {close.dtype}, 길이: {len(close)}")
        
        # 1. 이동평균 (가장 안전한 지표부터)
        print("📊 이동평균 계산 중...")
        indicators['SMA_5'] = self.indicators.sma(close, 5)
        calculation_log.append(f"SMA_5: {'✅' if indicators['SMA_5'] else '❌'}")
        
        indicators['SMA_20'] = self.indicators.sma(close, 20)
        calculation_log.append(f"SMA_20: {'✅' if indicators['SMA_20'] else '❌'}")
        
        indicators['SMA_60'] = self.indicators.sma(close, 60)
        calculation_log.append(f"SMA_60: {'✅' if indicators['SMA_60'] else '❌'}")
        
        indicators['SMA_120'] = self.indicators.sma(close, 120)
        calculation_log.append(f"SMA_120: {'✅' if indicators['SMA_120'] else '❌'}")
        
        indicators['EMA_12'] = self.indicators.ema(close, 12)
        calculation_log.append(f"EMA_12: {'✅' if indicators['EMA_12'] else '❌'}")
        
        indicators['EMA_26'] = self.indicators.ema(close, 26)
        calculation_log.append(f"EMA_26: {'✅' if indicators['EMA_26'] else '❌'}")
        
        # 2. 모멘텀 지표
        print("📊 모멘텀 지표 계산 중...")
        indicators['RSI'] = self.indicators.rsi(close, 14)
        calculation_log.append(f"RSI: {'✅' if indicators['RSI'] else '❌'}")
        
        # MACD
        macd_val, signal_val, hist_val = self.indicators.macd(close)
        indicators['MACD'] = macd_val
        indicators['MACD_SIGNAL'] = signal_val
        indicators['MACD_HISTOGRAM'] = hist_val
        calculation_log.append(f"MACD: {'✅' if macd_val else '❌'}")
        
        # 3. 스토캐스틱
        stoch_k, stoch_d = self.indicators.stochastic(high, low, close)
        indicators['STOCH_K'] = stoch_k
        indicators['STOCH_D'] = stoch_d
        calculation_log.append(f"Stochastic: {'✅' if stoch_k else '❌'}")
        
        # 4. 변동성 지표
        print("📊 변동성 지표 계산 중...")
        bb_upper, bb_middle, bb_lower, bb_position = self.indicators.bollinger_bands(close)
        indicators['BB_UPPER'] = bb_upper
        indicators['BB_MIDDLE'] = bb_middle
        indicators['BB_LOWER'] = bb_lower
        indicators['BB_POSITION'] = bb_position
        calculation_log.append(f"Bollinger Bands: {'✅' if bb_position else '❌'}")
        
        indicators['ATR'] = self.indicators.atr(high, low, close)
        calculation_log.append(f"ATR: {'✅' if indicators['ATR'] else '❌'}")
        
        # 5. 추세 지표
        print("📊 추세 지표 계산 중...")
        indicators['ADX'] = self.indicators.adx(high, low, close)
        calculation_log.append(f"ADX: {'✅' if indicators['ADX'] else '❌'}")
        
        # 6. 52주 고가/저가 (간단한 계산)
        try:
            high_52w = high.tail(min(252, len(high))).max()
            low_52w = low.tail(min(252, len(low))).min()
            current_price = close.iloc[-1]
            
            indicators['52W_HIGH'] = float(high_52w)
            indicators['52W_LOW'] = float(low_52w)
            indicators['52W_HIGH_RATIO'] = float((current_price / high_52w) * 100) if high_52w > 0 else None
            indicators['52W_LOW_RATIO'] = float((current_price / low_52w) * 100) if low_52w > 0 else None
            calculation_log.append("52W High/Low: ✅")
        except Exception as e:
            print(f"⚠️ 52주 고가/저가 계산 실패: {e}")
            indicators['52W_HIGH'] = None
            indicators['52W_LOW'] = None
            indicators['52W_HIGH_RATIO'] = None
            indicators['52W_LOW_RATIO'] = None
            calculation_log.append("52W High/Low: ❌")
        
        # 계산 결과 로그 출력
        successful_indicators = [k for k, v in indicators.items() if v is not None]
        print(f"✅ 계산 완료된 지표: {len(successful_indicators)}/{len(indicators)}개")
        print(f"📋 상세 결과: {', '.join(calculation_log[:10])}")
        
        return indicators
    
    def _generate_trading_signals(self, indicators: Dict[str, Any], ohlcv_data: pd.DataFrame) -> Dict[str, Any]:
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
            signals['ADX'] = 'TREND_STRONG'
        else:
            signals['ADX'] = 'TREND_WEAK'
        
        # 신호 통계
        buy_signals = sum(1 for signal in signals.values() if 'BUY' in signal)
        sell_signals = sum(1 for signal in signals.values() if 'SELL' in signal)
        neutral_signals = len(signals) - buy_signals - sell_signals
        
        # 총 점수 계산
        if signal_scores:
            total_score = sum(signal_scores)
            max_possible_score = len(signal_scores) * SIGNAL_LEVELS['STRONG_BUY']
            min_possible_score = len(signal_scores) * SIGNAL_LEVELS['STRONG_SELL']
            
            if max_possible_score != min_possible_score:
                normalized_score = ((total_score - min_possible_score) / 
                                   (max_possible_score - min_possible_score)) * 100
            else:
                normalized_score = 50
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
    
    def _assess_risk_level(self, indicators: Dict[str, Any], ohlcv_data: pd.DataFrame) -> str:
        """리스크 레벨 평가"""
        risk_factors = []
        
        # 변동성 체크
        atr = indicators.get('ATR')
        if atr is not None:
            close = ohlcv_data['Close']
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
    
    def _generate_analysis_summary(self, indicators: Dict[str, Any], 
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
        if atr is not None:
            close = ohlcv_data['Close']
            volatility_ratio = (atr / close.iloc[-1]) * 100
            if volatility_ratio > 5:
                summary['volatility_level'] = 'High'
            elif volatility_ratio > 2:
                summary['volatility_level'] = 'Medium'
            else:
                summary['volatility_level'] = 'Low'
        else:
            summary['volatility_level'] = 'Unknown'
        
        # 거래량 추세
        try:
            volume = ohlcv_data['Volume']
            recent_volume = volume.tail(5).mean()
            historical_volume = volume.tail(20).mean()
            
            if recent_volume > historical_volume * 1.2:
                summary['volume_trend'] = 'Increasing'
            elif recent_volume < historical_volume * 0.8:
                summary['volume_trend'] = 'Decreasing'
            else:
                summary['volume_trend'] = 'Normal'
        except:
            summary['volume_trend'] = 'Normal'
        
        return summary

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
    
    rsi = indicators.get('RSI')
    print(f"   RSI: {rsi:.1f}" if rsi else "   RSI: N/A")
    
    macd = indicators.get('MACD')
    print(f"   MACD: {macd:.2f}" if macd else "   MACD: N/A")
    
    bb_position = indicators.get('BB_POSITION')
    print(f"   볼린저밴드 위치: {bb_position:.1f}%" if bb_position else "   볼린저밴드 위치: N/A")
    
    adx = indicators.get('ADX')
    print(f"   ADX: {adx:.1f}" if adx else "   ADX: N/A")
    
    high_ratio = indicators.get('52W_HIGH_RATIO')
    print(f"   52주 고가 비율: {high_ratio:.1f}%" if high_ratio else "   52주 고가 비율: N/A")
    
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

# 버전 정보
__version__ = "1.2.0"
__author__ = "Value Investment System"
__description__ = "Technical Analysis Module - Type Safe Version"
