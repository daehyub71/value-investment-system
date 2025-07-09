"""
모멘텀 지표 모듈
매수/매도 타이밍 포착을 위한 지표들
"""

import pandas as pd
import numpy as np

class MomentumIndicators:
    def __init__(self):
        pass
    
    def rsi(self, data, period=14):
        """상대강도지수 (Relative Strength Index)"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def macd(self, data, fast=12, slow=26, signal=9):
        """MACD (Moving Average Convergence Divergence)"""
        ema_fast = data.ewm(span=fast).mean()
        ema_slow = data.ewm(span=slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
    
    def stochastic(self, high, low, close, k_period=14, d_period=3):
        """스토캐스틱 오실레이터"""
        pass
    
    def williams_r(self, high, low, close, period=14):
        """Williams %R"""
        pass
