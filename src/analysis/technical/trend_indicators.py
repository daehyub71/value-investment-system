"""
추세 지표 모듈
장기 추세 분석을 위한 지표들
"""

import pandas as pd
import numpy as np

class TrendIndicators:
    def __init__(self):
        pass
    
    def sma(self, data, period):
        """단순이동평균 (Simple Moving Average)"""
        return data.rolling(window=period).mean()
    
    def ema(self, data, period):
        """지수이동평균 (Exponential Moving Average)"""
        return data.ewm(span=period).mean()
    
    def parabolic_sar(self, high, low, close):
        """파라볼릭 SAR"""
        pass
    
    def adx(self, high, low, close, period=14):
        """평균방향지수 (Average Directional Index)"""
        pass
    
    def ichimoku_cloud(self, high, low, close):
        """이치모쿠 구름"""
        pass
