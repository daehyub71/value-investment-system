#!/usr/bin/env python3
"""
추세 지표 차트 시각화 시스템 (실제 DB 구조 대응)
기존 데이터를 활용하여 실시간 기술적 지표 계산 및 차트 생성

실행 방법:
python scripts/visualization/create_trend_chart_v2.py --stock_code=005930 --period=6M --output=html
python scripts/visualization/create_trend_chart_v2.py --stock_code=000660 --save_png --show_signals
"""

import os
import sys
import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import warnings

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# config 모듈에서 올바른 함수들 import
try:
    from config import config_manager
    logger = config_manager.get_logger(__name__) if config_manager else logging.getLogger(__name__)
except:
    logger = logging.getLogger(__name__)

# 경고 무시
warnings.filterwarnings('ignore')

class TechnicalIndicatorCalculator:
    """실시간 기술적 지표 계산기"""
    
    @staticmethod
    def calculate_sma(data: pd.Series, period: int) -> pd.Series:
        """단순이동평균 계산"""
        return data.rolling(window=period).mean()
    
    @staticmethod
    def calculate_ema(data: pd.Series, period: int) -> pd.Series:
        """지수이동평균 계산"""
        return data.ewm(span=period).mean()
    
    @staticmethod
    def calculate_rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """RSI 계산"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def calculate_macd(data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, pd.Series]:
        """MACD 계산"""
        ema_fast = data.ewm(span=fast).mean()
        ema_slow = data.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=signal).mean()
        macd_histogram = macd - macd_signal
        
        return {
            'macd': macd,
            'signal': macd_signal,
            'histogram': macd_histogram
        }
    
    @staticmethod
    def calculate_bollinger_bands(data: pd.Series, period: int = 20, std_dev: int = 2) -> Dict[str, pd.Series]:
        """볼린저 밴드 계산"""
        sma = data.rolling(window=period).mean()
        std = data.rolling(window=period).std()
        
        return {
            'upper': sma + (std * std_dev),
            'middle': sma,
            'lower': sma - (std * std_dev)
        }
    
    @staticmethod
    def calculate_stochastic(high: pd.Series, low: pd.Series, close: pd.Series, k_period: int = 14, d_period: int = 3) -> Dict[str, pd.Series]:
        """스토캐스틱 계산"""
        lowest_low = low.rolling(window=k_period).min()
        highest_high = high.rolling(window=k_period).max()
        
        k_percent = 100 * (close - lowest_low) / (highest_high - lowest_low)
        d_percent = k_percent.rolling(window=d_period).mean()
        
        return {
            'k': k_percent,
            'd': d_percent
        }


class TrendChartGenerator:
    """추세 지표 차트 생성 클래스 (실제 DB 구조 대응)"""
    
    def __init__(self):
        """초기화"""
        try:
            if config_manager:
                self.db_conn = config_manager.get_database_connection('stock')
            else:
                # 폴백: 직접 데이터베이스 연결
                db_path = project_root / 'data' / 'databases' / 'stock_data.db'
                self.db_conn = sqlite3.connect(str(db_path))
                self.db_conn.row_factory = sqlite3.Row
        except Exception as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            # 폴백: 직접 연결
            db_path = project_root / 'data' / 'databases' / 'stock_data.db'
            self.db_conn = sqlite3.connect(str(db_path))
            self.db_conn.row_factory = sqlite3.Row
            
        self.calculator = TechnicalIndicatorCalculator()
        self.chart_config = {
            'width': 1200,
            'height': 800,
            'theme': 'plotly_white',
            'font_size': 12,
            'title_font_size': 16,
            'colors': {
                'price': '#2563eb',
                'sma_5': '#ef4444',
                'sma_20': '#f59e0b',
                'sma_60': '#10b981',
                'sma_120': '#8b5cf6',
                'sma_200': '#64748b',
                'ema_12': '#06b6d4',
                'ema_26': '#f97316',
                'volume': '#e3f2fd',
                'rsi': '#9333ea',
                'macd': '#059669',
                'signal': '#dc2626'
            }
        }
        
    def load_stock_data(self, stock_code: str, period: str = '6M') -> pd.DataFrame:
        """
        주가 데이터 로드 및 기술적 지표 실시간 계산
        
        Args:
            stock_code: 주식 코드 (예: '005930')
            period: 조회 기간 ('1M', '3M', '6M', '1Y', '2Y')
        
        Returns:
            주가 데이터 + 기술적 지표 DataFrame
        """
        try:
            # 기간 계산
            end_date = datetime.now()
            if period == '1M':
                start_date = end_date - timedelta(days=30)
            elif period == '3M':
                start_date = end_date - timedelta(days=90)
            elif period == '6M':
                start_date = end_date - timedelta(days=180)
            elif period == '1Y':
                start_date = end_date - timedelta(days=365)
            elif period == '2Y':
                start_date = end_date - timedelta(days=730)
            else:
                start_date = end_date - timedelta(days=180)  # 기본 6개월
            
            # 실제 DB 구조에 맞는 쿼리 (기본 OHLCV만)
            query = """
                SELECT 
                    date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume
                FROM stock_prices 
                WHERE stock_code = ? 
                    AND date >= ? 
                    AND date <= ?
                ORDER BY date ASC
            """
            
            cursor = self.db_conn.cursor()
            cursor.execute(query, (
                stock_code, 
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            ))
            
            rows = cursor.fetchall()
            if not rows:
                logger.warning(f"데이터 없음: {stock_code} ({period})")
                return pd.DataFrame()
            
            # DataFrame 생성
            df = pd.DataFrame(rows, columns=[
                'date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume'
            ])
            
            # 컬럼명 표준화
            df.rename(columns={
                'open_price': 'open',
                'high_price': 'high', 
                'low_price': 'low',
                'close_price': 'close'
            }, inplace=True)
            
            # 날짜 인덱스 설정
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # 숫자 타입 변환
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
            
            # 기술적 지표 실시간 계산
            logger.info(f"기술적 지표 계산 중: {stock_code}")
            
            # 1. 이동평균선
            df['sma_5'] = self.calculator.calculate_sma(df['close'], 5)
            df['sma_20'] = self.calculator.calculate_sma(df['close'], 20)
            df['sma_60'] = self.calculator.calculate_sma(df['close'], 60)
            df['sma_120'] = self.calculator.calculate_sma(df['close'], 120)
            df['sma_200'] = self.calculator.calculate_sma(df['close'], 200)
            
            # 2. 지수이동평균
            df['ema_12'] = self.calculator.calculate_ema(df['close'], 12)
            df['ema_26'] = self.calculator.calculate_ema(df['close'], 26)
            
            # 3. RSI
            df['rsi'] = self.calculator.calculate_rsi(df['close'])
            
            # 4. MACD
            macd_data = self.calculator.calculate_macd(df['close'])
            df['macd'] = macd_data['macd']
            df['macd_signal'] = macd_data['signal']
            df['macd_histogram'] = macd_data['histogram']
            
            # 5. 볼린저 밴드
            bb_data = self.calculator.calculate_bollinger_bands(df['close'])
            df['bb_upper'] = bb_data['upper']
            df['bb_middle'] = bb_data['middle']
            df['bb_lower'] = bb_data['lower']
            
            # 6. 스토캐스틱
            stoch_data = self.calculator.calculate_stochastic(df['high'], df['low'], df['close'])
            df['stoch_k'] = stoch_data['k']
            df['stoch_d'] = stoch_data['d']
            
            # 7. 거래량 이동평균
            df['volume_sma_20'] = self.calculator.calculate_sma(df['volume'], 20)
            
            logger.info(f"데이터 로드 및 지표 계산 완료: {stock_code} ({len(df)}일, {period})")
            return df
            
        except Exception as e:
            logger.error(f"데이터 로드 실패: {stock_code} - {e}")
            return pd.DataFrame()
    
    def get_company_info(self, stock_code: str) -> Dict[str, str]:
        """회사 정보 조회"""
        try:
            query = """
                SELECT company_name, market_type, sector, industry 
                FROM company_info 
                WHERE stock_code = ?
            """
            cursor = self.db_conn.cursor()
            cursor.execute(query, (stock_code,))
            row = cursor.fetchone()
            
            if row:
                return {
                    'company_name': row[0] or stock_code,
                    'market_type': row[1] or 'KOSPI',
                    'sector': row[2] or '일반',
                    'industry': row[3] or '기타'
                }
            else:
                return {
                    'company_name': stock_code,
                    'market_type': 'KOSPI',
                    'sector': '일반',
                    'industry': '기타'
                }
        except Exception as e:
            logger.warning(f"회사 정보 조회 실패: {stock_code} - {e}")
            return {'company_name': stock_code, 'market_type': 'KOSPI', 'sector': '일반', 'industry': '기타'}
    
    def analyze_trend_signals(self, df: pd.DataFrame) -> Dict[str, Any]:
        """추세 신호 분석"""
        if df.empty or len(df) < 2:
            return {'signals': [], 'current_trend': '데이터 부족', 'trend_strength': 0}
        
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        signals = []
        
        try:
            # 현재 추세 분석
            trend_factors = []
            
            # 1. 주가 vs 이동평균 비교
            if not pd.isna(latest['close']) and not pd.isna(latest['sma_20']):
                if latest['close'] > latest['sma_20']:
                    signals.append({
                        'type': '긍정',
                        'message': f"주가({latest['close']:,.0f})가 20일 이평선({latest['sma_20']:,.0f}) 위",
                        'strength': 1
                    })
                    trend_factors.append(1)
                else:
                    signals.append({
                        'type': '부정',
                        'message': f"주가({latest['close']:,.0f})가 20일 이평선({latest['sma_20']:,.0f}) 아래",
                        'strength': -1
                    })
                    trend_factors.append(-1)
            
            # 2. 단기 vs 장기 이평선 비교
            if not pd.isna(latest['sma_5']) and not pd.isna(latest['sma_20']):
                if latest['sma_5'] > latest['sma_20']:
                    signals.append({
                        'type': '긍정',
                        'message': '5일선이 20일선 위 (단기 상승)',
                        'strength': 1
                    })
                    trend_factors.append(1)
                else:
                    signals.append({
                        'type': '부정',
                        'message': '5일선이 20일선 아래 (단기 하락)',
                        'strength': -1
                    })
                    trend_factors.append(-1)
            
            # 3. 장기 추세 (200일선)
            if not pd.isna(latest['close']) and not pd.isna(latest['sma_200']):
                if latest['close'] > latest['sma_200']:
                    signals.append({
                        'type': '긍정',
                        'message': '장기 상승 추세 (200일선 위)',
                        'strength': 2
                    })
                    trend_factors.append(2)
                else:
                    signals.append({
                        'type': '부정',
                        'message': '장기 하락 추세 (200일선 아래)',
                        'strength': -2
                    })
                    trend_factors.append(-2)
            
            # 4. RSI 분석
            if not pd.isna(latest['rsi']):
                if latest['rsi'] > 70:
                    signals.append({
                        'type': '주의',
                        'message': f'RSI 과매수 구간 ({latest["rsi"]:.1f})',
                        'strength': -1
                    })
                elif latest['rsi'] < 30:
                    signals.append({
                        'type': '기회',
                        'message': f'RSI 과매도 구간 ({latest["rsi"]:.1f})',
                        'strength': 1
                    })
                else:
                    signals.append({
                        'type': '중립',
                        'message': f'RSI 정상 범위 ({latest["rsi"]:.1f})',
                        'strength': 0
                    })
            
            # 5. MACD 분석
            if not pd.isna(latest['macd']) and not pd.isna(latest['macd_signal']):
                if latest['macd'] > latest['macd_signal']:
                    signals.append({
                        'type': '긍정',
                        'message': 'MACD 상승 신호',
                        'strength': 1
                    })
                    trend_factors.append(1)
                else:
                    signals.append({
                        'type': '부정',
                        'message': 'MACD 하락 신호',
                        'strength': -1
                    })
                    trend_factors.append(-1)
            
            # 6. 골든크로스/데드크로스 감지
            if (not pd.isna(latest['sma_5']) and not pd.isna(latest['sma_20']) and
                not pd.isna(prev['sma_5']) and not pd.isna(prev['sma_20'])):
                
                # 골든크로스: 단기선이 장기선을 상향 돌파
                if (prev['sma_5'] <= prev['sma_20'] and latest['sma_5'] > latest['sma_20']):
                    signals.append({
                        'type': '매수',
                        'message': '🟢 골든크로스 발생! (5일선 > 20일선)',
                        'strength': 3
                    })
                    trend_factors.append(3)
                
                # 데드크로스: 단기선이 장기선을 하향 돌파
                elif (prev['sma_5'] >= prev['sma_20'] and latest['sma_5'] < latest['sma_20']):
                    signals.append({
                        'type': '매도',
                        'message': '🔴 데드크로스 발생! (5일선 < 20일선)',
                        'strength': -3
                    })
                    trend_factors.append(-3)
            
            # 추세 강도 계산 (-10 ~ +10)
            trend_strength = sum(trend_factors) if trend_factors else 0
            trend_strength = max(-10, min(10, trend_strength))
            
            # 전체 추세 판단
            if trend_strength >= 3:
                current_trend = '강한 상승 추세'
            elif trend_strength >= 1:
                current_trend = '상승 추세'
            elif trend_strength <= -3:
                current_trend = '강한 하락 추세'
            elif trend_strength <= -1:
                current_trend = '하락 추세'
            else:
                current_trend = '횡보'
            
            return {
                'signals': signals,
                'current_trend': current_trend,
                'trend_strength': trend_strength
            }
            
        except Exception as e:
            logger.warning(f"추세 신호 분석 실패: {e}")
            return {'signals': [], 'current_trend': '분석 불가', 'trend_strength': 0}
    
    def create_trend_chart(self, stock_code: str, period: str = '6M', 
                          indicators: Dict[str, bool] = None) -> go.Figure:
        """
        추세 지표 차트 생성
        
        Args:
            stock_code: 주식 코드
            period: 조회 기간
            indicators: 표시할 지표 설정
        
        Returns:
            Plotly Figure 객체
        """
        
        # 기본 지표 설정
        if indicators is None:
            indicators = {
                'sma_5': True,
                'sma_20': True,
                'sma_60': False,
                'sma_120': False,
                'sma_200': True,
                'ema_12': False,
                'ema_26': False,
                'volume': True,
                'rsi': True,
                'macd': True
            }
        
        # 데이터 로드
        df = self.load_stock_data(stock_code, period)
        if df.empty:
            # 빈 차트 반환
            fig = go.Figure()
            fig.add_annotation(
                text=f"데이터가 없습니다: {stock_code}",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="red")
            )
            return fig
        
        # 회사 정보
        company_info = self.get_company_info(stock_code)
        
        # 추세 분석
        trend_analysis = self.analyze_trend_signals(df)
        
        # 서브플롯 생성 (가격, 거래량, RSI, MACD)
        subplot_titles = [
            f"{company_info['company_name']} ({stock_code}) - 추세 분석",
            "거래량",
            "RSI",
            "MACD"
        ]
        
        row_heights = [0.5, 0.15, 0.15, 0.2]  # 비율 조정
        
        fig = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            subplot_titles=subplot_titles,
            row_heights=row_heights,
            specs=[[{"secondary_y": False}],
                   [{"secondary_y": False}],
                   [{"secondary_y": False}],
                   [{"secondary_y": False}]]
        )
        
        # 1. 메인 차트: 캔들스틱 + 이동평균선
        fig.add_trace(
            go.Candlestick(
                x=df.index,
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name="가격",
                increasing_line_color=self.chart_config['colors']['price'],
                decreasing_line_color='red'
            ),
            row=1, col=1
        )
        
        # 이동평균선 추가
        ma_lines = [
            ('sma_5', '5일 이평선', 'dash'),
            ('sma_20', '20일 이평선', 'solid'),
            ('sma_60', '60일 이평선', 'dot'),
            ('sma_120', '120일 이평선', 'dashdot'),
            ('sma_200', '200일 이평선', 'solid'),
            ('ema_12', '12일 지수이평선', 'dash'),
            ('ema_26', '26일 지수이평선', 'dash')
        ]
        
        for col_name, name, line_style in ma_lines:
            if indicators.get(col_name, False) and col_name in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df[col_name],
                        mode='lines',
                        name=name,
                        line=dict(
                            color=self.chart_config['colors'].get(col_name, 'gray'),
                            width=2 if col_name in ['sma_20', 'sma_200'] else 1,
                            dash=line_style
                        ),
                        hovertemplate=f'{name}: %{{y:,.0f}}원<extra></extra>'
                    ),
                    row=1, col=1
                )
        
        # 2. 거래량 차트
        if indicators.get('volume', True):
            colors = ['red' if close < open else 'blue' 
                     for close, open in zip(df['close'], df['open'])]
            
            fig.add_trace(
                go.Bar(
                    x=df.index,
                    y=df['volume'],
                    name='거래량',
                    marker_color=colors,
                    opacity=0.6,
                    hovertemplate='거래량: %{y:,.0f}주<extra></extra>'
                ),
                row=2, col=1
            )
            
            # 거래량 이평선
            if 'volume_sma_20' in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['volume_sma_20'],
                        mode='lines',
                        name='거래량 20일 평균',
                        line=dict(color='orange', width=1),
                        hovertemplate='거래량 평균: %{y:,.0f}주<extra></extra>'
                    ),
                    row=2, col=1
                )
        
        # 3. RSI 차트
        if indicators.get('rsi', True) and 'rsi' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['rsi'],
                    mode='lines',
                    name='RSI',
                    line=dict(color=self.chart_config['colors']['rsi'], width=2),
                    hovertemplate='RSI: %{y:.1f}<extra></extra>'
                ),
                row=3, col=1
            )
            
            # RSI 기준선 (30, 70)
            fig.add_hline(y=70, line_dash="dash", line_color="red", 
                         annotation_text="과매수(70)", row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="blue", 
                         annotation_text="과매도(30)", row=3, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color="gray", row=3, col=1)
        
        # 4. MACD 차트
        if indicators.get('macd', True):
            if 'macd' in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['macd'],
                        mode='lines',
                        name='MACD',
                        line=dict(color=self.chart_config['colors']['macd'], width=2),
                        hovertemplate='MACD: %{y:.2f}<extra></extra>'
                    ),
                    row=4, col=1
                )
            
            if 'macd_signal' in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['macd_signal'],
                        mode='lines',
                        name='Signal',
                        line=dict(color=self.chart_config['colors']['signal'], width=1),
                        hovertemplate='Signal: %{y:.2f}<extra></extra>'
                    ),
                    row=4, col=1
                )
            
            if 'macd_histogram' in df.columns:
                colors = ['red' if val < 0 else 'green' for val in df['macd_histogram']]
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df['macd_histogram'],
                        name='Histogram',
                        marker_color=colors,
                        opacity=0.6,
                        hovertemplate='Histogram: %{y:.2f}<extra></extra>'
                    ),
                    row=4, col=1
                )
        
        # 레이아웃 설정
        fig.update_layout(
            title=dict(
                text=f"📈 {company_info['company_name']} ({stock_code}) 추세 분석<br>"
                     f"<sub>현재 추세: {trend_analysis['current_trend']} | "
                     f"추세 강도: {trend_analysis['trend_strength']}/10 | "
                     f"업종: {company_info['sector']}</sub>",
                font=dict(size=self.chart_config['title_font_size']),
                x=0.5
            ),
            width=self.chart_config['width'],
            height=self.chart_config['height'],
            template=self.chart_config['theme'],
            font=dict(size=self.chart_config['font_size']),
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # Y축 설정
        fig.update_yaxes(title_text="가격 (원)", row=1, col=1)
        fig.update_yaxes(title_text="거래량", row=2, col=1)
        fig.update_yaxes(title_text="RSI", range=[0, 100], row=3, col=1)
        fig.update_yaxes(title_text="MACD", row=4, col=1)
        
        # X축 설정
        fig.update_xaxes(title_text="날짜", row=4, col=1)
        
        logger.info(f"차트 생성 완료: {stock_code} ({period})")
        return fig
    
    def save_chart(self, fig: go.Figure, filename: str, format: str = 'html') -> str:
        """차트 저장"""
        try:
            output_dir = project_root / 'output' / 'charts'
            output_dir.mkdir(parents=True, exist_ok=True)
            
            if format.lower() == 'html':
                filepath = output_dir / f"{filename}.html"
                fig.write_html(str(filepath))
            elif format.lower() == 'png':
                filepath = output_dir / f"{filename}.png"
                fig.write_image(str(filepath))
            elif format.lower() == 'pdf':
                filepath = output_dir / f"{filename}.pdf"
                fig.write_image(str(filepath))
            else:
                raise ValueError(f"지원하지 않는 형식: {format}")
            
            logger.info(f"차트 저장 완료: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"차트 저장 실패: {e}")
            return ""
    
    def generate_analysis_report(self, stock_code: str, period: str = '6M') -> Dict[str, Any]:
        """종합 분석 리포트 생성"""
        try:
            df = self.load_stock_data(stock_code, period)
            if df.empty:
                return {'error': '데이터 없음'}
            
            company_info = self.get_company_info(stock_code)
            trend_analysis = self.analyze_trend_signals(df)
            
            # 기본 통계
            latest = df.iloc[-1]
            stats = {
                'current_price': latest['close'],
                'price_change': latest['close'] - df.iloc[-2]['close'] if len(df) > 1 else 0,
                'price_change_pct': ((latest['close'] - df.iloc[-2]['close']) / df.iloc[-2]['close'] * 100) if len(df) > 1 else 0,
                'volume': latest['volume'],
                'avg_volume': df['volume'].mean(),
                'high_52w': df['high'].max(),
                'low_52w': df['low'].min(),
                'volatility': df['close'].pct_change().std() * np.sqrt(252) * 100  # 연간 변동성
            }
            
            # 기술적 지표 현재값
            technical_indicators = {}
            for col in ['sma_5', 'sma_20', 'sma_60', 'sma_120', 'sma_200', 
                       'ema_12', 'ema_26', 'rsi', 'macd', 'macd_signal']:
                if col in df.columns and not pd.isna(latest[col]):
                    technical_indicators[col] = latest[col]
            
            return {
                'stock_code': stock_code,
                'company_info': company_info,
                'stats': stats,
                'technical_indicators': technical_indicators,
                'trend_analysis': trend_analysis,
                'data_period': f"{df.index[0].strftime('%Y-%m-%d')} ~ {df.index[-1].strftime('%Y-%m-%d')}",
                'data_points': len(df)
            }
            
        except Exception as e:
            logger.error(f"분석 리포트 생성 실패: {stock_code} - {e}")
            return {'error': str(e)}


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='추세 지표 차트 생성 (실제 DB 구조 대응)')
    parser.add_argument('--stock_code', type=str, required=True, help='주식 코드 (예: 005930)')
    parser.add_argument('--period', type=str, default='6M', choices=['1M', '3M', '6M', '1Y', '2Y'], help='조회 기간')
    parser.add_argument('--output', type=str, default='html', choices=['html', 'png', 'pdf'], help='출력 형식')
    parser.add_argument('--save_png', action='store_true', help='PNG 파일로도 저장')
    parser.add_argument('--show_signals', action='store_true', help='매매 신호 표시')
    parser.add_argument('--report', action='store_true', help='분석 리포트 출력')
    
    args = parser.parse_args()
    
    # 차트 생성기 초기화
    generator = TrendChartGenerator()
    
    try:
        # 지표 설정
        indicators = {
            'sma_5': True,
            'sma_20': True,
            'sma_60': False,
            'sma_120': False,
            'sma_200': True,
            'ema_12': False,
            'ema_26': False,
            'volume': True,
            'rsi': True,
            'macd': True
        }
        
        # 차트 생성
        print(f"📈 {args.stock_code} 추세 차트 생성 중 (실시간 지표 계산)...")
        fig = generator.create_trend_chart(args.stock_code, args.period, indicators)
        
        # 차트 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"trend_chart_{args.stock_code}_{args.period}_{timestamp}"
        
        saved_path = generator.save_chart(fig, filename, args.output)
        print(f"✅ 차트 저장 완료: {saved_path}")
        
        # PNG 추가 저장
        if args.save_png and args.output != 'png':
            png_path = generator.save_chart(fig, filename, 'png')
            print(f"✅ PNG 저장 완료: {png_path}")
        
        # 분석 리포트
        if args.report:
            print(f"\n📊 {args.stock_code} 종합 분석 리포트")
            print("=" * 50)
            
            report = generator.generate_analysis_report(args.stock_code, args.period)
            
            if 'error' in report:
                print(f"❌ 오류: {report['error']}")
            else:
                # 기본 정보
                company = report['company_info']
                stats = report['stats']
                trend = report['trend_analysis']
                
                print(f"회사명: {company['company_name']}")
                print(f"업종: {company['sector']} - {company['industry']}")
                print(f"시장: {company['market_type']}")
                print(f"분석 기간: {report['data_period']} ({report['data_points']}일)")
                
                print(f"\n💰 현재 주가 정보:")
                print(f"  현재가: {stats['current_price']:,.0f}원")
                print(f"  전일 대비: {stats['price_change']:+,.0f}원 ({stats['price_change_pct']:+.2f}%)")
                print(f"  거래량: {stats['volume']:,.0f}주 (평균: {stats['avg_volume']:,.0f}주)")
                print(f"  52주 고점: {stats['high_52w']:,.0f}원")
                print(f"  52주 저점: {stats['low_52w']:,.0f}원")
                print(f"  연간 변동성: {stats['volatility']:.1f}%")
                
                print(f"\n📈 추세 분석:")
                print(f"  현재 추세: {trend['current_trend']}")
                print(f"  추세 강도: {trend['trend_strength']}/10")
                print(f"  감지된 신호: {len(trend['signals'])}개")
                
                for signal in trend['signals'][:5]:  # 상위 5개만 표시
                    emoji = {'긍정': '🟢', '부정': '🔴', '중립': '🟡', '주의': '🟠', '기회': '💚', '매수': '⬆️', '매도': '⬇️'}.get(signal['type'], '📍')
                    print(f"    {emoji} {signal['message']}")
                    
                print(f"\n📊 주요 기술적 지표:")
                indicators = report['technical_indicators']
                if 'sma_20' in indicators:
                    print(f"  20일 이평선: {indicators['sma_20']:,.0f}원")
                if 'rsi' in indicators:
                    print(f"  RSI: {indicators['rsi']:.1f}")
                if 'macd' in indicators:
                    print(f"  MACD: {indicators['macd']:.2f}")
        
        print(f"\n🎯 실행 완료!")
        
    except Exception as e:
        logger.error(f"실행 실패: {e}")
        print(f"❌ 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()