#!/usr/bin/env python3
"""
최적화된 추세 지표 차트 시각화 시스템
저장된 기술적 지표를 활용하여 빠른 차트 생성

실행 방법:
python scripts/visualization/create_optimized_trend_chart.py --stock_code=000660 --period=6M
python scripts/visualization/create_optimized_trend_chart.py --stock_code=000660 --save_png --show_signals --report
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

try:
    from config import config_manager
    logger = config_manager.get_logger(__name__) if config_manager else logging.getLogger(__name__)
except:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

warnings.filterwarnings('ignore')


class OptimizedTrendChartGenerator:
    """최적화된 추세 지표 차트 생성 클래스"""
    
    def __init__(self):
        """초기화"""
        try:
            if config_manager:
                self.db_conn = config_manager.get_database_connection('stock')
            else:
                db_path = project_root / 'data' / 'databases' / 'stock_data.db'
                self.db_conn = sqlite3.connect(str(db_path))
                self.db_conn.row_factory = sqlite3.Row
        except Exception as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            db_path = project_root / 'data' / 'databases' / 'stock_data.db'
            self.db_conn = sqlite3.connect(str(db_path))
            self.db_conn.row_factory = sqlite3.Row
            
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
    
    def check_technical_indicators_availability(self, stock_code: str) -> Dict[str, Any]:
        """기술적 지표 데이터 가용성 확인"""
        try:
            cursor = self.db_conn.cursor()
            
            # technical_indicators 테이블에서 데이터 확인
            cursor.execute("""
                SELECT COUNT(*) as count,
                       MIN(date) as start_date,
                       MAX(date) as end_date
                FROM technical_indicators 
                WHERE stock_code = ?
            """, (stock_code,))
            
            result = cursor.fetchone()
            
            if result and result[0] > 0:
                return {
                    'available': True,
                    'count': result[0],
                    'start_date': result[1],
                    'end_date': result[2]
                }
            else:
                return {
                    'available': False,
                    'count': 0,
                    'start_date': None,
                    'end_date': None
                }
                
        except Exception as e:
            logger.error(f"기술적 지표 가용성 확인 실패: {stock_code} - {e}")
            return {'available': False, 'count': 0}
    
    def load_stock_data_with_indicators(self, stock_code: str, period: str = '6M') -> pd.DataFrame:
        """
        주가 데이터와 저장된 기술적 지표를 함께 로드
        
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
            
            # 기술적 지표 가용성 확인
            indicator_status = self.check_technical_indicators_availability(stock_code)
            
            if indicator_status['available']:
                # 저장된 기술적 지표가 있는 경우: JOIN 쿼리 사용
                logger.info(f"저장된 기술적 지표 사용: {stock_code} ({indicator_status['count']}개)")
                
                query = """
                    SELECT 
                        sp.date,
                        sp.open_price as open,
                        sp.high_price as high,
                        sp.low_price as low,
                        sp.close_price as close,
                        sp.volume,
                        ti.sma_5,
                        ti.sma_20,
                        ti.sma_60,
                        ti.sma_120,
                        ti.sma_200,
                        ti.ema_12,
                        ti.ema_26,
                        ti.rsi,
                        ti.macd,
                        ti.macd_signal,
                        ti.macd_histogram,
                        ti.bb_upper,
                        ti.bb_middle,
                        ti.bb_lower,
                        ti.stoch_k,
                        ti.stoch_d,
                        ti.obv,
                        ti.vwap,
                        ti.volume_sma_20,
                        ti.atr
                    FROM stock_prices sp
                    LEFT JOIN technical_indicators ti ON sp.stock_code = ti.stock_code AND sp.date = ti.date
                    WHERE sp.stock_code = ? 
                        AND sp.date >= ? 
                        AND sp.date <= ?
                    ORDER BY sp.date ASC
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
                    'date', 'open', 'high', 'low', 'close', 'volume',
                    'sma_5', 'sma_20', 'sma_60', 'sma_120', 'sma_200',
                    'ema_12', 'ema_26', 'rsi', 'macd', 'macd_signal', 'macd_histogram',
                    'bb_upper', 'bb_middle', 'bb_lower', 'stoch_k', 'stoch_d',
                    'obv', 'vwap', 'volume_sma_20', 'atr'
                ])
                
            else:
                # 저장된 기술적 지표가 없는 경우: 기본 주가 데이터만 로드
                logger.warning(f"기술적 지표 없음: {stock_code}. 기본 데이터만 로드")
                
                query = """
                    SELECT 
                        date,
                        open_price as open,
                        high_price as high,
                        low_price as low,
                        close_price as close,
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
                
                # DataFrame 생성 (지표 컬럼들을 NaN으로 채움)
                df = pd.DataFrame(rows, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
                
                # 기술적 지표 컬럼들을 NaN으로 추가
                indicator_columns = [
                    'sma_5', 'sma_20', 'sma_60', 'sma_120', 'sma_200',
                    'ema_12', 'ema_26', 'rsi', 'macd', 'macd_signal', 'macd_histogram',
                    'bb_upper', 'bb_middle', 'bb_lower', 'stoch_k', 'stoch_d',
                    'obv', 'vwap', 'volume_sma_20', 'atr'
                ]
                for col in indicator_columns:
                    df[col] = np.nan
            
            # 공통 처리
            # 날짜 인덱스 설정
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # 숫자 타입 변환
            numeric_columns = df.columns
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
            
            logger.info(f"데이터 로드 완료: {stock_code} ({len(df)}일, {period})")
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
            
            # 7. 볼린저 밴드 분석
            if not pd.isna(latest['close']) and not pd.isna(latest['bb_upper']) and not pd.isna(latest['bb_lower']):
                if latest['close'] > latest['bb_upper']:
                    signals.append({
                        'type': '주의',
                        'message': '볼린저 밴드 상단 돌파 (과매수 주의)',
                        'strength': -1
                    })
                elif latest['close'] < latest['bb_lower']:
                    signals.append({
                        'type': '기회',
                        'message': '볼린저 밴드 하단 터치 (매수 기회)',
                        'strength': 1
                    })
            
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
                'bb_bands': True,
                'volume': True,
                'rsi': True,
                'macd': True
            }
        
        # 데이터 로드
        df = self.load_stock_data_with_indicators(stock_code, period)
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
        
        # 기술적 지표 가용성 확인
        indicator_status = self.check_technical_indicators_availability(stock_code)
        has_indicators = indicator_status['available']
        
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
        
        row_heights = [0.5, 0.15, 0.15, 0.2]
        
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
        
        # 볼린저 밴드 (배경으로 먼저 그리기)
        if indicators.get('bb_bands', True) and has_indicators:
            if 'bb_upper' in df.columns and 'bb_lower' in df.columns:
                # 볼린저 밴드 영역 채우기
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['bb_upper'],
                        mode='lines',
                        line=dict(color='rgba(128,128,128,0.3)', width=1),
                        name='볼린저 상단',
                        showlegend=False
                    ),
                    row=1, col=1
                )
                
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['bb_lower'],
                        mode='lines',
                        line=dict(color='rgba(128,128,128,0.3)', width=1),
                        fill='tonexty',
                        fillcolor='rgba(128,128,128,0.1)',
                        name='볼린저 밴드',
                        showlegend=True
                    ),
                    row=1, col=1
                )
                
                # 중간선
                if 'bb_middle' in df.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=df.index,
                            y=df['bb_middle'],
                            mode='lines',
                            line=dict(color='gray', width=1, dash='dot'),
                            name='볼린저 중간',
                            showlegend=False
                        ),
                        row=1, col=1
                    )
        
        # 이동평균선 추가
        if has_indicators:
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
                    # NaN이 아닌 데이터가 있는지 확인
                    if not df[col_name].isna().all():
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
            
            # 거래량 이평선 (저장된 지표가 있는 경우)
            if has_indicators and 'volume_sma_20' in df.columns and not df['volume_sma_20'].isna().all():
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
        if indicators.get('rsi', True) and has_indicators and 'rsi' in df.columns and not df['rsi'].isna().all():
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
        if indicators.get('macd', True) and has_indicators:
            if 'macd' in df.columns and not df['macd'].isna().all():
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
            
            if 'macd_signal' in df.columns and not df['macd_signal'].isna().all():
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
            
            if 'macd_histogram' in df.columns and not df['macd_histogram'].isna().all():
                colors = ['red' if val < 0 else 'green' for val in df['macd_histogram'] if not pd.isna(val)]
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
        subtitle = f"현재 추세: {trend_analysis['current_trend']} | 추세 강도: {trend_analysis['trend_strength']}/10"
        if not has_indicators:
            subtitle += " | ⚠️ 기술적 지표 없음"
        
        fig.update_layout(
            title=dict(
                text=f"📈 {company_info['company_name']} ({stock_code}) 추세 분석<br>"
                     f"<sub>{subtitle}</sub>",
                font=dict(size=self.chart_config['font_size']),
            ),
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
            df = self.load_stock_data_with_indicators(stock_code, period)
            if df.empty:
                return {'error': '데이터 없음'}
            
            company_info = self.get_company_info(stock_code)
            trend_analysis = self.analyze_trend_signals(df)
            indicator_status = self.check_technical_indicators_availability(stock_code)
            
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
            if indicator_status['available']:
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
                'indicator_status': indicator_status,
                'data_period': f"{df.index[0].strftime('%Y-%m-%d')} ~ {df.index[-1].strftime('%Y-%m-%d')}",
                'data_points': len(df)
            }
            
        except Exception as e:
            logger.error(f"분석 리포트 생성 실패: {stock_code} - {e}")
            return {'error': str(e)}


    def get_stock_list(self, limit: int = None) -> List[str]:
        """저장된 주식 목록 조회"""
        try:
            query = """
                SELECT DISTINCT sp.stock_code, ci.company_name
                FROM stock_prices sp
                LEFT JOIN company_info ci ON sp.stock_code = ci.stock_code
                ORDER BY sp.stock_code
            """
            if limit:
                query += f" LIMIT {limit}"
            
            cursor = self.db_conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            
            return [row[0] for row in results]
            
        except Exception as e:
            logger.error(f"주식 목록 조회 실패: {e}")
            return []


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='최적화된 추세 지표 차트 생성')
    parser.add_argument('--stock_code', type=str, help='주식 코드 (예: 005930)')
    parser.add_argument('--all_stocks', action='store_true', help='모든 주식 처리')
    parser.add_argument('--limit', type=int, help='처리할 주식 수 제한')
    parser.add_argument('--period', type=str, default='6M', choices=['1M', '3M', '6M', '1Y', '2Y'], help='조회 기간')
    parser.add_argument('--output', type=str, default='html', choices=['html', 'png', 'pdf'], help='출력 형식')
    parser.add_argument('--save_png', action='store_true', help='PNG 파일로도 저장')
    parser.add_argument('--show_signals', action='store_true', help='매매 신호 표시')
    parser.add_argument('--report', action='store_true', help='분석 리포트 출력')
    parser.add_argument('--check_indicators', action='store_true', help='기술적 지표 가용성만 확인')
    parser.add_argument('--top_signals', action='store_true', help='매매 신호 상위 종목만 차트 생성')
    
    args = parser.parse_args()
    
    if not args.stock_code and not args.all_stocks:
        print("❌ --stock_code 또는 --all_stocks 옵션을 지정해주세요.")
        parser.print_help()
        return
    
    # 차트 생성기 초기화
    generator = OptimizedTrendChartGenerator()
    
    try:
        if args.all_stocks:
            # 전체 종목 처리
            stock_list = generator.get_stock_list(args.limit)
            
            if args.check_indicators:
                # 전체 종목 지표 상태 확인
                print(f"📊 전체 종목 기술적 지표 상태 확인")
                print("=" * 60)
                
                available_count = 0
                total_count = len(stock_list)
                
                for i, stock_code in enumerate(stock_list, 1):
                    indicator_status = generator.check_technical_indicators_availability(stock_code)
                    status_icon = "✅" if indicator_status['available'] else "❌"
                    company_info = generator.get_company_info(stock_code)
                    
                    print(f"{status_icon} {stock_code} {company_info['company_name'][:15]:15s} "
                          f"({indicator_status['count']:>4,}개) "
                          f"[{i:>3}/{total_count}]")
                    
                    if indicator_status['available']:
                        available_count += 1
                
                print(f"\n📊 전체 요약:")
                print(f"  지표 있음: {available_count}개 ({available_count/total_count*100:.1f}%)")
                print(f"  지표 없음: {total_count - available_count}개")
                print(f"  전체 종목: {total_count}개")
                
                return
            
            elif args.top_signals:
                # 매매 신호 상위 종목만 차트 생성
                print(f"🔍 매매 신호 상위 종목 찾는 중...")
                
                signal_scores = []
                
                for stock_code in stock_list[:args.limit or 100]:  # 최대 100개만 분석
                    try:
                        df = generator.load_stock_data_with_indicators(stock_code, args.period)
                        if not df.empty:
                            trend_analysis = generator.analyze_trend_signals(df)
                            signal_scores.append({
                                'stock_code': stock_code,
                                'trend_strength': trend_analysis['trend_strength'],
                                'signal_count': len(trend_analysis['signals'])
                            })
                    except Exception as e:
                        logger.warning(f"신호 분석 실패: {stock_code} - {e}")
                
                # 신호 강도 순으로 정렬
                signal_scores.sort(key=lambda x: x['trend_strength'], reverse=True)
                top_stocks = signal_scores[:10]  # 상위 10개만
                
                print(f"📈 매매 신호 상위 10개 종목:")
                for i, stock_info in enumerate(top_stocks, 1):
                    company_info = generator.get_company_info(stock_info['stock_code'])
                    print(f"  {i:2d}. {stock_info['stock_code']} {company_info['company_name'][:20]:20s} "
                          f"(강도: {stock_info['trend_strength']:+2d})")
                
                # 상위 종목들 차트 생성
                for stock_info in top_stocks:
                    stock_code = stock_info['stock_code']
                    print(f"\n📈 {stock_code} 차트 생성 중...")
                    
                    try:
                        fig = generator.create_trend_chart(stock_code, args.period)
                        
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename = f"top_signal_{stock_code}_{args.period}_{timestamp}"
                        
                        saved_path = generator.save_chart(fig, filename, args.output)
                        print(f"  ✅ 저장: {saved_path}")
                        
                    except Exception as e:
                        print(f"  ❌ 실패: {e}")
                
                print(f"\n🎯 상위 신호 종목 차트 생성 완료!")
                return
            
            else:
                # 전체 종목 차트 생성 (권장하지 않음)
                print(f"⚠️ 전체 종목 차트 생성은 시간이 많이 걸립니다.")
                print(f"📝 처리 예정: {len(stock_list)}개 종목")
                
                confirm = input("계속 진행하시겠습니까? (y/N): ")
                if confirm.lower() != 'y':
                    print("취소되었습니다.")
                    return
                
                success_count = 0
                for i, stock_code in enumerate(stock_list, 1):
                    print(f"진행상황: {i}/{len(stock_list)} - {stock_code}")
                    
                    try:
                        fig = generator.create_trend_chart(stock_code, args.period)
                        
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename = f"bulk_chart_{stock_code}_{args.period}_{timestamp}"
                        
                        saved_path = generator.save_chart(fig, filename, args.output)
                        print(f"  ✅ {stock_code} 완료")
                        success_count += 1
                        
                    except Exception as e:
                        print(f"  ❌ {stock_code} 실패: {e}")
                
                print(f"\n📊 전체 차트 생성 완료: {success_count}/{len(stock_list)} 성공")
                return
        
        # 단일 종목 처리 (기존 코드)
        stock_code = args.stock_code
        
        # 기술적 지표 가용성 확인
        if args.check_indicators:
            indicator_status = generator.check_technical_indicators_availability(stock_code)
            print(f"📊 {stock_code} 기술적 지표 상태:")
            print(f"  가용 여부: {'✅ 있음' if indicator_status['available'] else '❌ 없음'}")
            if indicator_status['available']:
                print(f"  데이터 수: {indicator_status['count']:,}개")
                print(f"  기간: {indicator_status['start_date']} ~ {indicator_status['end_date']}")
                print(f"💡 저장된 기술적 지표를 사용하여 빠른 차트 생성 가능")
            else:
                print(f"💡 기술적 지표를 먼저 계산해주세요:")
                print(f"    python calculate_technical_indicators.py --stock_code={stock_code}")
            return
        
        # 지표 설정
        indicators = {
            'sma_5': True,
            'sma_20': True,
            'sma_60': False,
            'sma_120': False,
            'sma_200': True,
            'ema_12': False,
            'ema_26': False,
            'bb_bands': True,
            'volume': True,
            'rsi': True,
            'macd': True
        }
        
        # 차트 생성
        print(f"📈 {stock_code} 최적화된 추세 차트 생성 중...")
        
        # 기술적 지표 상태 미리 확인
        indicator_status = generator.check_technical_indicators_availability(stock_code)
        if indicator_status['available']:
            print(f"✅ 저장된 기술적 지표 사용 ({indicator_status['count']:,}개)")
        else:
            print(f"⚠️ 기술적 지표 없음 - 기본 차트만 생성")
            print(f"💡 더 나은 차트를 위해 지표 계산을 권장합니다:")
            print(f"    python calculate_technical_indicators.py --stock_code={stock_code}")
        
        fig = generator.create_trend_chart(stock_code, args.period, indicators)
        
        # 차트 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"optimized_trend_chart_{stock_code}_{args.period}_{timestamp}"
        
        saved_path = generator.save_chart(fig, filename, args.output)
        print(f"✅ 차트 저장 완료: {saved_path}")
        
        # PNG 추가 저장
        if args.save_png and args.output != 'png':
            png_path = generator.save_chart(fig, filename, 'png')
            print(f"✅ PNG 저장 완료: {png_path}")
        
        # 분석 리포트
        if args.report:
            print(f"\n📊 {stock_code} 종합 분석 리포트")
            print("=" * 50)
            
            report = generator.generate_analysis_report(stock_code, args.period)
            
            if 'error' in report:
                print(f"❌ 오류: {report['error']}")
            else:
                # 기본 정보
                company = report['company_info']
                stats = report['stats']
                trend = report['trend_analysis']
                indicator_status = report['indicator_status']
                
                print(f"회사명: {company['company_name']}")
                print(f"업종: {company['sector']} - {company['industry']}")
                print(f"시장: {company['market_type']}")
                print(f"분석 기간: {report['data_period']} ({report['data_points']}일)")
                
                print(f"\n📊 기술적 지표 상태:")
                if indicator_status['available']:
                    print(f"  ✅ 저장된 지표 사용 ({indicator_status['count']:,}개)")
                    print(f"  📅 지표 기간: {indicator_status['start_date']} ~ {indicator_status['end_date']}")
                else:
                    print(f"  ❌ 저장된 지표 없음")
                
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
                    
                if report['technical_indicators']:
                    print(f"\n📊 주요 기술적 지표:")
                    indicators = report['technical_indicators']
                    if 'sma_20' in indicators:
                        print(f"  20일 이평선: {indicators['sma_20']:,.0f}원")
                    if 'sma_200' in indicators:
                        print(f"  200일 이평선: {indicators['sma_200']:,.0f}원")
                    if 'rsi' in indicators:
                        print(f"  RSI: {indicators['rsi']:.1f}")
                    if 'macd' in indicators:
                        print(f"  MACD: {indicators['macd']:.2f}")
                    if 'macd_signal' in indicators:
                        print(f"  MACD Signal: {indicators['macd_signal']:.2f}")
                else:
                    print(f"\n⚠️ 기술적 지표 데이터가 없습니다.")
                    print(f"💡 다음 명령어로 지표를 계산하세요:")
                    print(f"    python calculate_technical_indicators.py --stock_code={stock_code}")
        
        print(f"\n🎯 실행 완료!")
        
    except Exception as e:
        logger.error(f"실행 실패: {e}")
        print(f"❌ 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()