"""
차트 유틸리티
주식 분석을 위한 다양한 차트 생성 유틸리티 함수들
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class ChartError(Exception):
    """차트 생성 오류"""
    pass

class ChartTheme:
    """차트 테마 설정"""
    
    # 색상 팔레트
    COLORS = {
        'primary': '#1f77b4',
        'secondary': '#ff7f0e',
        'success': '#2ca02c',
        'danger': '#d62728',
        'warning': '#ff9800',
        'info': '#17a2b8',
        'light': '#f8f9fa',
        'dark': '#343a40',
        'bull': '#26a69a',  # 상승 (녹색)
        'bear': '#ef5350',  # 하락 (빨간색)
        'volume': '#90caf9',
        'ma': '#ffa726',
        'signal': '#ab47bc'
    }
    
    # 기본 레이아웃
    DEFAULT_LAYOUT = {
        'template': 'plotly_white',
        'font': {'family': 'Arial, sans-serif', 'size': 12},
        'title': {'x': 0.5, 'font': {'size': 16, 'color': '#1f2937'}},
        'showlegend': True,
        'legend': {'x': 0, 'y': 1, 'bgcolor': 'rgba(255,255,255,0.8)'},
        'margin': {'l': 40, 'r': 40, 't': 60, 'b': 40},
        'plot_bgcolor': 'white',
        'paper_bgcolor': 'white'
    }
    
    # 축 설정
    AXIS_CONFIG = {
        'showgrid': True,
        'gridcolor': '#e5e7eb',
        'linecolor': '#d1d5db',
        'tickfont': {'size': 10},
        'titlefont': {'size': 12, 'color': '#374151'}
    }

class StockChartGenerator:
    """주식 차트 생성 클래스"""
    
    def __init__(self, theme: ChartTheme = None):
        self.theme = theme or ChartTheme()
    
    def create_candlestick_chart(self, df: pd.DataFrame, 
                               title: str = "주가 차트",
                               volume: bool = True,
                               ma_periods: List[int] = [20, 60]) -> go.Figure:
        """캔들스틱 차트 생성"""
        try:
            # 데이터 검증
            required_columns = ['Open', 'High', 'Low', 'Close']
            if not all(col in df.columns for col in required_columns):
                raise ChartError(f"필수 컬럼 누락: {required_columns}")
            
            # 서브플롯 생성
            if volume and 'Volume' in df.columns:
                fig = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.1,
                    subplot_titles=('Price', 'Volume'),
                    row_heights=[0.7, 0.3]
                )
            else:
                fig = make_subplots(rows=1, cols=1)
            
            # 캔들스틱 차트 추가
            fig.add_trace(
                go.Candlestick(
                    x=df.index,
                    open=df['Open'],
                    high=df['High'],
                    low=df['Low'],
                    close=df['Close'],
                    name='Price',
                    increasing_line_color=self.theme.COLORS['bull'],
                    decreasing_line_color=self.theme.COLORS['bear']
                ),
                row=1, col=1
            )
            
            # 이동평균선 추가
            for period in ma_periods:
                if len(df) >= period:
                    ma_col = f'MA_{period}'
                    if ma_col not in df.columns:
                        df[ma_col] = df['Close'].rolling(window=period).mean()
                    
                    fig.add_trace(
                        go.Scatter(
                            x=df.index,
                            y=df[ma_col],
                            name=f'MA{period}',
                            line=dict(width=2),
                            opacity=0.8
                        ),
                        row=1, col=1
                    )
            
            # 거래량 차트 추가
            if volume and 'Volume' in df.columns:
                colors = [self.theme.COLORS['bull'] if close >= open else self.theme.COLORS['bear'] 
                         for close, open in zip(df['Close'], df['Open'])]
                
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df['Volume'],
                        name='Volume',
                        marker_color=colors,
                        opacity=0.7
                    ),
                    row=2, col=1
                )
            
            # 레이아웃 설정
            fig.update_layout(
                title=title,
                xaxis_title="Date",
                yaxis_title="Price",
                **self.theme.DEFAULT_LAYOUT
            )
            
            # X축 설정
            fig.update_xaxes(
                rangeslider_visible=False,
                **self.theme.AXIS_CONFIG
            )
            
            # Y축 설정
            fig.update_yaxes(**self.theme.AXIS_CONFIG)
            
            return fig
            
        except Exception as e:
            logger.error(f"캔들스틱 차트 생성 실패: {e}")
            raise ChartError(f"캔들스틱 차트를 생성할 수 없습니다: {e}")
    
    def create_line_chart(self, df: pd.DataFrame, 
                         columns: List[str],
                         title: str = "주가 추이",
                         colors: List[str] = None) -> go.Figure:
        """라인 차트 생성"""
        try:
            fig = go.Figure()
            
            if colors is None:
                colors = [self.theme.COLORS['primary'], self.theme.COLORS['secondary'], 
                         self.theme.COLORS['success'], self.theme.COLORS['warning']]
            
            for i, column in enumerate(columns):
                if column in df.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=df.index,
                            y=df[column],
                            name=column,
                            line=dict(color=colors[i % len(colors)], width=2),
                            mode='lines'
                        )
                    )
            
            fig.update_layout(
                title=title,
                xaxis_title="Date",
                yaxis_title="Price",
                **self.theme.DEFAULT_LAYOUT
            )
            
            fig.update_xaxes(**self.theme.AXIS_CONFIG)
            fig.update_yaxes(**self.theme.AXIS_CONFIG)
            
            return fig
            
        except Exception as e:
            logger.error(f"라인 차트 생성 실패: {e}")
            raise ChartError(f"라인 차트를 생성할 수 없습니다: {e}")
    
    def create_technical_indicator_chart(self, df: pd.DataFrame, 
                                       indicator_name: str,
                                       indicator_data: Dict[str, Any],
                                       title: str = None) -> go.Figure:
        """기술적 지표 차트 생성"""
        try:
            if title is None:
                title = f"{indicator_name} 지표"
            
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.1,
                subplot_titles=('Price', indicator_name),
                row_heights=[0.6, 0.4]
            )
            
            # 주가 차트
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['Close'],
                    name='Close Price',
                    line=dict(color=self.theme.COLORS['primary'], width=2)
                ),
                row=1, col=1
            )
            
            # 지표별 차트 추가
            if indicator_name.upper() == 'RSI':
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=indicator_data['rsi'],
                        name='RSI',
                        line=dict(color=self.theme.COLORS['secondary'], width=2)
                    ),
                    row=2, col=1
                )
                
                # RSI 기준선 추가
                fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
                fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
                fig.add_hline(y=50, line_dash="dot", line_color="gray", row=2, col=1)
            
            elif indicator_name.upper() == 'MACD':
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=indicator_data['macd'],
                        name='MACD',
                        line=dict(color=self.theme.COLORS['primary'], width=2)
                    ),
                    row=2, col=1
                )
                
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=indicator_data['signal'],
                        name='Signal',
                        line=dict(color=self.theme.COLORS['danger'], width=2)
                    ),
                    row=2, col=1
                )
                
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=indicator_data['histogram'],
                        name='Histogram',
                        marker_color=self.theme.COLORS['info'],
                        opacity=0.7
                    ),
                    row=2, col=1
                )
            
            elif indicator_name.upper() == 'BOLLINGER':
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=indicator_data['upper'],
                        name='Upper Band',
                        line=dict(color=self.theme.COLORS['danger'], width=1),
                        opacity=0.7
                    ),
                    row=1, col=1
                )
                
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=indicator_data['lower'],
                        name='Lower Band',
                        line=dict(color=self.theme.COLORS['success'], width=1),
                        fill='tonexty',
                        fillcolor='rgba(0,100,80,0.1)',
                        opacity=0.7
                    ),
                    row=1, col=1
                )
                
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=indicator_data['middle'],
                        name='Middle Band',
                        line=dict(color=self.theme.COLORS['warning'], width=1),
                        opacity=0.7
                    ),
                    row=1, col=1
                )
            
            fig.update_layout(
                title=title,
                **self.theme.DEFAULT_LAYOUT
            )
            
            fig.update_xaxes(**self.theme.AXIS_CONFIG)
            fig.update_yaxes(**self.theme.AXIS_CONFIG)
            
            return fig
            
        except Exception as e:
            logger.error(f"기술적 지표 차트 생성 실패: {e}")
            raise ChartError(f"기술적 지표 차트를 생성할 수 없습니다: {e}")

class FinancialChartGenerator:
    """재무 차트 생성 클래스"""
    
    def __init__(self, theme: ChartTheme = None):
        self.theme = theme or ChartTheme()
    
    def create_financial_ratio_chart(self, df: pd.DataFrame, 
                                   ratios: List[str],
                                   title: str = "재무비율 추이") -> go.Figure:
        """재무비율 차트 생성"""
        try:
            fig = make_subplots(
                rows=len(ratios), cols=1,
                shared_xaxes=True,
                vertical_spacing=0.1,
                subplot_titles=ratios
            )
            
            colors = [self.theme.COLORS['primary'], self.theme.COLORS['secondary'], 
                     self.theme.COLORS['success'], self.theme.COLORS['warning']]
            
            for i, ratio in enumerate(ratios):
                if ratio in df.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=df.index,
                            y=df[ratio],
                            name=ratio,
                            line=dict(color=colors[i % len(colors)], width=2),
                            mode='lines+markers'
                        ),
                        row=i+1, col=1
                    )
            
            fig.update_layout(
                title=title,
                **self.theme.DEFAULT_LAYOUT
            )
            
            fig.update_xaxes(**self.theme.AXIS_CONFIG)
            fig.update_yaxes(**self.theme.AXIS_CONFIG)
            
            return fig
            
        except Exception as e:
            logger.error(f"재무비율 차트 생성 실패: {e}")
            raise ChartError(f"재무비율 차트를 생성할 수 없습니다: {e}")
    
    def create_income_statement_chart(self, df: pd.DataFrame,
                                    title: str = "손익계산서 추이") -> go.Figure:
        """손익계산서 차트 생성"""
        try:
            fig = go.Figure()
            
            # 매출
            if 'revenue' in df.columns:
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df['revenue'],
                        name='매출',
                        marker_color=self.theme.COLORS['primary'],
                        opacity=0.8
                    )
                )
            
            # 영업이익
            if 'operating_income' in df.columns:
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df['operating_income'],
                        name='영업이익',
                        marker_color=self.theme.COLORS['success'],
                        opacity=0.8
                    )
                )
            
            # 순이익
            if 'net_income' in df.columns:
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df['net_income'],
                        name='순이익',
                        marker_color=self.theme.COLORS['warning'],
                        opacity=0.8
                    )
                )
            
            fig.update_layout(
                title=title,
                xaxis_title="연도",
                yaxis_title="금액 (원)",
                barmode='group',
                **self.theme.DEFAULT_LAYOUT
            )
            
            fig.update_xaxes(**self.theme.AXIS_CONFIG)
            fig.update_yaxes(**self.theme.AXIS_CONFIG)
            
            return fig
            
        except Exception as e:
            logger.error(f"손익계산서 차트 생성 실패: {e}")
            raise ChartError(f"손익계산서 차트를 생성할 수 없습니다: {e}")
    
    def create_balance_sheet_chart(self, df: pd.DataFrame,
                                 title: str = "대차대조표 추이") -> go.Figure:
        """대차대조표 차트 생성"""
        try:
            fig = go.Figure()
            
            # 자산
            if 'total_assets' in df.columns:
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df['total_assets'],
                        name='총자산',
                        marker_color=self.theme.COLORS['primary'],
                        opacity=0.8
                    )
                )
            
            # 부채
            if 'total_liabilities' in df.columns:
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df['total_liabilities'],
                        name='총부채',
                        marker_color=self.theme.COLORS['danger'],
                        opacity=0.8
                    )
                )
            
            # 자본
            if 'total_equity' in df.columns:
                fig.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df['total_equity'],
                        name='총자본',
                        marker_color=self.theme.COLORS['success'],
                        opacity=0.8
                    )
                )
            
            fig.update_layout(
                title=title,
                xaxis_title="연도",
                yaxis_title="금액 (원)",
                barmode='group',
                **self.theme.DEFAULT_LAYOUT
            )
            
            fig.update_xaxes(**self.theme.AXIS_CONFIG)
            fig.update_yaxes(**self.theme.AXIS_CONFIG)
            
            return fig
            
        except Exception as e:
            logger.error(f"대차대조표 차트 생성 실패: {e}")
            raise ChartError(f"대차대조표 차트를 생성할 수 없습니다: {e}")
    
    def create_pie_chart(self, data: Dict[str, float], 
                        title: str = "구성 비율") -> go.Figure:
        """파이 차트 생성"""
        try:
            fig = go.Figure(data=[
                go.Pie(
                    labels=list(data.keys()),
                    values=list(data.values()),
                    hole=0.3,
                    textinfo='label+percent',
                    textposition='outside',
                    marker=dict(
                        colors=list(self.theme.COLORS.values())[:len(data)]
                    )
                )
            ])
            
            fig.update_layout(
                title=title,
                **self.theme.DEFAULT_LAYOUT
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"파이 차트 생성 실패: {e}")
            raise ChartError(f"파이 차트를 생성할 수 없습니다: {e}")

class AnalysisChartGenerator:
    """분석 차트 생성 클래스"""
    
    def __init__(self, theme: ChartTheme = None):
        self.theme = theme or ChartTheme()
    
    def create_correlation_heatmap(self, df: pd.DataFrame, 
                                 title: str = "상관관계 히트맵") -> go.Figure:
        """상관관계 히트맵 생성"""
        try:
            corr_matrix = df.corr()
            
            fig = go.Figure(data=go.Heatmap(
                z=corr_matrix.values,
                x=corr_matrix.columns,
                y=corr_matrix.columns,
                colorscale='RdBu',
                zmid=0,
                text=corr_matrix.values.round(2),
                texttemplate="%{text}",
                textfont={"size": 10},
                hoverongaps=False
            ))
            
            fig.update_layout(
                title=title,
                **self.theme.DEFAULT_LAYOUT
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"상관관계 히트맵 생성 실패: {e}")
            raise ChartError(f"상관관계 히트맵을 생성할 수 없습니다: {e}")
    
    def create_scatter_plot(self, df: pd.DataFrame, 
                          x_column: str, y_column: str,
                          title: str = None) -> go.Figure:
        """산점도 생성"""
        try:
            if title is None:
                title = f"{x_column} vs {y_column}"
            
            fig = go.Figure()
            
            fig.add_trace(
                go.Scatter(
                    x=df[x_column],
                    y=df[y_column],
                    mode='markers',
                    marker=dict(
                        size=8,
                        color=self.theme.COLORS['primary'],
                        opacity=0.7
                    ),
                    text=df.index,
                    textposition="top center"
                )
            )
            
            # 추세선 추가
            z = np.polyfit(df[x_column], df[y_column], 1)
            p = np.poly1d(z)
            
            fig.add_trace(
                go.Scatter(
                    x=df[x_column],
                    y=p(df[x_column]),
                    mode='lines',
                    name='추세선',
                    line=dict(color=self.theme.COLORS['danger'], width=2, dash='dash')
                )
            )
            
            fig.update_layout(
                title=title,
                xaxis_title=x_column,
                yaxis_title=y_column,
                **self.theme.DEFAULT_LAYOUT
            )
            
            fig.update_xaxes(**self.theme.AXIS_CONFIG)
            fig.update_yaxes(**self.theme.AXIS_CONFIG)
            
            return fig
            
        except Exception as e:
            logger.error(f"산점도 생성 실패: {e}")
            raise ChartError(f"산점도를 생성할 수 없습니다: {e}")
    
    def create_performance_comparison(self, data: Dict[str, List[float]],
                                   title: str = "성과 비교") -> go.Figure:
        """성과 비교 차트 생성"""
        try:
            fig = go.Figure()
            
            colors = list(self.theme.COLORS.values())
            
            for i, (name, values) in enumerate(data.items()):
                fig.add_trace(
                    go.Box(
                        y=values,
                        name=name,
                        marker_color=colors[i % len(colors)],
                        boxpoints='all',
                        jitter=0.3,
                        pointpos=-1.8
                    )
                )
            
            fig.update_layout(
                title=title,
                yaxis_title="수익률 (%)",
                **self.theme.DEFAULT_LAYOUT
            )
            
            fig.update_xaxes(**self.theme.AXIS_CONFIG)
            fig.update_yaxes(**self.theme.AXIS_CONFIG)
            
            return fig
            
        except Exception as e:
            logger.error(f"성과 비교 차트 생성 실패: {e}")
            raise ChartError(f"성과 비교 차트를 생성할 수 없습니다: {e}")

# 전역 차트 생성기 인스턴스
stock_chart_generator = StockChartGenerator()
financial_chart_generator = FinancialChartGenerator()
analysis_chart_generator = AnalysisChartGenerator()

# 편의 함수들
def create_candlestick_chart(df: pd.DataFrame, title: str = "주가 차트", **kwargs) -> go.Figure:
    """캔들스틱 차트 생성"""
    return stock_chart_generator.create_candlestick_chart(df, title, **kwargs)

def create_line_chart(df: pd.DataFrame, columns: List[str], title: str = "주가 추이", **kwargs) -> go.Figure:
    """라인 차트 생성"""
    return stock_chart_generator.create_line_chart(df, columns, title, **kwargs)

def create_technical_chart(df: pd.DataFrame, indicator_name: str, indicator_data: Dict, **kwargs) -> go.Figure:
    """기술적 지표 차트 생성"""
    return stock_chart_generator.create_technical_indicator_chart(df, indicator_name, indicator_data, **kwargs)

def create_financial_ratio_chart(df: pd.DataFrame, ratios: List[str], **kwargs) -> go.Figure:
    """재무비율 차트 생성"""
    return financial_chart_generator.create_financial_ratio_chart(df, ratios, **kwargs)

def create_pie_chart(data: Dict[str, float], title: str = "구성 비율", **kwargs) -> go.Figure:
    """파이 차트 생성"""
    return financial_chart_generator.create_pie_chart(data, title, **kwargs)

def create_correlation_heatmap(df: pd.DataFrame, title: str = "상관관계 히트맵", **kwargs) -> go.Figure:
    """상관관계 히트맵 생성"""
    return analysis_chart_generator.create_correlation_heatmap(df, title, **kwargs)

def create_scatter_plot(df: pd.DataFrame, x_column: str, y_column: str, **kwargs) -> go.Figure:
    """산점도 생성"""
    return analysis_chart_generator.create_scatter_plot(df, x_column, y_column, **kwargs)

def save_chart(fig: go.Figure, filename: str, format: str = 'html', **kwargs):
    """차트 저장"""
    try:
        if format.lower() == 'html':
            fig.write_html(filename, **kwargs)
        elif format.lower() == 'png':
            fig.write_image(filename, **kwargs)
        elif format.lower() == 'svg':
            fig.write_image(filename, **kwargs)
        elif format.lower() == 'pdf':
            fig.write_image(filename, **kwargs)
        else:
            raise ValueError(f"지원하지 않는 형식: {format}")
        
        logger.info(f"차트 저장 완료: {filename}")
    except Exception as e:
        logger.error(f"차트 저장 실패: {e}")
        raise ChartError(f"차트를 저장할 수 없습니다: {e}")

# 사용 예시
if __name__ == "__main__":
    print("📊 차트 유틸리티 테스트")
    print("=" * 50)
    
    # 테스트 데이터 생성
    dates = pd.date_range(start='2024-01-01', periods=100, freq='D')
    
    # 주가 데이터 생성
    np.random.seed(42)
    base_price = 50000
    price_changes = np.random.randn(100) * 1000
    close_prices = base_price + np.cumsum(price_changes)
    
    stock_data = pd.DataFrame({
        'Date': dates,
        'Open': close_prices * (1 + np.random.randn(100) * 0.005),
        'High': close_prices * (1 + np.random.rand(100) * 0.02),
        'Low': close_prices * (1 - np.random.rand(100) * 0.02),
        'Close': close_prices,
        'Volume': np.random.randint(1000000, 5000000, 100)
    })
    stock_data.set_index('Date', inplace=True)
    
    # 캔들스틱 차트 테스트
    print("📈 캔들스틱 차트 테스트:")
    try:
        candlestick_fig = create_candlestick_chart(
            stock_data, 
            title="테스트 주식 캔들스틱 차트",
            volume=True,
            ma_periods=[20, 50]
        )
        print("캔들스틱 차트 생성 성공")
        
        # HTML 파일로 저장 (테스트용)
        # save_chart(candlestick_fig, 'test_candlestick.html')
        
    except Exception as e:
        print(f"캔들스틱 차트 생성 실패: {e}")
    
    # 라인 차트 테스트
    print("\n📊 라인 차트 테스트:")
    try:
        line_fig = create_line_chart(
            stock_data, 
            columns=['Close', 'Open'],
            title="주가 추이"
        )
        print("라인 차트 생성 성공")
    except Exception as e:
        print(f"라인 차트 생성 실패: {e}")
    
    # 기술적 지표 차트 테스트
    print("\n📈 기술적 지표 차트 테스트:")
    try:
        # RSI 계산 (간단한 버전)
        delta = stock_data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        rsi_data = {'rsi': rsi}
        
        technical_fig = create_technical_chart(
            stock_data,
            'RSI',
            rsi_data,
            title="RSI 지표"
        )
        print("기술적 지표 차트 생성 성공")
    except Exception as e:
        print(f"기술적 지표 차트 생성 실패: {e}")
    
    # 재무 데이터 생성 및 차트 테스트
    print("\n💰 재무 차트 테스트:")
    try:
        years = ['2020', '2021', '2022', '2023']
        financial_data = pd.DataFrame({
            'revenue': [1000000, 1200000, 1100000, 1300000],
            'operating_income': [100000, 150000, 120000, 180000],
            'net_income': [80000, 120000, 100000, 150000],
            'total_assets': [2000000, 2200000, 2100000, 2400000],
            'total_liabilities': [800000, 900000, 850000, 950000],
            'total_equity': [1200000, 1300000, 1250000, 1450000]
        }, index=years)
        
        income_fig = financial_chart_generator.create_income_statement_chart(
            financial_data,
            title="손익계산서 추이"
        )
        print("재무 차트 생성 성공")
    except Exception as e:
        print(f"재무 차트 생성 실패: {e}")
    
    # 파이 차트 테스트
    print("\n🥧 파이 차트 테스트:")
    try:
        portfolio_data = {
            '삼성전자': 30,
            'SK하이닉스': 25,
            'NAVER': 20,
            '카카오': 15,
            '현대차': 10
        }
        
        pie_fig = create_pie_chart(portfolio_data, "포트폴리오 구성")
        print("파이 차트 생성 성공")
    except Exception as e:
        print(f"파이 차트 생성 실패: {e}")
    
    # 상관관계 히트맵 테스트
    print("\n🔥 상관관계 히트맵 테스트:")
    try:
        # 상관관계 테스트 데이터 생성
        corr_data = pd.DataFrame({
            'Stock_A': np.random.randn(50),
            'Stock_B': np.random.randn(50),
            'Stock_C': np.random.randn(50),
            'Stock_D': np.random.randn(50)
        })
        
        # 일부 상관관계 추가
        corr_data['Stock_B'] = corr_data['Stock_A'] * 0.7 + corr_data['Stock_B'] * 0.3
        
        heatmap_fig = create_correlation_heatmap(corr_data, "주식 상관관계")
        print("상관관계 히트맵 생성 성공")
    except Exception as e:
        print(f"상관관계 히트맵 생성 실패: {e}")
    
    # 산점도 테스트
    print("\n🎯 산점도 테스트:")
    try:
        scatter_data = pd.DataFrame({
            'PER': np.random.uniform(5, 30, 20),
            'ROE': np.random.uniform(5, 25, 20)
        })
        
        scatter_fig = create_scatter_plot(scatter_data, 'PER', 'ROE', title="PER vs ROE")
        print("산점도 생성 성공")
    except Exception as e:
        print(f"산점도 생성 실패: {e}")
    
    print("\n✅ 모든 차트 유틸리티 테스트 완료!")
    print("실제 차트는 Streamlit 애플리케이션에서 확인할 수 있습니다.")