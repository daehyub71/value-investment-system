#!/usr/bin/env python3
"""
📈 기술분석 실행 스크립트 - 멀티 소스 데이터 지원 버전
Value Investment System - Technical Analysis Runner

데이터 소스 우선순위:
1. FinanceDataReader (KRX)
2. FinanceDataReader (Yahoo)  
3. yfinance (직접)
4. 샘플 데이터

실행 방법:
python run_technical_analysis_multi.py --stock_code 005930
python run_technical_analysis_multi.py --stock_code 035420
"""

import sys
import os
import argparse
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time
import json
from pathlib import Path

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 기술분석 모듈 import
try:
    from src.analysis.technical.technical_analysis import TechnicalAnalyzer, print_analysis_summary
    print("✅ 기술분석 모듈 import 성공!")
except ImportError as e:
    print(f"❌ 모듈 import 실패: {e}")
    print(f"프로젝트 루트: {project_root}")
    print("먼저 technical_analysis.py 파일이 올바른 위치에 있는지 확인하세요.")
    sys.exit(1)

# 다중 데이터 소스 import
DATA_SOURCES = {}

# FinanceDataReader
try:
    import FinanceDataReader as fdr
    DATA_SOURCES['fdr'] = True
    print("✅ FinanceDataReader 사용 가능")
except ImportError:
    DATA_SOURCES['fdr'] = False
    print("⚠️ FinanceDataReader 사용 불가")

# yfinance (야후 파이낸스)
try:
    import yfinance as yf
    DATA_SOURCES['yfinance'] = True
    print("✅ yfinance 사용 가능")
except ImportError:
    DATA_SOURCES['yfinance'] = False
    print("⚠️ yfinance 사용 불가")

# pandas_datareader
try:
    import pandas_datareader as pdr
    DATA_SOURCES['pdr'] = True
    print("✅ pandas_datareader 사용 가능")
except ImportError:
    DATA_SOURCES['pdr'] = False
    print("⚠️ pandas_datareader 사용 불가")

print(f"📊 사용 가능한 데이터 소스: {sum(DATA_SOURCES.values())}개")

# 한국 주식 정보 데이터베이스
KOREAN_STOCKS = {
    # KOSPI 대표 종목들
    '005930': {'name': '삼성전자', 'market': 'KOSPI', 'sector': '반도체', 'price_range': (60000, 80000)},
    '000660': {'name': 'SK하이닉스', 'market': 'KOSPI', 'sector': '반도체', 'price_range': (100000, 140000)},
    '373220': {'name': 'LG에너지솔루션', 'market': 'KOSPI', 'sector': '2차전지', 'price_range': (400000, 600000)},
    '207940': {'name': '삼성바이오로직스', 'market': 'KOSPI', 'sector': '바이오', 'price_range': (700000, 900000)},
    '005380': {'name': '현대차', 'market': 'KOSPI', 'sector': '자동차', 'price_range': (150000, 200000)},
    '006400': {'name': '삼성SDI', 'market': 'KOSPI', 'sector': '2차전지', 'price_range': (250000, 350000)},
    '051910': {'name': 'LG화학', 'market': 'KOSPI', 'sector': '화학', 'price_range': (300000, 450000)},
    '035420': {'name': 'NAVER', 'market': 'KOSPI', 'sector': 'IT서비스', 'price_range': (150000, 250000)},
    '028260': {'name': '삼성물산', 'market': 'KOSPI', 'sector': '종합상사', 'price_range': (100000, 150000)},
    '068270': {'name': '셀트리온', 'market': 'KOSPI', 'sector': '바이오', 'price_range': (150000, 200000)},
    '035720': {'name': '카카오', 'market': 'KOSPI', 'sector': 'IT서비스', 'price_range': (40000, 80000)},
    '105560': {'name': 'KB금융', 'market': 'KOSPI', 'sector': '은행', 'price_range': (40000, 60000)},
    '055550': {'name': '신한지주', 'market': 'KOSPI', 'sector': '은행', 'price_range': (30000, 50000)},
    '012330': {'name': '현대모비스', 'market': 'KOSPI', 'sector': '자동차부품', 'price_range': (200000, 300000)},
    '003670': {'name': '포스코홀딩스', 'market': 'KOSPI', 'sector': '철강', 'price_range': (300000, 450000)},
    '066570': {'name': 'LG전자', 'market': 'KOSPI', 'sector': '전자', 'price_range': (80000, 120000)},
    
    # KOSDAQ 대표 종목들
    '247540': {'name': '에코프로비엠', 'market': 'KOSDAQ', 'sector': '2차전지소재', 'price_range': (100000, 200000)},
    '086520': {'name': '에코프로', 'market': 'KOSDAQ', 'sector': '2차전지소재', 'price_range': (50000, 100000)},
    '091990': {'name': '셀트리온헬스케어', 'market': 'KOSDAQ', 'sector': '바이오', 'price_range': (60000, 100000)},
    '196170': {'name': '알테오젠', 'market': 'KOSDAQ', 'sector': '바이오', 'price_range': (40000, 80000)},
    '039030': {'name': '이오테크닉스', 'market': 'KOSDAQ', 'sector': '반도체장비', 'price_range': (100000, 200000)},
}

class MultiSourceDataCollector:
    """다중 소스 데이터 수집기"""
    
    def __init__(self):
        self.available_sources = [name for name, available in DATA_SOURCES.items() if available]
        print(f"🔗 활성화된 데이터 소스: {self.available_sources}")
    
    def get_stock_data_fdr_krx(self, stock_code: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """FinanceDataReader - KRX 소스"""
        if not DATA_SOURCES.get('fdr'):
            return None
        
        try:
            print(f"🌐 FDR-KRX 시도: {stock_code}")
            df = fdr.DataReader(stock_code, start_date, end_date, data_source='krx')
            if not df.empty:
                print(f"✅ FDR-KRX 성공: {len(df)}일 데이터")
                return df
        except Exception as e:
            print(f"❌ FDR-KRX 실패: {e}")
        
        return None
    
    def get_stock_data_fdr_yahoo(self, stock_code: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """FinanceDataReader - Yahoo 소스"""
        if not DATA_SOURCES.get('fdr'):
            return None
        
        # 한국 주식 접미사 처리
        symbols_to_try = []
        
        if stock_code in KOREAN_STOCKS:
            market = KOREAN_STOCKS[stock_code]['market']
            if market == 'KOSPI':
                symbols_to_try.append(f"{stock_code}.KS")
            elif market == 'KOSDAQ':
                symbols_to_try.append(f"{stock_code}.KQ")
        
        # 기본 형태들도 시도
        symbols_to_try.extend([f"{stock_code}.KS", f"{stock_code}.KQ", stock_code])
        
        for symbol in symbols_to_try:
            try:
                print(f"🌐 FDR-Yahoo 시도: {symbol}")
                df = fdr.DataReader(symbol, start_date, end_date, data_source='yahoo')
                if not df.empty:
                    print(f"✅ FDR-Yahoo 성공: {symbol}, {len(df)}일 데이터")
                    return df
            except Exception as e:
                print(f"❌ FDR-Yahoo 실패 ({symbol}): {e}")
        
        return None
    
    def get_stock_data_yfinance(self, stock_code: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """yfinance 직접 사용"""
        if not DATA_SOURCES.get('yfinance'):
            return None
        
        # 한국 주식 접미사 처리
        symbols_to_try = []
        
        if stock_code in KOREAN_STOCKS:
            market = KOREAN_STOCKS[stock_code]['market']
            if market == 'KOSPI':
                symbols_to_try.append(f"{stock_code}.KS")
            elif market == 'KOSDAQ':
                symbols_to_try.append(f"{stock_code}.KQ")
        
        symbols_to_try.extend([f"{stock_code}.KS", f"{stock_code}.KQ"])
        
        for symbol in symbols_to_try:
            try:
                print(f"🌐 yfinance 시도: {symbol}")
                ticker = yf.Ticker(symbol)
                df = ticker.history(start=start_date, end=end_date)
                
                if not df.empty:
                    # 컬럼명 표준화
                    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
                    df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
                    print(f"✅ yfinance 성공: {symbol}, {len(df)}일 데이터")
                    return df
            except Exception as e:
                print(f"❌ yfinance 실패 ({symbol}): {e}")
        
        return None
    
    def get_stock_data_any_source(self, stock_code: str, period_days: int = 300) -> Optional[pd.DataFrame]:
        """모든 소스를 순차적으로 시도"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        # 데이터 소스 시도 순서
        methods = [
            ('FDR-KRX', self.get_stock_data_fdr_krx),
            ('FDR-Yahoo', self.get_stock_data_fdr_yahoo),
            ('yfinance', self.get_stock_data_yfinance),
        ]
        
        for source_name, method in methods:
            try:
                df = method(stock_code, start_date, end_date)
                if df is not None and len(df) >= 50:  # 최소 50일 데이터 필요
                    print(f"✅ 데이터 수집 성공: {source_name}")
                    return df
            except Exception as e:
                print(f"❌ {source_name} 오류: {e}")
                continue
        
        print(f"❌ 모든 데이터 소스 실패: {stock_code}")
        return None
    
    def generate_realistic_sample_data(self, stock_code: str, period_days: int) -> pd.DataFrame:
        """현실적인 샘플 데이터 생성"""
        dates = pd.date_range(end=datetime.now(), periods=period_days, freq='D')
        
        # 종목 정보 가져오기
        stock_info = KOREAN_STOCKS.get(stock_code, {
            'name': f'종목{stock_code}',
            'market': 'KOSPI',
            'sector': '기타',
            'price_range': (50000, 100000)
        })
        
        print(f"📊 {stock_info['name']} 샘플 데이터 생성")
        print(f"   업종: {stock_info['sector']}")
        print(f"   시장: {stock_info['market']}")
        
        # 가격 범위 설정
        min_price, max_price = stock_info['price_range']
        base_price = (min_price + max_price) / 2
        
        # 시드 설정 (종목별 일관성)
        np.random.seed(hash(stock_code) % 2**32)
        
        # 현실적인 주가 패턴 생성
        # 1. 장기 추세 (업종별 특성 반영)
        sector_trend = {
            '반도체': 0.08,
            'IT서비스': 0.06,
            '2차전지': 0.12,
            '바이오': 0.15,
            '자동차': 0.04,
            '은행': 0.02,
            '화학': 0.03,
            '기타': 0.05
        }
        
        trend_rate = sector_trend.get(stock_info['sector'], 0.05)
        trend = np.linspace(0, trend_rate, period_days)
        
        # 2. 계절성 패턴
        seasonal = 0.03 * np.sin(np.linspace(0, 4*np.pi, period_days))
        
        # 3. 변동성 (업종별)
        sector_volatility = {
            '반도체': 0.025,
            'IT서비스': 0.022,
            '2차전지': 0.035,
            '바이오': 0.040,
            '자동차': 0.018,
            '은행': 0.015,
            '화학': 0.020,
            '기타': 0.020
        }
        
        volatility = sector_volatility.get(stock_info['sector'], 0.020)
        noise = np.random.normal(0, volatility, period_days)
        
        # 4. 이벤트 효과 (드문 큰 변동)
        event_days = np.random.choice(period_days, size=period_days//50, replace=False)
        events = np.zeros(period_days)
        events[event_days] = np.random.normal(0, volatility*3, len(event_days))
        
        # 수익률 합성
        returns = trend + seasonal + noise + events
        
        # 주가 계산
        prices = [base_price]
        for ret in returns[1:]:
            new_price = prices[-1] * (1 + ret)
            # 가격 범위 제한
            new_price = max(new_price, min_price * 0.7)  # 하한
            new_price = min(new_price, max_price * 1.3)   # 상한
            prices.append(new_price)
        
        # OHLC 데이터 생성
        data = pd.DataFrame({
            'Open': [p * np.random.uniform(0.995, 1.005) for p in prices],
            'High': [p * np.random.uniform(1.002, 1.025) for p in prices],
            'Low': [p * np.random.uniform(0.975, 0.998) for p in prices],
            'Close': prices,
            'Volume': np.random.lognormal(
                13 + np.random.uniform(-0.5, 0.5),  # 거래량 기본값
                0.4 + volatility * 10,  # 변동성에 비례한 거래량 변동
                period_days
            ).astype(int)
        }, index=dates)
        
        # High/Low 보정
        for i in range(len(data)):
            high = max(data.iloc[i]['Open'], data.iloc[i]['Close'], data.iloc[i]['High'])
            low = min(data.iloc[i]['Open'], data.iloc[i]['Close'], data.iloc[i]['Low'])
            data.iloc[i, data.columns.get_loc('High')] = high
            data.iloc[i, data.columns.get_loc('Low')] = low
        
        # 결과 출력
        current_price = data['Close'].iloc[-1]
        start_price = data['Close'].iloc[0]
        total_return = (current_price / start_price - 1) * 100
        
        print(f"📊 샘플 데이터 완성:")
        print(f"   기간: {data.index[0].strftime('%Y-%m-%d')} ~ {data.index[-1].strftime('%Y-%m-%d')}")
        print(f"   시작가: {start_price:,.0f}원")
        print(f"   종료가: {current_price:,.0f}원")
        print(f"   수익률: {total_return:+.1f}%")
        print(f"   최고가: {data['High'].max():,.0f}원")
        print(f"   최저가: {data['Low'].min():,.0f}원")
        print(f"   평균거래량: {data['Volume'].mean():,.0f}주")
        
        return data

class TechnicalAnalysisRunner:
    """기술분석 실행기 - 멀티 소스 지원"""
    
    def __init__(self, db_path: str = "data/databases/stock_data.db"):
        self.db_path = db_path
        self.analyzer = TechnicalAnalyzer()
        self.data_collector = MultiSourceDataCollector()
        self.ensure_database_exists()
    
    def ensure_database_exists(self):
        """데이터베이스 및 테이블 생성"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # 기술분석 결과 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS technical_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    analysis_date TEXT NOT NULL,
                    current_price REAL,
                    total_score REAL,
                    recommendation TEXT,
                    risk_level TEXT,
                    rsi REAL,
                    macd REAL,
                    bb_position REAL,
                    adx REAL,
                    sma_20 REAL,
                    volume_trend TEXT,
                    data_source TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(stock_code, analysis_date)
                )
            ''')
    
    def get_stock_data(self, stock_code: str, period_days: int = 300) -> Tuple[Optional[pd.DataFrame], str]:
        """주가 데이터 가져오기 (소스 정보 포함)"""
        print(f"\n📊 {stock_code} 데이터 수집 시작...")
        
        # 실제 데이터 시도
        df = self.data_collector.get_stock_data_any_source(stock_code, period_days)
        
        if df is not None and len(df) >= 50:
            return df, "real_data"
        
        # 샘플 데이터 생성
        print(f"📊 {stock_code}: 실제 데이터 없음, 샘플 데이터로 대체")
        df = self.data_collector.generate_realistic_sample_data(stock_code, period_days)
        return df, "sample_data"
    
    def save_analysis_result(self, result: Dict, data_source: str):
        """분석 결과를 DB에 저장"""
        if 'error' in result:
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                indicators = result['technical_indicators']
                
                conn.execute('''
                    INSERT OR REPLACE INTO technical_analysis
                    (stock_code, analysis_date, current_price, total_score, 
                     recommendation, risk_level, rsi, macd, bb_position, 
                     adx, sma_20, volume_trend, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', [
                    result['stock_code'],
                    datetime.now().strftime('%Y-%m-%d'),
                    result['current_price'],
                    result['overall_score'],
                    result['recommendation'],
                    result['risk_level'],
                    indicators.get('RSI'),
                    indicators.get('MACD'),
                    indicators.get('BB_POSITION'),
                    indicators.get('ADX'),
                    indicators.get('SMA_20'),
                    result['analysis_summary'].get('volume_trend', 'Normal'),
                    data_source
                ])
                print(f"💾 {result['stock_code']}: DB 저장 완료 (소스: {data_source})")
        except Exception as e:
            print(f"⚠️ DB 저장 실패: {e}")
    
    def analyze_single_stock(self, stock_code: str, save_to_db: bool = True) -> Dict:
        """개별 종목 분석"""
        print(f"\n🎯 {stock_code} 기술분석 시작...")
        
        # 종목 정보 출력
        if stock_code in KOREAN_STOCKS:
            info = KOREAN_STOCKS[stock_code]
            print(f"📋 종목명: {info['name']}")
            print(f"📊 시장: {info['market']}")
            print(f"🏭 업종: {info['sector']}")
        
        # 주가 데이터 가져오기
        ohlcv_data, data_source = self.get_stock_data(stock_code)
        if ohlcv_data is None or len(ohlcv_data) < 50:
            return {'error': f'{stock_code}: 충분한 데이터가 없습니다.'}
        
        # 기술분석 실행
        result = self.analyzer.analyze_stock(stock_code, ohlcv_data)
        
        # 결과 출력
        if 'error' not in result:
            # 데이터 소스 정보 추가
            result['data_source'] = data_source
            result['data_info'] = {
                'source': data_source,
                'period_days': len(ohlcv_data),
                'start_date': ohlcv_data.index[0].strftime('%Y-%m-%d'),
                'end_date': ohlcv_data.index[-1].strftime('%Y-%m-%d')
            }
            
            print_analysis_summary(result)
            
            # 데이터 소스 정보 출력
            source_emoji = "🌐" if data_source == "real_data" else "🎲"
            source_name = "실제 데이터" if data_source == "real_data" else "샘플 데이터"
            print(f"\n{source_emoji} 데이터 소스: {source_name}")
            print(f"📅 분석 기간: {result['data_info']['start_date']} ~ {result['data_info']['end_date']}")
            print(f"📊 데이터 일수: {result['data_info']['period_days']}일")
            
            # DB 저장
            if save_to_db:
                self.save_analysis_result(result, data_source)
        
        return result
    
    def analyze_multiple_stocks(self, stock_codes: List[str], delay_seconds: float = 1.0) -> Dict[str, Dict]:
        """다중 종목 분석"""
        results = {}
        total_stocks = len(stock_codes)
        
        print(f"\n🔄 {total_stocks}개 종목 일괄 분석 시작...")
        print("=" * 60)
        
        for i, stock_code in enumerate(stock_codes, 1):
            print(f"\n[{i}/{total_stocks}] {stock_code} 분석 중...")
            
            result = self.analyze_single_stock(stock_code, save_to_db=True)
            results[stock_code] = result
            
            # API 제한을 위한 딜레이
            if i < total_stocks:
                time.sleep(delay_seconds)
        
        # 요약 통계
        self.print_summary_statistics(results)
        
        return results
    
    def get_sample_stocks(self) -> List[str]:
        """샘플 종목 리스트"""
        return list(KOREAN_STOCKS.keys())[:10]
    
    def print_summary_statistics(self, results: Dict[str, Dict]):
        """결과 요약 통계 출력"""
        successful_results = [r for r in results.values() if 'error' not in r]
        failed_results = [r for r in results.values() if 'error' in r]
        
        print(f"\n📊 분석 결과 요약")
        print("=" * 50)
        print(f"✅ 성공: {len(successful_results)}개")
        print(f"❌ 실패: {len(failed_results)}개")
        
        if successful_results:
            # 데이터 소스별 분류
            data_sources = {}
            for result in successful_results:
                source = result.get('data_source', 'unknown')
                data_sources[source] = data_sources.get(source, 0) + 1
            
            print(f"\n📊 데이터 소스별 분포:")
            for source, count in data_sources.items():
                emoji = "🌐" if source == "real_data" else "🎲"
                name = "실제 데이터" if source == "real_data" else "샘플 데이터"
                print(f"  {emoji} {name}: {count}개")
            
            # 추천도별 분류
            recommendations = {}
            for result in successful_results:
                rec = result.get('recommendation', 'NEUTRAL')
                recommendations[rec] = recommendations.get(rec, 0) + 1
            
            print(f"\n📈 추천도 분포:")
            for rec, count in sorted(recommendations.items()):
                emoji = "🟢" if "BUY" in rec else "🔴" if "SELL" in rec else "🟡"
                print(f"  {emoji} {rec}: {count}개")
            
            # 상위 매수 추천
            buy_recommendations = [r for r in successful_results 
                                 if r.get('recommendation') in ['STRONG_BUY', 'BUY']]
            
            if buy_recommendations:
                buy_recommendations.sort(key=lambda x: x.get('overall_score', 0), reverse=True)
                print(f"\n🟢 상위 매수 추천:")
                for i, result in enumerate(buy_recommendations[:5], 1):
                    stock_code = result['stock_code']
                    score = result.get('overall_score', 0)
                    rec = result.get('recommendation', '')
                    price = result.get('current_price', 0)
                    name = KOREAN_STOCKS.get(stock_code, {}).get('name', stock_code)
                    print(f"  {i}. {name}({stock_code}): {rec} (점수: {score:.1f}, 가격: {price:,.0f}원)")
        
        if failed_results:
            print(f"\n❌ 분석 실패 종목:")
            for stock_code, result in results.items():
                if 'error' in result:
                    name = KOREAN_STOCKS.get(stock_code, {}).get('name', stock_code)
                    print(f"  • {name}({stock_code}): {result['error']}")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description='📈 기술분석 실행 스크립트 (멀티 소스 데이터 지원)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
사용 예시:
  %(prog)s --stock_code 005930                    # 삼성전자 분석
  %(prog)s --stock_code 035420                    # NAVER 분석 (샘플 데이터)
  %(prog)s --multiple 005930,000660,035420        # 여러 종목 분석
  %(prog)s --sample_analysis                      # 샘플 종목들 분석
  %(prog)s --stock_code 005930 --save result.json # 결과를 파일로 저장

데이터 소스 우선순위:
  1. FinanceDataReader (KRX)
  2. FinanceDataReader (Yahoo)  
  3. yfinance (직접)
  4. 샘플 데이터 (현실적 패턴)

특징:
  - 실제 데이터 실패 시 자동으로 현실적인 샘플 데이터 생성
  - 종목별 업종, 가격대 특성 반영
  - 데이터 소스 정보 저장 및 표시
        '''
    )
    
    # 실행 모드
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--stock_code', type=str, help='단일 종목 분석 (예: 005930)')
    group.add_argument('--multiple', type=str, help='복수 종목 분석 (쉼표로 구분, 예: 005930,000660)')
    group.add_argument('--sample_analysis', action='store_true', help='샘플 종목 10개 분석')
    
    # 옵션
    parser.add_argument('--save', type=str, help='결과를 JSON 파일로 저장')
    parser.add_argument('--delay', type=float, default=1.0, help='API 호출 간격 (초, 기본값: 1.0)')
    parser.add_argument('--no_db', action='store_true', help='DB 저장 안 함')
    parser.add_argument('--list_stocks', action='store_true', help='지원 종목 리스트 출력')
    
    args = parser.parse_args()
    
    # 종목 리스트 출력
    if args.list_stocks:
        print("📋 지원 종목 리스트:")
        print("=" * 60)
        for code, info in KOREAN_STOCKS.items():
            print(f"{code}: {info['name']} ({info['market']}, {info['sector']})")
        return
    
    print("🚀 기술분석 실행 스크립트 시작 (멀티 소스 데이터 지원)")
    print("=" * 60)
    
    try:
        runner = TechnicalAnalysisRunner()
        save_to_db = not args.no_db
        
        if args.stock_code:
            # 단일 종목 분석
            result = runner.analyze_single_stock(args.stock_code, save_to_db)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.multiple:
            # 복수 종목 분석
            stock_codes = [code.strip() for code in args.multiple.split(',')]
            print(f"📈 지정 종목 {len(stock_codes)}개 분석")
            
            results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.sample_analysis:
            # 샘플 종목 분석
            stock_codes = runner.get_sample_stocks()
            print(f"📊 샘플 종목 {len(stock_codes)}개 분석")
            
            results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        print(f"\n✨ 기술분석 실행 완료!")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ 사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 실행 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
