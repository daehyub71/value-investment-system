#!/usr/bin/env python3
"""
📈 전체 종목 기술분석 실행 스크립트 - company_info 테이블 기반
Value Investment System - Technical Analysis Runner for All Stocks

수정 사항:
- KOREAN_STOCKS 딕셔너리 대신 company_info 테이블에서 전체 종목 조회
- technical_indicators 테이블에 저장
- 데이터베이스 연결 개선
- 대량 데이터 처리 최적화

실행 방법:
python run_technical_analysis_all_stocks.py --list_stocks           # 종목 리스트 확인
python run_technical_analysis_all_stocks.py --sample_analysis       # 샘플 10개 분석
python run_technical_analysis_all_stocks.py --all_stocks           # 전체 분석
python run_technical_analysis_all_stocks.py --kospi_only           # 코스피만
python run_technical_analysis_all_stocks.py --kosdaq_only          # 코스닥만
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
    sys.exit(1)

# 데이터베이스 설정 import
try:
    from config.database_config import get_db_connection, get_database_path
    print("✅ 데이터베이스 설정 import 성공!")
except ImportError as e:
    print(f"❌ 데이터베이스 설정 import 실패: {e}")
    print("기본 데이터베이스 경로 사용")

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

# yfinance
try:
    import yfinance as yf
    DATA_SOURCES['yfinance'] = True
    print("✅ yfinance 사용 가능")
except ImportError:
    DATA_SOURCES['yfinance'] = False
    print("⚠️ yfinance 사용 불가")

print(f"📊 사용 가능한 데이터 소스: {sum(DATA_SOURCES.values())}개")

class DatabaseStockManager:
    """데이터베이스 기반 종목 관리"""
    
    def __init__(self, db_path: str = "data/databases/stock_data.db"):
        self.db_path = db_path
        self.ensure_database_exists()
    
    def ensure_database_exists(self):
        """데이터베이스 존재 확인"""
        if not os.path.exists(self.db_path):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            print(f"⚠️ 데이터베이스 파일이 없습니다: {self.db_path}")
            print("데이터베이스를 먼저 생성해주세요:")
            print("python scripts/setup_project.py")
    
    def get_all_stocks(self) -> List[Dict[str, str]]:
        """company_info 테이블에서 전체 종목 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                query = """
                SELECT stock_code, company_name, market_type, sector, industry 
                FROM company_info 
                WHERE stock_code IS NOT NULL 
                AND stock_code != ''
                ORDER BY market_type, stock_code
                """
                
                cursor = conn.execute(query)
                stocks = [dict(row) for row in cursor.fetchall()]
                
                print(f"📊 데이터베이스에서 {len(stocks)}개 종목 조회 완료")
                return stocks
                
        except Exception as e:
            print(f"❌ 종목 조회 실패: {e}")
            return []
    
    def get_stocks_by_market(self, market_type: str) -> List[Dict[str, str]]:
        """시장별 종목 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                query = """
                SELECT stock_code, company_name, market_type, sector, industry 
                FROM company_info 
                WHERE market_type = ? 
                AND stock_code IS NOT NULL 
                AND stock_code != ''
                ORDER BY stock_code
                """
                
                cursor = conn.execute(query, (market_type,))
                stocks = [dict(row) for row in cursor.fetchall()]
                
                print(f"📊 {market_type} 시장에서 {len(stocks)}개 종목 조회 완료")
                return stocks
                
        except Exception as e:
            print(f"❌ {market_type} 종목 조회 실패: {e}")
            return []
    
    def get_market_statistics(self) -> Dict[str, int]:
        """시장별 종목 수 통계"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                SELECT market_type, COUNT(*) as count 
                FROM company_info 
                WHERE stock_code IS NOT NULL 
                AND stock_code != ''
                GROUP BY market_type
                ORDER BY market_type
                """
                
                cursor = conn.execute(query)
                results = cursor.fetchall()
                
                stats = {}
                total = 0
                for row in results:
                    market_type = row[0] if row[0] else 'UNKNOWN'
                    count = row[1]
                    stats[market_type] = count
                    total += count
                
                stats['TOTAL'] = total
                return stats
                
        except Exception as e:
            print(f"❌ 시장 통계 조회 실패: {e}")
            return {}

class StableDataCollector:
    """안정화된 데이터 수집기"""
    
    def __init__(self):
        self.available_sources = [name for name, available in DATA_SOURCES.items() if available]
        print(f"🔗 활성화된 데이터 소스: {self.available_sources}")
    
    def validate_stock_code(self, stock_code: str) -> str:
        """종목 코드 유효성 검사 및 표준화"""
        # 앞의 0 제거 후 다시 6자리로 패딩
        cleaned_code = stock_code.lstrip('0')
        if cleaned_code:
            standardized_code = cleaned_code.zfill(6)
        else:
            standardized_code = '000000'
        
        if stock_code != standardized_code:
            print(f"🔍 종목 코드 표준화: {stock_code} -> {standardized_code}")
        return standardized_code
    
    def get_stock_data_fdr_simple(self, stock_code: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """FinanceDataReader - 단순 방식"""
        if not DATA_SOURCES.get('fdr'):
            return None
        
        standardized_code = self.validate_stock_code(stock_code)
        
        try:
            # 가장 기본적인 방식만 사용
            df = fdr.DataReader(standardized_code, start=start_date, end=end_date)
            if not df.empty:
                return df
        except Exception as e:
            pass  # 조용히 실패
        
        return None
    
    def get_stock_data_any_source(self, stock_code: str, period_days: int = 300) -> Optional[pd.DataFrame]:
        """단순화된 데이터 수집"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        # FDR만 사용 (가장 안정적)
        df = self.get_stock_data_fdr_simple(stock_code, start_date, end_date)
        if df is not None and len(df) >= 20:
            # 간단한 데이터 검증
            df = self.simple_data_validation(df)
            return df
        
        return None
    
    def simple_data_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """간단한 데이터 검증"""
        # 1. 결측값 제거
        df = df.dropna()
        
        # 2. 음수값 제거
        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numeric_columns:
            if col in df.columns:
                df = df[df[col] > 0]
        
        # 3. High/Low 일관성 보정
        if all(col in df.columns for col in ['Open', 'High', 'Low', 'Close']):
            for i in df.index:
                values = [df.loc[i, 'Open'], df.loc[i, 'Close']]
                current_high = df.loc[i, 'High']
                current_low = df.loc[i, 'Low']
                
                # High는 모든 값보다 크거나 같아야 함
                actual_high = max(values + [current_high])
                # Low는 모든 값보다 작거나 같아야 함  
                actual_low = min(values + [current_low])
                
                df.loc[i, 'High'] = actual_high
                df.loc[i, 'Low'] = actual_low
        
        return df
    
    def generate_sample_data(self, stock_info: Dict[str, str], period_days: int) -> pd.DataFrame:
        """간단한 샘플 데이터 생성"""
        dates = pd.date_range(end=datetime.now(), periods=period_days, freq='D')
        
        stock_code = stock_info['stock_code']
        company_name = stock_info.get('company_name', f'종목{stock_code}')
        
        # 기본 가격 범위 설정 (시장별)
        market_type = stock_info.get('market_type', 'KOSPI')
        if market_type == 'KOSDAQ':
            base_price = 15000  # 코스닥 평균
            price_range = (5000, 50000)
        else:
            base_price = 50000  # 코스피 평균
            price_range = (10000, 200000)
        
        min_price, max_price = price_range
        
        # 간단한 랜덤워크
        np.random.seed(hash(stock_code) % 2**32)
        returns = np.random.normal(0.0005, 0.02, period_days)  # 연 12%, 변동성 30%
        
        prices = [base_price]
        for ret in returns[1:]:
            new_price = prices[-1] * (1 + ret)
            # 범위 제한
            new_price = max(min(new_price, max_price * 1.2), min_price * 0.8)
            prices.append(new_price)
        
        # OHLC 생성
        data = pd.DataFrame({
            'Open': [p * np.random.uniform(0.995, 1.005) for p in prices],
            'High': [p * np.random.uniform(1.005, 1.025) for p in prices],
            'Low': [p * np.random.uniform(0.975, 0.995) for p in prices],
            'Close': prices,
            'Volume': np.random.lognormal(13, 0.5, period_days).astype(int)
        }, index=dates)
        
        # High/Low 보정
        for i in range(len(data)):
            high = max(data.iloc[i]['Open'], data.iloc[i]['Close'], data.iloc[i]['High'])
            low = min(data.iloc[i]['Open'], data.iloc[i]['Close'], data.iloc[i]['Low'])
            data.iloc[i, data.columns.get_loc('High')] = high
            data.iloc[i, data.columns.get_loc('Low')] = low
        
        return data

class TechnicalAnalysisRunner:
    """기술분석 실행기 - 전체 종목 대응 버전"""
    
    def __init__(self, db_path: str = "data/databases/stock_data.db"):
        self.db_path = db_path
        self.analyzer = TechnicalAnalyzer()
        self.data_collector = StableDataCollector()
        self.stock_manager = DatabaseStockManager(db_path)
        self.ensure_database_exists()
    
    def ensure_database_exists(self):
        """데이터베이스 스키마 확인/업데이트"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # technical_indicators 테이블 생성/업데이트 (정확한 스키마)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS technical_indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    
                    -- 📊 추세 지표
                    sma_5 REAL,                  -- 5일 단순이동평균
                    sma_20 REAL,                 -- 20일 단순이동평균
                    sma_60 REAL,                 -- 60일 단순이동평균
                    sma_120 REAL,                -- 120일 단순이동평균
                    sma_200 REAL,                -- 200일 단순이동평균
                    ema_12 REAL,                 -- 12일 지수이동평균
                    ema_26 REAL,                 -- 26일 지수이동평균
                    parabolic_sar REAL,          -- 파라볼릭 SAR
                    adx REAL,                    -- 평균방향지수
                    plus_di REAL,                -- +DI
                    minus_di REAL,               -- -DI
                    
                    -- ⚡ 모멘텀 지표
                    rsi REAL,                    -- RSI (14일)
                    macd REAL,                   -- MACD
                    macd_signal REAL,            -- MACD 신호선
                    macd_histogram REAL,         -- MACD 히스토그램
                    stochastic_k REAL,           -- 스토캠스틱 %K
                    stochastic_d REAL,           -- 스토캠스틱 %D
                    williams_r REAL,             -- Williams %R
                    cci REAL,                    -- 상품채널지수
                    mfi REAL,                    -- 자금흐름지수
                    momentum REAL,               -- 모멘텀 오실레이터
                    
                    -- 📈 변동성 지표
                    bollinger_upper REAL,        -- 볼린저 밴드 상한
                    bollinger_middle REAL,       -- 볼린저 밴드 중간
                    bollinger_lower REAL,        -- 볼린저 밴드 하한
                    bollinger_width REAL,        -- 볼린저 밴드 폭
                    atr REAL,                    -- 평균진실범위
                    keltner_upper REAL,          -- 켈트너 채널 상한
                    keltner_lower REAL,          -- 켈트너 채널 하한
                    donchian_upper REAL,         -- 도너찬 채널 상한
                    donchian_lower REAL,         -- 도너찬 채널 하한
                    
                    -- 📊 거래량 지표
                    obv REAL,                    -- 누적거래량
                    vwap REAL,                   -- 거래량가중평균가
                    cmf REAL,                    -- 차이킨자금흐름
                    volume_ratio REAL,           -- 거래량 비율
                    
                    -- 🎯 종합 신호
                    trend_signal INTEGER,        -- 추세 신호 (-1: 하락, 0: 보합, 1: 상승)
                    momentum_signal INTEGER,     -- 모멘텀 신호
                    volatility_signal INTEGER,   -- 변동성 신호
                    volume_signal INTEGER,       -- 거래량 신호
                    technical_score REAL,        -- 기술적 분석 종합 점수 (0-100)
                    
                    -- 52주 관련 지표
                    week_52_high REAL,           -- 52주 최고가
                    week_52_low REAL,            -- 52주 최저가
                    week_52_high_ratio REAL,     -- 52주 최고가 대비 비율
                    week_52_low_ratio REAL,      -- 52주 최저가 대비 비율
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(stock_code, date)
                )
            ''')
            
            # 인덱스 생성 (안전하게)
            try:
                conn.execute('CREATE INDEX IF NOT EXISTS idx_technical_indicators_code_date ON technical_indicators(stock_code, date)')
                
                # 컬럼 존재 여부 확인 후 인덱스 생성
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(technical_indicators)")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'technical_score' in columns:
                    conn.execute('CREATE INDEX IF NOT EXISTS idx_technical_indicators_score ON technical_indicators(technical_score)')
                if 'trend_signal' in columns:
                    conn.execute('CREATE INDEX IF NOT EXISTS idx_technical_indicators_trend ON technical_indicators(trend_signal)')
                
                conn.commit()
                print("✅ technical_indicators 테이블 스키마 확인 완료")
                print(f"📊 테이블 컬럼 수: {len(columns)}개")
            except Exception as e:
                print(f"⚠️ 인덱스 생성 중 오류: {e}")
                print("테이블은 생성되었지만 일부 인덱스 생성이 실패했습니다.")
    
    def get_stock_data(self, stock_info: Dict[str, str], period_days: int = 300) -> Tuple[Optional[pd.DataFrame], str]:
        """데이터 가져오기"""
        stock_code = stock_info['stock_code']
        
        # 실제 데이터 시도
        df = self.data_collector.get_stock_data_any_source(stock_code, period_days)
        
        if df is not None and len(df) >= 20:
            return df, "real_data"
        
        # 샘플 데이터 생성
        df = self.data_collector.generate_sample_data(stock_info, period_days)
        return df, "sample_data"
    
    def safe_save_analysis_result(self, result: Dict, stock_info: Dict[str, str], data_source: str):
        """안전한 DB 저장 - technical_indicators 테이블"""
        if 'error' in result:
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 먼저 테이블 구조 확인
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(technical_indicators)")
                columns_info = cursor.fetchall()
                available_columns = [col[1] for col in columns_info]
                
                indicators = result.get('technical_indicators', {})
                
                # 안전한 값 추출
                def safe_get(value):
                    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                        return None
                    return float(value) if isinstance(value, (int, float)) else None
                
                # 사용 가능한 컬럼에 따라 데이터 준비
                data_dict = {
                    'stock_code': result['stock_code'],
                    'date': datetime.now().strftime('%Y-%m-%d'),
                }
                
                # 기술지표 데이터 추가 (정확한 컬럼명 사용)
                indicator_mapping = {
                    'sma_5': safe_get(indicators.get('SMA_5')),
                    'sma_20': safe_get(indicators.get('SMA_20')),
                    'sma_60': safe_get(indicators.get('SMA_60')),
                    'sma_120': safe_get(indicators.get('SMA_120')),
                    'sma_200': safe_get(indicators.get('SMA_200')),
                    'ema_12': safe_get(indicators.get('EMA_12')),
                    'ema_26': safe_get(indicators.get('EMA_26')),
                    'rsi': safe_get(indicators.get('RSI')),
                    'macd': safe_get(indicators.get('MACD')),
                    'macd_signal': safe_get(indicators.get('MACD_SIGNAL')),
                    'macd_histogram': safe_get(indicators.get('MACD_HISTOGRAM')),
                    'bollinger_upper': safe_get(indicators.get('BB_UPPER')),
                    'bollinger_middle': safe_get(indicators.get('BB_MIDDLE')),
                    'bollinger_lower': safe_get(indicators.get('BB_LOWER')),
                    'stochastic_k': safe_get(indicators.get('STOCH_K')),
                    'stochastic_d': safe_get(indicators.get('STOCH_D')),
                    'adx': safe_get(indicators.get('ADX')),
                    'atr': safe_get(indicators.get('ATR')),
                    'week_52_high': safe_get(indicators.get('52W_HIGH')),
                    'week_52_low': safe_get(indicators.get('52W_LOW')),
                    'week_52_high_ratio': safe_get(indicators.get('52W_HIGH_RATIO')),
                    'week_52_low_ratio': safe_get(indicators.get('52W_LOW_RATIO')),
                    'technical_score': safe_get(result.get('overall_score')),
                }
                
                # 사용 가능한 컬럼만 추가
                for key, value in indicator_mapping.items():
                    if key in available_columns:
                        data_dict[key] = value
                
                # 동적 INSERT 문 생성
                columns = list(data_dict.keys())
                placeholders = ['?' for _ in columns]
                values = [data_dict[col] for col in columns]
                
                # updated_at 컬럼이 있으면 추가
                if 'updated_at' in available_columns:
                    columns.append('updated_at')
                    placeholders.append('CURRENT_TIMESTAMP')
                
                insert_sql = f'''
                    INSERT OR REPLACE INTO technical_indicators 
                    ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                '''
                
                conn.execute(insert_sql, values)
                print(f"💾 {result['stock_code']}: DB 저장 완료")
                
        except Exception as e:
            print(f"⚠️ DB 저장 실패 ({result.get('stock_code', 'UNKNOWN')}): {e}")
    
    def analyze_single_stock(self, stock_info: Dict[str, str], save_to_db: bool = True, show_details: bool = False) -> Dict:
        """단일 종목 분석"""
        stock_code = stock_info['stock_code']
        company_name = stock_info.get('company_name', stock_code)
        
        if show_details:
            print(f"\n🎯 {stock_code} ({company_name}) 기술분석 시작...")
            print(f"📊 시장: {stock_info.get('market_type', 'UNKNOWN')}")
            print(f"🏭 업종: {stock_info.get('sector', 'UNKNOWN')}")
        
        # 데이터 수집
        ohlcv_data, data_source = self.get_stock_data(stock_info)
        if ohlcv_data is None or len(ohlcv_data) < 20:
            return {'error': f'{stock_code}: 충분한 데이터가 없습니다.'}
        
        # 기술분석 실행
        try:
            result = self.analyzer.analyze_stock(stock_code, ohlcv_data)
        except Exception as e:
            return {'error': f'{stock_code}: 기술분석 실행 실패 - {str(e)}'}
        
        # 결과 처리
        if 'error' not in result:
            result['data_source'] = data_source
            result['company_name'] = company_name
            result['market_type'] = stock_info.get('market_type')
            result['sector'] = stock_info.get('sector')
            
            if show_details:
                print_analysis_summary(result)
                source_emoji = "🌐" if data_source == "real_data" else "🎲"
                source_name = "실제 데이터" if data_source == "real_data" else "샘플 데이터"
                print(f"{source_emoji} 데이터 소스: {source_name}")
            
            # DB 저장
            if save_to_db:
                self.safe_save_analysis_result(result, stock_info, data_source)
        
        return result
    
    def analyze_multiple_stocks(self, stock_list: List[Dict[str, str]], delay_seconds: float = 0.1) -> Dict[str, Dict]:
        """다중 종목 분석"""
        results = {}
        total_stocks = len(stock_list)
        
        print(f"\n🔄 {total_stocks}개 종목 일괄 분석 시작...")
        print("=" * 80)
        
        successful_count = 0
        failed_count = 0
        
        for i, stock_info in enumerate(stock_list, 1):
            stock_code = stock_info['stock_code']
            company_name = stock_info.get('company_name', stock_code)
            
            print(f"[{i:4d}/{total_stocks}] {stock_code} ({company_name[:10]:10s}) ", end="")
            
            try:
                result = self.analyze_single_stock(stock_info, save_to_db=True, show_details=False)
                results[stock_code] = result
                
                if 'error' in result:
                    print("❌ FAIL")
                    failed_count += 1
                else:
                    score = result.get('overall_score', 0)
                    rec = result.get('recommendation', 'NEUTRAL')
                    source = "🌐" if result.get('data_source') == "real_data" else "🎲"
                    print(f"✅ {rec:12s} (점수: {score:5.1f}) {source}")
                    successful_count += 1
                    
            except Exception as e:
                print(f"❌ ERROR: {str(e)[:30]}")
                results[stock_code] = {'error': str(e)}
                failed_count += 1
            
            # API 제한을 위한 딜레이
            if i < total_stocks and delay_seconds > 0:
                time.sleep(delay_seconds)
            
            # 진행률 표시 (100개마다)
            if i % 100 == 0:
                print(f"\n📊 진행률: {i}/{total_stocks} ({i/total_stocks*100:.1f}%) - 성공: {successful_count}, 실패: {failed_count}")
        
        # 최종 요약 통계
        self.print_summary_statistics(results, stock_list)
        
        return results
    
    def print_summary_statistics(self, results: Dict[str, Dict], stock_list: List[Dict[str, str]]):
            """결과 요약 통계 - None 값 안전 처리"""
            successful_results = [r for r in results.values() if 'error' not in r]
            failed_results = [r for r in results.values() if 'error' in r]
            
            print(f"\n📊 분석 결과 요약")
            print("=" * 80)
            print(f"✅ 성공: {len(successful_results)}개")
            print(f"❌ 실패: {len(failed_results)}개")
            print(f"📈 성공률: {len(successful_results)/len(results)*100:.1f}%")
            
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
                    print(f"  {emoji} {name}: {count}개 ({count/len(successful_results)*100:.1f}%)")
                
                # 추천도별 분류
                recommendations = {}
                for result in successful_results:
                    rec = result.get('recommendation', 'NEUTRAL')
                    recommendations[rec] = recommendations.get(rec, 0) + 1
                
                print(f"\n📈 추천도 분포:")
                for rec, count in sorted(recommendations.items()):
                    emoji = "🟢" if "BUY" in rec else "🔴" if "SELL" in rec else "🟡"
                    print(f"  {emoji} {rec}: {count}개 ({count/len(successful_results)*100:.1f}%)")
                
                # 시장별 분류
                if stock_list:
                    market_stats = {}
                    for stock_info in stock_list:
                        market = stock_info.get('market_type', 'UNKNOWN')
                        market_stats[market] = market_stats.get(market, 0) + 1
                    
                    print(f"\n📊 시장별 분포:")
                    for market, count in market_stats.items():
                        print(f"  📈 {market}: {count}개")
                
                # 상위 추천 종목 (상위 10개) - None 값 안전 처리
                buy_recommendations = [r for r in successful_results 
                                    if r.get('recommendation') in ['STRONG_BUY', 'BUY']]
                
                if buy_recommendations:
                    buy_recommendations.sort(key=lambda x: x.get('overall_score', 0), reverse=True)
                    print(f"\n🟢 상위 매수 추천 종목 (Top 10):")
                    for i, result in enumerate(buy_recommendations[:10], 1):
                        stock_code = result.get('stock_code', 'N/A')
                        score = result.get('overall_score', 0)
                        rec = result.get('recommendation', 'NEUTRAL')
                        price = result.get('current_price', 0)
                        name = result.get('company_name', stock_code)
                        market = result.get('market_type', 'N/A')
                        source_emoji = "🌐" if result.get('data_source') == "real_data" else "🎲"
                        
                        # None 값 안전 처리
                        safe_name = name if name is not None else 'N/A'
                        safe_market = market if market is not None else 'N/A'
                        safe_rec = rec if rec is not None else 'N/A'
                        safe_score = score if score is not None else 0.0
                        safe_price = price if price is not None else 0.0
                        
                        try:
                            print(f"  {i:2d}. {safe_name[:15]:15s}({stock_code}) {safe_market:6s}: {safe_rec:12s} (점수: {safe_score:5.1f}, 가격: {safe_price:8,.0f}원) {source_emoji}")
                        except (ValueError, TypeError) as e:
                            # 포맷팅 실패 시 안전한 출력
                            print(f"  {i:2d}. {safe_name[:15]}({stock_code}) {safe_market}: {safe_rec} (점수: {safe_score}, 가격: {safe_price}원) {source_emoji}")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description='📈 전체 종목 기술분석 실행 스크립트 (DB 기반)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
사용 예시:
  %(prog)s --list_stocks                          # 데이터베이스 종목 리스트 확인
  %(prog)s --stock_code 005930                    # 단일 종목 분석
  %(prog)s --sample_analysis                      # 샘플 10개 종목 분석
  %(prog)s --all_stocks                           # 전체 종목 분석
  %(prog)s --kospi_only                           # 코스피 종목만 분석
  %(prog)s --kosdaq_only                          # 코스닥 종목만 분석
  %(prog)s --top_100                              # 상위 100개 종목만

특징:
  ✅ company_info 테이블 기반 전체 종목 지원
  ✅ technical_indicators 테이블에 저장
  ✅ 대량 데이터 처리 최적화
  ✅ 실시간 진행 상황 표시
  ✅ 시장별 필터링 지원

저장 테이블: technical_indicators
        '''
    )
    
    # 종목 리스트 확인
    parser.add_argument('--list_stocks', action='store_true', help='데이터베이스 종목 리스트 출력')
    
    # 실행 모드
    group = parser.add_mutually_exclusive_group(required=not any('--list_stocks' in arg for arg in sys.argv))
    group.add_argument('--stock_code', type=str, help='단일 종목 분석 (예: 005930)')
    group.add_argument('--sample_analysis', action='store_true', help='샘플 10개 종목 분석')
    group.add_argument('--all_stocks', action='store_true', help='전체 종목 분석')
    group.add_argument('--kospi_only', action='store_true', help='코스피 종목만 분석')
    group.add_argument('--kosdaq_only', action='store_true', help='코스닥 종목만 분석')
    group.add_argument('--top_100', action='store_true', help='상위 100개 종목 분석')
    
    # 옵션
    parser.add_argument('--save', type=str, help='결과를 JSON 파일로 저장')
    parser.add_argument('--delay', type=float, default=0.1, help='종목간 딜레이 (초)')
    parser.add_argument('--db_path', type=str, default="data/databases/stock_data.db", help='데이터베이스 경로')
    
    args = parser.parse_args()
    
    # 데이터베이스 경로 설정
    db_path = os.path.abspath(args.db_path)
    
    try:
        runner = TechnicalAnalysisRunner(db_path)
        
        # 종목 리스트 출력
        if args.list_stocks:
            print("📋 데이터베이스 종목 리스트:")
            print("=" * 100)
            
            # 시장 통계
            stats = runner.stock_manager.get_market_statistics()
            print(f"\n📊 시장별 종목 수:")
            for market, count in stats.items():
                if market != 'TOTAL':
                    print(f"  📈 {market}: {count:,}개")
            print(f"  📊 총계: {stats.get('TOTAL', 0):,}개")
            
            # 전체 종목 리스트 (처음 50개만 표시)
            all_stocks = runner.stock_manager.get_all_stocks()
            print(f"\n📋 종목 리스트 (처음 50개):")
            print(f"{'종목코드':<8} {'종목명':<20} {'시장':<8} {'업종':<15}")
            print("-" * 60)
            
            for i, stock in enumerate(all_stocks[:50]):
                stock_code = stock['stock_code'] or 'N/A'
                company_name = stock['company_name'] or 'N/A'
                market_type = stock.get('market_type', 'UNKNOWN') or 'UNKNOWN'
                sector = stock.get('sector', 'UNKNOWN') or 'UNKNOWN'
                
                # 문자열 길이 제한
                company_name_short = company_name[:20] if company_name != 'N/A' else 'N/A'
                sector_short = sector[:15] if sector != 'UNKNOWN' else 'UNKNOWN'
                
                print(f"{stock_code:<10} {company_name_short:<20} {market_type:<8} {sector_short:<15}")
            
            if len(all_stocks) > 50:
                print(f"... 및 {len(all_stocks) - 50:,}개 종목 더")
            
            print(f"\n💡 사용 예시:")
            print(f"  python {sys.argv[0]} --sample_analysis")
            print(f"  python {sys.argv[0]} --kospi_only --save kospi_results.json")
            print(f"  python {sys.argv[0]} --all_stocks")
            return
        
        print("🚀 전체 종목 기술분석 실행 스크립트 시작")
        print("=" * 80)
        print(f"📁 데이터베이스 경로: {db_path}")
        
        if args.stock_code:
            # 단일 종목 분석
            all_stocks = runner.stock_manager.get_all_stocks()
            target_stock = None
            for stock in all_stocks:
                if stock['stock_code'] == args.stock_code:
                    target_stock = stock
                    break
            
            if target_stock:
                result = runner.analyze_single_stock(target_stock, save_to_db=True, show_details=True)
                if args.save:
                    with open(args.save, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
                    print(f"💾 결과 저장 완료: {args.save}")
            else:
                print(f"❌ 종목 코드 {args.stock_code}를 찾을 수 없습니다.")
        
        elif args.sample_analysis:
            # 샘플 종목 분석
            all_stocks = runner.stock_manager.get_all_stocks()
            stock_list = all_stocks[:10]
            print(f"📊 샘플 종목 {len(stock_list)}개 분석")
            
            results = runner.analyze_multiple_stocks(stock_list, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.all_stocks:
            # 전체 종목 분석
            all_stocks = runner.stock_manager.get_all_stocks()
            print(f"📊 전체 종목 {len(all_stocks):,}개 분석")
            
            results = runner.analyze_multiple_stocks(all_stocks, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.kospi_only:
            # 코스피 종목만 분석
            kospi_stocks = runner.stock_manager.get_stocks_by_market('KOSPI')
            print(f"📊 코스피 종목 {len(kospi_stocks):,}개 분석")
            
            results = runner.analyze_multiple_stocks(kospi_stocks, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.kosdaq_only:
            # 코스닥 종목만 분석
            kosdaq_stocks = runner.stock_manager.get_stocks_by_market('KOSDAQ')
            print(f"📊 코스닥 종목 {len(kosdaq_stocks):,}개 분석")
            
            results = runner.analyze_multiple_stocks(kosdaq_stocks, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.top_100:
            # 상위 100개 종목 분석
            all_stocks = runner.stock_manager.get_all_stocks()
            stock_list = all_stocks[:100]
            print(f"📊 상위 {len(stock_list)}개 종목 분석")
            
            results = runner.analyze_multiple_stocks(stock_list, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        print(f"\n✨ 기술분석 실행 완료!")
        print(f"📊 결과는 technical_indicators 테이블에 저장되었습니다.")
        
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
