#!/usr/bin/env python3
"""
📈 기술분석 실행 스크립트 - 최종 수정 버전
Value Investment System - Technical Analysis Runner

수정 사항:
- --list_stocks 옵션을 독립적으로 사용 가능
- 지원 종목 목록 확인 기능 개선

실행 방법:
python run_technical_analysis_final.py --list_stocks           # 종목 리스트 확인
python run_technical_analysis_final.py --sample_analysis       # 샘플 분석
python run_technical_analysis_final.py --all_stocks           # 전체 분석
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

# 데이터베이스 설정 import
try:
    from config.database_config import get_db_connection, get_database_path
    print("✅ 데이터베이스 설정 import 성공!")
except ImportError as e:
    print(f"❌ 데이터베이스 설정 import 실패: {e}")
    print("기본 데이터베이스 경로 사용")

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
    
    def get_sample_stocks(self, count: int = 10) -> List[Dict[str, str]]:
        """샘플 종목 조회"""
        all_stocks = self.get_all_stocks()
        return all_stocks[:count]

# 한국 주식 정보 데이터베이스 - 호환성을 위한 기본값 (DB 조회 실패시 사용)
KOREAN_STOCKS_FALLBACK = {
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
}

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
            print(f"🌐 FDR 단순 시도: {standardized_code}")
            # 가장 기본적인 방식만 사용
            df = fdr.DataReader(standardized_code, start=start_date, end=end_date)
            if not df.empty:
                print(f"✅ FDR 단순 성공: {len(df)}일 데이터")
                return df
        except Exception as e:
            print(f"❌ FDR 단순 실패: {str(e)[:100]}...")
        
        return None
    
    def get_stock_data_any_source(self, stock_code: str, period_days: int = 300) -> Optional[pd.DataFrame]:
        """단순화된 데이터 수집"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        print(f"📅 데이터 수집 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
        
        # FDR만 사용 (가장 안정적)
        df = self.get_stock_data_fdr_simple(stock_code, start_date, end_date)
        if df is not None and len(df) >= 20:
            print(f"✅ 데이터 수집 성공")
            print(f"📊 수집된 데이터: {len(df)}일")
            print(f"📅 기간: {df.index[0].strftime('%Y-%m-%d')} ~ {df.index[-1].strftime('%Y-%m-%d')}")
            print(f"💰 최근 종가: {df['Close'].iloc[-1]:,.0f}원")
            
            # 간단한 데이터 검증
            df = self.simple_data_validation(df)
            return df
        
        print(f"❌ 데이터 수집 실패: {stock_code}")
        return None
    
    def simple_data_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """간단한 데이터 검증"""
        print("🔍 데이터 검증 중...")
        
        # 1. 결측값 제거
        initial_length = len(df)
        df = df.dropna()
        if len(df) != initial_length:
            print(f"⚠️ 결측값 제거: {initial_length - len(df)}개 행 제거")
        
        # 2. 음수값 제거
        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numeric_columns:
            if col in df.columns:
                before_count = len(df)
                df = df[df[col] > 0]
                after_count = len(df)
                if before_count != after_count:
                    print(f"⚠️ {col} 음수/0값 제거: {before_count - after_count}개")
        
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
        
        print(f"✅ 데이터 검증 완료: {len(df)}일 데이터")
        print(f"📊 가격 범위: {df['Low'].min():,.0f}원 ~ {df['High'].max():,.0f}원")
        print(f"📊 평균 거래량: {df['Volume'].mean():,.0f}주")
        
        return df
    
    def generate_sample_data(self, stock_code: str, period_days: int) -> pd.DataFrame:
        """간단한 샘플 데이터 생성"""
        dates = pd.date_range(end=datetime.now(), periods=period_days, freq='D')
        
        # 종목 정보
        stock_info = KOREAN_STOCKS.get(stock_code, {
            'name': f'종목{stock_code}',
            'market': 'KOSPI',
            'sector': '기타',
            'price_range': (50000, 100000)
        })
        
        print(f"📊 {stock_info['name']} 샘플 데이터 생성")
        
        # 가격 설정
        min_price, max_price = stock_info['price_range']
        base_price = (min_price + max_price) / 2
        
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
        
        print(f"📊 샘플 데이터 완성: {len(data)}일")
        return data

class TechnicalAnalysisRunner:
    """기술분석 실행기 - 데이터베이스 기반 버전"""
    
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
            # 기존 테이블 확인
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(technical_analysis)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # data_source 컬럼이 없으면 추가
            if 'data_source' not in columns:
                print("🔧 DB 스키마 업데이트: data_source 컬럼 추가")
                try:
                    conn.execute('ALTER TABLE technical_analysis ADD COLUMN data_source TEXT')
                except:
                    pass  # 이미 존재하는 경우
            
            # 테이블 생성
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
            
            print("✅ DB 스키마 확인 완료")
    
    def get_stock_data(self, stock_code: str, period_days: int = 300) -> Tuple[Optional[pd.DataFrame], str]:
        """데이터 가져오기"""
        print(f"\n📊 {stock_code} 데이터 수집 시작...")
        
        # 실제 데이터 시도
        df = self.data_collector.get_stock_data_any_source(stock_code, period_days)
        
        if df is not None and len(df) >= 20:
            return df, "real_data"
        
        # 샘플 데이터 생성
        print(f"\n📊 {stock_code}: 실제 데이터 없음, 샘플 데이터 생성")
        df = self.data_collector.generate_sample_data(stock_code, period_days)
        return df, "sample_data"
    
    def safe_save_analysis_result(self, result: Dict, data_source: str):
        """안전한 DB 저장"""
        if 'error' in result:
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                indicators = result.get('technical_indicators', {})
                
                # 안전한 값 추출
                def safe_get(value):
                    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                        return None
                    return float(value) if isinstance(value, (int, float)) else None
                
                conn.execute('''
                    INSERT OR REPLACE INTO technical_analysis
                    (stock_code, analysis_date, current_price, total_score, 
                     recommendation, risk_level, rsi, macd, bb_position, 
                     adx, sma_20, volume_trend, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', [
                    result['stock_code'],
                    datetime.now().strftime('%Y-%m-%d'),
                    safe_get(result.get('current_price')),
                    safe_get(result.get('overall_score')),
                    result.get('recommendation', 'NEUTRAL'),
                    result.get('risk_level', 'MEDIUM'),
                    safe_get(indicators.get('RSI')),
                    safe_get(indicators.get('MACD')),
                    safe_get(indicators.get('BB_POSITION')),
                    safe_get(indicators.get('ADX')),
                    safe_get(indicators.get('SMA_20')),
                    result.get('analysis_summary', {}).get('volume_trend', 'Normal'),
                    data_source
                ])
                print(f"💾 {result['stock_code']}: DB 저장 완료")
        except Exception as e:
            print(f"⚠️ DB 저장 실패: {e}")
    
    def analyze_single_stock(self, stock_code: str, save_to_db: bool = True) -> Dict:
        """단일 종목 분석"""
        print(f"\n🎯 {stock_code} 기술분석 시작...")
        
        # 종목 정보 출력
        if stock_code in KOREAN_STOCKS:
            info = KOREAN_STOCKS[stock_code]
            print(f"📋 종목명: {info['name']}")
            print(f"📊 시장: {info['market']}")
            print(f"🏭 업종: {info['sector']}")
        
        # 데이터 수집
        ohlcv_data, data_source = self.get_stock_data(stock_code)
        if ohlcv_data is None or len(ohlcv_data) < 20:
            return {'error': f'{stock_code}: 충분한 데이터가 없습니다.'}
        
        # 기술분석 실행
        try:
            result = self.analyzer.analyze_stock(stock_code, ohlcv_data)
        except Exception as e:
            print(f"❌ 기술분석 실행 오류: {e}")
            return {'error': f'{stock_code}: 기술분석 실행 실패 - {str(e)}'}
        
        # 결과 처리
        if 'error' not in result:
            result['data_source'] = data_source
            result['data_info'] = {
                'source': data_source,
                'period_days': len(ohlcv_data),
                'start_date': ohlcv_data.index[0].strftime('%Y-%m-%d'),
                'end_date': ohlcv_data.index[-1].strftime('%Y-%m-%d')
            }
            
            print_analysis_summary(result)
            
            # 소스 정보 출력
            source_emoji = "🌐" if data_source == "real_data" else "🎲"
            source_name = "실제 데이터" if data_source == "real_data" else "샘플 데이터"
            print(f"\n{source_emoji} 데이터 소스: {source_name}")
            print(f"📅 분석 기간: {result['data_info']['start_date']} ~ {result['data_info']['end_date']}")
            print(f"📊 데이터 일수: {result['data_info']['period_days']}일")
            
            # 계산된 지표 정보
            indicators = result.get('technical_indicators', {})
            calculated_indicators = [key for key, value in indicators.items() 
                                   if value is not None and not (isinstance(value, float) and np.isnan(value))]
            print(f"📈 계산된 지표: {len(calculated_indicators)}개")
            if calculated_indicators:
                print(f"   ✅ {', '.join(calculated_indicators[:5])}{'...' if len(calculated_indicators) > 5 else ''}")
            
            # DB 저장
            if save_to_db:
                self.safe_save_analysis_result(result, data_source)
        
        return result
    
    def analyze_multiple_stocks(self, stock_codes: List[str], delay_seconds: float = 0.5) -> Dict[str, Dict]:
        """다중 종목 분석"""
        results = {}
        total_stocks = len(stock_codes)
        
        print(f"\n🔄 {total_stocks}개 종목 일괄 분석 시작...")
        print("=" * 60)
        
        for i, stock_code in enumerate(stock_codes, 1):
            print(f"\n[{i}/{total_stocks}] {stock_code} 분석 중...")
            
            try:
                result = self.analyze_single_stock(stock_code, save_to_db=True)
                results[stock_code] = result
            except Exception as e:
                print(f"❌ {stock_code} 분석 실패: {e}")
                results[stock_code] = {'error': str(e)}
            
            # API 제한을 위한 딜레이
            if i < total_stocks:
                time.sleep(delay_seconds)
        
        # 요약 통계
        self.print_summary_statistics(results)
        
        return results
    
    def print_summary_statistics(self, results: Dict[str, Dict]):
        """결과 요약 통계"""
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
            
            # 상위 추천 종목
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
                    source_emoji = "🌐" if result.get('data_source') == "real_data" else "🎲"
                    print(f"  {i}. {name}({stock_code}): {rec} (점수: {score:.1f}, 가격: {price:,.0f}원) {source_emoji}")
        
        if failed_results:
            print(f"\n❌ 분석 실패 종목:")
            for stock_code, result in results.items():
                if 'error' in result:
                    name = KOREAN_STOCKS.get(stock_code, {}).get('name', stock_code)
                    print(f"  • {name}({stock_code}): {result['error']}")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description='📈 기술분석 실행 스크립트 (최종 수정 버전)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
사용 예시:
  %(prog)s --list_stocks                          # 지원 종목 리스트 확인
  %(prog)s --stock_code 005930                    # 삼성전자 분석
  %(prog)s --multiple 005930,000660,035420        # 여러 종목 분석
  %(prog)s --sample_analysis                      # 샘플 종목 분석
  %(prog)s --all_stocks                           # 전체 지원 종목 분석
  %(prog)s --kospi_top10                          # KOSPI 상위 10개
  %(prog)s --kosdaq_top10                         # KOSDAQ 상위 10개

특징:
  ✅ 안정화된 데이터 수집
  ✅ 강화된 오류 처리  
  ✅ 자동 DB 스키마 업데이트
  ✅ 실시간 진행 상황 표시
  ✅ 상세한 분석 결과

지원 종목: KOSPI/KOSDAQ 주요 30개 종목
        '''
    )
    
    # list_stocks는 별도 옵션으로 처리
    parser.add_argument('--list_stocks', action='store_true', help='지원 종목 리스트 출력')
    
    # 실행 모드 (list_stocks와 독립적)
    group = parser.add_mutually_exclusive_group(required=not any('--list_stocks' in arg for arg in sys.argv))
    group.add_argument('--stock_code', type=str, help='단일 종목 분석 (예: 005930)')
    group.add_argument('--multiple', type=str, help='복수 종목 분석 (쉼표로 구분)')
    group.add_argument('--sample_analysis', action='store_true', help='샘플 종목 10개 분석')
    group.add_argument('--all_stocks', action='store_true', help='전체 지원 종목 분석')
    group.add_argument('--kospi_top10', action='store_true', help='KOSPI 상위 10개 분석')
    group.add_argument('--kosdaq_top10', action='store_true', help='KOSDAQ 상위 10개 분석')
    
    # 옵션
    parser.add_argument('--save', type=str, help='결과를 JSON 파일로 저장')
    parser.add_argument('--delay', type=float, default=0.5, help='종목간 딜레이 (초)')
    parser.add_argument('--no_db', action='store_true', help='DB 저장 안 함')
    
    args = parser.parse_args()
    
    # 종목 리스트 출력 (독립적 처리)
    if args.list_stocks:
        print("📋 지원 종목 리스트:")
        print("=" * 80)
        
        kospi_stocks = [k for k, v in KOREAN_STOCKS.items() if v['market'] == 'KOSPI']
        kosdaq_stocks = [k for k, v in KOREAN_STOCKS.items() if v['market'] == 'KOSDAQ']
        
        print(f"\n🏛️ KOSPI ({len(kospi_stocks)}개):")
        for code in kospi_stocks:
            info = KOREAN_STOCKS[code]
            print(f"  {code}: {info['name']:<15} ({info['sector']})")
        
        print(f"\n🏢 KOSDAQ ({len(kosdaq_stocks)}개):")
        for code in kosdaq_stocks:
            info = KOREAN_STOCKS[code]
            print(f"  {code}: {info['name']:<15} ({info['sector']})")
        
        print(f"\n📊 총 {len(KOREAN_STOCKS)}개 종목 지원")
        print("\n💡 사용 예시:")
        print("  python run_technical_analysis_final.py --sample_analysis")
        print("  python run_technical_analysis_final.py --stock_code 005930")
        print("  python run_technical_analysis_final.py --all_stocks --save results.json")
        return
    
    print("🚀 기술분석 실행 스크립트 시작 (최종 수정 버전)")
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
            sample_stocks = runner.stock_manager.get_sample_stocks(10)
            if sample_stocks:
                stock_codes = [stock['stock_code'] for stock in sample_stocks]
                print(f"📊 샘플 종목 {len(stock_codes)}개 분석")
                results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            else:
                print("❌ DB에서 종목을 찾을 수 없습니다. 기본 샘플 사용")
                stock_codes = list(KOREAN_STOCKS_FALLBACK.keys())[:10]
                results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.all_stocks:
            # 전체 종목 분석
            all_stocks = runner.stock_manager.get_all_stocks()
            if all_stocks:
                stock_codes = [stock['stock_code'] for stock in all_stocks]
                print(f"📊 전체 종목 {len(stock_codes)}개 분석")
                results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            else:
                print("❌ DB에서 종목을 찾을 수 없습니다. 기본 샘플 사용")
                stock_codes = list(KOREAN_STOCKS_FALLBACK.keys())
                results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.kospi_top10:
            # KOSPI 상위 10개
            kospi_stocks = runner.stock_manager.get_stocks_by_market('KOSPI')[:10]
            if kospi_stocks:
                stock_codes = [stock['stock_code'] for stock in kospi_stocks]
                print(f"📊 KOSPI 상위 {len(stock_codes)}개 분석")
                results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            else:
                print("❌ KOSPI 종목을 찾을 수 없습니다. 기본 샘플 사용")
                kospi_stocks = [k for k, v in KOREAN_STOCKS_FALLBACK.items() if v['market'] == 'KOSPI'][:10]
                results = runner.analyze_multiple_stocks(kospi_stocks, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.kosdaq_top10:
            # KOSDAQ 상위 10개
            kosdaq_stocks = runner.stock_manager.get_stocks_by_market('KOSDAQ')[:10]
            if kosdaq_stocks:
                stock_codes = [stock['stock_code'] for stock in kosdaq_stocks]
                print(f"📊 KOSDAQ 상위 {len(stock_codes)}개 분석")
                results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            else:
                print("❌ KOSDAQ 종목을 찾을 수 없습니다. 기본 샘플 사용")
                # KOSDAQ 종목이 없으면 전체에서 랜덤 선택
                all_stocks = runner.stock_manager.get_all_stocks()
                if all_stocks:
                    kosdaq_stocks = [s for s in all_stocks if s.get('market_type') == 'KOSDAQ'][:10]
                    if kosdaq_stocks:
                        stock_codes = [stock['stock_code'] for stock in kosdaq_stocks]
                        results = runner.analyze_multiple_stocks(stock_codes, args.delay)
                    else:
                        # 최후 수단: 샘플 사용
                        stock_codes = list(KOREAN_STOCKS_FALLBACK.keys())[:10]
                        results = runner.analyze_multiple_stocks(stock_codes, args.delay)
                else:
                    stock_codes = list(KOREAN_STOCKS_FALLBACK.keys())[:10]
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
