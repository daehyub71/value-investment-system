#!/usr/bin/env python3
"""
📈 기술분석 실행 스크립트
Value Investment System - Technical Analysis Runner

실행 방법:
python run_technical_analysis.py --stock_code=005930
python run_technical_analysis.py --all_kospi
python run_technical_analysis.py --watchlist

주요 기능:
1. 개별 종목 기술분석
2. KOSPI/KOSDAQ 전체 분석
3. 관심종목 리스트 분석
4. 결과를 DB에 저장
5. 실시간 업데이트
"""

import sys
import os
import argparse
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import FinanceDataReader as fdr
import time

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 기술분석 모듈 import (방금 생성한 모듈)
try:
    from technical_analysis_module import TechnicalAnalysisEngine
    from talib_setup_config import TALibSetup
except ImportError as e:
    print(f"❌ 모듈 import 실패: {e}")
    print("먼저 TA-Lib 설정을 완료하세요: python talib_setup_config.py")
    sys.exit(1)

class TechnicalAnalysisRunner:
    """기술분석 실행기"""
    
    def __init__(self, db_path: str = "data/databases/stock_data.db"):
        self.db_path = db_path
        self.engine = TechnicalAnalysisEngine()
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
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(stock_code, analysis_date)
                )
            ''')
            
            # 주가 데이터 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(stock_code, date)
                )
            ''')
    
    def get_stock_data(self, stock_code: str, period_days: int = 300) -> Optional[pd.DataFrame]:
        """주가 데이터 가져오기 (DB 우선, 없으면 API에서)"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        # 1. DB에서 데이터 확인
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT date, open, high, low, close, volume
                FROM daily_prices 
                WHERE stock_code = ? AND date >= ?
                ORDER BY date
            '''
            df_db = pd.read_sql_query(
                query, 
                conn, 
                params=[stock_code, start_date.strftime('%Y-%m-%d')]
            )
        
        # 2. DB 데이터가 충분한지 확인
        if len(df_db) >= 200:  # 충분한 데이터가 있으면 DB 사용
            df_db['date'] = pd.to_datetime(df_db['date'])
            df_db.set_index('date', inplace=True)
            df_db.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            print(f"📊 {stock_code}: DB에서 {len(df_db)}일 데이터 로드")
            return df_db
        
        # 3. API에서 데이터 가져오기
        try:
            print(f"🌐 {stock_code}: API에서 데이터 다운로드 중...")
            df_api = fdr.DataReader(stock_code, start_date, end_date)
            
            if df_api.empty:
                print(f"❌ {stock_code}: 데이터를 찾을 수 없습니다.")
                return None
            
            # 4. API 데이터를 DB에 저장
            self.save_stock_data_to_db(stock_code, df_api)
            
            print(f"✅ {stock_code}: {len(df_api)}일 데이터 다운로드 완료")
            return df_api
            
        except Exception as e:
            print(f"❌ {stock_code}: 데이터 다운로드 실패 - {e}")
            return None
    
    def save_stock_data_to_db(self, stock_code: str, df: pd.DataFrame):
        """주가 데이터를 DB에 저장"""
        with sqlite3.connect(self.db_path) as conn:
            for date, row in df.iterrows():
                try:
                    conn.execute('''
                        INSERT OR REPLACE INTO daily_prices 
                        (stock_code, date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', [
                        stock_code,
                        date.strftime('%Y-%m-%d'),
                        float(row['Open']),
                        float(row['High']),
                        float(row['Low']),
                        float(row['Close']),
                        int(row['Volume']) if not pd.isna(row['Volume']) else 0
                    ])
                except Exception as e:
                    continue  # 오류가 있는 행은 건너뛰기
    
    def save_analysis_result(self, result: Dict):
        """분석 결과를 DB에 저장"""
        if 'error' in result:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            indicators = result['technical_indicators']
            signals = result['trading_signals']
            
            conn.execute('''
                INSERT OR REPLACE INTO technical_analysis
                (stock_code, analysis_date, current_price, total_score, 
                 recommendation, risk_level, rsi, macd, bb_position, 
                 adx, sma_20, volume_trend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                result['stock_code'],
                datetime.now().strftime('%Y-%m-%d'),
                result['current_price'],
                signals['total_score'],
                signals['recommendation'],
                signals['risk_level'],
                indicators['momentum']['RSI'],
                indicators['momentum']['MACD'],
                indicators['volatility']['BB_POSITION'],
                indicators['trend']['ADX'],
                indicators['trend']['SMA_20'],
                'Normal'  # 거래량 트렌드는 추후 구현
            ])
    
    def analyze_single_stock(self, stock_code: str, save_to_db: bool = True) -> Dict:
        """개별 종목 분석"""
        print(f"\n📊 {stock_code} 기술분석 시작...")
        
        # 주가 데이터 가져오기
        ohlcv_data = self.get_stock_data(stock_code)
        if ohlcv_data is None or len(ohlcv_data) < 50:
            return {'error': f'{stock_code}: 충분한 데이터가 없습니다.'}
        
        # 기술분석 실행
        result = self.engine.analyze_stock(ohlcv_data, stock_code)
        
        # 결과 출력
        if 'error' not in result:
            self.print_analysis_result(result)
            
            # DB 저장
            if save_to_db:
                self.save_analysis_result(result)
                print(f"💾 {stock_code}: 분석 결과 저장 완료")
        
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
    
    def get_kospi_stocks(self, top_n: int = 50) -> List[str]:
        """KOSPI 주요 종목 리스트"""
        try:
            kospi_stocks = fdr.StockListing('KOSPI')
            # 시가총액 상위 N개 종목
            top_stocks = kospi_stocks.nlargest(top_n, 'Marcap')['Code'].tolist()
            return top_stocks
        except Exception as e:
            print(f"❌ KOSPI 종목 리스트 가져오기 실패: {e}")
            # 기본 주요 종목
            return [
                '005930',  # 삼성전자
                '000660',  # SK하이닉스
                '373220',  # LG에너지솔루션
                '207940',  # 삼성바이오로직스
                '005380',  # 현대차
                '006400',  # 삼성SDI
                '051910',  # LG화학
                '035420',  # NAVER
                '028260',  # 삼성물산
                '068270'   # 셀트리온
            ]
    
    def get_watchlist_stocks(self) -> List[str]:
        """관심종목 리스트 (설정 파일에서 읽기)"""
        # 실제로는 설정 파일이나 DB에서 읽어옴
        return [
            '005930',  # 삼성전자
            '000660',  # SK하이닉스
            '035420',  # NAVER
            '005380',  # 현대차
            '051910',  # LG화학
            '028260',  # 삼성물산
            '066570',  # LG전자
            '003550',  # LG
            '096770',  # SK이노베이션
            '034730'   # SK
        ]
    
    def print_analysis