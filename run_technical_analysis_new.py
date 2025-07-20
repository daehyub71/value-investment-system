#!/usr/bin/env python3
"""
📈 기술분석 실행 스크립트 - 업데이트 버전
Value Investment System - Technical Analysis Runner

실행 방법:
python run_technical_analysis_new.py --stock_code 005930
python run_technical_analysis_new.py --all_kospi
python run_technical_analysis_new.py --help

주요 기능:
1. 개별 종목 기술분석
2. KOSPI/KOSDAQ 전체 분석
3. 관심종목 리스트 분석
4. 결과를 JSON으로 저장
5. 실시간 분석 결과 출력
"""

import sys
import os
import argparse
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
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

# 데이터 수집용 import (기본적인 것들)
try:
    import FinanceDataReader as fdr
    DATA_SOURCE_AVAILABLE = True
    print("✅ FinanceDataReader 사용 가능")
except ImportError:
    DATA_SOURCE_AVAILABLE = False
    print("⚠️ FinanceDataReader 사용 불가 - 샘플 데이터로 테스트")

class TechnicalAnalysisRunner:
    """기술분석 실행기"""
    
    def __init__(self, db_path: str = "data/databases/stock_data.db"):
        self.db_path = db_path
        self.analyzer = TechnicalAnalyzer()
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
        """주가 데이터 가져오기"""
        if not DATA_SOURCE_AVAILABLE:
            return self.generate_sample_data(stock_code, period_days)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        try:
            print(f"🌐 {stock_code}: API에서 데이터 다운로드 중...")
            df = fdr.DataReader(stock_code, start_date, end_date)
            
            if df.empty:
                print(f"❌ {stock_code}: 데이터를 찾을 수 없습니다.")
                return self.generate_sample_data(stock_code, period_days)
            
            print(f"✅ {stock_code}: {len(df)}일 데이터 다운로드 완료")
            return df
            
        except Exception as e:
            print(f"❌ {stock_code}: 데이터 다운로드 실패 - {e}")
            print(f"📊 {stock_code}: 샘플 데이터로 테스트합니다.")
            return self.generate_sample_data(stock_code, period_days)
    
    def generate_sample_data(self, stock_code: str, period_days: int) -> pd.DataFrame:
        """샘플 데이터 생성 (실제 데이터를 가져올 수 없을 때)"""
        dates = pd.date_range(end=datetime.now(), periods=period_days, freq='D')
        np.random.seed(hash(stock_code) % 2**32)  # 종목별로 일관된 데이터
        
        # 종목별 기본 가격 설정
        stock_prices = {
            '005930': 70000,  # 삼성전자
            '000660': 120000, # SK하이닉스
            '035420': 200000, # NAVER
            '005380': 180000, # 현대차
            '051910': 400000, # LG화학
        }
        
        base_price = stock_prices.get(stock_code, 50000)
        
        # 추세 + 노이즈 패턴 생성
        trend = np.linspace(0, 0.05, period_days)  # 5% 상승 추세
        noise = np.random.normal(0, 0.02, period_days)  # 2% 노이즈
        returns = trend + noise
        
        prices = [base_price]
        for ret in returns[1:]:
            new_price = prices[-1] * (1 + ret)
            prices.append(max(new_price, base_price * 0.6))  # 40% 하락 제한
        
        # OHLC 데이터 생성
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
    
    def save_analysis_result(self, result: Dict):
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
                     adx, sma_20, volume_trend)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    result['analysis_summary'].get('volume_trend', 'Normal')
                ])
        except Exception as e:
            print(f"⚠️ DB 저장 실패: {e}")
    
    def analyze_single_stock(self, stock_code: str, save_to_db: bool = True) -> Dict:
        """개별 종목 분석"""
        print(f"\n📊 {stock_code} 기술분석 시작...")
        
        # 주가 데이터 가져오기
        ohlcv_data = self.get_stock_data(stock_code)
        if ohlcv_data is None or len(ohlcv_data) < 50:
            return {'error': f'{stock_code}: 충분한 데이터가 없습니다.'}
        
        # 기술분석 실행
        result = self.analyzer.analyze_stock(stock_code, ohlcv_data)
        
        # 결과 출력
        if 'error' not in result:
            print_analysis_summary(result)
            
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
        if DATA_SOURCE_AVAILABLE:
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
        """관심종목 리스트"""
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
    
    def print_summary_statistics(self, results: Dict[str, Dict]):
        """결과 요약 통계 출력"""
        successful_results = [r for r in results.values() if 'error' not in r]
        failed_results = [r for r in results.values() if 'error' in r]
        
        print(f"\n📊 분석 결과 요약")
        print("=" * 50)
        print(f"✅ 성공: {len(successful_results)}개")
        print(f"❌ 실패: {len(failed_results)}개")
        
        if successful_results:
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
                    print(f"  {i}. {stock_code}: {rec} (점수: {score:.1f})")
            
            # 하위 매도 주의
            sell_recommendations = [r for r in successful_results 
                                  if r.get('recommendation') in ['STRONG_SELL', 'SELL']]
            
            if sell_recommendations:
                sell_recommendations.sort(key=lambda x: x.get('overall_score', 0))
                print(f"\n🔴 매도 주의:")
                for i, result in enumerate(sell_recommendations[:3], 1):
                    stock_code = result['stock_code']
                    score = result.get('overall_score', 0)
                    rec = result.get('recommendation', '')
                    print(f"  {i}. {stock_code}: {rec} (점수: {score:.1f})")
        
        if failed_results:
            print(f"\n❌ 분석 실패 종목:")
            for result in failed_results:
                if 'stock_code' in result:
                    print(f"  • {result['stock_code']}: {result['error']}")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description='📈 기술분석 실행 스크립트',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
사용 예시:
  %(prog)s --stock_code 005930                    # 삼성전자 분석
  %(prog)s --stock_code 005930 --save result.json # 결과를 파일로 저장
  %(prog)s --all_kospi --top 20                   # KOSPI 상위 20개 종목 분석
  %(prog)s --watchlist                            # 관심종목 분석
  %(prog)s --multiple 005930,000660,035420        # 여러 종목 분석
        '''
    )
    
    # 실행 모드
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--stock_code', type=str, help='단일 종목 분석 (예: 005930)')
    group.add_argument('--all_kospi', action='store_true', help='KOSPI 전체 분석')
    group.add_argument('--watchlist', action='store_true', help='관심종목 분석')
    group.add_argument('--multiple', type=str, help='복수 종목 분석 (쉼표로 구분, 예: 005930,000660)')
    
    # 옵션
    parser.add_argument('--top', type=int, default=50, help='분석할 상위 종목 수 (기본값: 50)')
    parser.add_argument('--save', type=str, help='결과를 JSON 파일로 저장')
    parser.add_argument('--delay', type=float, default=1.0, help='API 호출 간격 (초, 기본값: 1.0)')
    parser.add_argument('--no_db', action='store_true', help='DB 저장 안 함')
    
    args = parser.parse_args()
    
    print("🚀 기술분석 실행 스크립트 시작")
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
        
        elif args.all_kospi:
            # KOSPI 전체 분석
            stock_codes = runner.get_kospi_stocks(args.top)
            print(f"📊 KOSPI 상위 {len(stock_codes)}개 종목 분석")
            
            results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 결과 저장 완료: {args.save}")
        
        elif args.watchlist:
            # 관심종목 분석
            stock_codes = runner.get_watchlist_stocks()
            print(f"⭐ 관심종목 {len(stock_codes)}개 분석")
            
            results = runner.analyze_multiple_stocks(stock_codes, args.delay)
            
            if args.save:
                with open(args.save, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2, default=str)
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
