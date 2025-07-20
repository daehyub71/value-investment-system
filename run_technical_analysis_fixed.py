#!/usr/bin/env python3
"""
📈 기술분석 실행 스크립트 - 한국 주식 지원 버전
Value Investment System - Technical Analysis Runner

실행 방법:
python run_technical_analysis_fixed.py --stock_code 005930
python run_technical_analysis_fixed.py --all_kospi
python run_technical_analysis_fixed.py --help

한국 주식 데이터 지원:
- KOSPI: 종목코드.KS (예: 005930.KS)
- KOSDAQ: 종목코드.KQ (예: 035420.KQ)
- 자동 접미사 처리
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

# 데이터 수집용 import
try:
    import FinanceDataReader as fdr
    DATA_SOURCE_AVAILABLE = True
    print("✅ FinanceDataReader 사용 가능")
except ImportError:
    DATA_SOURCE_AVAILABLE = False
    print("⚠️ FinanceDataReader 사용 불가 - 샘플 데이터로 테스트")

# 한국 주식 시장 구분 맵핑
KOREAN_STOCK_MARKETS = {
    # KOSPI 대표 종목들
    '005930': 'KS',  # 삼성전자
    '000660': 'KS',  # SK하이닉스
    '373220': 'KS',  # LG에너지솔루션
    '207940': 'KS',  # 삼성바이오로직스
    '005380': 'KS',  # 현대차
    '006400': 'KS',  # 삼성SDI
    '051910': 'KS',  # LG화학
    '028260': 'KS',  # 삼성물산
    '068270': 'KS',  # 셀트리온
    '035720': 'KS',  # 카카오
    '105560': 'KS',  # KB금융
    '055550': 'KS',  # 신한지주
    '035420': 'KS',  # NAVER
    '012330': 'KS',  # 현대모비스
    '003670': 'KS',  # 포스코홀딩스
    '066570': 'KS',  # LG전자
    '096770': 'KS',  # SK이노베이션
    '003550': 'KS',  # LG
    '034730': 'KS',  # SK
    '015760': 'KS',  # 한국전력
    
    # KOSDAQ 대표 종목들
    '247540': 'KQ',  # 에코프로비엠
    '086520': 'KQ',  # 에코프로
    '091990': 'KQ',  # 셀트리온헬스케어
    '196170': 'KQ',  # 알테오젠
    '039030': 'KQ',  # 이오테크닉스
    '357780': 'KQ',  # 솔브레인
    '121600': 'KQ',  # 나노신소재
    '058470': 'KQ',  # 리노공업
    '112040': 'KQ',  # 위메이드
    '293490': 'KQ',  # 카카오게임즈
}

class TechnicalAnalysisRunner:
    """기술분석 실행기 - 한국 주식 지원"""
    
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
    
    def get_market_suffix(self, stock_code: str) -> str:
        """종목코드에 따른 시장 접미사 반환"""
        # 이미 접미사가 있는 경우
        if '.' in stock_code:
            return stock_code
        
        # 미리 정의된 맵핑에서 찾기
        if stock_code in KOREAN_STOCK_MARKETS:
            suffix = KOREAN_STOCK_MARKETS[stock_code]
            return f"{stock_code}.{suffix}"
        
        # 종목코드 규칙으로 추정
        # 보통 KOSPI는 6자리, KOSDAQ는 6자리
        # 하지만 확실한 구분이 어려우므로 기본값은 KS (KOSPI)
        return f"{stock_code}.KS"
    
    def get_stock_data(self, stock_code: str, period_days: int = 300) -> Optional[pd.DataFrame]:
        """주가 데이터 가져오기 - 한국 주식 지원"""
        if not DATA_SOURCE_AVAILABLE:
            return self.generate_sample_data(stock_code, period_days)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        # 시장 접미사 처리
        stock_symbol = self.get_market_suffix(stock_code)
        
        try:
            print(f"🌐 {stock_code} ({stock_symbol}): API에서 데이터 다운로드 중...")
            
            # 먼저 접미사가 있는 형태로 시도
            try:
                df = fdr.DataReader(stock_symbol, start_date, end_date)
                if not df.empty:
                    print(f"✅ {stock_code}: {len(df)}일 데이터 다운로드 완료 (시장: {stock_symbol.split('.')[-1]})")
                    return df
            except:
                pass
            
            # 접미사 없이 시도 (일부 데이터 소스의 경우)
            try:
                df = fdr.DataReader(stock_code, start_date, end_date)
                if not df.empty:
                    print(f"✅ {stock_code}: {len(df)}일 데이터 다운로드 완료")
                    return df
            except:
                pass
            
            # KOSDAQ로 재시도
            if not stock_symbol.endswith('.KQ'):
                kosdaq_symbol = f"{stock_code}.KQ"
                try:
                    df = fdr.DataReader(kosdaq_symbol, start_date, end_date)
                    if not df.empty:
                        print(f"✅ {stock_code}: {len(df)}일 데이터 다운로드 완료 (KOSDAQ)")
                        return df
                except:
                    pass
            
            print(f"❌ {stock_code}: API에서 데이터를 찾을 수 없습니다.")
            print(f"📊 {stock_code}: 샘플 데이터로 테스트합니다.")
            return self.generate_sample_data(stock_code, period_days)
            
        except Exception as e:
            print(f"❌ {stock_code}: 데이터 다운로드 실패 - {e}")
            print(f"📊 {stock_code}: 샘플 데이터로 테스트합니다.")
            return self.generate_sample_data(stock_code, period_days)
    
    def generate_sample_data(self, stock_code: str, period_days: int) -> pd.DataFrame:
        """샘플 데이터 생성 (실제 데이터를 가져올 수 없을 때)"""
        dates = pd.date_range(end=datetime.now(), periods=period_days, freq='D')
        np.random.seed(hash(stock_code) % 2**32)  # 종목별로 일관된 데이터
        
        # 종목별 기본 가격 설정 (실제와 유사한 가격대)
        stock_prices = {
            '005930': 70000,   # 삼성전자
            '000660': 120000,  # SK하이닉스
            '035420': 200000,  # NAVER
            '005380': 180000,  # 현대차
            '051910': 400000,  # LG화학
            '373220': 450000,  # LG에너지솔루션
            '207940': 800000,  # 삼성바이오로직스
            '006400': 300000,  # 삼성SDI
            '028260': 120000,  # 삼성물산
            '068270': 180000,  # 셀트리온
            '035720': 80000,   # 카카오
            '105560': 50000,   # KB금융
            '055550': 40000,   # 신한지주
            '012330': 250000,  # 현대모비스
            '003670': 400000,  # 포스코홀딩스
            '066570': 120000,  # LG전자
        }
        
        base_price = stock_prices.get(stock_code, 50000)
        
        # 현실적인 주가 패턴 생성
        # 추세 (장기), 사이클 (중기), 노이즈 (단기)
        trend = np.linspace(0, np.random.uniform(-0.1, 0.15), period_days)  # -10% ~ +15% 추세
        cycle = 0.05 * np.sin(np.linspace(0, 4*np.pi, period_days))  # 사이클 패턴
        noise = np.random.normal(0, 0.02, period_days)  # 일일 변동성
        
        returns = trend + cycle + noise
        
        prices = [base_price]
        for ret in returns[1:]:
            new_price = prices[-1] * (1 + ret)
            # 극단적인 변동 제한
            new_price = max(new_price, base_price * 0.5)  # 50% 이상 하락 방지
            new_price = min(new_price, base_price * 2.0)   # 200% 이상 상승 방지
            prices.append(new_price)
        
        # OHLC 데이터 생성 (현실적인 패턴)
        data = pd.DataFrame({
            'Open': [p * np.random.uniform(0.995, 1.005) for p in prices],
            'High': [p * np.random.uniform(1.002, 1.025) for p in prices],
            'Low': [p * np.random.uniform(0.975, 0.998) for p in prices],
            'Close': prices,
            'Volume': np.random.lognormal(13 + np.random.uniform(-1, 1), 0.5, period_days).astype(int)
        }, index=dates)
        
        # High/Low 보정 (Open, Close를 포함한 범위)
        for i in range(len(data)):
            high = max(data.iloc[i]['Open'], data.iloc[i]['Close'], data.iloc[i]['High'])
            low = min(data.iloc[i]['Open'], data.iloc[i]['Close'], data.iloc[i]['Low'])
            data.iloc[i, data.columns.get_loc('High')] = high
            data.iloc[i, data.columns.get_loc('Low')] = low
        
        print(f"📊 {stock_code}: 샘플 데이터 생성 완료 ({len(data)}일)")
        print(f"   기간: {data.index[0].strftime('%Y-%m-%d')} ~ {data.index[-1].strftime('%Y-%m-%d')}")
        print(f"   시작가: {data['Close'].iloc[0]:,.0f}원")
        print(f"   현재가: {data['Close'].iloc[-1]:,.0f}원")
        print(f"   수익률: {((data['Close'].iloc[-1] / data['Close'].iloc[0]) - 1) * 100:+.1f}%")
        
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
                print(f"💾 {result['stock_code']}: DB 저장 완료")
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
        
        # 기본 주요 종목 (KOSPI 대표 종목들)
        kospi_stocks = [
            '005930',  # 삼성전자
            '000660',  # SK하이닉스
            '373220',  # LG에너지솔루션
            '207940',  # 삼성바이오로직스
            '005380',  # 현대차
            '006400',  # 삼성SDI
            '051910',  # LG화학
            '035420',  # NAVER
            '028260',  # 삼성물산
            '068270',  # 셀트리온
            '035720',  # 카카오
            '105560',  # KB금융
            '055550',  # 신한지주
            '012330',  # 현대모비스
            '003670',  # 포스코홀딩스
            '066570',  # LG전자
            '096770',  # SK이노베이션
            '003550',  # LG
            '034730',  # SK
            '015760',  # 한국전력
        ]
        
        return kospi_stocks[:top_n]
    
    def get_kosdaq_stocks(self, top_n: int = 20) -> List[str]:
        """KOSDAQ 주요 종목 리스트"""
        if DATA_SOURCE_AVAILABLE:
            try:
                kosdaq_stocks = fdr.StockListing('KOSDAQ')
                # 시가총액 상위 N개 종목
                top_stocks = kosdaq_stocks.nlargest(top_n, 'Marcap')['Code'].tolist()
                return top_stocks
            except Exception as e:
                print(f"❌ KOSDAQ 종목 리스트 가져오기 실패: {e}")
        
        # 기본 주요 종목 (KOSDAQ 대표 종목들)
        kosdaq_stocks = [
            '247540',  # 에코프로비엠
            '086520',  # 에코프로
            '091990',  # 셀트리온헬스케어
            '196170',  # 알테오젠
            '039030',  # 이오테크닉스
            '357780',  # 솔브레인
            '121600',  # 나노신소재
            '058470',  # 리노공업
            '112040',  # 위메이드
            '293490',  # 카카오게임즈
        ]
        
        return kosdaq_stocks[:top_n]
    
    def get_watchlist_stocks(self) -> List[str]:
        """관심종목 리스트"""
        return [
            '005930',  # 삼성전자
            '000660',  # SK하이닉스
            '035420',  # NAVER
            '005380',  # 현대차
            '051910',  # LG화학
            '373220',  # LG에너지솔루션
            '035720',  # 카카오
            '066570',  # LG전자
            '003550',  # LG
            '247540'   # 에코프로비엠
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
                    price = result.get('current_price', 0)
                    print(f"  {i}. {stock_code}: {rec} (점수: {score:.1f}, 가격: {price:,.0f}원)")
            
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
                    price = result.get('current_price', 0)
                    print(f"  {i}. {stock_code}: {rec} (점수: {score:.1f}, 가격: {price:,.0f}원)")
        
        if failed_results:
            print(f"\n❌ 분석 실패 종목:")
            for stock_code, result in results.items():
                if 'error' in result:
                    print(f"  • {stock_code}: {result['error']}")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description='📈 기술분석 실행 스크립트 (한국 주식 지원)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
사용 예시:
  %(prog)s --stock_code 005930                    # 삼성전자 분석
  %(prog)s --stock_code 035420                    # NAVER 분석
  %(prog)s --stock_code 005930 --save result.json # 결과를 파일로 저장
  %(prog)s --all_kospi --top 20                   # KOSPI 상위 20개 종목 분석
  %(prog)s --all_kosdaq --top 10                  # KOSDAQ 상위 10개 종목 분석
  %(prog)s --watchlist                            # 관심종목 분석
  %(prog)s --multiple 005930,000660,035420        # 여러 종목 분석

지원 시장:
  - KOSPI: 자동으로 .KS 접미사 추가
  - KOSDAQ: 자동으로 .KQ 접미사 추가
  - 데이터 없을 시 샘플 데이터로 테스트
        '''
    )
    
    # 실행 모드
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--stock_code', type=str, help='단일 종목 분석 (예: 005930)')
    group.add_argument('--all_kospi', action='store_true', help='KOSPI 전체 분석')
    group.add_argument('--all_kosdaq', action='store_true', help='KOSDAQ 전체 분석')
    group.add_argument('--watchlist', action='store_true', help='관심종목 분석')
    group.add_argument('--multiple', type=str, help='복수 종목 분석 (쉼표로 구분, 예: 005930,000660)')
    
    # 옵션
    parser.add_argument('--top', type=int, default=50, help='분석할 상위 종목 수 (기본값: 50)')
    parser.add_argument('--save', type=str, help='결과를 JSON 파일로 저장')
    parser.add_argument('--delay', type=float, default=1.0, help='API 호출 간격 (초, 기본값: 1.0)')
    parser.add_argument('--no_db', action='store_true', help='DB 저장 안 함')
    
    args = parser.parse_args()
    
    print("🚀 기술분석 실행 스크립트 시작 (한국 주식 지원)")
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
        
        elif args.all_kosdaq:
            # KOSDAQ 전체 분석
            stock_codes = runner.get_kosdaq_stocks(args.top)
            print(f"📊 KOSDAQ 상위 {len(stock_codes)}개 종목 분석")
            
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
                    json.dump(results, f, ensure_ascii_False, indent=2, default=str)
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
