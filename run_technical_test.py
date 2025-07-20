#!/usr/bin/env python3
"""
📈 기술분석 실행 스크립트 - 완전 호환 버전
Value Investment System - Technical Analysis Runner

실행 방법:
python run_technical_test.py --stock_code 005930
python run_technical_test.py --multiple 005930,000660,035420
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

print(f"🔗 프로젝트 루트: {project_root}")
print(f"🔗 Python 경로: {sys.path[0]}")

# 기술분석 모듈 직접 import
try:
    sys.path.insert(0, str(project_root / "src" / "analysis" / "technical"))
    from technical_analysis import TechnicalAnalyzer, print_analysis_summary
    print("✅ 기술분석 모듈 import 성공!")
except ImportError as e:
    print(f"❌ 모듈 import 실패: {e}")
    
    # 대안: 파일 직접 실행
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "technical_analysis", 
            project_root / "src" / "analysis" / "technical" / "technical_analysis.py"
        )
        technical_analysis = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(technical_analysis)
        
        TechnicalAnalyzer = technical_analysis.TechnicalAnalyzer
        print_analysis_summary = technical_analysis.print_analysis_summary
        print("✅ 기술분석 모듈 직접 로드 성공!")
    except Exception as e2:
        print(f"❌ 직접 로드도 실패: {e2}")
        sys.exit(1)

# FinanceDataReader
try:
    import FinanceDataReader as fdr
    FDR_AVAILABLE = True
    print("✅ FinanceDataReader 사용 가능")
except ImportError:
    FDR_AVAILABLE = False
    print("⚠️ FinanceDataReader 사용 불가")

def get_stock_data(stock_code: str) -> pd.DataFrame:
    """주가 데이터 가져오기"""
    if FDR_AVAILABLE:
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=300)
            
            print(f"🌐 {stock_code}: 실제 데이터 다운로드 중...")
            df = fdr.DataReader(stock_code, start=start_date, end=end_date)
            
            if not df.empty:
                print(f"✅ {stock_code}: {len(df)}일 데이터 다운로드 완료")
                return df
        except Exception as e:
            print(f"❌ {stock_code}: 다운로드 실패 - {e}")
    
    # 샘플 데이터 생성
    print(f"📊 {stock_code}: 샘플 데이터 생성")
    return generate_sample_data(stock_code)

def generate_sample_data(stock_code: str) -> pd.DataFrame:
    """샘플 데이터 생성"""
    dates = pd.date_range(end=datetime.now(), periods=200, freq='D')
    np.random.seed(hash(stock_code) % 2**32)
    
    # 종목별 기본 가격
    base_prices = {
        '005930': 70000,  # 삼성전자
        '000660': 120000, # SK하이닉스
        '035420': 200000, # NAVER
    }
    base_price = base_prices.get(stock_code, 50000)
    
    # 랜덤워크 생성
    returns = np.random.normal(0.001, 0.02, 200)  # 일평균 0.1%, 변동성 2%
    prices = [base_price]
    
    for ret in returns[1:]:
        new_price = prices[-1] * (1 + ret)
        new_price = max(new_price, base_price * 0.7)  # 30% 하락 제한
        new_price = min(new_price, base_price * 1.5)  # 50% 상승 제한
        prices.append(new_price)
    
    # OHLC 데이터 생성
    data = pd.DataFrame({
        'Open': [p * np.random.uniform(0.995, 1.005) for p in prices],
        'High': [p * np.random.uniform(1.005, 1.025) for p in prices],
        'Low': [p * np.random.uniform(0.975, 0.995) for p in prices],
        'Close': prices,
        'Volume': np.random.lognormal(13, 0.5, 200).astype(int)
    }, index=dates)
    
    # High/Low 보정
    for i in range(len(data)):
        high = max(data.iloc[i]['Open'], data.iloc[i]['Close'], data.iloc[i]['High'])
        low = min(data.iloc[i]['Open'], data.iloc[i]['Close'], data.iloc[i]['Low'])
        data.iloc[i, data.columns.get_loc('High')] = high
        data.iloc[i, data.columns.get_loc('Low')] = low
    
    print(f"📊 샘플 데이터 생성 완료: {len(data)}일")
    return data

def analyze_stock(stock_code: str):
    """단일 종목 분석"""
    print(f"\n🎯 {stock_code} 기술분석 시작...")
    
    # 종목 정보
    stock_names = {
        '005930': '삼성전자',
        '000660': 'SK하이닉스', 
        '035420': 'NAVER'
    }
    
    if stock_code in stock_names:
        print(f"📋 종목명: {stock_names[stock_code]}")
    
    # 데이터 수집
    try:
        ohlcv_data = get_stock_data(stock_code)
        if len(ohlcv_data) < 20:
            print(f"❌ {stock_code}: 데이터 부족")
            return
        
        # 기술분석 실행
        analyzer = TechnicalAnalyzer()
        result = analyzer.analyze_stock(stock_code, ohlcv_data)
        
        # 결과 출력
        if 'error' in result:
            print(f"❌ 분석 실패: {result['error']}")
        else:
            print_analysis_summary(result)
            
            # 추가 정보
            indicators = result['technical_indicators']
            calculated_count = len([v for v in indicators.values() if v is not None])
            print(f"\n📊 계산된 지표: {calculated_count}/{len(indicators)}개")
            
            # 성공적으로 계산된 지표들 표시
            successful_indicators = [k for k, v in indicators.items() if v is not None]
            if successful_indicators:
                print(f"✅ 성공 지표: {', '.join(successful_indicators[:8])}")
                if len(successful_indicators) > 8:
                    print(f"   및 {len(successful_indicators) - 8}개 더...")
    
    except Exception as e:
        print(f"❌ 분석 중 오류: {e}")
        import traceback
        traceback.print_exc()

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='기술분석 테스트 스크립트')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--stock_code', type=str, help='단일 종목 분석')
    group.add_argument('--multiple', type=str, help='복수 종목 분석 (쉼표 구분)')
    group.add_argument('--test', action='store_true', help='기본 테스트')
    
    args = parser.parse_args()
    
    print("🚀 기술분석 테스트 스크립트 시작")
    print("=" * 50)
    
    try:
        if args.stock_code:
            analyze_stock(args.stock_code)
        
        elif args.multiple:
            stock_codes = [code.strip() for code in args.multiple.split(',')]
            for i, stock_code in enumerate(stock_codes, 1):
                print(f"\n[{i}/{len(stock_codes)}] {stock_code} 분석...")
                analyze_stock(stock_code)
                if i < len(stock_codes):
                    print("\n" + "-" * 30)
        
        elif args.test:
            test_stocks = ['005930', '000660', '035420']
            for i, stock_code in enumerate(test_stocks, 1):
                print(f"\n[{i}/{len(test_stocks)}] {stock_code} 테스트...")
                analyze_stock(stock_code)
                if i < len(test_stocks):
                    print("\n" + "-" * 30)
        
        print(f"\n✨ 분석 완료!")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ 사용자 중단")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
