#!/usr/bin/env python3
"""
기술분석 모듈 및 실행 스크립트 테스트
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_import():
    """import 테스트"""
    print("🔍 기술분석 모듈 import 테스트")
    print("=" * 40)
    
    try:
        from src.analysis.technical.technical_analysis import TechnicalAnalyzer, print_analysis_summary
        print("✅ TechnicalAnalyzer import 성공!")
        
        # 간단한 인스턴스 생성 테스트
        analyzer = TechnicalAnalyzer()
        print("✅ TechnicalAnalyzer 인스턴스 생성 성공!")
        
        # 모듈 정보 확인
        from src.analysis.technical.technical_analysis import get_module_info
        info = get_module_info()
        print(f"✅ 모듈 정보: {info['name']} v{info['version']}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import 실패: {e}")
        return False
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        return False

def test_sample_analysis():
    """샘플 데이터로 분석 테스트"""
    print("\n🧪 샘플 데이터 기술분석 테스트")
    print("=" * 40)
    
    try:
        from src.analysis.technical.technical_analysis import TechnicalAnalyzer
        import pandas as pd
        import numpy as np
        from datetime import datetime
        
        # 샘플 데이터 생성
        dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
        np.random.seed(42)
        
        base_price = 70000
        trend = np.linspace(0, 0.1, 100)
        noise = np.random.normal(0, 0.015, 100)
        returns = trend + noise
        
        prices = [base_price]
        for ret in returns[1:]:
            new_price = prices[-1] * (1 + ret)
            prices.append(max(new_price, base_price * 0.8))
        
        sample_data = pd.DataFrame({
            'Open': [p * np.random.uniform(0.995, 1.005) for p in prices],
            'High': [p * np.random.uniform(1.005, 1.025) for p in prices],
            'Low': [p * np.random.uniform(0.975, 0.995) for p in prices],
            'Close': prices,
            'Volume': np.random.lognormal(13, 0.5, 100).astype(int)
        }, index=dates)
        
        # High/Low 보정
        for i in range(len(sample_data)):
            high = max(sample_data.iloc[i]['Open'], sample_data.iloc[i]['Close'], sample_data.iloc[i]['High'])
            low = min(sample_data.iloc[i]['Open'], sample_data.iloc[i]['Close'], sample_data.iloc[i]['Low'])
            sample_data.iloc[i, sample_data.columns.get_loc('High')] = high
            sample_data.iloc[i, sample_data.columns.get_loc('Low')] = low
        
        print(f"📊 샘플 데이터 생성 완료: {len(sample_data)}일")
        print(f"   시작가: {sample_data['Close'].iloc[0]:,.0f}원")
        print(f"   종료가: {sample_data['Close'].iloc[-1]:,.0f}원")
        print(f"   수익률: {((sample_data['Close'].iloc[-1] / sample_data['Close'].iloc[0]) - 1) * 100:+.1f}%")
        
        # 기술분석 실행
        analyzer = TechnicalAnalyzer()
        result = analyzer.analyze_stock("TEST_001", sample_data)
        
        if 'error' in result:
            print(f"❌ 분석 실패: {result['error']}")
            return False
        
        print(f"✅ 기술분석 완료!")
        print(f"   종합 점수: {result['overall_score']:.1f}/100")
        print(f"   투자 추천: {result['recommendation']}")
        print(f"   리스크 레벨: {result['risk_level']}")
        
        # 주요 지표 확인
        indicators = result['technical_indicators']
        print(f"   RSI: {indicators.get('RSI', 'N/A'):.1f}" if indicators.get('RSI') else "   RSI: N/A")
        print(f"   MACD: {indicators.get('MACD', 'N/A'):.2f}" if indicators.get('MACD') else "   MACD: N/A")
        
        return True
        
    except Exception as e:
        print(f"❌ 샘플 분석 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """테스트 실행"""
    print("🚀 기술분석 시스템 통합 테스트")
    print("=" * 60)
    print(f"프로젝트 루트: {project_root}")
    print(f"현재 작업 디렉토리: {os.getcwd()}")
    print()
    
    # 1. Import 테스트
    import_success = test_import()
    
    if not import_success:
        print(f"\n❌ Import 테스트 실패 - 실행을 중단합니다.")
        return False
    
    # 2. 샘플 분석 테스트
    analysis_success = test_sample_analysis()
    
    if not analysis_success:
        print(f"\n❌ 분석 테스트 실패")
        return False
    
    print(f"\n🎉 모든 테스트 통과!")
    print("=" * 60)
    print("📚 사용 가능한 스크립트:")
    print("1. 단일 종목 분석:")
    print("   python run_technical_analysis_new.py --stock_code 005930")
    print()
    print("2. 복수 종목 분석:")
    print("   python run_technical_analysis_new.py --multiple 005930,000660,035420")
    print()
    print("3. KOSPI 상위 종목 분석:")
    print("   python run_technical_analysis_new.py --all_kospi --top 20")
    print()
    print("4. 관심종목 분석:")
    print("   python run_technical_analysis_new.py --watchlist")
    print()
    print("5. 결과 저장:")
    print("   python run_technical_analysis_new.py --stock_code 005930 --save result.json")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
