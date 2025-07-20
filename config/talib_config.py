"""
🔧 TA-Lib 설정 및 환경 구성 스크립트
Value Investment System의 기술분석을 위한 TA-Lib 라이브러리 설정

주요 기능:
1. TA-Lib 설치 상태 확인
2. 필수 의존성 검증
3. 기본 기술지표 테스트
4. 환경 설정 가이드
"""

import sys
import subprocess
import importlib
import platform
import os
from typing import Dict, List, Tuple

class TALibSetup:
    """TA-Lib 설정 및 검증 클래스"""
    
    def __init__(self):
        self.system_info = {
            'platform': platform.system(),
            'architecture': platform.machine(),
            'python_version': platform.python_version()
        }
        self.required_packages = [
            'numpy',
            'pandas', 
            'talib',
            'matplotlib',
            'plotly'
        ]
    
    def check_system_info(self) -> Dict:
        """시스템 정보 확인"""
        print("🖥️ 시스템 정보:")
        print(f"   운영체제: {self.system_info['platform']}")
        print(f"   아키텍처: {self.system_info['architecture']}")
        print(f"   파이썬 버전: {self.system_info['python_version']}")
        print()
        return self.system_info
    
    def check_package_installation(self) -> Dict[str, bool]:
        """필수 패키지 설치 상태 확인"""
        installation_status = {}
        
        print("📦 패키지 설치 상태 확인:")
        for package in self.required_packages:
            try:
                importlib.import_module(package)
                installation_status[package] = True
                print(f"   ✅ {package}: 설치됨")
            except ImportError:
                installation_status[package] = False
                print(f"   ❌ {package}: 설치 필요")
        
        print()
        return installation_status
    
    def install_missing_packages(self, missing_packages: List[str]) -> bool:
        """누락된 패키지 설치"""
        if not missing_packages:
            print("✅ 모든 필수 패키지가 설치되어 있습니다.")
            return True
        
        print(f"📥 누락된 패키지 설치 중: {', '.join(missing_packages)}")
        
        for package in missing_packages:
            try:
                if package == 'talib':
                    # TA-Lib는 특별한 설치 과정이 필요할 수 있음
                    self._install_talib()
                else:
                    subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
                print(f"   ✅ {package} 설치 완료")
            except subprocess.CalledProcessError as e:
                print(f"   ❌ {package} 설치 실패: {e}")
                return False
        
        return True
    
    def _install_talib(self):
        """TA-Lib 특별 설치 처리"""
        system = self.system_info['platform']
        
        if system == 'Windows':
            # Windows에서는 바이너리 휠 사용
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'TA-Lib'])
            except subprocess.CalledProcessError:
                print("   ⚠️ TA-Lib 직접 설치 실패. 대안 방법을 시도합니다...")
                # 대안: 미리 컴파일된 휠 사용
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--find-links', 
                                     'https://www.lfd.uci.edu/~gohlke/pythonlibs/', 'TA-Lib'])
        
        elif system == 'Darwin':  # macOS
            # macOS에서는 homebrew 사용 권장
            print("   ℹ️ macOS에서는 먼저 brew install ta-lib 실행을 권장합니다.")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'TA-Lib'])
        
        else:  # Linux
            # Linux에서는 소스 컴파일 필요
            print("   ℹ️ Linux에서는 ta-lib 소스 설치가 필요할 수 있습니다.")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'TA-Lib'])
    
    def test_talib_functions(self) -> bool:
        """TA-Lib 기본 함수 테스트"""
        print("🧪 TA-Lib 기능 테스트:")
        
        try:
            import talib
            import numpy as np
            
            # 테스트 데이터 생성
            test_data = np.random.random(100) * 100 + 50
            test_high = test_data * 1.02
            test_low = test_data * 0.98
            test_volume = np.random.randint(10000, 100000, 100)
            
            # 기본 지표 테스트
            test_results = {}
            
            # 이동평균
            try:
                sma = talib.SMA(test_data, timeperiod=20)
                test_results['SMA'] = '✅ 정상'
            except Exception as e:
                test_results['SMA'] = f'❌ 실패: {e}'
            
            # RSI
            try:
                rsi = talib.RSI(test_data, timeperiod=14)
                test_results['RSI'] = '✅ 정상'
            except Exception as e:
                test_results['RSI'] = f'❌ 실패: {e}'
            
            # MACD
            try:
                macd, signal, hist = talib.MACD(test_data)
                test_results['MACD'] = '✅ 정상'
            except Exception as e:
                test_results['MACD'] = f'❌ 실패: {e}'
            
            # 볼린저 밴드
            try:
                upper, middle, lower = talib.BBANDS(test_data)
                test_results['BBANDS'] = '✅ 정상'
            except Exception as e:
                test_results['BBANDS'] = f'❌ 실패: {e}'
            
            # ATR
            try:
                atr = talib.ATR(test_high, test_low, test_data)
                test_results['ATR'] = '✅ 정상'
            except Exception as e:
                test_results['ATR'] = f'❌ 실패: {e}'
            
            # OBV
            try:
                obv = talib.OBV(test_data, test_volume)
                test_results['OBV'] = '✅ 정상'
            except Exception as e:
                test_results['OBV'] = f'❌ 실패: {e}'
            
            # 결과 출력
            for indicator, result in test_results.items():
                print(f"   {indicator}: {result}")
            
            # 전체 성공 여부 확인
            success_count = sum(1 for result in test_results.values() if '✅' in result)
            total_count = len(test_results)
            
            print(f"\n   📊 테스트 결과: {success_count}/{total_count} 성공")
            
            if success_count == total_count:
                print("   🎉 모든 TA-Lib 기능이 정상 작동합니다!")
                return True
            else:
                print("   ⚠️ 일부 TA-Lib 기능에 문제가 있습니다.")
                return False
                
        except ImportError:
            print("   ❌ TA-Lib를 import할 수 없습니다. 설치를 확인하세요.")
            return False
        except Exception as e:
            print(f"   ❌ 예상치 못한 오류: {e}")
            return False
    
    def create_config_file(self) -> str:
        """TA-Lib 설정 파일 생성"""
        config_content = '''"""
TA-Lib 기술분석 설정 파일
Value Investment System - Technical Analysis Configuration
"""

# TA-Lib 기본 설정
TALIB_CONFIG = {
    # 이동평균 설정
    'SMA_PERIODS': [5, 20, 60, 120, 200],
    'EMA_PERIODS': [12, 26],
    
    # 모멘텀 지표 설정
    'RSI_PERIOD': 14,
    'MACD_FAST': 12,
    'MACD_SLOW': 26,
    'MACD_SIGNAL': 9,
    'STOCH_K': 14,
    'STOCH_D': 3,
    'WILLIAMS_R_PERIOD': 14,
    'CCI_PERIOD': 14,
    
    # 변동성 지표 설정
    'BOLLINGER_PERIOD': 20,
    'BOLLINGER_STD': 2,
    'ATR_PERIOD': 14,
    'KELTNER_PERIOD': 20,
    'KELTNER_MULTIPLIER': 2.0,
    'DONCHIAN_PERIOD': 20,
    
    # 거래량 지표 설정
    'VWAP_WINDOW': 20,
    'CMF_PERIOD': 20,
    
    # 추세 지표 설정
    'ADX_PERIOD': 14,
    'PARABOLIC_SAR_ACCEL': 0.02,
    'PARABOLIC_SAR_MAX': 0.2,
    
    # 신호 임계값 설정
    'RSI_OVERSOLD': 30,
    'RSI_OVERBOUGHT': 70,
    'STOCH_OVERSOLD': 20,
    'STOCH_OVERBOUGHT': 80,
    'WILLIAMS_R_OVERSOLD': -80,
    'WILLIAMS_R_OVERBOUGHT': -20,
    
    # 52주 신고가/신저가 기간
    'WEEKS_52_PERIOD': 252  # 거래일 기준
}

# 가중치 설정 (기술분석 30% 비중 내에서)
TECHNICAL_WEIGHTS = {
    'trend_indicators': 0.35,      # 35% - 추세가 가장 중요
    'momentum_indicators': 0.30,   # 30% - 모멘텀
    'volatility_indicators': 0.20, # 20% - 변동성
    'volume_indicators': 0.15      # 15% - 거래량
}

# 개별 지표 가중치
INDICATOR_WEIGHTS = {
    # 추세 지표
    'SMA_SIGNAL': 0.4,
    'EMA_SIGNAL': 0.3,
    'ADX_SIGNAL': 0.2,
    'PARABOLIC_SAR_SIGNAL': 0.1,
    
    # 모멘텀 지표
    'RSI_SIGNAL': 0.35,
    'MACD_SIGNAL': 0.25,
    'STOCH_SIGNAL': 0.20,
    'WILLIAMS_R_SIGNAL': 0.10,
    'CCI_SIGNAL': 0.10,
    
    # 변동성 지표
    'BOLLINGER_SIGNAL': 0.50,
    'ATR_SIGNAL': 0.20,
    'KELTNER_SIGNAL': 0.20,
    'DONCHIAN_SIGNAL': 0.10,
    
    # 거래량 지표
    'OBV_SIGNAL': 0.40,
    'VWAP_SIGNAL': 0.35,
    'CMF_SIGNAL': 0.25
}

# 매매신호 레벨 정의
SIGNAL_LEVELS = {
    'STRONG_BUY': 2,
    'BUY': 1,
    'HOLD': 0,
    'SELL': -1,
    'STRONG_SELL': -2
}

# 투자 추천 임계값
RECOMMENDATION_THRESHOLDS = {
    'STRONG_BUY': 80,
    'BUY': 65,
    'HOLD': 35,
    'SELL': 20,
    'STRONG_SELL': 0
}

# 리스크 레벨 기준
RISK_LEVELS = {
    'LOW': {'volatility_max': 15, 'atr_percentile': 30},
    'MEDIUM': {'volatility_max': 25, 'atr_percentile': 70},
    'HIGH': {'volatility_max': 100, 'atr_percentile': 100}
}
'''
        
        config_file_path = 'talib_config.py'
        
        try:
            with open(config_file_path, 'w', encoding='utf-8') as f:
                f.write(config_content)
            print(f"📄 설정 파일 생성 완료: {config_file_path}")
            return config_file_path
        except Exception as e:
            print(f"❌ 설정 파일 생성 실패: {e}")
            return ""
    
    def generate_requirements_txt(self) -> str:
        """requirements.txt 파일 생성"""
        requirements_content = '''# Value Investment System - 기술분석 모듈 의존성
# Technical Analysis Dependencies

# 기본 데이터 처리
numpy>=1.21.0
pandas>=1.3.0

# 기술분석 라이브러리
TA-Lib>=0.4.24

# 시각화
matplotlib>=3.4.0
plotly>=5.0.0
seaborn>=0.11.0

# 웹 프레임워크
streamlit>=1.28.0

# 데이터 수집
yfinance>=0.2.0
FinanceDataReader>=0.9.0
requests>=2.25.0

# 기타 유틸리티
python-dateutil>=2.8.0
pytz>=2021.1
'''
        
        requirements_file = 'requirements_talib.txt'
        
        try:
            with open(requirements_file, 'w', encoding='utf-8') as f:
                f.write(requirements_content)
            print(f"📄 requirements.txt 생성 완료: {requirements_file}")
            return requirements_file
        except Exception as e:
            print(f"❌ requirements.txt 생성 실패: {e}")
            return ""
    
    def run_complete_setup(self) -> bool:
        """전체 설정 프로세스 실행"""
        print("🚀 TA-Lib 완전 설정 시작")
        print("=" * 50)
        
        # 1. 시스템 정보 확인
        self.check_system_info()
        
        # 2. 패키지 설치 상태 확인
        installation_status = self.check_package_installation()
        
        # 3. 누락된 패키지 설치
        missing_packages = [pkg for pkg, installed in installation_status.items() if not installed]
        if missing_packages:
            install_success = self.install_missing_packages(missing_packages)
            if not install_success:
                print("❌ 패키지 설치 실패. 수동 설치가 필요합니다.")
                return False
        
        # 4. TA-Lib 기능 테스트
        test_success = self.test_talib_functions()
        if not test_success:
            print("❌ TA-Lib 테스트 실패.")
            return False
        
        # 5. 설정 파일 생성
        config_file = self.create_config_file()
        requirements_file = self.generate_requirements_txt()
        
        # 6. 완료 메시지
        print("\n" + "=" * 50)
        print("🎉 TA-Lib 설정 완료!")
        print("\n📋 다음 단계:")
        print("1. 기술분석 모듈 import 테스트")
        print("2. 실제 주가 데이터로 분석 실행")
        print("3. 웹 대시보드에서 차트 확인")
        
        return True

def main():
    """메인 실행 함수"""
    setup = TALibSetup()
    success = setup.run_complete_setup()
    
    if success:
        print("\n✅ 모든 설정이 완료되었습니다!")
        print("이제 기술분석 모듈을 사용할 수 있습니다.")
        
        # 간단한 사용 예시 출력
        print("\n📘 사용 예시:")
        print("""
from technical_analysis_module import TechnicalAnalysisEngine
import pandas as pd

# 주가 데이터 로드 (OHLCV)
# ohlcv_data = pd.read_csv('stock_data.csv')

# 기술분석 실행
engine = TechnicalAnalysisEngine()
result = engine.analyze_stock(ohlcv_data, "005930")

print(f"투자 추천: {result['trading_signals']['recommendation']}")
print(f"기술분석 점수: {result['trading_signals']['total_score']}/100")
        """)
    else:
        print("\n❌ 설정 중 문제가 발생했습니다.")
        print("수동 설치가 필요할 수 있습니다.")

if __name__ == "__main__":
    main()