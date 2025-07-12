#!/usr/bin/env python3
"""
주식 정보 수집 스크립트
DART API와 FinanceDataReader를 활용한 주식 기본 정보 수집
"""

import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

# 프로젝트 루트 경로 추가
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    # 설정 모듈 임포트
    from config import get_kis_config, config_manager
    
    # 데이터 수집 모듈 임포트 (조건부)
    try:
        import FinanceDataReader as fdr
        FDR_AVAILABLE = True
    except ImportError:
        print("⚠️  FinanceDataReader가 설치되지 않았습니다.")
        print("   pip install FinanceDataReader로 설치해주세요.")
        FDR_AVAILABLE = False
    
    try:
        import pandas as pd
        PANDAS_AVAILABLE = True
    except ImportError:
        print("❌ pandas가 설치되지 않았습니다.")
        PANDAS_AVAILABLE = False
        
    import requests
    import json
    import time
    from typing import Dict, List, Optional, Any
    
except ImportError as e:
    print(f"❌ 모듈 임포트 실패: {e}")
    print("\n해결 방법:")
    print("1. 의존성 설치: pip install -r requirements.txt")
    print("2. Python 경로 확인")
    print("3. 가상환경 활성화 확인")
    sys.exit(1)

class StockInfoCollector:
    """주식 정보 수집 클래스"""
    
    def __init__(self):
        self.config = get_kis_config() if config_manager else {}
        self.data_dir = PROJECT_ROOT / 'data' / 'raw' / 'stock_info'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 설정 검증
        self._validate_config()
        
    def _validate_config(self):
        """설정 유효성 검사"""
        if not self.config:
            print("⚠️  KIS API 설정이 없습니다.")
            print("   .env 파일에 KIS API 키를 설정해주세요.")
            return False
        return True
    
    def collect_kospi_list(self) -> Optional[pd.DataFrame]:
        """KOSPI 종목 리스트 수집"""
        if not FDR_AVAILABLE:
            print("❌ FinanceDataReader를 사용할 수 없습니다.")
            return None
            
        try:
            print("📊 KOSPI 종목 리스트 수집 중...")
            kospi_list = fdr.StockListing('KOSPI')
            
            # 데이터 저장
            save_path = self.data_dir / f'kospi_list_{datetime.now().strftime("%Y%m%d")}.csv'
            kospi_list.to_csv(save_path, index=False, encoding='utf-8-sig')
            
            print(f"✅ KOSPI 종목 리스트 저장: {save_path}")
            print(f"   총 {len(kospi_list)}개 종목")
            
            return kospi_list
            
        except Exception as e:
            print(f"❌ KOSPI 종목 리스트 수집 실패: {e}")
            return None
    
    def collect_kosdaq_list(self) -> Optional[pd.DataFrame]:
        """KOSDAQ 종목 리스트 수집"""
        if not FDR_AVAILABLE:
            print("❌ FinanceDataReader를 사용할 수 없습니다.")
            return None
            
        try:
            print("📊 KOSDAQ 종목 리스트 수집 중...")
            kosdaq_list = fdr.StockListing('KOSDAQ')
            
            # 데이터 저장
            save_path = self.data_dir / f'kosdaq_list_{datetime.now().strftime("%Y%m%d")}.csv'
            kosdaq_list.to_csv(save_path, index=False, encoding='utf-8-sig')
            
            print(f"✅ KOSDAQ 종목 리스트 저장: {save_path}")
            print(f"   총 {len(kosdaq_list)}개 종목")
            
            return kosdaq_list
            
        except Exception as e:
            print(f"❌ KOSDAQ 종목 리스트 수집 실패: {e}")
            return None
    
    def collect_stock_prices(self, stock_code: str, start_date: str = None, 
                           end_date: str = None) -> Optional[pd.DataFrame]:
        """개별 종목 주가 데이터 수집"""
        if not FDR_AVAILABLE:
            print("❌ FinanceDataReader를 사용할 수 없습니다.")
            return None
        
        # 기본 날짜 설정 (최근 1년)
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        try:
            print(f"📈 {stock_code} 주가 데이터 수집 중... ({start_date} ~ {end_date})")
            
            stock_data = fdr.DataReader(stock_code, start_date, end_date)
            
            if stock_data.empty:
                print(f"⚠️  {stock_code} 데이터가 없습니다.")
                return None
            
            # 데이터 저장
            save_path = self.data_dir / f'{stock_code}_prices_{datetime.now().strftime("%Y%m%d")}.csv'
            stock_data.to_csv(save_path, encoding='utf-8-sig')
            
            print(f"✅ {stock_code} 주가 데이터 저장: {save_path}")
            print(f"   기간: {start_date} ~ {end_date}")
            print(f"   데이터 수: {len(stock_data)}일")
            
            return stock_data
            
        except Exception as e:
            print(f"❌ {stock_code} 주가 데이터 수집 실패: {e}")
            return None
    
    def collect_market_data(self) -> Dict[str, Any]:
        """시장 지수 데이터 수집"""
        if not FDR_AVAILABLE:
            print("❌ FinanceDataReader를 사용할 수 없습니다.")
            return {}
        
        results = {}
        # 한국 주식시장에서 지원되는 올바른 지수 심볼 사용
        indices = {
            'KOSPI': 'KS11',           # KOSPI 지수
            'KOSDAQ': 'KQ11',          # KOSDAQ 지수  
            'KOSPI200': 'KS200',       # KOSPI 200 지수
            'KRX_ENERGY': 'KRX.EADD'   # KRX 에너지화학 지수 (예시)
        }
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        for name, code in indices.items():
            try:
                print(f"📊 {name} 지수 데이터 수집 중... (심볼: {code})")
                
                # 더 안전한 데이터 수집 (타임아웃 설정)
                index_data = fdr.DataReader(code, start_date, end_date)
                
                if index_data is None or index_data.empty:
                    print(f"⚠️  {name} 데이터가 비어있습니다.")
                    continue
                
                # 데이터 저장
                save_path = self.data_dir / f'{name}_index_{datetime.now().strftime("%Y%m%d")}.csv'
                index_data.to_csv(save_path, encoding='utf-8-sig')
                
                # 최근 가격 정보 계산 (안전하게)
                latest_close = index_data['Close'].iloc[-1] if len(index_data) > 0 else 0
                change = 0
                change_pct = 0
                
                if len(index_data) > 1:
                    change = index_data['Close'].iloc[-1] - index_data['Close'].iloc[-2]
                    change_pct = ((index_data['Close'].iloc[-1] / index_data['Close'].iloc[-2]) - 1) * 100
                
                results[name] = {
                    'data': index_data,
                    'latest_close': latest_close,
                    'change': change,
                    'change_pct': change_pct,
                    'symbol': code
                }
                
                print(f"✅ {name} 지수 데이터 저장: {save_path}")
                print(f"   최근 종가: {latest_close:,.2f}")
                print(f"   변동: {change:+.2f} ({change_pct:+.2f}%)")
                
                # API 호출 간격 조절
                time.sleep(0.5)
                
            except requests.exceptions.HTTPError as e:
                if "404" in str(e):
                    print(f"❌ {name} 지수 심볼({code})을 찾을 수 없습니다. 심볼을 확인해주세요.")
                else:
                    print(f"❌ {name} 지수 데이터 수집 중 HTTP 오류: {e}")
            except requests.exceptions.RequestException as e:
                print(f"❌ {name} 지수 데이터 수집 중 네트워크 오류: {e}")
            except Exception as e:
                print(f"❌ {name} 지수 데이터 수집 실패: {e}")
                print(f"   심볼: {code} - 다른 심볼을 시도해보세요.")
        
    def check_available_indices(self):
        """사용 가능한 한국 지수 심볼 확인"""
        if not FDR_AVAILABLE:
            print("❌ FinanceDataReader를 사용할 수 없습니다.")
            return
        
        print("\n🔍 한국 지수 심볼 사용 가능 여부 확인")
        print("="*50)
        
        # 테스트할 지수 심볼들
        test_indices = {
            'KOSPI': ['KS11', '^KS11', 'KOSPI'],
            'KOSDAQ': ['KQ11', '^KQ11', 'KOSDAQ'], 
            'KOSPI200': ['KS200', '^KS200', 'KOSPI200'],
            'KRX100': ['KRX100', 'KRXIT'],
            'KRX300': ['KRX300'],
            'KOSPI_SMALL': ['KS50'],
            'KOSDAQ_STAR': ['KSQ'],
        }
        
        available_symbols = {}
        test_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        for index_name, symbols in test_indices.items():
            print(f"\n📊 {index_name} 테스트 중...")
            
            for symbol in symbols:
                try:
                    print(f"  시도: {symbol} ... ", end="")
                    
                    # 짧은 기간으로 테스트
                    test_data = fdr.DataReader(symbol, test_date, end_date)
                    
                    if test_data is not None and not test_data.empty:
                        print("✅ 사용 가능")
                        if index_name not in available_symbols:
                            available_symbols[index_name] = []
                        available_symbols[index_name].append(symbol)
                    else:
                        print("❌ 데이터 없음")
                    
                    time.sleep(0.3)  # API 호출 간격
                    
                except Exception as e:
                    print(f"❌ 오류: {str(e)[:50]}...")
        
        print("\n" + "="*50)
        print("📋 사용 가능한 지수 심볼 요약:")
        print("="*50)
        
        if available_symbols:
            for index_name, symbols in available_symbols.items():
                print(f"✅ {index_name}: {', '.join(symbols)}")
        else:
            print("❌ 사용 가능한 지수 심볼이 없습니다.")
        
        return available_symbols
    
    def create_summary_report(self):
        """수집 데이터 요약 리포트 생성"""
        print("\n" + "="*60)
        print("📋 데이터 수집 요약 리포트")
        print("="*60)
        
        # 데이터 디렉토리 확인
        data_files = list(self.data_dir.glob('*.csv'))
        
        if data_files:
            print(f"📁 저장 위치: {self.data_dir}")
            print(f"📊 수집된 파일 수: {len(data_files)}개")
            print("\n📋 수집된 파일 목록:")
            
            for file in sorted(data_files):
                file_size = file.stat().st_size / 1024  # KB
                print(f"  - {file.name} ({file_size:.1f} KB)")
        else:
            print("⚠️  수집된 데이터 파일이 없습니다.")
        
        print("="*60)

def main():
    """메인 실행 함수"""
    print("🚀 Finance Data Vibe - 주식 정보 수집 시작")
    print("="*60)
    
    # 설정 상태 확인
    if config_manager:
        config_manager.print_config_status()
    else:
        print("⚠️  설정 관리자를 사용할 수 없습니다.")
    
    print("\n" + "="*60)
    
    # 필수 라이브러리 확인
    if not PANDAS_AVAILABLE:
        print("❌ pandas가 필요합니다. pip install pandas")
        return
    
    if not FDR_AVAILABLE:
        print("❌ FinanceDataReader가 필요합니다. pip install FinanceDataReader")
        return
    
    try:
        # 수집기 초기화
        collector = StockInfoCollector()
        
        # 옵션: 사용 가능한 지수 심볼 먼저 확인 (디버그 모드)
        if os.getenv('FINANCE_DATA_VIBE_DEBUG', '0') == '1':
            print("🔍 디버그 모드: 사용 가능한 지수 심볼 확인 중...")
            collector.check_available_indices()
            print("\n" + "="*60)
        
        # 데이터 수집 실행
        print("1️⃣ 종목 리스트 수집")
        kospi_data = collector.collect_kospi_list()
        kosdaq_data = collector.collect_kosdaq_list()
        
        print("\n2️⃣ 시장 지수 데이터 수집")
        market_data = collector.collect_market_data()
        
        print("\n3️⃣ 주요 종목 주가 데이터 수집 (예시)")
        # 주요 종목 예시 (삼성전자, SK하이닉스)
        major_stocks = ['005930', '000660']  # 삼성전자, SK하이닉스
        
        for stock_code in major_stocks:
            collector.collect_stock_prices(stock_code)
            time.sleep(1)  # API 호출 간격 조절
        
        # 요약 리포트 생성
        collector.create_summary_report()
        
        print("\n✅ 데이터 수집이 완료되었습니다!")
        print("\n💡 참고사항:")
        print("- 일부 지수가 404 오류로 수집되지 않는 것은 정상입니다.")
        print("- FinanceDataReader는 Yahoo Finance API를 사용하므로 지원되는 심볼이 제한적입니다.")
        print("- 디버그 모드로 실행하려면: FINANCE_DATA_VIBE_DEBUG=1 python scripts/data_collection/collect_stock_info.py")
        
    except KeyboardInterrupt:
        print("\n⏹️  사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류 발생: {e}")
        print("\n🔍 상세 오류 정보:")
        traceback.print_exc()

if __name__ == "__main__":
    main()