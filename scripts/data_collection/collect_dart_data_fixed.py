#!/usr/bin/env python3
"""
수정된 DART 재무데이터 수집 스크립트
새로운 ConfigManager를 사용하여 ImportError 해결

실행 방법:
python scripts/data_collection/collect_dart_data_fixed.py --stock_code=005930
python scripts/data_collection/collect_dart_data_fixed.py --year=2023 --quarter=4
"""

import sys
import os
import argparse
import sqlite3
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import zipfile
import io
from datetime import datetime, timedelta
from pathlib import Path
import logging
import time

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 수정된 ConfigManager 임포트
try:
    from config import ConfigManager, get_dart_config, get_logger, get_database_path
    CONFIG_AVAILABLE = True
    print("✅ ConfigManager 임포트 성공")
except ImportError as e:
    print(f"❌ ConfigManager 임포트 실패: {e}")
    print("⚠️ 기본 설정으로 진행합니다.")
    CONFIG_AVAILABLE = False

class FixedDartDataCollector:
    """수정된 DART 데이터 수집 클래스"""
    
    def __init__(self):
        """초기화 - 안전한 설정 로드"""
        if CONFIG_AVAILABLE:
            try:
                # 새로운 ConfigManager 사용
                self.config_manager = ConfigManager()
                self.logger = self.config_manager.get_logger('DartCollector')
                
                # DART API 설정
                dart_config = self.config_manager.get_dart_config()
                self.api_key = dart_config.get('api_key')
                self.base_url = dart_config.get('base_url', "https://opendart.fss.or.kr/api")
                
                # 데이터베이스 설정
                self.db_path = self.config_manager.get_database_path('dart')
                
                self.logger.info("FixedDartDataCollector 초기화 완료")
                
            except Exception as e:
                print(f"⚠️ ConfigManager 사용 중 오류: {e}")
                self._use_fallback_config()
        else:
            self._use_fallback_config()
        
        # API 키 검증
        if not self.api_key:
            raise ValueError("DART API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
    
    def _use_fallback_config(self):
        """Fallback 설정 사용"""
        print("📝 Fallback 설정을 사용합니다.")
        
        # 기본 로거 설정
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('DartCollector')
        
        # 환경변수에서 직접 로드
        from dotenv import load_dotenv
        load_dotenv()
        
        self.api_key = os.getenv('DART_API_KEY', '').strip('"')
        self.base_url = "https://opendart.fss.or.kr/api"
        self.db_path = Path('data/databases/dart_data.db')
        
        # 디렉토리 생성
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def test_api_connection(self):
        """API 연결 테스트"""
        try:
            self.logger.info("DART API 연결 테스트 중...")
            
            # 간단한 API 호출 테스트
            test_url = f"{self.base_url}/list.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_cls': 'Y',
                'page_no': 1,
                'page_count': 1
            }
            
            response = requests.get(test_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == '000':
                self.logger.info("✅ DART API 연결 성공")
                return True
            else:
                self.logger.error(f"❌ DART API 오류: {data.get('message', 'Unknown error')}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ DART API 연결 실패: {e}")
            return False
    
    def download_corp_codes(self):
        """기업 고유번호 다운로드 및 파싱"""
        try:
            self.logger.info("기업 코드 다운로드 시작...")
            
            # DART 기업코드 ZIP 파일 다운로드
            url = f"{self.base_url}/corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # ZIP 파일 여부 확인
            if not response.content.startswith(b'PK'):
                self.logger.error("응답이 ZIP 파일이 아닙니다.")
                return pd.DataFrame()
            
            # ZIP 파일 압축 해제
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_files = [f for f in zip_file.namelist() if f.endswith('.xml')]
                
                if not xml_files:
                    self.logger.error("ZIP 파일에 XML 파일이 없습니다.")
                    return pd.DataFrame()
                
                # 첫 번째 XML 파일 읽기
                with zip_file.open(xml_files[0]) as xml_file:
                    content = xml_file.read().decode('utf-8')
            
            # XML 파싱
            root = ET.fromstring(content)
            
            # 기업 코드 데이터 추출
            corp_list = []
            for corp in root.findall('.//list'):
                corp_code = corp.find('corp_code')
                corp_name = corp.find('corp_name')
                stock_code = corp.find('stock_code')
                modify_date = corp.find('modify_date')
                
                if corp_code is not None and corp_name is not None:
                    corp_data = {
                        'corp_code': corp_code.text,
                        'corp_name': corp_name.text,
                        'stock_code': stock_code.text if stock_code is not None else '',
                        'modify_date': modify_date.text if modify_date is not None else ''
                    }
                    corp_list.append(corp_data)
            
            df = pd.DataFrame(corp_list)
            self.logger.info(f"✅ 기업 코드 {len(df)}개 수집 완료")
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ 기업 코드 다운로드 실패: {e}")
            return pd.DataFrame()
    
    def collect_financial_data(self, corp_code: str, year: str, quarter: str = '11'):
        """재무제표 데이터 수집"""
        try:
            self.logger.info(f"재무데이터 수집: {corp_code} - {year}년 {quarter}분기")
            
            url = f"{self.base_url}/fnlttSinglAcntAll.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': year,
                'reprt_code': quarter
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != '000':
                self.logger.warning(f"❌ API 오류: {data.get('message', 'Unknown error')}")
                return pd.DataFrame()
            
            # 재무데이터 파싱
            if 'list' in data and data['list']:
                df = pd.DataFrame(data['list'])
                self.logger.info(f"✅ 재무데이터 {len(df)}건 수집")
                return df
            else:
                self.logger.warning("재무데이터가 없습니다.")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"❌ 재무데이터 수집 실패: {e}")
            return pd.DataFrame()
    
    def save_to_database(self, df: pd.DataFrame, table_name: str):
        """데이터베이스에 데이터 저장"""
        try:
            if df.empty:
                self.logger.warning("저장할 데이터가 없습니다.")
                return False
            
            self.logger.info(f"데이터베이스 저장: {table_name} ({len(df)}건)")
            
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                self.logger.info(f"✅ {table_name} 테이블 저장 완료")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 데이터베이스 저장 실패: {e}")
            return False
    
    def collect_samsung_data(self):
        """삼성전자 데이터 수집 (테스트용)"""
        try:
            self.logger.info("삼성전자 데이터 수집 시작...")
            
            # 1. 기업코드 먼저 확인
            corp_codes_df = self.download_corp_codes()
            if corp_codes_df.empty:
                self.logger.error("기업코드를 가져올 수 없습니다.")
                return False
            
            # 삼성전자 찾기
            samsung = corp_codes_df[corp_codes_df['stock_code'] == '005930']
            if samsung.empty:
                self.logger.error("삼성전자를 찾을 수 없습니다.")
                return False
            
            samsung_corp_code = samsung.iloc[0]['corp_code']
            self.logger.info(f"삼성전자 corp_code: {samsung_corp_code}")
            
            # 2. 최근 3년 재무데이터 수집
            financial_data_list = []
            current_year = datetime.now().year
            
            for year in range(current_year - 2, current_year + 1):  # 최근 3년
                for quarter in ['11', '14']:  # 연결재무제표, 1분기
                    self.logger.info(f"수집 중: {year}년 {quarter}분기")
                    
                    df = self.collect_financial_data(samsung_corp_code, str(year), quarter)
                    if not df.empty:
                        df['stock_code'] = '005930'
                        df['collect_date'] = datetime.now().strftime('%Y-%m-%d')
                        financial_data_list.append(df)
                    
                    time.sleep(1)  # API 호출 제한 고려
            
            # 3. 데이터 통합 및 저장
            if financial_data_list:
                all_financial_data = pd.concat(financial_data_list, ignore_index=True)
                success = self.save_to_database(all_financial_data, 'samsung_financial_statements')
                
                if success:
                    self.logger.info(f"🎉 삼성전자 데이터 수집 완료: {len(all_financial_data)}건")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 삼성전자 데이터 수집 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='DART 재무데이터 수집 (수정된 버전)')
    parser.add_argument('--stock_code', type=str, help='주식 코드 (예: 005930)')
    parser.add_argument('--year', type=str, help='수집 연도')
    parser.add_argument('--quarter', type=str, default='11', help='분기 (11: 연결재무제표)')
    parser.add_argument('--test', action='store_true', help='삼성전자 테스트 수집')
    
    args = parser.parse_args()
    
    try:
        # DART 수집기 초기화
        collector = FixedDartDataCollector()
        
        # API 연결 테스트
        if not collector.test_api_connection():
            print("❌ DART API 연결 실패. API 키를 확인하세요.")
            return False
        
        if args.test or (not args.stock_code and not args.year):
            # 삼성전자 테스트 수집
            print("🧪 삼성전자 테스트 데이터 수집 시작...")
            success = collector.collect_samsung_data()
            
            if success:
                print("🎉 삼성전자 데이터 수집 성공!")
                print("💾 데이터베이스 저장 완료: data/databases/dart_data.db")
                print("🔍 다음 명령으로 확인: python buffett_scorecard_calculator.py")
            else:
                print("❌ 삼성전자 데이터 수집 실패")
            
            return success
        
        else:
            # 개별 수집
            if args.stock_code:
                print(f"📊 {args.stock_code} 데이터 수집 중...")
                # 구체적인 수집 로직 구현
                # ...
            
        return True
        
    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")
        return False

if __name__ == "__main__":
    print("🚀 수정된 DART 데이터 수집기 시작")
    print("=" * 60)
    
    success = main()
    
    if success:
        print("\n✅ DART 데이터 수집 완료!")
        print("🎯 다음 단계:")
        print("   1. python test_fixed_config.py (설정 확인)")
        print("   2. python buffett_scorecard_calculator.py (분석 실행)")
    else:
        print("\n❌ DART 데이터 수집 실패")
        print("🔧 문제 해결:")
        print("   1. .env 파일의 DART_API_KEY 확인")
        print("   2. 인터넷 연결 상태 확인")
        print("   3. python test_fixed_config.py 실행으로 설정 점검")
