#!/usr/bin/env python3
"""
DART API 파라미터 수정된 데이터 수집기
"조회된 데이타가 없습니다" 오류 해결 버전
"""

import sys
import os
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

# ConfigManager 임포트
try:
    from config import ConfigManager, get_dart_config, get_logger, get_database_path
    CONFIG_AVAILABLE = True
    print("✅ ConfigManager 임포트 성공")
except ImportError as e:
    print(f"❌ ConfigManager 임포트 실패: {e}")
    CONFIG_AVAILABLE = False

class ImprovedDartDataCollector:
    """개선된 DART 데이터 수집기 - API 파라미터 수정"""
    
    def __init__(self):
        """초기화"""
        if CONFIG_AVAILABLE:
            try:
                self.config_manager = ConfigManager()
                self.logger = self.config_manager.get_logger('ImprovedDartCollector')
                dart_config = self.config_manager.get_dart_config()
                self.api_key = dart_config.get('api_key')
                self.base_url = dart_config.get('base_url', "https://opendart.fss.or.kr/api")
                self.db_path = self.config_manager.get_database_path('dart')
                
            except Exception as e:
                print(f"⚠️ ConfigManager 오류: {e}")
                self._use_fallback_config()
        else:
            self._use_fallback_config()
        
        if not self.api_key:
            raise ValueError("DART API 키가 설정되지 않았습니다.")
    
    def _use_fallback_config(self):
        """Fallback 설정"""
        from dotenv import load_dotenv
        load_dotenv()
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('ImprovedDartCollector')
        
        self.api_key = os.getenv('DART_API_KEY', '').strip('"')
        self.base_url = "https://opendart.fss.or.kr/api"
        self.db_path = Path('data/databases/dart_data.db')
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def test_api_with_correct_params(self):
        """올바른 파라미터로 API 테스트"""
        try:
            self.logger.info("개선된 DART API 연결 테스트...")
            
            # 1. 더 넓은 날짜 범위로 공시 조회 테스트
            test_url = f"{self.base_url}/list.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_cls': 'Y',           # 유가증권
                'bgn_de': '20240101',      # 2024년 전체
                'end_de': '20241231',
                'page_no': 1,
                'page_count': 10
            }
            
            response = requests.get(test_url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == '000':
                self.logger.info("✅ DART API 연결 성공")
                if 'list' in data and data['list']:
                    self.logger.info(f"✅ 공시 데이터 {len(data['list'])}건 확인")
                return True
            elif data.get('status') == '020':
                # 조회된 데이터가 없는 경우 - 날짜 범위 조정
                self.logger.warning("해당 기간 공시가 없음. 날짜 범위 조정 시도...")
                return self._test_with_different_dates()
            else:
                self.logger.error(f"❌ DART API 오류: {data.get('message', 'Unknown error')}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ DART API 테스트 실패: {e}")
            return False
    
    def _test_with_different_dates(self):
        """다른 날짜 범위로 테스트"""
        try:
            # 최근 1개월로 범위 축소
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            test_url = f"{self.base_url}/list.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_cls': 'Y',
                'bgn_de': start_date.strftime('%Y%m%d'),
                'end_de': end_date.strftime('%Y%m%d'),
                'page_no': 1,
                'page_count': 10
            }
            
            response = requests.get(test_url, params=params, timeout=15)
            data = response.json()
            
            if data.get('status') == '000':
                self.logger.info("✅ 날짜 범위 조정 후 성공")
                return True
            else:
                self.logger.warning(f"여전히 데이터 없음: {data.get('message')}")
                return False
                
        except Exception as e:
            self.logger.error(f"날짜 범위 조정 테스트 실패: {e}")
            return False
    
    def download_corp_codes_improved(self):
        """개선된 기업코드 다운로드"""
        try:
            self.logger.info("기업코드 다운로드 시작...")
            
            url = f"{self.base_url}/corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            if not response.content.startswith(b'PK'):
                # JSON 오류 응답 확인
                try:
                    error_data = response.json()
                    self.logger.error(f"API 오류: {error_data.get('message', 'Unknown')}")
                    return pd.DataFrame()
                except:
                    self.logger.error("ZIP 파일이 아닌 응답 수신")
                    return pd.DataFrame()
            
            # ZIP 파일 처리
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_files = [f for f in zip_file.namelist() if f.endswith('.xml')]
                
                if not xml_files:
                    self.logger.error("ZIP에 XML 파일 없음")
                    return pd.DataFrame()
                
                with zip_file.open(xml_files[0]) as xml_file:
                    content = xml_file.read().decode('utf-8')
            
            # XML 파싱
            root = ET.fromstring(content)
            corp_list = []
            
            for corp in root.findall('.//list'):
                corp_code = corp.find('corp_code')
                corp_name = corp.find('corp_name')
                stock_code = corp.find('stock_code')
                
                if corp_code is not None and corp_name is not None:
                    corp_data = {
                        'corp_code': corp_code.text,
                        'corp_name': corp_name.text,
                        'stock_code': stock_code.text if stock_code is not None else ''
                    }
                    corp_list.append(corp_data)
            
            df = pd.DataFrame(corp_list)
            self.logger.info(f"✅ 기업코드 {len(df)}개 수집완료")
            
            return df
            
        except Exception as e:
            self.logger.error(f"❌ 기업코드 다운로드 실패: {e}")
            return pd.DataFrame()
    
    def get_samsung_financial_data(self):
        """삼성전자 재무데이터 수집 (여러 연도 시도)"""
        try:
            self.logger.info("삼성전자 재무데이터 수집 시작...")
            
            samsung_corp_code = '00126380'  # 삼성전자 기업코드
            
            # 여러 연도와 보고서 타입 시도
            year_report_combinations = [
                ('2023', '11011'),  # 2023년 사업보고서
                ('2023', '11012'),  # 2023년 반기보고서
                ('2023', '11013'),  # 2023년 1분기보고서
                ('2022', '11011'),  # 2022년 사업보고서
                ('2024', '11013'),  # 2024년 1분기보고서
            ]
            
            all_financial_data = []
            
            for year, report_code in year_report_combinations:
                try:
                    self.logger.info(f"시도: {year}년 {report_code} 보고서")
                    
                    url = f"{self.base_url}/fnlttSinglAcntAll.json"
                    params = {
                        'crtfc_key': self.api_key,
                        'corp_code': samsung_corp_code,
                        'bsns_year': year,
                        'reprt_code': report_code
                    }
                    
                    response = requests.get(url, params=params, timeout=30)
                    data = response.json()
                    
                    if data.get('status') == '000' and 'list' in data:
                        financial_df = pd.DataFrame(data['list'])
                        financial_df['stock_code'] = '005930'
                        financial_df['collect_year'] = year
                        financial_df['collect_date'] = datetime.now().strftime('%Y-%m-%d')
                        
                        all_financial_data.append(financial_df)
                        self.logger.info(f"✅ {year}년 {report_code}: {len(financial_df)}건 수집")
                        
                        time.sleep(1)  # API 호출 제한
                        
                    else:
                        self.logger.warning(f"⚠️ {year}년 {report_code}: {data.get('message', 'No data')}")
                
                except Exception as e:
                    self.logger.warning(f"⚠️ {year}년 {report_code} 수집 실패: {e}")
                    continue
            
            if all_financial_data:
                combined_df = pd.concat(all_financial_data, ignore_index=True)
                self.logger.info(f"🎉 총 {len(combined_df)}건 재무데이터 수집 완료")
                return combined_df
            else:
                self.logger.error("❌ 모든 시도에서 재무데이터 수집 실패")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"❌ 삼성전자 재무데이터 수집 실패: {e}")
            return pd.DataFrame()
    
    def save_to_database(self, df: pd.DataFrame, table_name: str):
        """데이터베이스 저장"""
        try:
            if df.empty:
                self.logger.warning("저장할 데이터가 없습니다.")
                return False
            
            self.logger.info(f"데이터베이스 저장: {table_name} ({len(df)}건)")
            
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                self.logger.info(f"✅ {table_name} 저장 완료")
                
                # 저장 확인
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                count = pd.read_sql_query(count_query, conn).iloc[0, 0]
                self.logger.info(f"✅ 저장 확인: {count}건")
                
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 데이터베이스 저장 실패: {e}")
            return False
    
    def run_improved_collection(self):
        """개선된 데이터 수집 실행"""
        try:
            self.logger.info("🚀 개선된 DART 데이터 수집 시작")
            
            # 1. API 연결 테스트
            if not self.test_api_with_correct_params():
                self.logger.error("❌ DART API 연결 실패")
                return False
            
            # 2. 기업코드 다운로드
            self.logger.info("📋 기업코드 다운로드...")
            corp_codes_df = self.download_corp_codes_improved()
            
            if not corp_codes_df.empty:
                # 기업코드 저장
                self.save_to_database(corp_codes_df, 'corp_codes')
                
                # 삼성전자 확인
                samsung_rows = corp_codes_df[corp_codes_df['stock_code'] == '005930']
                if not samsung_rows.empty:
                    self.logger.info(f"✅ 삼성전자 발견: {samsung_rows.iloc[0]['corp_name']}")
                else:
                    self.logger.warning("⚠️ 삼성전자를 찾을 수 없음")
            
            # 3. 삼성전자 재무데이터 수집
            self.logger.info("💰 삼성전자 재무데이터 수집...")
            financial_df = self.get_samsung_financial_data()
            
            if not financial_df.empty:
                success = self.save_to_database(financial_df, 'samsung_financial_statements')
                
                if success:
                    self.logger.info("🎉 삼성전자 데이터 수집 및 저장 완료!")
                    
                    # 주요 계정과목 확인
                    account_names = financial_df['account_nm'].unique()[:10]
                    self.logger.info(f"주요 계정과목: {list(account_names)}")
                    
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 데이터 수집 실행 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    print("🚀 개선된 DART 데이터 수집기 시작")
    print("=" * 60)
    
    try:
        collector = ImprovedDartDataCollector()
        success = collector.run_improved_collection()
        
        if success:
            print("\n🎉 DART 데이터 수집 성공!")
            print("📊 수집된 데이터:")
            print("   • corp_codes 테이블: 기업코드 정보")
            print("   • samsung_financial_statements 테이블: 삼성전자 재무데이터")
            print("\n🎯 다음 단계:")
            print("   python buffett_scorecard_calculator_fixed.py")
            print("   (실제 데이터로 워런 버핏 스코어카드 계산)")
            
        else:
            print("\n❌ DART 데이터 수집 실패")
            print("🔧 문제 해결:")
            print("   1. python diagnose_dart_api.py (API 상세 진단)")
            print("   2. .env 파일의 DART_API_KEY 재확인")
            print("   3. 기존 데이터베이스로 스코어카드 테스트:")
            print("      python buffett_scorecard_calculator_fixed.py")
        
        return success
        
    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")
        return False

if __name__ == "__main__":
    main()
