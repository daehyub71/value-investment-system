#!/usr/bin/env python3
"""
DART API 제약사항 해결 - 최종 버전
3개월 제한 및 기업코드 우선 수집 방식
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
    from config import ConfigManager
    CONFIG_AVAILABLE = True
    print("✅ ConfigManager 임포트 성공")
except ImportError as e:
    print(f"❌ ConfigManager 임포트 실패: {e}")
    CONFIG_AVAILABLE = False

class FinalDartDataCollector:
    """DART API 제약사항 해결 - 최종 데이터 수집기"""
    
    def __init__(self):
        """초기화"""
        if CONFIG_AVAILABLE:
            try:
                self.config_manager = ConfigManager()
                self.logger = self.config_manager.get_logger('FinalDartCollector')
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
        self.logger = logging.getLogger('FinalDartCollector')
        
        self.api_key = os.getenv('DART_API_KEY', '').strip('"')
        self.base_url = "https://opendart.fss.or.kr/api"
        self.db_path = Path('data/databases/dart_data.db')
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def download_and_save_corp_codes(self):
        """기업코드 다운로드 및 저장 (1단계)"""
        try:
            self.logger.info("🏢 기업코드 다운로드 시작...")
            
            url = f"{self.base_url}/corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            if not response.content.startswith(b'PK'):
                self.logger.error("❌ ZIP 파일 다운로드 실패")
                return False
            
            self.logger.info(f"✅ ZIP 파일 다운로드 성공: {len(response.content)} bytes")
            
            # ZIP 파일 처리
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_files = [f for f in zip_file.namelist() if f.endswith('.xml')]
                
                if not xml_files:
                    self.logger.error("❌ ZIP에 XML 파일 없음")
                    return False
                
                with zip_file.open(xml_files[0]) as xml_file:
                    content = xml_file.read().decode('utf-8')
            
            self.logger.info("📋 XML 파싱 시작...")
            
            # XML 파싱
            root = ET.fromstring(content)
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
                        'modify_date': modify_date.text if modify_date is not None else '',
                        'created_date': datetime.now().strftime('%Y-%m-%d')
                    }
                    corp_list.append(corp_data)
            
            if not corp_list:
                self.logger.error("❌ 기업코드 파싱 실패")
                return False
            
            df = pd.DataFrame(corp_list)
            self.logger.info(f"✅ 기업코드 {len(df)}개 파싱 완료")
            
            # 데이터베이스 저장
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql('corp_codes', conn, if_exists='replace', index=False)
                self.logger.info(f"✅ corp_codes 테이블 저장 완료")
                
                # 삼성전자 확인
                samsung_query = "SELECT * FROM corp_codes WHERE stock_code = '005930'"
                samsung_data = pd.read_sql_query(samsung_query, conn)
                
                if not samsung_data.empty:
                    samsung_info = samsung_data.iloc[0]
                    self.logger.info(f"🎯 삼성전자 발견!")
                    self.logger.info(f"   기업명: {samsung_info['corp_name']}")
                    self.logger.info(f"   기업코드: {samsung_info['corp_code']}")
                    self.logger.info(f"   주식코드: {samsung_info['stock_code']}")
                    
                    return samsung_info['corp_code']  # 삼성전자 corp_code 반환
                else:
                    self.logger.warning("⚠️ 삼성전자를 찾을 수 없음")
                    return None
            
        except Exception as e:
            self.logger.error(f"❌ 기업코드 다운로드 실패: {e}")
            return False
    
    def collect_samsung_financial_with_corp_code(self, corp_code):
        """삼성전자 재무데이터 수집 (corp_code 사용)"""
        try:
            self.logger.info(f"💰 삼성전자 재무데이터 수집 시작 (corp_code: {corp_code})")
            
            # 여러 연도와 보고서 타입 시도
            year_report_combinations = [
                ('2023', '11011'),  # 2023년 사업보고서
                ('2023', '11012'),  # 2023년 반기보고서  
                ('2023', '11013'),  # 2023년 1분기보고서
                ('2022', '11011'),  # 2022년 사업보고서
                ('2024', '11013'),  # 2024년 1분기보고서
                ('2024', '11012'),  # 2024년 반기보고서
            ]
            
            all_financial_data = []
            success_count = 0
            
            for year, report_code in year_report_combinations:
                try:
                    self.logger.info(f"📊 시도: {year}년 보고서 타입 {report_code}")
                    
                    url = f"{self.base_url}/fnlttSinglAcntAll.json"
                    params = {
                        'crtfc_key': self.api_key,
                        'corp_code': corp_code,  # corp_code 사용으로 날짜 제한 없음
                        'bsns_year': year,
                        'reprt_code': report_code
                    }
                    
                    response = requests.get(url, params=params, timeout=30)
                    data = response.json()
                    
                    self.logger.info(f"   응답 상태: {data.get('status')} - {data.get('message', 'No message')}")
                    
                    if data.get('status') == '000' and 'list' in data and data['list']:
                        financial_df = pd.DataFrame(data['list'])
                        
                        # 추가 정보 컬럼
                        financial_df['stock_code'] = '005930'
                        financial_df['collect_year'] = year
                        financial_df['report_code'] = report_code
                        financial_df['collect_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        all_financial_data.append(financial_df)
                        success_count += 1
                        
                        self.logger.info(f"✅ {year}년 {report_code}: {len(financial_df)}건 수집 성공")
                        
                        # 주요 계정과목 확인
                        if len(financial_df) > 0:
                            sample_accounts = financial_df['account_nm'].unique()[:5]
                            self.logger.info(f"   주요 계정: {list(sample_accounts)}")
                        
                    else:
                        self.logger.warning(f"⚠️ {year}년 {report_code}: {data.get('message', 'No data')}")
                    
                    time.sleep(0.5)  # API 호출 제한 고려
                
                except Exception as e:
                    self.logger.warning(f"⚠️ {year}년 {report_code} 수집 실패: {e}")
                    continue
            
            if all_financial_data:
                # 모든 데이터 통합
                combined_df = pd.concat(all_financial_data, ignore_index=True)
                self.logger.info(f"🎉 총 {len(combined_df)}건 재무데이터 수집 완료 (성공: {success_count}개 보고서)")
                
                # 데이터베이스 저장
                with sqlite3.connect(self.db_path) as conn:
                    combined_df.to_sql('samsung_financial_statements', conn, if_exists='replace', index=False)
                    self.logger.info("✅ samsung_financial_statements 테이블 저장 완료")
                    
                    # 저장 확인
                    count_query = "SELECT COUNT(*) as count FROM samsung_financial_statements"
                    count_result = pd.read_sql_query(count_query, conn)
                    self.logger.info(f"✅ 저장 확인: {count_result.iloc[0]['count']}건")
                    
                    # 주요 계정과목별 데이터 확인
                    self._analyze_financial_data(conn)
                
                return True
            else:
                self.logger.error("❌ 모든 재무데이터 수집 시도 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 삼성전자 재무데이터 수집 실패: {e}")
            return False
    
    def _analyze_financial_data(self, conn):
        """수집된 재무데이터 분석"""
        try:
            self.logger.info("📈 수집된 재무데이터 분석...")
            
            # 주요 계정과목별 데이터 확인
            key_accounts = ['매출액', '영업이익', '당기순이익', '자산총계', '자본총계', '부채총계']
            
            for account in key_accounts:
                query = f"""
                SELECT account_nm, thstrm_amount, bsns_year, reprt_code 
                FROM samsung_financial_statements 
                WHERE account_nm LIKE '%{account}%'
                ORDER BY bsns_year DESC, reprt_code DESC
                LIMIT 3
                """
                
                result = pd.read_sql_query(query, conn)
                if not result.empty:
                    self.logger.info(f"✅ {account} 관련 데이터:")
                    for _, row in result.iterrows():
                        amount = row['thstrm_amount']
                        if pd.notna(amount) and str(amount).replace(',', '').replace('-', '').isdigit():
                            formatted_amount = f"{float(str(amount).replace(',', '')):,.0f}"
                            self.logger.info(f"   {row['bsns_year']}년: {formatted_amount}")
            
        except Exception as e:
            self.logger.warning(f"재무데이터 분석 중 오류: {e}")
    
    def test_recent_disclosures_with_3month_limit(self):
        """최근 3개월 공시 테스트 (제한사항 준수)"""
        try:
            self.logger.info("📋 최근 3개월 공시 테스트...")
            
            # 최근 3개월 범위 계산
            end_date = datetime.now()
            start_date = end_date - timedelta(days=90)  # 3개월
            
            url = f"{self.base_url}/list.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_cls': 'Y',  # 유가증권
                'bgn_de': start_date.strftime('%Y%m%d'),
                'end_de': end_date.strftime('%Y%m%d'),
                'page_no': 1,
                'page_count': 10
            }
            
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
            if data.get('status') == '000':
                self.logger.info("✅ 최근 3개월 공시 조회 성공")
                if 'list' in data and data['list']:
                    self.logger.info(f"✅ 공시 {len(data['list'])}건 확인")
                    return True
            else:
                self.logger.warning(f"⚠️ 공시 조회 실패: {data.get('message')}")
                
            return False
            
        except Exception as e:
            self.logger.error(f"❌ 공시 테스트 실패: {e}")
            return False
    
    def run_final_collection(self):
        """최종 데이터 수집 실행"""
        try:
            self.logger.info("🚀 DART 최종 데이터 수집 시작")
            print("\n" + "="*60)
            print("🎯 DART API 제약사항 해결 - 최종 수집 실행")
            print("="*60)
            
            # 1단계: 기업코드 다운로드 및 삼성전자 corp_code 확보
            samsung_corp_code = self.download_and_save_corp_codes()
            
            if not samsung_corp_code:
                self.logger.error("❌ 삼성전자 corp_code 확보 실패")
                return False
            
            # 2단계: 삼성전자 재무데이터 수집 (corp_code 사용)
            financial_success = self.collect_samsung_financial_with_corp_code(samsung_corp_code)
            
            if not financial_success:
                self.logger.error("❌ 삼성전자 재무데이터 수집 실패")
                return False
            
            # 3단계: 최근 공시 테스트 (선택사항)
            disclosure_success = self.test_recent_disclosures_with_3month_limit()
            
            # 성공 여부 종합
            if financial_success:
                self.logger.info("🎉 DART 데이터 수집 성공!")
                print("\n🎉 DART 데이터 수집 완료!")
                print("✅ 수집된 데이터:")
                print("   • corp_codes 테이블: 전체 기업코드")
                print("   • samsung_financial_statements 테이블: 삼성전자 재무데이터")
                print(f"   • 데이터베이스 위치: {self.db_path}")
                
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 최종 수집 실행 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    print("🚀 DART API 제약사항 해결 - 최종 데이터 수집기")
    print("=" * 60)
    print("📋 해결된 제약사항:")
    print("   • corp_code 없이는 검색기간 3개월 제한")
    print("   • 기업코드 우선 다운로드 후 개별 기업 데이터 수집")
    print("   • ZIP 파일 정상 다운로드 확인됨")
    
    try:
        collector = FinalDartDataCollector()
        success = collector.run_final_collection()
        
        if success:
            print("\n🎯 다음 단계:")
            print("   python buffett_scorecard_calculator_fixed.py")
            print("   (실제 DART 데이터로 워런 버핏 스코어카드 계산)")
            print("\n   python test_immediate_scorecard.py")
            print("   (전체 시스템 종합 테스트)")
            
        else:
            print("\n❌ DART 데이터 수집 실패")
            print("🔧 대안:")
            print("   python buffett_scorecard_calculator_fixed.py")
            print("   (기존 데이터로 스코어카드 테스트)")
        
        return success
        
    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")
        return False

if __name__ == "__main__":
    main()
