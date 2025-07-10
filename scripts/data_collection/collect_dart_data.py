# =============================================================================
# 2. scripts/data_collection/collect_dart_data.py
# =============================================================================

#!/usr/bin/env python3
"""
DART 재무데이터 수집 스크립트
실행: python scripts/data_collection/collect_dart_data.py
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import requests
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import sqlite3
import time
from io import BytesIO
from datetime import datetime
import logging
from config import get_dart_config, get_db_connection

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DartCollector:
    def __init__(self):
        self.dart_config = get_dart_config()
        self.api_key = self.dart_config['api_key']
        self.base_url = "https://opendart.fss.or.kr/api/"
        
        if not self.api_key:
            raise ValueError("DART API KEY가 설정되지 않았습니다. .env 파일을 확인해주세요.")
    
    def collect_corp_codes(self):
        """기업 고유번호 수집"""
        try:
            logger.info("DART 기업 고유번호 수집 시작...")
            
            url = f"{self.base_url}corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            # ZIP 파일 처리
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                xml_content = zip_file.read('CORPCODE.xml')
            
            # XML 파싱
            root = ET.fromstring(xml_content)
            
            corp_data = []
            for item in root.findall('list'):
                corp_code = item.find('corp_code').text if item.find('corp_code') is not None else ''
                corp_name = item.find('corp_name').text if item.find('corp_name') is not None else ''
                stock_code = item.find('stock_code').text if item.find('stock_code') is not None else ''
                modify_date = item.find('modify_date').text if item.find('modify_date') is not None else ''
                
                corp_data.append({
                    'corp_code': corp_code,
                    'corp_name': corp_name,
                    'stock_code': stock_code if stock_code and stock_code.strip() else None,
                    'modify_date': modify_date
                })
            
            corp_df = pd.DataFrame(corp_data)
            
            # 상장기업만 필터링
            corp_df = corp_df[corp_df['stock_code'].notna()]
            
            logger.info(f"기업 고유번호 수집 완료: {len(corp_df)}개 기업")
            return corp_df
            
        except Exception as e:
            logger.error(f"기업 고유번호 수집 실패: {e}")
            return pd.DataFrame()
    
    def collect_financial_statements(self, corp_code, business_year, report_code='11011'):
        """재무제표 데이터 수집"""
        try:
            url = f"{self.base_url}fnlttSinglAcntAll.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': business_year,
                'reprt_code': report_code
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data['status'] != '000':
                logger.warning(f"재무제표 수집 실패 ({corp_code}): {data.get('message', 'Unknown error')}")
                return pd.DataFrame()
            
            financial_data = []
            for item in data['list']:
                financial_data.append({
                    'corp_code': corp_code,
                    'bsns_year': business_year,
                    'reprt_code': report_code,
                    'account_nm': item['account_nm'],
                    'thstrm_amount': self._parse_amount(item.get('thstrm_amount', '0')),
                    'frmtrm_amount': self._parse_amount(item.get('frmtrm_amount', '0')),
                    'bfefrmtrm_amount': self._parse_amount(item.get('bfefrmtrm_amount', '0'))
                })
            
            return pd.DataFrame(financial_data)
            
        except Exception as e:
            logger.error(f"재무제표 수집 실패 ({corp_code}): {e}")
            return pd.DataFrame()
    
    def _parse_amount(self, amount_str):
        """금액 문자열을 정수로 변환"""
        if not amount_str or amount_str == '-':
            return 0
        
        try:
            return int(amount_str.replace(',', ''))
        except ValueError:
            return 0
    
    def save_corp_codes_to_db(self, corp_codes):
        """기업 고유번호를 데이터베이스에 저장"""
        try:
            with get_db_connection('dart') as conn:
                conn.execute("DELETE FROM corp_codes")
                corp_codes.to_sql('corp_codes', conn, if_exists='append', index=False)
            
            logger.info(f"기업 고유번호 DB 저장 완료: {len(corp_codes)}개")
            return True
            
        except Exception as e:
            logger.error(f"기업 고유번호 DB 저장 실패: {e}")
            return False
    
    def collect_all_financial_data(self, years=None):
        """전체 상장기업 재무데이터 수집"""
        try:
            if years is None:
                current_year = datetime.now().year
                years = [str(current_year - i) for i in range(5)]
            
            # 기업 고유번호 가져오기
            with get_db_connection('dart') as conn:
                corp_codes = pd.read_sql(
                    "SELECT corp_code, stock_code FROM corp_codes WHERE stock_code IS NOT NULL", 
                    conn
                )
            
            if corp_codes.empty:
                logger.error("기업 고유번호가 없습니다. 먼저 기업 고유번호를 수집해주세요.")
                return False
            
            total_count = len(corp_codes) * len(years)
            current_count = 0
            success_count = 0
            
            for _, row in corp_codes.iterrows():
                corp_code = row['corp_code']
                stock_code = row['stock_code']
                
                for year in years:
                    current_count += 1
                    logger.info(f"재무데이터 수집: {current_count}/{total_count} - {stock_code} ({year})")
                    
                    financial_data = self.collect_financial_statements(corp_code, year)
                    
                    if not financial_data.empty:
                        with get_db_connection('dart') as conn:
                            conn.execute(
                                "DELETE FROM financial_statements WHERE corp_code = ? AND bsns_year = ?", 
                                (corp_code, year)
                            )
                            financial_data.to_sql('financial_statements', conn, if_exists='append', index=False)
                        success_count += 1
                    
                    # API 호출 제한
                    time.sleep(1.0)
            
            logger.info(f"전체 재무데이터 수집 완료: {success_count}/{total_count}")
            return True
            
        except Exception as e:
            logger.error(f"전체 재무데이터 수집 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        collector = DartCollector()
        
        print("🏢 DART 데이터 수집을 시작합니다...")
        print("\n1. 기업 고유번호 수집 중...")
        
        # 기업 고유번호 수집
        corp_codes = collector.collect_corp_codes()
        
        if corp_codes.empty:
            print("❌ 기업 고유번호 수집 실패!")
            return False
        
        # 데이터베이스에 저장
        collector.save_corp_codes_to_db(corp_codes)
        print(f"✅ 기업 고유번호 수집 완료: {len(corp_codes)}개 기업")
        
        print("\n2. 재무제표 데이터 수집 중...")
        print("⚠️  이 작업은 시간이 오래 걸립니다 (2-3시간)...")
        
        # 사용자 확인
        user_input = input("계속 진행하시겠습니까? (y/N): ")
        if user_input.lower() != 'y':
            print("재무데이터 수집을 건너뜁니다.")
            return True
        
        # 재무데이터 수집
        success = collector.collect_all_financial_data()
        
        if success:
            print("✅ DART 데이터 수집 완료!")
        else:
            print("❌ 재무데이터 수집 실패!")
            
        return success
        
    except Exception as e:
        logger.error(f"DART 데이터 수집 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)