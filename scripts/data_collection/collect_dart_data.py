#!/usr/bin/env python3
"""
DART 재무데이터 수집 스크립트
DART Open API를 활용한 기업 재무제표 및 공시정보 수집

실행 방법:
python scripts/data_collection/collect_dart_data.py --year=2023 --quarter=4
python scripts/data_collection/collect_dart_data.py --corp_code=00126380 --all_years
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

from config import ConfigManager

class DartDataCollector:
    """DART 데이터 수집 클래스"""
    
    def __init__(self):
        # ConfigManager를 통한 통합 설정 관리
        self.config_manager = ConfigManager()
        self.logger = self.config_manager.get_logger('DartCollector')
        
        # API 설정 가져오기
        dart_config = self.config_manager.get_dart_config()
        self.api_key = dart_config.get('api_key')
        self.base_url = dart_config.get('base_url', "https://opendart.fss.or.kr/api")
        
        if not self.api_key:
            raise ValueError("DART API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
        
        self.logger.info("DART 데이터 수집기 초기화 완료")
    
    def download_corp_codes(self):
        """기업 고유번호 다운로드 및 파싱"""
        try:
            # DART 기업코드 ZIP 파일 다운로드
            url = f"{self.base_url}/corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            self.logger.info("기업코드 파일 다운로드 중...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # 응답 내용 확인
            self.logger.debug(f"응답 크기: {len(response.content)} bytes")
            self.logger.debug(f"Content-Type: {response.headers.get('content-type', 'unknown')}")
            
            # 응답이 JSON 에러인지 확인
            try:
                if response.headers.get('content-type', '').startswith('application/json'):
                    error_data = response.json()
                    if error_data.get('status') != '000':
                        error_msg = error_data.get('message', 'Unknown API error')
                        self.logger.error(f"DART API 오류: {error_msg}")
                        return pd.DataFrame()
            except:
                pass  # JSON이 아닌 경우 무시
            
            # ZIP 파일 여부 확인
            if not response.content.startswith(b'PK'):
                self.logger.error("응답이 ZIP 파일이 아닙니다.")
                self.logger.debug(f"응답 시작 부분: {response.content[:100]}")
                return pd.DataFrame()
            
            # ZIP 파일 압축 해제
            try:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    xml_content = zip_file.read('CORPCODE.xml')
            except zipfile.BadZipFile as e:
                self.logger.error(f"ZIP 파일 해제 실패: {e}")
                self.logger.debug(f"응답 내용: {response.content[:200]}")
                return pd.DataFrame()
            
            # XML 파싱
            root = ET.fromstring(xml_content)
            
            corp_data = []
            for corp in root.findall('list'):
                corp_info = {
                    'corp_code': corp.find('corp_code').text if corp.find('corp_code') is not None else '',
                    'corp_name': corp.find('corp_name').text if corp.find('corp_name') is not None else '',
                    'stock_code': corp.find('stock_code').text if corp.find('stock_code') is not None else '',
                    'modify_date': corp.find('modify_date').text if corp.find('modify_date') is not None else '',
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # 주식코드가 있는 상장기업만 수집
                if corp_info['stock_code']:
                    corp_data.append(corp_info)
            
            self.logger.info(f"기업코드 파싱 완료: {len(corp_data)}개 상장기업")
            return pd.DataFrame(corp_data)
            
        except Exception as e:
            self.logger.error(f"기업코드 다운로드 실패: {e}")
            return pd.DataFrame()
    
    def get_financial_statements(self, corp_code, bsns_year, reprt_code='11011'):
        """재무제표 데이터 수집"""
        try:
            url = f"{self.base_url}/fnlttSinglAcntAll.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': bsns_year,
                'reprt_code': reprt_code,  # 11011: 사업보고서, 11012: 반기보고서, 11013: 1분기, 11014: 3분기
                'fs_div': 'OFS'  # OFS: 개별재무제표, CFS: 연결재무제표
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') != '000':
                self.logger.warning(f"재무제표 조회 실패 ({corp_code}, {bsns_year}): {data.get('message', 'Unknown error')}")
                return pd.DataFrame()
            
            # 재무제표 데이터 처리
            financial_data = []
            for item in data.get('list', []):
                fs_info = {
                    'corp_code': corp_code,
                    'bsns_year': int(bsns_year),
                    'reprt_code': reprt_code,
                    'fs_div': item.get('fs_div', ''),
                    'fs_nm': item.get('fs_nm', ''),
                    'account_nm': item.get('account_nm', ''),
                    'thstrm_amount': self._parse_amount(item.get('thstrm_amount', '')),  # 당기금액
                    'frmtrm_amount': self._parse_amount(item.get('frmtrm_amount', '')),   # 전기금액
                    'bfefrmtrm_amount': self._parse_amount(item.get('bfefrmtrm_amount', '')),  # 전전기금액
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                financial_data.append(fs_info)
            
            self.logger.info(f"재무제표 수집 완료 ({corp_code}, {bsns_year}): {len(financial_data)}개 계정")
            return pd.DataFrame(financial_data)
            
        except Exception as e:
            self.logger.error(f"재무제표 수집 실패 ({corp_code}, {bsns_year}): {e}")
            return pd.DataFrame()
    
    def get_disclosures(self, corp_code, start_date, end_date, page_no=1, page_count=100):
        """공시정보 수집"""
        try:
            url = f"{self.base_url}/list.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bgn_de': start_date.replace('-', ''),  # YYYYMMDD 형식
                'end_de': end_date.replace('-', ''),
                'page_no': page_no,
                'page_count': page_count
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') != '000':
                self.logger.warning(f"공시정보 조회 실패 ({corp_code}): {data.get('message', 'Unknown error')}")
                return pd.DataFrame()
            
            # 공시정보 데이터 처리
            disclosure_data = []
            for item in data.get('list', []):
                disclosure_info = {
                    'corp_code': corp_code,
                    'corp_name': item.get('corp_name', ''),
                    'stock_code': item.get('stock_code', ''),
                    'report_nm': item.get('report_nm', ''),
                    'rcept_no': item.get('rcept_no', ''),
                    'flr_nm': item.get('flr_nm', ''),
                    'rcept_dt': item.get('rcept_dt', ''),
                    'rm': item.get('rm', ''),
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                disclosure_data.append(disclosure_info)
            
            self.logger.info(f"공시정보 수집 완료 ({corp_code}): {len(disclosure_data)}건")
            return pd.DataFrame(disclosure_data)
            
        except Exception as e:
            self.logger.error(f"공시정보 수집 실패 ({corp_code}): {e}")
            return pd.DataFrame()
    
    def _parse_amount(self, amount_str):
        """금액 문자열을 숫자로 변환"""
        if not amount_str or amount_str == '-':
            return None
        
        try:
            # 콤마 제거하고 숫자로 변환
            clean_amount = amount_str.replace(',', '').replace('(', '-').replace(')', '')
            return float(clean_amount)
        except:
            return None
    
    def save_to_database(self, corp_data=None, financial_data=None, disclosure_data=None):
        """데이터베이스에 저장"""
        try:
            conn = self.config_manager.get_database_connection('dart')
            
            # 기업코드 저장
            if corp_data is not None and not corp_data.empty:
                corp_data.to_sql('corp_codes', conn, if_exists='replace', index=False)
                self.logger.info(f"기업코드 저장 완료: {len(corp_data)}건")
            
            # 재무제표 저장
            if financial_data is not None and not financial_data.empty:
                financial_data.to_sql('financial_statements', conn, if_exists='append', index=False)
                self.logger.info(f"재무제표 저장 완료: {len(financial_data)}건")
            
            # 공시정보 저장
            if disclosure_data is not None and not disclosure_data.empty:
                disclosure_data.to_sql('disclosures', conn, if_exists='append', index=False)
                self.logger.info(f"공시정보 저장 완료: {len(disclosure_data)}건")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"데이터베이스 저장 실패: {e}")
            return False
    
    def collect_corp_codes(self):
        """기업코드 수집 및 저장"""
        self.logger.info("기업코드 수집 시작")
        
        corp_data = self.download_corp_codes()
        if not corp_data.empty:
            success = self.save_to_database(corp_data=corp_data)
            if success:
                self.logger.info("✅ 기업코드 수집 완료")
                return True
        
        return False
    
    def collect_financial_data(self, corp_code=None, year=None, quarter=None):
        """재무데이터 수집"""
        try:
            # 기업코드가 지정되지 않으면 전체 상장기업 대상
            if corp_code is None:
                conn = self.config_manager.get_database_connection('dart')
                corp_df = pd.read_sql("SELECT corp_code, corp_name FROM corp_codes WHERE stock_code != ''", conn)
                conn.close()
            else:
                corp_df = pd.DataFrame([{'corp_code': corp_code, 'corp_name': ''}])
            
            if corp_df.empty:
                self.logger.error("대상 기업이 없습니다. 먼저 기업코드를 수집하세요.")
                return False
            
            # 연도/분기 설정
            years = [year] if year else [2023, 2022, 2021]
            reprt_codes = {
                1: '11013',  # 1분기
                2: '11012',  # 반기
                3: '11014',  # 3분기  
                4: '11011'   # 사업보고서
            }
            
            if quarter:
                reprt_codes = {quarter: reprt_codes[quarter]}
            
            total_count = len(corp_df) * len(years) * len(reprt_codes)
            current_count = 0
            
            for _, corp_row in corp_df.iterrows():
                corp_code = corp_row['corp_code']
                corp_name = corp_row['corp_name']
                
                for year in years:
                    for quarter, reprt_code in reprt_codes.items():
                        current_count += 1
                        
                        self.logger.info(f"진행률: {current_count}/{total_count} - {corp_name}({corp_code}) {year}년 {quarter}분기")
                        
                        # 재무제표 수집
                        financial_data = self.get_financial_statements(corp_code, year, reprt_code)
                        
                        if not financial_data.empty:
                            self.save_to_database(financial_data=financial_data)
                        
                        # API 호출 제한 대응 (초당 10회 제한)
                        time.sleep(0.1)
            
            self.logger.info("✅ 재무데이터 수집 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"재무데이터 수집 실패: {e}")
            return False
    
    def collect_disclosure_data(self, corp_code=None, days=90):
        """공시정보 수집 (company_info 테이블 기반 전체 종목)"""
        try:
            # 기업코드가 지정되지 않으면 company_info 테이블의 전체 종목 대상
            if corp_code is None:
                # company_info 테이블에서 전체 종목 가져오기
                stock_conn = self.config_manager.get_database_connection('stock')
                company_df = pd.read_sql("SELECT stock_code, company_name FROM company_info", stock_conn)
                stock_conn.close()
                
                if company_df.empty:
                    self.logger.error("company_info 테이블에 데이터가 없습니다.")
                    return False
                
                # corp_codes 테이블에서 stock_code → corp_code 매핑 가져오기
                dart_conn = self.config_manager.get_database_connection('dart')
                corp_mapping_df = pd.read_sql("SELECT stock_code, corp_code, corp_name FROM corp_codes WHERE stock_code != ''", dart_conn)
                dart_conn.close()
                
                if corp_mapping_df.empty:
                    self.logger.error("corp_codes 테이블에 데이터가 없습니다. 먼저 기업코드를 수집하세요.")
                    return False
                
                # stock_code 기준으로 매핑
                merged_df = company_df.merge(corp_mapping_df, on='stock_code', how='inner')
                
                if merged_df.empty:
                    self.logger.error("company_info와 corp_codes 테이블 간 매핑 데이터가 없습니다.")
                    return False
                
                self.logger.info(f"📊 전체 수집 대상: company_info 테이블 {len(company_df):,}개 → corp_codes 매핑 {len(merged_df):,}개 종목")
                target_companies = merged_df[['corp_code', 'corp_name', 'stock_code']].to_dict('records')
                
            else:
                # 특정 기업코드 지정된 경우
                target_companies = [{'corp_code': corp_code, 'corp_name': '', 'stock_code': ''}]
            
            # 날짜 범위 설정 (3개월 = 90일)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            self.logger.info(f"📅 공시정보 수집 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} ({days}일)")
            
            success_count = 0
            total_disclosures = 0
            
            for idx, company in enumerate(target_companies):
                corp_code = company['corp_code']
                corp_name = company['corp_name']
                stock_code = company.get('stock_code', '')
                
                self.logger.info(f"📈 진행률: {idx+1:,}/{len(target_companies):,} - {corp_name}({stock_code}) 공시정보 수집")
                
                try:
                    # 공시정보 수집
                    disclosure_data = self.get_disclosures(
                        corp_code, 
                        start_date.strftime('%Y-%m-%d'),
                        end_date.strftime('%Y-%m-%d')
                    )
                    
                    if not disclosure_data.empty:
                        if self.save_to_database(disclosure_data=disclosure_data):
                            success_count += 1
                            total_disclosures += len(disclosure_data)
                            self.logger.debug(f"✅ {corp_name}: {len(disclosure_data)}건 수집")
                        else:
                            self.logger.warning(f"⚠️ {corp_name}: 데이터베이스 저장 실패")
                    else:
                        self.logger.debug(f"📄 {corp_name}: 공시정보 없음")
                        
                except Exception as e:
                    self.logger.error(f"❌ {corp_name}({corp_code}) 공시정보 수집 실패: {e}")
                    continue
                
                # API 호출 제한 대응 (초당 10회 제한)
                time.sleep(0.15)
                
                # 진행률 로그 (100개씩)
                if (idx + 1) % 100 == 0:
                    self.logger.info(f"🔄 중간 현황: {idx+1:,}/{len(target_companies):,} 처리완료, 성공: {success_count:,}개, 총 공시: {total_disclosures:,}건")
            
            self.logger.info(f"✅ 공시정보 수집 완료")
            self.logger.info(f"📊 최종 결과: 대상 {len(target_companies):,}개 → 성공 {success_count:,}개 → 총 공시 {total_disclosures:,}건")
            return True
            
        except Exception as e:
            self.logger.error(f"공시정보 수집 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='DART 재무데이터 수집 스크립트')
    parser.add_argument('--corp_codes', action='store_true', help='기업코드 수집')
    parser.add_argument('--financial', action='store_true', help='재무데이터 수집')
    parser.add_argument('--disclosures', action='store_true', help='공시정보 수집')
    parser.add_argument('--corp_code', type=str, help='특정 기업코드 (8자리)')
    parser.add_argument('--year', type=int, help='수집 연도')
    parser.add_argument('--quarter', type=int, choices=[1, 2, 3, 4], help='수집 분기')
    parser.add_argument('--days', type=int, default=90, help='공시정보 수집 기간 (일수, 기본값: 90일 = 3개월)')
    parser.add_argument('--all', action='store_true', help='전체 데이터 수집 (기업코드+재무+공시)')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 수집기 초기화
    try:
        collector = DartDataCollector()
        logger = collector.logger
        
        # 로그 레벨 설정
        if args.log_level:
            logging.getLogger().setLevel(getattr(logging, args.log_level))
            logger.setLevel(getattr(logging, args.log_level))
        
    except ValueError as e:
        print(f"❌ 초기화 실패: {e}")
        sys.exit(1)
    
    try:
        if args.all:
            # 전체 데이터 수집
            logger.info("전체 DART 데이터 수집 시작")
            
            # 1. 기업코드 수집
            if not collector.collect_corp_codes():
                logger.error("기업코드 수집 실패")
                sys.exit(1)
            
            # 2. 재무데이터 수집
            if not collector.collect_financial_data(year=args.year, quarter=args.quarter):
                logger.error("재무데이터 수집 실패")
                sys.exit(1)
            
            # 3. 공시정보 수집
            if not collector.collect_disclosure_data(days=args.days):
                logger.error("공시정보 수집 실패")
                sys.exit(1)
            
            logger.info("✅ 전체 DART 데이터 수집 완료")
            
        elif args.corp_codes:
            # 기업코드만 수집
            if collector.collect_corp_codes():
                logger.info("✅ 기업코드 수집 성공")
            else:
                logger.error("❌ 기업코드 수집 실패")
                sys.exit(1)
                
        elif args.financial:
            # 재무데이터만 수집
            if collector.collect_financial_data(args.corp_code, args.year, args.quarter):
                logger.info("✅ 재무데이터 수집 성공")
            else:
                logger.error("❌ 재무데이터 수집 실패")
                sys.exit(1)
                
        elif args.disclosures:
            # 공시정보만 수집
            if collector.collect_disclosure_data(args.corp_code, args.days):
                logger.info("✅ 공시정보 수집 성공")
            else:
                logger.error("❌ 공시정보 수집 실패")
                sys.exit(1)
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
