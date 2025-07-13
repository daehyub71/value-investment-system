#!/usr/bin/env python3
"""
개선된 DART 데이터 수집기
Rate Limiting, 재시도 로직, 배치 처리가 적용된 안정적인 데이터 수집

사용법:
python improved_dart_collector.py --companies=10 --delay=2
"""

import sys
import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import logging
from functools import wraps

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import ConfigManager

class RateLimiter:
    """API 호출 제한 관리"""
    
    def __init__(self, calls_per_minute: int = 60):
        self.calls_per_minute = calls_per_minute
        self.calls = []
        self.min_interval = 60.0 / calls_per_minute  # 최소 간격 (초)
        
    def wait_if_needed(self):
        """필요시 대기"""
        now = time.time()
        
        # 1분 이내의 호출만 유지
        self.calls = [call_time for call_time in self.calls if now - call_time < 60]
        
        if len(self.calls) >= self.calls_per_minute:
            # 가장 오래된 호출로부터 60초 대기
            sleep_time = 60 - (now - self.calls[0]) + 1
            if sleep_time > 0:
                print(f"⏱️ API 한도 관리를 위해 {sleep_time:.1f}초 대기...")
                time.sleep(sleep_time)
        
        # 최소 간격 적용
        if self.calls:
            time_since_last = now - self.calls[-1]
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                time.sleep(sleep_time)
    
    def record_call(self):
        """호출 기록"""
        self.calls.append(time.time())

def retry_on_failure(max_retries: int = 3, delay: float = 5.0):
    """실패 시 재시도 데코레이터"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = delay * (2 ** attempt)  # 지수 백오프
                        print(f"⚠️ 재시도 {attempt + 1}/{max_retries}: {e}")
                        print(f"⏱️ {wait_time}초 후 재시도...")
                        time.sleep(wait_time)
                    else:
                        print(f"❌ 재시도 실패: {e}")
            
            raise last_exception
        return wrapper
    return decorator

class ImprovedDartCollector:
    """개선된 DART 데이터 수집기"""
    
    def __init__(self, request_delay: float = 1.5):
        self.config_manager = ConfigManager()
        self.logger = self.config_manager.get_logger('ImprovedDartCollector')
        
        # API 설정
        dart_config = self.config_manager.get_dart_config()
        self.api_key = dart_config.get('api_key')
        self.base_url = "https://opendart.fss.or.kr/api"
        
        if not self.api_key:
            raise ValueError("DART API 키가 설정되지 않았습니다.")
        
        # Rate Limiter 초기화 (분당 40회로 보수적 설정)
        self.rate_limiter = RateLimiter(calls_per_minute=40)
        self.request_delay = request_delay
        
        self.logger.info("개선된 DART 데이터 수집기 초기화 완료")
    
    @retry_on_failure(max_retries=3, delay=2.0)
    def safe_api_call(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """안전한 API 호출"""
        # Rate limiting 적용
        self.rate_limiter.wait_if_needed()
        
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            # 호출 기록
            self.rate_limiter.record_call()
            
            data = response.json()
            
            # API 응답 상태 확인
            status = data.get('status', 'unknown')
            message = data.get('message', '')
            
            if status == '020':  # 사용한도 초과
                raise Exception(f"사용한도 초과: {message}")
            elif status == '011':  # API 키 오류
                raise Exception(f"API 키 오류: {message}")
            elif status != '000':  # 기타 오류
                raise Exception(f"API 오류 ({status}): {message}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"네트워크 오류: {e}")
        except Exception as e:
            # 추가 대기 시간 적용
            time.sleep(self.request_delay)
            raise e
    
    def get_company_outline(self, corp_code: str) -> Optional[Dict[str, Any]]:
        """기업 개요 정보 수집"""
        try:
            url = f"{self.base_url}/company.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code
            }
            
            data = self.safe_api_call(url, params)
            
            if data and data.get('status') == '000':
                return data
            
            return None
            
        except Exception as e:
            self.logger.warning(f"기업개요 조회 실패 ({corp_code}): {e}")
            return None
    
    def get_financial_statements_safe(self, corp_code: str, bsns_year: int) -> pd.DataFrame:
        """안전한 재무제표 수집"""
        try:
            url = f"{self.base_url}/fnlttSinglAcntAll.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': str(bsns_year),
                'reprt_code': '11011',  # 사업보고서
                'fs_div': 'OFS'  # 개별재무제표
            }
            
            data = self.safe_api_call(url, params)
            
            if not data or not data.get('list'):
                return pd.DataFrame()
            
            # 재무제표 데이터 처리
            financial_data = []
            for item in data['list']:
                fs_info = {
                    'corp_code': corp_code,
                    'bsns_year': bsns_year,
                    'reprt_code': '11011',
                    'fs_div': item.get('fs_div', ''),
                    'fs_nm': item.get('fs_nm', ''),
                    'account_nm': item.get('account_nm', ''),
                    'thstrm_amount': self._parse_amount(item.get('thstrm_amount', '')),
                    'frmtrm_amount': self._parse_amount(item.get('frmtrm_amount', '')),
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                financial_data.append(fs_info)
            
            return pd.DataFrame(financial_data)
            
        except Exception as e:
            self.logger.warning(f"재무제표 조회 실패 ({corp_code}, {bsns_year}): {e}")
            return pd.DataFrame()
    
    def _parse_amount(self, amount_str: str) -> Optional[float]:
        """금액 문자열을 숫자로 변환"""
        if not amount_str or amount_str == '-':
            return None
        
        try:
            clean_amount = amount_str.replace(',', '').replace('(', '-').replace(')', '')
            return float(clean_amount)
        except:
            return None
    
    def collect_limited_data(self, max_companies: int = 20, target_year: int = 2023):
        """제한된 수의 기업 데이터 수집"""
        try:
            self.logger.info(f"제한된 데이터 수집 시작: 최대 {max_companies}개 기업")
            
            # 기업 목록 가져오기
            conn = self.config_manager.get_database_connection('dart')
            query = """
            SELECT corp_code, corp_name, stock_code 
            FROM corp_codes 
            WHERE stock_code != '' 
            ORDER BY corp_name 
            LIMIT ?
            """
            corp_df = pd.read_sql(query, conn, params=(max_companies,))
            conn.close()
            
            if corp_df.empty:
                self.logger.error("대상 기업이 없습니다.")
                return False
            
            self.logger.info(f"수집 대상: {len(corp_df)}개 기업, {target_year}년 데이터")
            
            success_count = 0
            total_financial_records = 0
            
            for idx, corp_row in corp_df.iterrows():
                corp_code = corp_row['corp_code']
                corp_name = corp_row['corp_name']
                
                print(f"\n📊 진행률: {idx+1}/{len(corp_df)} - {corp_name}")
                
                try:
                    # 1. 기업 개요 수집
                    outline_data = self.get_company_outline(corp_code)
                    if outline_data:
                        print(f"  ✅ 기업개요 수집 완료")
                    
                    # 2. 재무제표 수집
                    financial_data = self.get_financial_statements_safe(corp_code, target_year)
                    
                    if not financial_data.empty:
                        # 데이터베이스 저장
                        conn = self.config_manager.get_database_connection('dart')
                        
                        # 재무제표 저장
                        financial_data.to_sql('financial_statements', conn, 
                                            if_exists='append', index=False)
                        
                        # 기업개요 저장 (있는 경우)
                        if outline_data:
                            outline_df = pd.DataFrame([{
                                'corp_code': corp_code,
                                'corp_name': outline_data.get('corp_name', ''),
                                'corp_eng_name': outline_data.get('corp_eng_name', ''),
                                'stock_name': outline_data.get('stock_name', ''),
                                'stock_code': outline_data.get('stock_code', ''),
                                'ceo_nm': outline_data.get('ceo_nm', ''),
                                'corp_cls': outline_data.get('corp_cls', ''),
                                'jurir_no': outline_data.get('jurir_no', ''),
                                'bizr_no': outline_data.get('bizr_no', ''),
                                'adres': outline_data.get('adres', ''),
                                'hm_url': outline_data.get('hm_url', ''),
                                'ir_url': outline_data.get('ir_url', ''),
                                'phn_no': outline_data.get('phn_no', ''),
                                'fax_no': outline_data.get('fax_no', ''),
                                'induty_code': outline_data.get('induty_code', ''),
                                'est_dt': outline_data.get('est_dt', ''),
                                'acc_mt': outline_data.get('acc_mt', ''),
                                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }])
                            outline_df.to_sql('company_outlines', conn, 
                                            if_exists='append', index=False)
                        
                        conn.commit()
                        conn.close()
                        
                        success_count += 1
                        total_financial_records += len(financial_data)
                        print(f"  ✅ 재무데이터 저장: {len(financial_data)}건")
                    else:
                        print(f"  ⚠️ 재무데이터 없음")
                
                except Exception as e:
                    print(f"  ❌ 처리 실패: {e}")
                    continue
                
                # 진행상황 출력
                if (idx + 1) % 5 == 0:
                    print(f"\n📈 중간 결과: {success_count}/{idx+1}개 기업 성공, {total_financial_records}건 수집")
            
            # 최종 결과
            print(f"\n🎉 수집 완료!")
            print(f"📊 성공: {success_count}/{len(corp_df)}개 기업")
            print(f"📋 총 재무데이터: {total_financial_records}건")
            
            self.logger.info(f"제한된 데이터 수집 완료: {success_count}/{len(corp_df)}개 기업")
            return True
            
        except Exception as e:
            self.logger.error(f"데이터 수집 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='개선된 DART 데이터 수집기')
    parser.add_argument('--companies', type=int, default=10, 
                       help='수집할 기업 수 (기본: 10개)')
    parser.add_argument('--year', type=int, default=2023,
                       help='수집할 연도 (기본: 2023)')
    parser.add_argument('--delay', type=float, default=1.5,
                       help='요청 간격 (초, 기본: 1.5)')
    
    args = parser.parse_args()
    
    try:
        print(f"🚀 개선된 DART 데이터 수집기 시작")
        print(f"📋 설정: {args.companies}개 기업, {args.year}년, {args.delay}초 간격")
        print("=" * 60)
        
        collector = ImprovedDartCollector(request_delay=args.delay)
        
        success = collector.collect_limited_data(
            max_companies=args.companies,
            target_year=args.year
        )
        
        if success:
            print("\n✅ 데이터 수집이 성공적으로 완료되었습니다!")
        else:
            print("\n❌ 데이터 수집에 실패했습니다.")
            
    except Exception as e:
        print(f"\n❌ 실행 실패: {e}")

if __name__ == "__main__":
    main()