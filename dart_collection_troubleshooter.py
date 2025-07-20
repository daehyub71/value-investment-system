#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DART 수집 문제 진단 및 해결 스크립트
현재 발생한 "모든 기업에서 데이터 없음" 문제를 분석하고 해결

주요 기능:
1. API 키 유효성 검사
2. 일일 호출 한도 확인
3. 네트워크 연결 테스트
4. 수집된 데이터 현황 분석
5. 해결 방안 제시
"""

import os
import sys
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import logging
from dotenv import load_dotenv

load_dotenv()

class DartTroubleshooter:
    """DART 수집 문제 진단 도구"""
    
    def __init__(self):
        self.setup_logging()
        self.api_key = os.getenv('DART_API_KEY')
        self.base_url = "https://opendart.fss.or.kr/api"
        
        # 데이터베이스 경로
        self.db_path = Path('data/databases')
        self.dart_db_path = self.db_path / 'dart_data.db'
        self.stock_db_path = self.db_path / 'stock_data.db'
    
    def setup_logging(self):
        """로깅 설정"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('DartTroubleshooter')
    
    def check_api_key_validity(self):
        """API 키 유효성 검사"""
        print("🔑 DART API 키 유효성 검사 중...")
        
        if not self.api_key:
            return False, "DART_API_KEY 환경변수가 설정되지 않았습니다."
        
        if len(self.api_key) != 40:
            return False, f"API 키 길이가 올바르지 않습니다. (현재: {len(self.api_key)}자, 필요: 40자)"
        
        # 간단한 API 호출 테스트
        try:
            test_url = f"{self.base_url}/corpCode.xml"
            response = requests.get(test_url, params={
                'crtfc_key': self.api_key
            }, timeout=10)
            
            if response.status_code == 200:
                if "api_key" in response.text.lower() and "invalid" in response.text.lower():
                    return False, "API 키가 유효하지 않습니다."
                elif len(response.content) > 1000:  # XML 데이터가 있다면
                    return True, "API 키가 유효합니다."
                else:
                    return False, "API 응답이 비어있습니다."
            elif response.status_code == 429:
                return False, "API 호출 한도를 초과했습니다. (429 Too Many Requests)"
            elif response.status_code == 403:
                return False, "API 접근이 거부되었습니다. (403 Forbidden)"
            else:
                return False, f"API 호출 실패 (HTTP {response.status_code}): {response.text[:200]}"
                
        except requests.exceptions.Timeout:
            return False, "API 호출 시간 초과 (네트워크 연결 확인 필요)"
        except requests.exceptions.ConnectionError:
            return False, "네트워크 연결 오류 (인터넷 연결 확인 필요)"
        except Exception as e:
            return False, f"API 테스트 중 오류: {str(e)}"
    
    def check_call_limits(self):
        """API 호출 한도 확인"""
        print("📊 API 호출 현황 분석 중...")
        
        # 수집된 데이터를 통해 오늘 호출 횟수 추정
        call_info = {
            'estimated_calls_today': 0,
            'successful_companies': 0,
            'failed_companies': 0,
            'last_successful_time': None,
            'problem_start_time': None
        }
        
        try:
            if self.dart_db_path.exists():
                with sqlite3.connect(self.dart_db_path) as conn:
                    # 오늘 수집된 데이터 확인
                    today = datetime.now().strftime('%Y-%m-%d')
                    
                    today_query = """
                    SELECT COUNT(DISTINCT corp_code) as companies,
                           COUNT(*) as records,
                           MIN(created_at) as first_record,
                           MAX(created_at) as last_record
                    FROM financial_statements 
                    WHERE DATE(created_at) = ?
                    """
                    
                    today_df = pd.read_sql_query(today_query, conn, params=[today])
                    
                    if not today_df.empty and today_df.iloc[0]['companies'] > 0:
                        call_info['successful_companies'] = today_df.iloc[0]['companies']
                        call_info['last_successful_time'] = today_df.iloc[0]['last_record']
                        # 기업당 평균 3-5회 API 호출 추정
                        call_info['estimated_calls_today'] = call_info['successful_companies'] * 4
                    
        except Exception as e:
            self.logger.error(f"호출 한도 확인 중 오류: {e}")
        
        return call_info
    
    def test_specific_companies(self):
        """특정 회사들로 API 테스트"""
        print("🧪 주요 기업들로 API 테스트 중...")
        
        # 테스트할 주요 기업들 (corp_code 알려진 것들)
        test_companies = [
            ('삼성전자', '00126380'),
            ('SK하이닉스', '00164779'),
            ('LG에너지솔루션', '00256627'),
            ('카카오', '00193697'),
            ('NAVER', '00167896')
        ]
        
        test_results = []
        
        for company_name, corp_code in test_companies:
            try:
                print(f"   테스트 중: {company_name} ({corp_code})")
                
                # 재무제표 API 호출 테스트
                url = f"{self.base_url}/fnlttSinglAcntAll.json"
                params = {
                    'crtfc_key': self.api_key,
                    'corp_code': corp_code,
                    'bsns_year': '2023',
                    'reprt_code': '11011'  # 사업보고서
                }
                
                response = requests.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('status') == '000':  # 성공
                        records_count = len(data.get('list', []))
                        test_results.append({
                            'company': company_name,
                            'status': 'SUCCESS',
                            'records': records_count,
                            'message': f"{records_count}개 재무 항목 수집"
                        })
                    elif data.get('status') == '013':  # 데이터 없음
                        test_results.append({
                            'company': company_name,
                            'status': 'NO_DATA',
                            'records': 0,
                            'message': "해당 기간 데이터 없음 (정상)"
                        })
                    elif data.get('status') == '020':  # 호출 한도 초과
                        test_results.append({
                            'company': company_name,
                            'status': 'RATE_LIMIT',
                            'records': 0,
                            'message': "API 호출 한도 초과!"
                        })
                        break  # 더 이상 테스트 불필요
                    else:
                        test_results.append({
                            'company': company_name,
                            'status': 'ERROR',
                            'records': 0,
                            'message': f"API 오류: {data.get('message', 'Unknown')}"
                        })
                else:
                    test_results.append({
                        'company': company_name,
                        'status': 'HTTP_ERROR',
                        'records': 0,
                        'message': f"HTTP {response.status_code}"
                    })
                
                time.sleep(1)  # API 호출 간격
                
            except Exception as e:
                test_results.append({
                    'company': company_name,
                    'status': 'EXCEPTION',
                    'records': 0,
                    'message': str(e)[:50]
                })
        
        return test_results
    
    def analyze_collection_progress(self):
        """수집 진행률 분석"""
        print("📈 수집 진행률 분석 중...")
        
        progress = {
            'total_companies': 0,
            'companies_with_data': 0,
            'total_financial_records': 0,
            'collection_rate': 0,
            'recent_activity': []
        }
        
        try:
            # 전체 기업 수
            if self.stock_db_path.exists():
                with sqlite3.connect(self.stock_db_path) as conn:
                    total_query = "SELECT COUNT(*) as count FROM company_info"
                    total_df = pd.read_sql_query(total_query, conn)
                    progress['total_companies'] = total_df.iloc[0]['count']
            
            # 수집된 데이터 분석
            if self.dart_db_path.exists():
                with sqlite3.connect(self.dart_db_path) as conn:
                    # 데이터가 있는 기업 수
                    companies_query = """
                    SELECT COUNT(DISTINCT corp_code) as companies,
                           COUNT(*) as total_records
                    FROM financial_statements
                    """
                    companies_df = pd.read_sql_query(companies_query, conn)
                    
                    if not companies_df.empty:
                        progress['companies_with_data'] = companies_df.iloc[0]['companies']
                        progress['total_financial_records'] = companies_df.iloc[0]['total_records']
                    
                    # 수집률 계산
                    if progress['total_companies'] > 0:
                        progress['collection_rate'] = (progress['companies_with_data'] / progress['total_companies']) * 100
                    
                    # 최근 활동 분석
                    recent_query = """
                    SELECT DATE(created_at) as date, 
                           COUNT(DISTINCT corp_code) as companies,
                           COUNT(*) as records
                    FROM financial_statements 
                    WHERE created_at >= date('now', '-7 days')
                    GROUP BY DATE(created_at)
                    ORDER BY date DESC
                    """
                    recent_df = pd.read_sql_query(recent_query, conn)
                    progress['recent_activity'] = recent_df.to_dict('records')
        
        except Exception as e:
            self.logger.error(f"진행률 분석 중 오류: {e}")
        
        return progress
    
    def diagnose_and_recommend(self):
        """종합 진단 및 권장사항 제시"""
        print("\n" + "="*70)
        print("🏥 DART 수집 종합 진단 보고서")
        print("="*70)
        
        # 1. API 키 검사
        api_valid, api_message = self.check_api_key_validity()
        print(f"\n🔑 API 키 상태: {'✅ 정상' if api_valid else '❌ 문제'}")
        print(f"   상세: {api_message}")
        
        # 2. 호출 한도 확인
        call_info = self.check_call_limits()
        print(f"\n📊 API 호출 현황:")
        print(f"   오늘 수집된 기업 수: {call_info['successful_companies']}개")
        print(f"   추정 API 호출 수: {call_info['estimated_calls_today']}회")
        print(f"   마지막 성공 시간: {call_info['last_successful_time']}")
        
        # 3. 실제 API 테스트
        rate_limit_detected = False
        if api_valid:
            test_results = self.test_specific_companies()
            print(f"\n🧪 주요 기업 API 테스트 결과:")
            
            success_count = 0
            
            for result in test_results:
                status_icon = {
                    'SUCCESS': '✅',
                    'NO_DATA': '⚠️',
                    'RATE_LIMIT': '🚫',
                    'ERROR': '❌',
                    'HTTP_ERROR': '❌',
                    'EXCEPTION': '❌'
                }.get(result['status'], '❓')
                
                print(f"   {status_icon} {result['company']}: {result['message']}")
                
                if result['status'] == 'SUCCESS':
                    success_count += 1
                elif result['status'] == 'RATE_LIMIT':
                    rate_limit_detected = True
        
        # 4. 수집 진행률 분석
        progress = self.analyze_collection_progress()
        print(f"\n📈 수집 진행률:")
        print(f"   전체 기업: {progress['total_companies']}개")
        print(f"   수집 완료: {progress['companies_with_data']}개")
        print(f"   진행률: {progress['collection_rate']:.1f}%")
        print(f"   총 재무 레코드: {progress['total_financial_records']:,}개")
        
        # 5. 종합 진단 및 권장사항
        print(f"\n💡 종합 진단 및 권장사항:")
        
        if not api_valid:
            print("🚨 긴급: API 키 문제로 수집 불가")
            print("   해결책: .env 파일의 DART_API_KEY 확인 및 재발급")
        
        elif rate_limit_detected or call_info['estimated_calls_today'] > 8000:
            print("🚨 긴급: API 호출 한도 초과 (일일 10,000회 제한)")
            print("   해결책:")
            print("   1. 내일까지 대기 (자정에 한도 리셋)")
            print("   2. 수집 속도 조절 (delay 증가)")
            print("   3. 배치 크기 축소")
        
        elif 'success_count' in locals() and success_count > 0:
            print("✅ API는 정상 작동 중")
            print("   현재 '데이터 없음'은 정상적인 현상일 수 있음")
            print("   권장사항: 수집 계속 진행")
        
        else:
            print("⚠️ 네트워크 또는 서버 문제 가능성")
            print("   해결책:")
            print("   1. 네트워크 연결 확인")
            print("   2. 30분 후 재시도")
            print("   3. VPN 사용 시 해제")
        
        # 6. 다음 단계 제안
        print(f"\n🚀 다음 단계:")
        
        if progress['companies_with_data'] >= 1000:
            print("   ✅ 충분한 데이터가 수집되었습니다!")
            print("   → python buffett_scorecard_calculator_fixed.py 실행")
            print("   → 워런 버핏 스코어카드 테스트 시작")
        
        elif rate_limit_detected:
            print("   ⏳ 내일 자정 이후 수집 재개")
            print("   → 기존 데이터로 스코어카드 테스트 가능")
        
        else:
            print("   🔄 수집 스크립트 재시작")
            print("   → 더 신중한 호출 간격으로 재시작")

def main():
    """메인 실행 함수"""
    print("🔧 DART 수집 문제 진단 도구 시작")
    
    troubleshooter = DartTroubleshooter()
    troubleshooter.diagnose_and_recommend()
    
    print(f"\n" + "="*70)
    print("진단 완료! 위의 권장사항을 따라 문제를 해결하세요.")

if __name__ == "__main__":
    main()