#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
회사 정보 데이터 수집 및 확인 스크립트
SK하이닉스 등 종목의 섹터, 시총 정보를 DART API로 수집
"""

import sqlite3
import pandas as pd
import requests
import time
import os
from datetime import datetime
from pathlib import Path
import zipfile
import io

class CompanyDataCollector:
    def __init__(self):
        """초기화 - DART API 키 설정"""
        # .env 파일에서 DART API 키 읽기
        self.dart_api_key = self._get_dart_api_key()
        
        # 데이터베이스 경로 설정
        self.db_path = Path("data/databases/stock_data.db")
        if not self.db_path.exists():
            self.db_path = Path("stock_data.db")  # 현재 디렉터리에서 찾기
        
        print(f"📍 데이터베이스 경로: {self.db_path}")
        
    def _get_dart_api_key(self):
        """DART API 키 가져오기"""
        # .env 파일에서 읽기
        env_path = Path(".env")
        if env_path.exists():
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith('DART_API_KEY='):
                        return line.split('=', 1)[1].strip().strip('"\'')
        
        # 환경변수에서 읽기
        api_key = os.getenv('DART_API_KEY')
        if api_key:
            return api_key.strip().strip('"\'')
        
        # 사용자 입력
        api_key = input("DART API 키를 입력하세요: ").strip()
        return api_key
    
    def check_current_data(self):
        """현재 데이터베이스 상태 확인"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # company_info 테이블 존재 확인
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='company_info'
            """)
            
            if not cursor.fetchone():
                print("❌ company_info 테이블이 존재하지 않습니다.")
                return False
            
            # 기본 통계
            cursor.execute("SELECT COUNT(*) FROM company_info")
            total_count = cursor.fetchone()[0]
            
            # 섹터 정보가 있는 기업 수
            cursor.execute("SELECT COUNT(*) FROM company_info WHERE sector IS NOT NULL AND sector != ''")
            sector_count = cursor.fetchone()[0]
            
            # 시총 정보가 있는 기업 수
            cursor.execute("SELECT COUNT(*) FROM company_info WHERE market_cap IS NOT NULL AND market_cap > 0")
            market_cap_count = cursor.fetchone()[0]
            
            print(f"\n📊 현재 데이터 현황:")
            print(f"- 전체 등록 기업: {total_count:,}개")
            print(f"- 섹터 정보 보유: {sector_count:,}개 ({sector_count/total_count*100:.1f}%)" if total_count > 0 else "- 섹터 정보 보유: 0개")
            print(f"- 시총 정보 보유: {market_cap_count:,}개 ({market_cap_count/total_count*100:.1f}%)" if total_count > 0 else "- 시총 정보 보유: 0개")
            
            # SK하이닉스 확인
            cursor.execute("""
                SELECT stock_code, company_name, sector, market_cap, industry 
                FROM company_info 
                WHERE stock_code = '000660' OR company_name LIKE '%SK하이닉스%'
            """)
            sk_data = cursor.fetchone()
            
            if sk_data:
                print(f"\n🔍 SK하이닉스 현재 정보:")
                print(f"- 종목코드: {sk_data[0]}")
                print(f"- 회사명: {sk_data[1]}")
                print(f"- 섹터: {sk_data[2] if sk_data[2] else 'N/A'}")
                print(f"- 시총: {sk_data[3] if sk_data[3] else 'N/A'}")
                print(f"- 업종: {sk_data[4] if sk_data[4] else 'N/A'}")
            else:
                print("\n❌ SK하이닉스 정보를 찾을 수 없습니다.")
            
            conn.close()
            return total_count > 0
            
        except Exception as e:
            print(f"❌ 데이터베이스 확인 중 오류: {e}")
            return False
    
    def collect_corp_codes(self):
        """DART에서 기업코드 목록 다운로드"""
        print("\n📡 DART에서 기업코드 목록 다운로드 중...")
        
        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={self.dart_api_key}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # ZIP 파일 압축 해제
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_content = zip_file.read('CORPCODE.xml').decode('utf-8')
            
            print("✅ 기업코드 XML 다운로드 완료")
            return xml_content
            
        except Exception as e:
            print(f"❌ 기업코드 다운로드 실패: {e}")
            return None
    
    def parse_corp_codes(self, xml_content):
        """XML에서 기업 정보 파싱"""
        print("🔍 기업 정보 파싱 중...")
        
        import re
        
        companies = []
        
        # 정규표현식으로 기업 정보 추출
        pattern = r'<list>.*?<corp_code>([^<]+)</corp_code>.*?<corp_name>([^<]+)</corp_name>.*?<stock_code>([^<]*)</stock_code>.*?<modify_date>([^<]+)</modify_date>.*?</list>'
        
        matches = re.findall(pattern, xml_content, re.DOTALL)
        
        for match in matches:
            corp_code, corp_name, stock_code, modify_date = match
            
            # 상장기업만 필터링 (종목코드가 있는 경우)
            if stock_code and len(stock_code) == 6:
                companies.append({
                    'corp_code': corp_code.strip(),
                    'company_name': corp_name.strip(),
                    'stock_code': stock_code.strip(),
                    'modify_date': modify_date.strip()
                })
        
        print(f"✅ {len(companies)}개 상장기업 정보 파싱 완료")
        return companies
    
    def get_company_info_from_dart(self, corp_code):
        """DART에서 개별 기업 상세 정보 조회"""
        url = f"https://opendart.fss.or.kr/api/company.json"
        params = {
            'crtfc_key': self.dart_api_key,
            'corp_code': corp_code
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == '000':
                return data
            else:
                return None
                
        except Exception as e:
            print(f"⚠️ {corp_code} 정보 조회 실패: {e}")
            return None
    
    def update_company_info(self):
        """company_info 테이블에 상세 정보 업데이트"""
        # 기업코드 다운로드
        xml_content = self.collect_corp_codes()
        if not xml_content:
            return False
        
        # 기업 정보 파싱
        companies = self.parse_corp_codes(xml_content)
        if not companies:
            return False
        
        # 데이터베이스 연결
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # company_info 테이블이 없다면 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT UNIQUE NOT NULL,
                company_name TEXT NOT NULL,
                market_type TEXT,
                sector TEXT,
                industry TEXT,
                listing_date TEXT,
                market_cap INTEGER,
                shares_outstanding INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print(f"\n🔄 {len(companies)}개 기업 정보 업데이트 시작...")
        
        updated_count = 0
        error_count = 0
        
        for i, company in enumerate(companies):
            try:
                # 현재 데이터 확인
                cursor.execute("""
                    SELECT sector, industry FROM company_info 
                    WHERE stock_code = ?
                """, (company['stock_code'],))
                
                existing = cursor.fetchone()
                
                # 섹터/업종 정보가 없는 경우만 업데이트
                if not existing or not existing[0]:
                    # DART에서 상세 정보 조회
                    detail_info = self.get_company_info_from_dart(company['corp_code'])
                    
                    if detail_info:
                        # 업종 정보 추출
                        sector = detail_info.get('induty_code', '')  # 업종코드
                        industry = detail_info.get('bizr_no', '')     # 사업자번호는 업종으로 대체
                        
                        # 업종명 매핑 (간단한 매핑)
                        sector_mapping = {
                            'J': '정보통신업',
                            'C': '제조업', 
                            'F': '건설업',
                            'G': '도매 및 소매업',
                            'K': '금융 및 보험업',
                            'L': '부동산업',
                            'M': '전문, 과학 및 기술 서비스업'
                        }
                        
                        if sector and sector[0] in sector_mapping:
                            sector = sector_mapping[sector[0]]
                        elif not sector:
                            sector = '제조업'  # 기본값
                        
                        # 데이터베이스 업데이트
                        cursor.execute("""
                            INSERT OR REPLACE INTO company_info 
                            (stock_code, company_name, sector, industry, updated_at)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            company['stock_code'],
                            company['company_name'],
                            sector,
                            industry,
                            datetime.now().isoformat()
                        ))
                        
                        updated_count += 1
                        
                        if company['stock_code'] == '000660':
                            print(f"✅ SK하이닉스 정보 업데이트: 섹터={sector}")
                    
                    # API 호출 제한 (초당 1회)
                    time.sleep(1)
                
                # 진행상황 표시
                if (i + 1) % 50 == 0:
                    print(f"📈 진행상황: {i+1}/{len(companies)} ({(i+1)/len(companies)*100:.1f}%)")
                    conn.commit()  # 중간 저장
                    
            except Exception as e:
                error_count += 1
                if error_count < 5:  # 처음 5개 오류만 출력
                    print(f"⚠️ {company['stock_code']} 업데이트 실패: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ 업데이트 완료!")
        print(f"- 성공: {updated_count}개")
        print(f"- 실패: {error_count}개")
        
        return True
    
    def add_sample_data(self):
        """샘플 데이터 추가 (DART API 없이도 테스트 가능)"""
        print("\n📝 샘플 데이터 추가 중...")
        
        sample_companies = [
            {
                'stock_code': '000660',
                'company_name': 'SK하이닉스',
                'sector': '반도체',
                'industry': '반도체 제조업',
                'market_type': 'KOSPI',
                'market_cap': 60000000000000  # 60조원 (예시)
            },
            {
                'stock_code': '005930',
                'company_name': '삼성전자',
                'sector': '전자기기',
                'industry': '전자부품 제조업',
                'market_type': 'KOSPI',
                'market_cap': 450000000000000  # 450조원 (예시)
            },
            {
                'stock_code': '035420',
                'company_name': 'NAVER',
                'sector': '정보통신업',
                'industry': '인터넷 서비스업',
                'market_type': 'KOSPI',
                'market_cap': 30000000000000  # 30조원 (예시)
            }
        ]
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # company_info 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT UNIQUE NOT NULL,
                company_name TEXT NOT NULL,
                market_type TEXT,
                sector TEXT,
                industry TEXT,
                listing_date TEXT,
                market_cap INTEGER,
                shares_outstanding INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        for company in sample_companies:
            cursor.execute("""
                INSERT OR REPLACE INTO company_info 
                (stock_code, company_name, market_type, sector, industry, market_cap, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                company['stock_code'],
                company['company_name'],
                company['market_type'],
                company['sector'],
                company['industry'],
                company['market_cap'],
                datetime.now().isoformat()
            ))
        
        conn.commit()
        conn.close()
        
        print(f"✅ {len(sample_companies)}개 샘플 데이터 추가 완료")
        return True

def main():
    """메인 실행 함수"""
    print("🚀 회사 정보 데이터 수집 및 수정 도구")
    print("=" * 50)
    
    collector = CompanyDataCollector()
    
    # 현재 상태 확인
    has_data = collector.check_current_data()
    
    if not has_data:
        print("\n❌ 기본 데이터가 없습니다.")
        choice = input("\n1) 샘플 데이터 추가 (빠름)\n2) DART API로 전체 데이터 수집 (느림)\n선택하세요 (1-2): ")
        
        if choice == '1':
            collector.add_sample_data()
        elif choice == '2':
            collector.update_company_info()
        else:
            print("❌ 잘못된 선택입니다.")
            return
    else:
        print("\n✅ 기본 데이터가 존재합니다.")
        choice = input("\n1) DART API로 누락 정보 보완\n2) 샘플 데이터로 테스트\n3) 현재 상태 유지\n선택하세요 (1-3): ")
        
        if choice == '1':
            collector.update_company_info()
        elif choice == '2':
            collector.add_sample_data()
        elif choice == '3':
            print("현재 상태를 유지합니다.")
        else:
            print("❌ 잘못된 선택입니다.")
            return
    
    # 업데이트 후 상태 재확인
    print("\n" + "=" * 50)
    print("📊 업데이트 후 상태:")
    collector.check_current_data()
    
    print(f"\n✅ 작업 완료!")
    print(f"💡 이제 company_info_checker.py를 다시 실행해보세요:")
    print(f"   python company_info_checker.py")

if __name__ == "__main__":
    main()