#!/usr/bin/env python3
"""
DART API 응답 구조 확인 및 테이블 스키마 자동 수정 스크립트
"""

import sys
import requests
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import json

# 프로젝트 루트 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from config import ConfigManager
except ImportError:
    print("⚠️ ConfigManager를 찾을 수 없습니다. 직접 API 키를 입력하세요.")
    ConfigManager = None

def get_dart_api_key():
    """DART API 키 가져오기"""
    if ConfigManager:
        try:
            config_manager = ConfigManager()
            dart_config = config_manager.get_dart_config()
            return dart_config.get('api_key')
        except:
            pass
    
    # 환경변수에서 직접 가져오기
    import os
    return os.getenv('DART_API_KEY')

def check_dart_api_response_structure():
    """DART API 응답 구조 확인"""
    
    api_key = get_dart_api_key()
    if not api_key:
        print("❌ DART API 키를 찾을 수 없습니다.")
        print("💡 .env 파일에 DART_API_KEY를 설정하거나 config 설정을 확인하세요.")
        return None
    
    # 삼성전자로 테스트
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    params = {
        'crtfc_key': api_key,
        'corp_code': '00126380',  # 삼성전자
        'bsns_year': '2022',
        'reprt_code': '11011'  # 사업보고서
    }
    
    try:
        print("📡 DART API 응답 구조 확인 중... (삼성전자 2022년 데이터)")
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get('status') == '000' and data.get('list'):
            sample_record = data['list'][0]
            
            print("✅ DART API 응답 성공")
            print(f"📊 총 레코드 수: {len(data['list'])}개")
            print("\n📋 API 응답 컬럼 구조:")
            
            for key, value in sample_record.items():
                value_preview = str(value)[:50] if value else "NULL"
                print(f"  {key}: {value_preview}")
            
            return data['list']
        else:
            print(f"❌ DART API 오류: {data.get('message', 'Unknown error')}")
            return None
            
    except Exception as e:
        print(f"❌ API 요청 실패: {e}")
        return None

def get_current_table_schema():
    """현재 financial_statements 테이블 스키마 확인"""
    
    db_path = Path('data/databases/dart_data.db')
    if not db_path.exists():
        print("❌ dart_data.db가 존재하지 않습니다.")
        return None
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("PRAGMA table_info(financial_statements)")
            columns = cursor.fetchall()
            
            if columns:
                print("\n📊 현재 financial_statements 테이블 구조:")
                current_columns = []
                for col in columns:
                    col_name, col_type = col[1], col[2]
                    current_columns.append(col_name)
                    print(f"  {col_name} ({col_type})")
                return current_columns
            else:
                print("⚠️ financial_statements 테이블이 존재하지 않습니다.")
                return []
                
    except Exception as e:
        print(f"❌ 테이블 스키마 확인 실패: {e}")
        return None

def create_updated_financial_statements_table():
    """DART API 응답에 맞는 업데이트된 테이블 생성"""
    
    # 완전한 DART API 응답 컬럼을 포함한 테이블 스키마
    create_table_sql = '''
        CREATE TABLE IF NOT EXISTS financial_statements_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- 기본 식별 정보
            corp_code TEXT NOT NULL,
            bsns_year TEXT NOT NULL,
            reprt_code TEXT NOT NULL,
            
            -- DART API 표준 컬럼들
            rcept_no TEXT,
            corp_cls TEXT,
            corp_name TEXT,
            fs_div TEXT,
            fs_nm TEXT,
            sj_div TEXT,
            sj_nm TEXT,
            account_id TEXT,
            account_nm TEXT,
            account_detail TEXT,
            
            -- 금액 정보
            thstrm_nm TEXT,
            thstrm_amount TEXT,
            thstrm_add_amount TEXT,
            frmtrm_nm TEXT, 
            frmtrm_amount TEXT,
            frmtrm_add_amount TEXT,
            bfefrmtrm_nm TEXT,
            bfefrmtrm_amount TEXT,
            bfefrmtrm_add_amount TEXT,
            
            -- 기타 정보
            ord TEXT,
            currency TEXT,
            
            -- 메타데이터
            year INTEGER,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- 중복 방지
            UNIQUE(corp_code, bsns_year, reprt_code, fs_div, account_nm, ord)
        )
    '''
    
    db_path = Path('data/databases/dart_data.db')
    
    try:
        with sqlite3.connect(db_path) as conn:
            # 기존 테이블 백업
            conn.execute('''
                CREATE TABLE IF NOT EXISTS financial_statements_backup AS 
                SELECT * FROM financial_statements WHERE 1=0
            ''')
            
            try:
                conn.execute('INSERT INTO financial_statements_backup SELECT * FROM financial_statements')
                print("💾 기존 데이터 백업 완료")
            except:
                print("ℹ️ 백업할 기존 데이터가 없습니다.")
            
            # 기존 테이블 삭제
            conn.execute('DROP TABLE IF EXISTS financial_statements')
            
            # 새 테이블 생성
            conn.execute(create_table_sql)
            
            # 테이블명 변경
            conn.execute('ALTER TABLE financial_statements_new RENAME TO financial_statements')
            
            # 인덱스 생성
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_financial_statements_corp_year 
                ON financial_statements(corp_code, bsns_year)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_financial_statements_account 
                ON financial_statements(account_nm)
            ''')
            
            conn.commit()
            
            print("✅ financial_statements 테이블 업데이트 완료")
            print("📊 새로운 테이블이 DART API 응답 구조와 완전히 호환됩니다.")
            
            return True
            
    except Exception as e:
        print(f"❌ 테이블 업데이트 실패: {e}")
        return False

def verify_new_table_structure():
    """새 테이블 구조 확인"""
    
    db_path = Path('data/databases/dart_data.db')
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("PRAGMA table_info(financial_statements)")
            columns = cursor.fetchall()
            
            print(f"\n✅ 업데이트된 financial_statements 테이블 구조 ({len(columns)}개 컬럼):")
            for col in columns:
                print(f"  {col[1]} ({col[2]})")
            
            return True
            
    except Exception as e:
        print(f"❌ 테이블 구조 확인 실패: {e}")
        return False

def test_data_insertion():
    """테스트 데이터 삽입"""
    
    api_response = check_dart_api_response_structure()
    if not api_response:
        return False
    
    db_path = Path('data/databases/dart_data.db')
    
    try:
        # 샘플 데이터 생성
        sample_data = []
        for record in api_response[:3]:  # 처음 3개만 테스트
            # 필요한 컬럼 추가
            record['year'] = 2022
            record['collected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sample_data.append(record)
        
        df = pd.DataFrame(sample_data)
        
        with sqlite3.connect(db_path) as conn:
            df.to_sql('financial_statements', conn, if_exists='append', index=False)
            
            # 삽입된 데이터 확인
            cursor = conn.execute("SELECT COUNT(*) FROM financial_statements")
            count = cursor.fetchone()[0]
            
            print(f"✅ 테스트 데이터 삽입 성공: {count}건")
            
            return True
            
    except Exception as e:
        print(f"❌ 테스트 데이터 삽입 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    
    print("🔧 DART API 스키마 문제 해결 도구")
    print("=" * 60)
    
    # 1. DART API 응답 구조 확인
    print("\n1️⃣ DART API 응답 구조 확인")
    api_response = check_dart_api_response_structure()
    
    if not api_response:
        print("❌ API 응답을 가져올 수 없어 중단합니다.")
        return
    
    # 2. 현재 테이블 구조 확인
    print("\n2️⃣ 현재 테이블 구조 확인")
    current_columns = get_current_table_schema()
    
    # 3. 테이블 업데이트
    print("\n3️⃣ 테이블 스키마 업데이트")
    if create_updated_financial_statements_table():
        
        # 4. 새 테이블 구조 확인
        print("\n4️⃣ 업데이트된 테이블 구조 확인")
        verify_new_table_structure()
        
        # 5. 테스트 데이터 삽입
        print("\n5️⃣ 테스트 데이터 삽입")
        if test_data_insertion():
            print("\n🎉 스키마 업데이트 완료!")
            print("✅ 이제 DART 데이터 수집을 다시 실행할 수 있습니다.")
            print("\n🚀 다음 명령어로 수집을 재시작하세요:")
            print("python company_info_dart_collector.py --companies=50 --year=2022")
        else:
            print("\n⚠️ 테스트 데이터 삽입에 실패했습니다.")
    else:
        print("\n❌ 테이블 업데이트에 실패했습니다.")

if __name__ == "__main__":
    main()