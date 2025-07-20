#!/usr/bin/env python3
"""
2025년 1분기 DART 재무데이터 수집 현황 확인
"""

import sqlite3
import pandas as pd
from datetime import datetime

def check_dart_financial_data():
    """DART 재무데이터 현황 상세 분석"""
    
    print("🔍 DART 재무데이터 수집 현황 분석")
    print("=" * 60)
    
    try:
        # 데이터베이스 연결
        conn = sqlite3.connect('data/databases/dart_data.db')
        
        print("\n📊 1. 연도별/분기별 수집 현황")
        print("-" * 40)
        
        # 연도별 분기별 기업 수 확인
        query1 = """
        SELECT 
            bsns_year as 연도,
            reprt_code as 보고서코드,
            COUNT(DISTINCT corp_code) as 기업수,
            COUNT(*) as 총레코드수
        FROM financial_statements 
        WHERE bsns_year >= 2024 
        GROUP BY bsns_year, reprt_code 
        ORDER BY bsns_year DESC, reprt_code
        """
        
        result1 = pd.read_sql(query1, conn)
        
        # 보고서 코드 설명 매핑
        report_mapping = {
            '11013': '1분기보고서',
            '11012': '반기보고서', 
            '11014': '3분기보고서',
            '11011': '사업보고서(연간)'
        }
        
        if not result1.empty:
            for _, row in result1.iterrows():
                report_name = report_mapping.get(row['보고서코드'], row['보고서코드'])
                print(f"📈 {row['연도']}년 {report_name}: {row['기업수']:,}개 기업, {row['총레코드수']:,}건")
        else:
            print("❌ 2024년 이후 데이터가 없습니다.")
        
        print("\n📊 2. 2025년 1분기 상세 분석")
        print("-" * 40)
        
        # 2025년 1분기 상세 확인
        query2 = """
        SELECT 
            COUNT(DISTINCT corp_code) as 기업수,
            COUNT(DISTINCT account_nm) as 계정과목수,
            COUNT(*) as 총레코드수,
            MIN(created_at) as 최초수집일,
            MAX(created_at) as 최근수집일
        FROM financial_statements 
        WHERE bsns_year = 2025 AND reprt_code = '11013'
        """
        
        result2 = pd.read_sql(query2, conn)
        
        if not result2.empty and result2.iloc[0]['기업수'] > 0:
            row = result2.iloc[0]
            print(f"✅ 2025년 1분기 데이터 존재!")
            print(f"   📊 수집 기업 수: {row['기업수']:,}개")
            print(f"   📋 계정과목 수: {row['계정과목수']:,}개")
            print(f"   📈 총 레코드: {row['총레코드수']:,}건")
            print(f"   📅 수집 기간: {row['최초수집일']} ~ {row['최근수집일']}")
        else:
            print("❌ 2025년 1분기 데이터가 없습니다.")
        
        print("\n📊 3. 주요 종목 2025년 1분기 데이터 확인")
        print("-" * 40)
        
        # 주요 종목별 2025년 1분기 데이터 확인
        query3 = """
        SELECT 
            cc.corp_name as 회사명,
            cc.stock_code as 종목코드,
            COUNT(DISTINCT fs.account_nm) as 계정과목수,
            COUNT(*) as 레코드수
        FROM financial_statements fs
        JOIN corp_codes cc ON fs.corp_code = cc.corp_code
        WHERE fs.bsns_year = 2025 
          AND fs.reprt_code = '11013'
          AND cc.corp_name IN ('삼성전자', 'SK하이닉스', 'NAVER', 'LG에너지솔루션', '카카오')
        GROUP BY cc.corp_name, cc.stock_code
        ORDER BY 레코드수 DESC
        """
        
        result3 = pd.read_sql(query3, conn)
        
        if not result3.empty:
            print("🎯 주요 종목 2025년 1분기 수집 현황:")
            for _, row in result3.iterrows():
                print(f"   ✅ {row['회사명']} ({row['종목코드']}): {row['계정과목수']}개 계정, {row['레코드수']}건")
        else:
            print("❌ 주요 종목의 2025년 1분기 데이터가 없습니다.")
        
        print("\n📊 4. 워런 버핏 스코어카드 계산 가능 여부")
        print("-" * 40)
        
        # 핵심 계정과목 존재 여부 확인
        key_accounts = ['매출액', '영업이익', '당기순이익', '총자산', '자본총계', '부채총계']
        
        query4 = """
        SELECT 
            account_nm as 계정과목,
            COUNT(DISTINCT corp_code) as 기업수
        FROM financial_statements 
        WHERE bsns_year = 2025 
          AND reprt_code = '11013'
          AND account_nm IN ('매출액', '영업이익', '당기순이익', '총자산', '자본총계', '부채총계', '유동자산', '유동부채')
        GROUP BY account_nm
        ORDER BY 기업수 DESC
        """
        
        result4 = pd.read_sql(query4, conn)
        
        if not result4.empty:
            print("💰 워런 버핏 계산용 핵심 계정과목 보유 현황:")
            for _, row in result4.iterrows():
                print(f"   📊 {row['계정과목']}: {row['기업수']:,}개 기업")
        else:
            print("❌ 핵심 계정과목 데이터가 부족합니다.")
        
        print("\n📊 5. 삼성전자 상세 재무데이터 샘플")
        print("-" * 40)
        
        # 삼성전자 2025년 1분기 주요 데이터 확인
        query5 = """
        SELECT 
            fs.account_nm as 계정과목,
            fs.thstrm_amount as 당기금액,
            fs.frmtrm_amount as 전기금액
        FROM financial_statements fs
        JOIN corp_codes cc ON fs.corp_code = cc.corp_code
        WHERE cc.corp_name LIKE '%삼성전자%'
          AND fs.bsns_year = 2025 
          AND fs.reprt_code = '11013'
          AND fs.account_nm IN ('매출액', '영업이익', '당기순이익', '총자산', '자본총계')
        ORDER BY 
            CASE fs.account_nm 
                WHEN '매출액' THEN 1
                WHEN '영업이익' THEN 2  
                WHEN '당기순이익' THEN 3
                WHEN '총자산' THEN 4
                WHEN '자본총계' THEN 5
                ELSE 6
            END
        """
        
        result5 = pd.read_sql(query5, conn)
        
        if not result5.empty:
            print("🏢 삼성전자 2025년 1분기 핵심 재무지표:")
            for _, row in result5.iterrows():
                당기 = f"{int(row['당기금액']):,}" if pd.notna(row['당기금액']) else "N/A"
                전기 = f"{int(row['전기금액']):,}" if pd.notna(row['전기금액']) else "N/A"
                print(f"   💰 {row['계정과목']}: {당기} (전기: {전기})")
        else:
            print("❌ 삼성전자 2025년 1분기 데이터가 없습니다.")
        
        conn.close()
        
        print("\n" + "=" * 60)
        print("🎯 종합 결론")
        print("=" * 60)
        
        # 결론 도출
        has_2025q1 = not result2.empty and result2.iloc[0]['기업수'] > 0
        has_key_accounts = not result4.empty and len(result4) >= 4
        has_samsung = not result5.empty
        
        if has_2025q1 and has_key_accounts:
            print("✅ 2025년 1분기 데이터 수집 완료!")
            print("✅ 워런 버핏 스코어카드 계산 즉시 가능!")
            print("🚀 다음 단계: 스코어카드 계산 로직 구현")
        elif has_2025q1:
            print("🟡 2025년 1분기 데이터는 있으나 일부 계정과목 부족")
            print("🔧 추가 데이터 수집 또는 데이터 정제 필요")
        else:
            print("❌ 2025년 1분기 데이터 수집 필요")
            print("📥 DART API로 최신 데이터 수집 권장")
        
    except Exception as e:
        print(f"❌ 데이터베이스 확인 중 오류: {e}")
        print("💡 데이터베이스 파일 경로를 확인하세요.")

if __name__ == "__main__":
    check_dart_financial_data()
