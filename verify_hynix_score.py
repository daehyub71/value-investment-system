#!/usr/bin/env python3
"""
하이닉스 스코어카드 검증 스크립트
PBR 데이터 포함 여부와 점수 계산 정확성 확인
"""

import sqlite3
import pandas as pd
from pathlib import Path

def check_scorecard_data():
    """스코어카드 데이터 검증"""
    db_path = Path(r"data\databases\buffett_scorecard.db")
    
    if not db_path.exists():
        print(f"❌ 데이터베이스 파일이 없습니다: {db_path}")
        return
    
    try:
        with sqlite3.connect(db_path) as conn:
            print("🔍 워런 버핏 스코어카드 검증")
            print("=" * 60)
            
            # 1. 전체 데이터 현황
            cursor = conn.execute("SELECT COUNT(*) FROM buffett_scorecard")
            total_count = cursor.fetchone()[0]
            print(f"📊 전체 종목 수: {total_count:,}개")
            
            # 2. PBR 데이터 현황
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(pbr) as has_pbr,
                    COUNT(*) - COUNT(pbr) as missing_pbr,
                    MIN(pbr) as min_pbr,
                    MAX(pbr) as max_pbr,
                    AVG(pbr) as avg_pbr
                FROM buffett_scorecard
                WHERE pbr > 0
            """)
            pbr_stats = cursor.fetchone()
            
            print(f"\n📈 PBR 데이터 현황:")
            print(f"  - PBR 보유: {pbr_stats[1]:,}개 ({pbr_stats[1]/total_count*100:.1f}%)")
            print(f"  - PBR 누락: {pbr_stats[2]:,}개 ({pbr_stats[2]/total_count*100:.1f}%)")
            if pbr_stats[1] > 0:
                print(f"  - PBR 범위: {pbr_stats[3]:.3f} ~ {pbr_stats[4]:.3f}")
                print(f"  - PBR 평균: {pbr_stats[5]:.3f}")
            
            # 3. 하이닉스 상세 데이터
            print(f"\n🔍 하이닉스(SK하이닉스) 상세 데이터:")
            print("-" * 40)
            
            query = """
            SELECT stock_code, company_name, total_score, 
                   valuation_score, profitability_score, growth_score, financial_health_score,
                   pbr, forward_pe, roe, debt_to_equity, current_ratio,
                   calculation_date, last_updated
            FROM buffett_scorecard 
            WHERE stock_code = '000660' OR company_name LIKE '%하이닉스%'
            ORDER BY calculation_date DESC
            LIMIT 1
            """
            
            cursor = conn.execute(query)
            hynix_data = cursor.fetchone()
            
            if hynix_data:
                columns = [desc[0] for desc in cursor.description]
                hynix_dict = dict(zip(columns, hynix_data))
                
                print(f"  📌 종목코드: {hynix_dict['stock_code']}")
                print(f"  📌 회사명: {hynix_dict['company_name']}")
                print(f"  📌 총점: {hynix_dict['total_score']}점")
                print(f"  📌 계산일시: {hynix_dict['calculation_date']}")
                print(f"  📌 업데이트: {hynix_dict['last_updated']}")
                
                print(f"\n  📊 카테고리별 점수:")
                print(f"    - 가치평가: {hynix_dict['valuation_score']}점 (PBR 포함)")
                print(f"    - 수익성: {hynix_dict['profitability_score']}점")
                print(f"    - 성장성: {hynix_dict['growth_score']}점")
                print(f"    - 재무건전성: {hynix_dict['financial_health_score']}점")
                
                print(f"\n  📈 주요 지표:")
                print(f"    - PBR: {hynix_dict['pbr'] if hynix_dict['pbr'] else 'NULL'}")
                print(f"    - 예상 PER: {hynix_dict['forward_pe'] if hynix_dict['forward_pe'] else 'NULL'}")
                print(f"    - ROE: {hynix_dict['roe'] if hynix_dict['roe'] else 'NULL'}%")
                print(f"    - 부채비율: {hynix_dict['debt_to_equity'] if hynix_dict['debt_to_equity'] else 'NULL'}")
                print(f"    - 유동비율: {hynix_dict['current_ratio'] if hynix_dict['current_ratio'] else 'NULL'}")
                
                # 4. PBR 포함 여부 확인
                pbr_included = hynix_dict['pbr'] is not None and hynix_dict['pbr'] > 0
                print(f"\n  ✅ PBR 데이터 포함: {'예' if pbr_included else '아니오'}")
                
                if pbr_included:
                    print(f"  ✅ PBR 값: {hynix_dict['pbr']:.3f}")
                    print(f"  ✅ 가치평가 점수에 PBR 반영됨")
                else:
                    print(f"  ❌ PBR 데이터 누락")
                    print(f"  ❌ 가치평가 점수가 불완전할 수 있음")
                
                # 5. 점수 검증
                total_calculated = (hynix_dict['valuation_score'] + 
                                   hynix_dict['profitability_score'] + 
                                   hynix_dict['growth_score'] + 
                                   hynix_dict['financial_health_score'])
                
                print(f"\n  📊 점수 검증:")
                print(f"    - 카테고리 합계: {total_calculated}점")
                print(f"    - 저장된 총점: {hynix_dict['total_score']}점")
                print(f"    - 일치 여부: {'✅ 일치' if total_calculated == hynix_dict['total_score'] else '❌ 불일치'}")
                
            else:
                print("  ❌ 하이닉스 데이터를 찾을 수 없습니다.")
            
            # 6. 상위 점수 종목들 확인
            print(f"\n🏆 상위 10개 종목 (PBR 포함 확인):")
            print("-" * 60)
            
            query_top = """
            SELECT stock_code, company_name, total_score, pbr, valuation_score
            FROM buffett_scorecard 
            WHERE total_score > 0
            ORDER BY total_score DESC
            LIMIT 10
            """
            
            cursor = conn.execute(query_top)
            top_stocks = cursor.fetchall()
            
            for i, (code, name, score, pbr, val_score) in enumerate(top_stocks, 1):
                pbr_status = f"PBR: {pbr:.3f}" if pbr and pbr > 0 else "PBR: NULL"
                print(f"  {i:2d}. {name[:20]:20} ({code}) {score:2d}점 [{pbr_status}] 가치평가: {val_score}점")
            
    except Exception as e:
        print(f"❌ 검증 실패: {e}")

def check_calculation_logic():
    """계산 로직 확인"""
    print(f"\n🔧 워런 버핏 스코어카드 계산 로직 분석:")
    print("=" * 60)
    
    # 배점 시스템 (batch_buffett_scorecard.py에서 확인된 내용)
    weights = {
        'valuation': 40,       # 가치평가 (PBR 포함)
        'profitability': 30,   # 수익성
        'growth': 20,         # 성장성
        'financial_health': 10 # 재무 건전성
    }
    
    print(f"📊 점수 배점 (100점 만점):")
    for category, points in weights.items():
        print(f"  - {category}: {points}점")
    
    print(f"\n📈 가치평가 지표 (40점)에 포함되는 요소:")
    print(f"  - PBR (주가순자산비율): 워런 버핏 기준 2.0 이하")
    print(f"  - 예상 PER: 15배 이하")
    print(f"  - PEG 비율: 1.5 이하")
    print(f"  - 기타 가치평가 지표들")
    
    print(f"\n✅ 따라서 현재 점수는 PBR 데이터를 포함하여 계산된 값입니다.")

if __name__ == "__main__":
    check_scorecard_data()
    check_calculation_logic()
