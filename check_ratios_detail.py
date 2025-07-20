#!/usr/bin/env python3
"""
Financial Ratios 테이블 상세 수집 현황 확인 도구
종목별 데이터 완성도와 필수 정보 수집 상태를 체크

실행 방법:
python check_ratios_detail.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any

def check_financial_ratios_detail():
    """Financial Ratios 테이블 상세 수집 현황 확인"""
    db_path = Path('data/databases/stock_data.db')
    
    if not db_path.exists():
        print("❌ stock_data.db 파일을 찾을 수 없습니다.")
        return
    
    try:
        with sqlite3.connect(db_path) as conn:
            # 1. 사용 가능한 financial_ratios 테이블 찾기
            tables = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name LIKE '%financial_ratio%'
                ORDER BY name
            """).fetchall()
            
            if not tables:
                print("❌ financial_ratios 관련 테이블을 찾을 수 없습니다.")
                return
            
            print("📊 Financial Ratios 테이블 상세 수집 현황")
            print("=" * 80)
            
            for table in tables:
                table_name = table[0]
                print(f"\n🗃️ 테이블: {table_name}")
                print("-" * 60)
                
                # 기본 통계
                total_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                unique_stocks = conn.execute(f"SELECT COUNT(DISTINCT stock_code) FROM {table_name}").fetchone()[0]
                
                print(f"📈 기본 통계:")
                print(f"   총 레코드: {total_count:,}개")
                print(f"   고유 종목: {unique_stocks:,}개")
                
                if total_count == 0:
                    print("   ❌ 데이터가 없습니다.")
                    continue
                
                # 테이블 구조 확인
                columns_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
                columns = [col[1] for col in columns_info]
                print(f"   컬럼 수: {len(columns)}개")
                
                # 핵심 컬럼 존재 확인
                key_columns = ['stock_code', 'company_name', 'current_price', 'per', 'pbr', 'market_cap']
                existing_key_columns = [col for col in key_columns if col in columns]
                print(f"   핵심 컬럼: {len(existing_key_columns)}/{len(key_columns)}개 존재")
                
                # 데이터 완전성 체크
                print(f"\n📋 데이터 완전성 체크:")
                
                for col in existing_key_columns:
                    if col == 'stock_code':
                        continue
                        
                    try:
                        # NULL이 아니고 0이 아닌 데이터 수
                        valid_count = conn.execute(f"""
                            SELECT COUNT(*) FROM {table_name} 
                            WHERE {col} IS NOT NULL AND {col} != 0 AND {col} != ''
                        """).fetchone()[0]
                        
                        percentage = (valid_count / total_count * 100) if total_count > 0 else 0
                        print(f"   {col}: {valid_count:,}/{total_count:,} ({percentage:.1f}%)")
                        
                    except Exception as e:
                        print(f"   {col}: 확인 실패 ({e})")
                
                # 주요 종목 데이터 확인
                print(f"\n🏢 주요 종목 데이터 확인:")
                major_stocks = ['005930', '000660', '035420', '005380', '051910']
                
                for stock_code in major_stocks:
                    try:
                        stock_data = conn.execute(f"""
                            SELECT stock_code, company_name, per, pbr, current_price
                            FROM {table_name} 
                            WHERE stock_code = ?
                            LIMIT 1
                        """, (stock_code,)).fetchone()
                        
                        if stock_data:
                            code, name, per, pbr, price = stock_data
                            name = name[:10] if name else "Unknown"
                            per_str = f"{per:.1f}" if per else "N/A"
                            pbr_str = f"{pbr:.1f}" if pbr else "N/A"
                            price_str = f"{price:,}원" if price else "N/A"
                            print(f"   ✅ {code} {name}: PER {per_str}, PBR {pbr_str}, 가격 {price_str}")
                        else:
                            print(f"   ❌ {stock_code}: 데이터 없음")
                            
                    except Exception as e:
                        print(f"   ❌ {stock_code}: 조회 실패 ({e})")
                
                # PER/PBR 분포 분석
                print(f"\n📊 PER/PBR 분포 분석:")
                
                try:
                    per_stats = conn.execute(f"""
                        SELECT 
                            COUNT(*) as total,
                            AVG(per) as avg_per,
                            MIN(per) as min_per,
                            MAX(per) as max_per,
                            COUNT(CASE WHEN per > 0 AND per <= 50 THEN 1 END) as valid_per
                        FROM {table_name}
                        WHERE per IS NOT NULL AND per > 0
                    """).fetchone()
                    
                    if per_stats and per_stats[0] > 0:
                        total, avg_per, min_per, max_per, valid_per = per_stats
                        print(f"   PER: {total:,}개 데이터")
                        print(f"        평균: {avg_per:.1f}, 범위: {min_per:.1f}~{max_per:.1f}")
                        print(f"        유효범위(0-50): {valid_per:,}개 ({valid_per/total*100:.1f}%)")
                    else:
                        print(f"   PER: 유효한 데이터 없음")
                        
                except Exception as e:
                    print(f"   PER: 분석 실패 ({e})")
                
                try:
                    pbr_stats = conn.execute(f"""
                        SELECT 
                            COUNT(*) as total,
                            AVG(pbr) as avg_pbr,
                            MIN(pbr) as min_pbr,
                            MAX(pbr) as max_pbr,
                            COUNT(CASE WHEN pbr > 0 AND pbr <= 10 THEN 1 END) as valid_pbr
                        FROM {table_name}
                        WHERE pbr IS NOT NULL AND pbr > 0
                    """).fetchone()
                    
                    if pbr_stats and pbr_stats[0] > 0:
                        total, avg_pbr, min_pbr, max_pbr, valid_pbr = pbr_stats
                        print(f"   PBR: {total:,}개 데이터")
                        print(f"        평균: {avg_pbr:.1f}, 범위: {min_pbr:.1f}~{max_pbr:.1f}")
                        print(f"        유효범위(0-10): {valid_pbr:,}개 ({valid_pbr/total*100:.1f}%)")
                    else:
                        print(f"   PBR: 유효한 데이터 없음")
                        
                except Exception as e:
                    print(f"   PBR: 분석 실패 ({e})")
                
                # 데이터 품질 문제 탐지
                print(f"\n⚠️ 데이터 품질 문제 탐지:")
                
                # 모든 PER이 동일한 값인지 체크
                try:
                    per_unique = conn.execute(f"""
                        SELECT COUNT(DISTINCT per) FROM {table_name} 
                        WHERE per IS NOT NULL AND per > 0
                    """).fetchone()[0]
                    
                    if per_unique == 1:
                        same_per = conn.execute(f"""
                            SELECT per FROM {table_name} 
                            WHERE per IS NOT NULL AND per > 0 
                            LIMIT 1
                        """).fetchone()[0]
                        print(f"   🚨 모든 PER이 동일: {same_per}")
                    elif per_unique < 10:
                        print(f"   ⚠️ PER 다양성 부족: {per_unique}개 고유값만 존재")
                    else:
                        print(f"   ✅ PER 다양성 양호: {per_unique}개 고유값")
                        
                except Exception as e:
                    print(f"   PER 품질 체크 실패: {e}")
                
                # 최신 업데이트 정보
                try:
                    latest_update = conn.execute(f"""
                        SELECT MAX(updated_at) FROM {table_name}
                    """).fetchone()[0]
                    
                    if latest_update:
                        print(f"\n🕐 최신 업데이트: {latest_update}")
                    
                    # 최근 업데이트된 종목 5개
                    recent_stocks = conn.execute(f"""
                        SELECT stock_code, company_name, per, pbr, updated_at
                        FROM {table_name}
                        ORDER BY updated_at DESC
                        LIMIT 5
                    """).fetchall()
                    
                    if recent_stocks:
                        print(f"📅 최근 업데이트 종목:")
                        for stock in recent_stocks:
                            code, name, per, pbr, updated = stock
                            name = name[:8] if name else "Unknown"
                            per_str = f"{per:.1f}" if per else "N/A"
                            pbr_str = f"{pbr:.1f}" if pbr else "N/A"
                            print(f"   {code} {name:10} PER:{per_str:6} PBR:{pbr_str:6} {updated}")
                            
                except Exception as e:
                    print(f"최신 정보 조회 실패: {e}")
            
            # 전체 요약
            print(f"\n" + "=" * 80)
            print(f"📋 전체 요약:")
            
            # 가장 많은 데이터를 가진 테이블 찾기
            best_table = None
            max_count = 0
            
            for table in tables:
                table_name = table[0]
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    if count > max_count:
                        max_count = count
                        best_table = table_name
                except:
                    pass
            
            if best_table:
                print(f"💼 권장 테이블: {best_table} ({max_count:,}개 레코드)")
                
                # 수집 완료도 평가
                try:
                    # stock_prices 테이블과 비교
                    stock_prices_count = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_prices").fetchone()[0]
                    financial_count = conn.execute(f"SELECT COUNT(DISTINCT stock_code) FROM {best_table}").fetchone()[0]
                    
                    coverage = (financial_count / stock_prices_count * 100) if stock_prices_count > 0 else 0
                    print(f"📊 수집 완료도: {financial_count:,}/{stock_prices_count:,} ({coverage:.1f}%)")
                    
                    if coverage >= 80:
                        print(f"✅ 수집 상태: 우수")
                    elif coverage >= 50:
                        print(f"🟡 수집 상태: 보통 (추가 수집 권장)")
                    else:
                        print(f"🔴 수집 상태: 부족 (대량 수집 필요)")
                        
                except Exception as e:
                    print(f"완료도 계산 실패: {e}")
            else:
                print(f"❌ 사용 가능한 테이블이 없습니다.")
    
    except Exception as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")

if __name__ == "__main__":
    check_financial_ratios_detail()
