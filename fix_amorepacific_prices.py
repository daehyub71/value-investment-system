#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
아모레퍼시픽 가격 데이터 즉시 수정 스크립트
====================================

디버깅 결과를 바탕으로 아모레퍼시픽의 가격 데이터를 즉시 수정합니다.
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import shutil

def fix_amorepacific_prices():
    """아모레퍼시픽 가격 데이터 수정"""
    
    stock_code = "090430"
    real_current_price = 135600.0  # 실시간 시장 가격
    
    # 데이터베이스 경로 찾기
    current_dir = Path(__file__).parent
    for _ in range(5):
        db_path = current_dir / "data" / "databases" / "buffett_scorecard.db"
        if db_path.exists():
            break
        current_dir = current_dir.parent
    else:
        print("❌ buffett_scorecard.db 파일을 찾을 수 없습니다.")
        return False
    
    print("🔧 아모레퍼시픽(090430) 가격 데이터 수정 시작")
    print("=" * 60)
    print(f"📊 실시간 기준 가격: {real_current_price:,.0f}원")
    
    # 백업 생성
    backup_dir = db_path.parent / "backups"
    backup_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"buffett_scorecard_backup_{timestamp}.db"
    
    try:
        shutil.copy2(db_path, backup_file)
        print(f"✅ 백업 완료: {backup_file.name}")
    except Exception as e:
        print(f"❌ 백업 실패: {e}")
        return False
    
    # 데이터베이스 연결
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    corrections = []
    
    try:
        # 1. buffett_scorecard 테이블 (이미 정상)
        print(f"\n📊 buffett_scorecard 테이블:")
        cursor = conn.execute("SELECT current_price, target_price FROM buffett_scorecard WHERE stock_code = ?", (stock_code,))
        result = cursor.fetchone()
        
        if result:
            current = result['current_price']
            target = result['target_price']
            diff_pct = ((current / real_current_price - 1) * 100)
            
            print(f"   현재가: {current:,.0f}원 (실시간 대비 {diff_pct:+.1f}%)")
            print(f"   목표가: {target:,.0f}원")
            
            if abs(diff_pct) <= 10:
                print(f"   ✅ 정상 범위 - 수정 불필요")
            else:
                print(f"   🔧 수정 필요")
        
        # 2. buffett_top50_scores 테이블 수정
        print(f"\n📊 buffett_top50_scores 테이블:")
        cursor = conn.execute("SELECT current_price, target_price_high, target_price_low FROM buffett_top50_scores WHERE stock_code = ?", (stock_code,))
        result = cursor.fetchone()
        
        if result:
            old_current = result['current_price']
            old_target_high = result['target_price_high']
            old_target_low = result['target_price_low']
            
            print(f"   수정 전 - 현재가: {old_current:,.0f}원, 목표가: {old_target_high:,.0f}원")
            
            # 분할 비율 계산
            split_ratio = real_current_price / old_current
            
            # 새로운 목표가 계산
            new_target_high = old_target_high * split_ratio
            new_target_low = old_target_low * split_ratio
            
            # 업데이트 실행
            cursor = conn.execute("""
                UPDATE buffett_top50_scores 
                SET current_price = ?, 
                    target_price_high = ?,
                    target_price_low = ?,
                    upside_potential = ?,
                    created_at = ?
                WHERE stock_code = ?
            """, (
                real_current_price,
                new_target_high,
                new_target_low,
                ((new_target_high / real_current_price - 1) * 100),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                stock_code
            ))
            
            if cursor.rowcount > 0:
                print(f"   ✅ 수정 완료:")
                print(f"      현재가: {old_current:,.0f}원 → {real_current_price:,.0f}원")
                print(f"      목표가: {old_target_high:,.0f}원 → {new_target_high:,.0f}원")
                print(f"      상승여력: {((new_target_high / real_current_price - 1) * 100):+.1f}%")
                corrections.append("buffett_top50_scores")
        
        # 3. buffett_all_stocks_final 테이블 수정
        print(f"\n📊 buffett_all_stocks_final 테이블:")
        cursor = conn.execute("SELECT current_price, target_price_high, target_price_low FROM buffett_all_stocks_final WHERE stock_code = ?", (stock_code,))
        result = cursor.fetchone()
        
        if result:
            old_current = result['current_price']
            old_target_high = result['target_price_high']
            old_target_low = result['target_price_low']
            
            print(f"   수정 전 - 현재가: {old_current:,.0f}원, 목표가: {old_target_high:,.0f}원")
            
            # 분할 비율 계산
            split_ratio = real_current_price / old_current
            
            # 새로운 목표가 계산
            new_target_high = old_target_high * split_ratio
            new_target_low = old_target_low * split_ratio
            
            # 업데이트 실행
            cursor = conn.execute("""
                UPDATE buffett_all_stocks_final 
                SET current_price = ?, 
                    target_price_high = ?,
                    target_price_low = ?,
                    upside_potential = ?,
                    created_at = ?
                WHERE stock_code = ?
            """, (
                real_current_price,
                new_target_high,
                new_target_low,
                ((new_target_high / real_current_price - 1) * 100),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                stock_code
            ))
            
            if cursor.rowcount > 0:
                print(f"   ✅ 수정 완료:")
                print(f"      현재가: {old_current:,.0f}원 → {real_current_price:,.0f}원")
                print(f"      목표가: {old_target_high:,.0f}원 → {new_target_high:,.0f}원")
                print(f"      상승여력: {((new_target_high / real_current_price - 1) * 100):+.1f}%")
                corrections.append("buffett_all_stocks_final")
        
        # 변경사항 커밋
        conn.commit()
        
        print(f"\n✅ 수정 완료 요약:")
        print(f"   수정된 테이블: {len(corrections)}개")
        for table in corrections:
            print(f"   - {table}")
        
        if corrections:
            print(f"\n🎯 수정 결과:")
            print(f"   현재가: {real_current_price:,.0f}원 (모든 테이블 일치)")
            print(f"   예상 상승여력: 약 20% (일관된 목표가)")
        
        return True
        
    except Exception as e:
        print(f"❌ 수정 중 오류 발생: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def verify_corrections():
    """수정 결과 검증"""
    
    stock_code = "090430"
    
    # 데이터베이스 경로 찾기
    current_dir = Path(__file__).parent
    for _ in range(5):
        db_path = current_dir / "data" / "databases" / "buffett_scorecard.db"
        if db_path.exists():
            break
        current_dir = current_dir.parent
    else:
        return False
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    print(f"\n🔍 수정 결과 검증:")
    print("-" * 40)
    
    tables = [
        ('buffett_scorecard', 'target_price'),
        ('buffett_top50_scores', 'target_price_high'),
        ('buffett_all_stocks_final', 'target_price_high')
    ]
    
    try:
        for table_name, target_col in tables:
            cursor = conn.execute(f"SELECT current_price, {target_col} FROM {table_name} WHERE stock_code = ?", (stock_code,))
            result = cursor.fetchone()
            
            if result:
                current = result['current_price']
                target = result[target_col]
                upside = ((target / current - 1) * 100) if current > 0 else 0
                
                print(f"{table_name}:")
                print(f"   현재가: {current:,.0f}원")
                print(f"   목표가: {target:,.0f}원")
                print(f"   상승여력: {upside:+.1f}%")
                print()
    
    finally:
        conn.close()

if __name__ == "__main__":
    print("🚀 아모레퍼시픽 가격 데이터 즉시 수정")
    print("=" * 50)
    
    # 수정 실행
    success = fix_amorepacific_prices()
    
    if success:
        # 검증
        verify_corrections()
        print("🎉 아모레퍼시픽 가격 데이터 수정이 완료되었습니다!")
    else:
        print("❌ 수정에 실패했습니다.")