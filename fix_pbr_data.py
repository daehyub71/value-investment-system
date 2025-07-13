#!/usr/bin/env python3
"""
PBR 데이터 전용 수정 프로그램
기존 buffett_scorecard 테이블에서 PBR이 누락된 종목들을 찾아서 수정

실행 방법:
python fix_pbr_data.py --status                 # PBR 현황 조회
python fix_pbr_data.py --codes 005930,000660    # 특정 종목만 수정
python fix_pbr_data.py --all                    # 모든 PBR 누락 종목 수정
python fix_pbr_data.py --dry-run --all          # 테스트 실행
"""

import sys
import os
import sqlite3
import time
import json
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd

try:
    import yfinance as yf
    print("✅ yfinance 라이브러리 사용 가능")
except ImportError:
    print("❌ yfinance가 필요합니다: pip install yfinance")
    sys.exit(1)

class PBRDataFixer:
    """PBR 데이터 전용 수정 클래스"""
    
    def __init__(self, delay: float = 1.0):
        self.logger = logging.getLogger(__name__)
        self.delay = delay
        self.scorecard_db = Path('data/databases/buffett_scorecard.db')
        
        if not self.scorecard_db.exists():
            raise FileNotFoundError(f"데이터베이스 파일이 없습니다: {self.scorecard_db}")
    
    def get_korean_ticker(self, stock_code: str) -> str:
        """한국 주식 코드를 Yahoo Finance 티커로 변환"""
        if len(stock_code) == 6 and stock_code.isdigit():
            if stock_code.startswith(('0', '1', '2', '3')):
                return f"{stock_code}.KS"
            else:
                return f"{stock_code}.KQ"
        return stock_code
    
    def show_pbr_status(self):
        """PBR 데이터 상태 조회"""
        try:
            with sqlite3.connect(self.scorecard_db) as conn:
                cursor = conn.execute('''
                    SELECT 
                        COUNT(*) as total,
                        COUNT(pbr) as has_pbr,
                        COUNT(*) - COUNT(pbr) as missing_pbr
                    FROM buffett_scorecard
                ''')
                
                total, has_pbr, missing_pbr = cursor.fetchone()
                
                print("📊 PBR 데이터 현황")
                print("=" * 50)
                print(f"전체 종목: {total:,}개")
                if total > 0:
                    print(f"PBR 보유: {has_pbr:,}개 ({has_pbr/total*100:.1f}%)")
                    print(f"PBR 누락: {missing_pbr:,}개 ({missing_pbr/total*100:.1f}%)")
                else:
                    print("데이터가 없습니다.")
                    return
                
                if missing_pbr > 0:
                    cursor = conn.execute('''
                        SELECT stock_code, company_name, market_cap
                        FROM buffett_scorecard 
                        WHERE pbr IS NULL OR pbr = 0 OR pbr < 0.01
                        ORDER BY market_cap DESC NULLS LAST
                        LIMIT 10
                    ''')
                    
                    missing_stocks = cursor.fetchall()
                    print(f"\n📋 PBR 누락 상위 10개 종목:")
                    for i, (code, name, mcap) in enumerate(missing_stocks, 1):
                        mcap_text = f"{mcap:,.0f}원" if mcap else "N/A"
                        print(f"  {i:2d}. {name} ({code}) - 시가총액: {mcap_text}")
                
        except Exception as e:
            print(f"❌ PBR 상태 조회 실패: {e}")
    
    def calculate_pbr_from_yahoo(self, stock_code: str) -> Dict[str, Any]:
        """Yahoo Finance에서 PBR 계산"""
        ticker = self.get_korean_ticker(stock_code)
        
        try:
            print(f"  🔍 {stock_code} PBR 계산 중...", end=" ")
            
            stock = yf.Ticker(ticker)
            info = stock.info
            
            if not info:
                raise ValueError("Yahoo Finance 데이터 없음")
            
            market_cap = info.get('marketCap')
            if not market_cap:
                raise ValueError("시가총액 정보 없음")
            
            balance_sheet = stock.balance_sheet
            if balance_sheet is None or balance_sheet.empty:
                raise ValueError("재무제표 데이터 없음")
            
            # 자기자본 찾기
            target_fields = [
                'Stockholders Equity',
                'Common Stock Equity',
                'Total Equity Gross Minority Interest'
            ]
            
            for field_name in target_fields:
                if field_name in balance_sheet.index:
                    equity_value = balance_sheet.loc[field_name].iloc[0]
                    if pd.notna(equity_value) and equity_value > 0:
                        pbr = market_cap / equity_value
                        if 0.01 <= pbr <= 50:
                            print(f"✅ PBR: {pbr:.3f}")
                            return {
                                'success': True,
                                'pbr': pbr,
                                'equity_field': field_name,
                                'market_cap': market_cap,
                                'equity_value': equity_value
                            }
                        else:
                            raise ValueError(f"PBR 값 이상: {pbr:.3f}")
                    break
            
            raise ValueError("자기자본 정보 추출 실패")
            
        except Exception as e:
            print(f"❌ 실패: {e}")
            return {'success': False, 'error': str(e)}
    
    def update_pbr_in_database(self, stock_code: str, pbr: float, dry_run: bool = False) -> bool:
        """데이터베이스에 PBR 업데이트"""
        try:
            if dry_run:
                print(f"  [DRY RUN] PBR: {pbr:.3f} 업데이트 예정")
                return True
            
            with sqlite3.connect(self.scorecard_db) as conn:
                conn.execute('''
                    UPDATE buffett_scorecard 
                    SET pbr = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE stock_code = ?
                ''', (pbr, stock_code))
                conn.commit()
            
            print(f"  ✅ PBR {pbr:.3f} 업데이트 완료")
            return True
            
        except Exception as e:
            print(f"  ❌ 업데이트 실패: {e}")
            return False
    
    def fix_pbr_for_stocks(self, stock_codes: List[str] = None, dry_run: bool = False):
        """PBR 수정 실행"""
        
        if stock_codes:
            # 특정 종목들만 처리
            stocks_to_fix = []
            for code in stock_codes:
                try:
                    with sqlite3.connect(self.scorecard_db) as conn:
                        cursor = conn.execute('''
                            SELECT stock_code, company_name, market_cap
                            FROM buffett_scorecard 
                            WHERE stock_code = ?
                        ''', (code,))
                        
                        row = cursor.fetchone()
                        if row:
                            stocks_to_fix.append(row)
                        else:
                            print(f"⚠️ 종목 {code}을 데이터베이스에서 찾을 수 없습니다.")
                except Exception as e:
                    print(f"❌ 종목 {code} 조회 실패: {e}")
        else:
            # 모든 PBR 누락 종목 처리
            try:
                with sqlite3.connect(self.scorecard_db) as conn:
                    cursor = conn.execute('''
                        SELECT stock_code, company_name, market_cap
                        FROM buffett_scorecard 
                        WHERE pbr IS NULL OR pbr = 0 OR pbr < 0.01
                        ORDER BY market_cap DESC NULLS LAST
                    ''')
                    stocks_to_fix = cursor.fetchall()
                    
            except Exception as e:
                print(f"❌ PBR 누락 종목 조회 실패: {e}")
                return
        
        if not stocks_to_fix:
            print("✅ 수정할 PBR 데이터가 없습니다.")
            return
        
        mode_text = "[테스트 모드]" if dry_run else "[실제 업데이트]"
        print(f"🔧 PBR 데이터 수정 시작 {mode_text}")
        print(f"📊 대상 종목: {len(stocks_to_fix)}개")
        print("=" * 70)
        
        success_count = 0
        failed_count = 0
        
        for i, (stock_code, company_name, market_cap) in enumerate(stocks_to_fix, 1):
            print(f"\n[{i:3d}/{len(stocks_to_fix)}] {company_name} ({stock_code})")
            
            try:
                pbr_data = self.calculate_pbr_from_yahoo(stock_code)
                
                if pbr_data['success']:
                    if self.update_pbr_in_database(stock_code, pbr_data['pbr'], dry_run):
                        success_count += 1
                    else:
                        failed_count += 1
                else:
                    failed_count += 1
                
            except Exception as e:
                failed_count += 1
                print(f"  ❌ 처리 중 오류: {e}")
            
            time.sleep(self.delay)
        
        # 결과 요약
        print("\n" + "=" * 70)
        print(f"🎯 PBR 수정 작업 완료 {mode_text}")
        print("=" * 70)
        print(f"✅ 성공: {success_count}개")
        print(f"❌ 실패: {failed_count}개")
        if (success_count + failed_count) > 0:
            print(f"📈 성공률: {success_count/(success_count+failed_count)*100:.1f}%")
        
        if not dry_run and success_count > 0:
            print(f"\n💾 {success_count}개 종목의 PBR 데이터가 업데이트되었습니다.")


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PBR 데이터 전용 수정 프로그램')
    parser.add_argument('--all', action='store_true', help='모든 PBR 누락 종목 수정')
    parser.add_argument('--codes', type=str, help='특정 종목 코드들 (쉼표로 구분, 예: 005930,000660)')
    parser.add_argument('--dry-run', action='store_true', help='테스트 실행 (실제 업데이트 안함)')
    parser.add_argument('--status', action='store_true', help='PBR 데이터 현황만 조회')
    parser.add_argument('--delay', type=float, default=1.0, help='API 호출 간 딜레이 (초)')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    try:
        fixer = PBRDataFixer(delay=args.delay)
        
        if args.status:
            fixer.show_pbr_status()
        elif args.codes:
            stock_codes = [code.strip() for code in args.codes.split(',')]
            print(f"🎯 지정된 {len(stock_codes)}개 종목 PBR 수정")
            fixer.fix_pbr_for_stocks(stock_codes=stock_codes, dry_run=args.dry_run)
        elif args.all:
            fixer.fix_pbr_for_stocks(dry_run=args.dry_run)
        else:
            print("ℹ️ 사용법:")
            print("  python fix_pbr_data.py --status              # PBR 현황 조회")
            print("  python fix_pbr_data.py --all                # 모든 PBR 누락 종목 수정")
            print("  python fix_pbr_data.py --codes 005930,000660  # 특정 종목만 수정")
            print("  python fix_pbr_data.py --dry-run --all      # 테스트 실행")
            print()
            fixer.show_pbr_status()
            
    except FileNotFoundError as e:
        print(f"❌ 파일을 찾을 수 없습니다: {e}")
        print("💡 먼저 batch_buffett_scorecard.py를 실행하여 기본 데이터를 생성하세요.")
    except Exception as e:
        print(f"❌ 예기치 못한 오류: {e}")


if __name__ == "__main__":
    main()
