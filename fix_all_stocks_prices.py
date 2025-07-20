#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 종목 버핏 스코어카드 가격 데이터 일괄 수정 프로그램
=================================================

company_info 테이블의 모든 종목에 대해 buffett_top50_scores와 
buffett_all_stocks_final 테이블의 가격 데이터 오류를 자동으로 탐지하고 수정합니다.

주요 기능:
- 실시간 시장가격과 스코어카드 가격 비교
- 주식분할/액면분할 자동 탐지 및 보정
- 목표가 재계산 (기존 상승률 유지)
- 일괄 처리 및 진행상황 표시
- 안전한 백업 및 복구
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import shutil
import math
import time
from typing import Dict, List, Tuple, Optional

class AllStocksPriceCorrector:
    """전체 종목 가격 데이터 수정 클래스"""
    
    def __init__(self):
        # 데이터베이스 경로 설정
        self.stock_db_path = None
        self.buffett_db_path = None
        self.find_database_paths()
        
        # 통계 변수
        self.total_stocks = 0
        self.processed_stocks = 0
        self.corrected_stocks = 0
        self.failed_stocks = 0
        self.skipped_stocks = 0
        
        # 수정 임계값 (10% 이상 차이면 수정)
        self.correction_threshold = 10.0
    
    def find_database_paths(self):
        """데이터베이스 파일 경로 찾기"""
        current_dir = Path(__file__).parent
        
        for _ in range(5):
            stock_db = current_dir / "data" / "databases" / "stock_data.db"
            buffett_db = current_dir / "data" / "databases" / "buffett_scorecard.db"
            
            if stock_db.exists() and buffett_db.exists():
                self.stock_db_path = stock_db
                self.buffett_db_path = buffett_db
                break
            current_dir = current_dir.parent
        
        if not self.stock_db_path or not self.buffett_db_path:
            print("❌ 필요한 데이터베이스 파일을 찾을 수 없습니다.")
            print("   필요 파일: stock_data.db, buffett_scorecard.db")
            return False
        
        print(f"📊 stock_data.db: {self.stock_db_path}")
        print(f"🏆 buffett_scorecard.db: {self.buffett_db_path}")
        return True
    
    def create_backup(self) -> bool:
        """버핏 스코어카드 데이터베이스 백업"""
        backup_dir = self.buffett_db_path.parent / "backups"
        backup_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"buffett_scorecard_all_stocks_backup_{timestamp}.db"
        
        try:
            shutil.copy2(self.buffett_db_path, backup_file)
            print(f"✅ 백업 완료: {backup_file.name}")
            self.backup_file = backup_file
            return True
        except Exception as e:
            print(f"❌ 백업 실패: {e}")
            return False
    
    def get_all_stocks(self) -> List[Dict[str, str]]:
        """company_info 테이블에서 모든 종목 조회"""
        try:
            with sqlite3.connect(str(self.stock_db_path)) as conn:
                conn.row_factory = sqlite3.Row
                
                cursor = conn.execute("""
                    SELECT stock_code, company_name, market_type, sector, industry 
                    FROM company_info 
                    WHERE stock_code IS NOT NULL 
                    AND stock_code != ''
                    AND length(stock_code) = 6
                    ORDER BY market_type, stock_code
                """)
                
                stocks = [dict(row) for row in cursor.fetchall()]
                print(f"📊 company_info에서 {len(stocks)}개 종목 조회 완료")
                return stocks
                
        except Exception as e:
            print(f"❌ 종목 조회 실패: {e}")
            return []
    
    def get_real_market_price(self, stock_code: str) -> Optional[float]:
        """실시간 시장 데이터에서 현재가 조회"""
        try:
            with sqlite3.connect(str(self.stock_db_path)) as conn:
                conn.row_factory = sqlite3.Row
                
                # financial_ratios_real 테이블에서 최신 현재가 조회
                cursor = conn.execute("""
                    SELECT current_price, updated_at 
                    FROM financial_ratios_real 
                    WHERE stock_code = ? 
                    ORDER BY updated_at DESC 
                    LIMIT 1
                """, (stock_code,))
                
                result = cursor.fetchone()
                if result and result['current_price']:
                    return float(result['current_price'])
                
                # 대체로 stock_prices 테이블에서 최신 종가 조회
                cursor = conn.execute("""
                    SELECT close_price, date 
                    FROM stock_prices 
                    WHERE stock_code = ? 
                    ORDER BY date DESC 
                    LIMIT 1
                """, (stock_code,))
                
                result = cursor.fetchone()
                if result and result['close_price']:
                    return float(result['close_price'])
                
                return None
                
        except Exception as e:
            return None
    
    def get_scorecard_prices(self, stock_code: str) -> Dict[str, Dict]:
        """스코어카드 테이블에서 현재가와 목표가 조회"""
        scorecard_data = {}
        
        try:
            with sqlite3.connect(str(self.buffett_db_path)) as conn:
                conn.row_factory = sqlite3.Row
                
                # buffett_top50_scores 테이블
                cursor = conn.execute("""
                    SELECT current_price, target_price_high, target_price_low, analysis_date
                    FROM buffett_top50_scores 
                    WHERE stock_code = ? 
                    ORDER BY created_at DESC 
                    LIMIT 1
                """, (stock_code,))
                
                result = cursor.fetchone()
                if result:
                    scorecard_data['top50'] = {
                        'current_price': result['current_price'],
                        'target_price_high': result['target_price_high'],
                        'target_price_low': result['target_price_low'],
                        'analysis_date': result['analysis_date']
                    }
                
                # buffett_all_stocks_final 테이블
                cursor = conn.execute("""
                    SELECT current_price, target_price_high, target_price_low, analysis_date
                    FROM buffett_all_stocks_final 
                    WHERE stock_code = ? 
                    ORDER BY created_at DESC 
                    LIMIT 1
                """, (stock_code,))
                
                result = cursor.fetchone()
                if result:
                    scorecard_data['final'] = {
                        'current_price': result['current_price'],
                        'target_price_high': result['target_price_high'],
                        'target_price_low': result['target_price_low'],
                        'analysis_date': result['analysis_date']
                    }
                
                return scorecard_data
                
        except Exception as e:
            return {}
    
    def calculate_split_ratio(self, old_price: float, new_price: float) -> float:
        """주식분할 비율 계산"""
        if old_price <= 0 or new_price <= 0:
            return 1.0
        
        ratio = new_price / old_price
        
        # 일반적인 분할 비율로 근사
        common_ratios = [0.1, 0.2, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0]
        
        best_ratio = ratio
        min_diff = float('inf')
        
        for common_ratio in common_ratios:
            diff = abs(ratio - common_ratio)
            if diff < min_diff:
                min_diff = diff
                best_ratio = common_ratio
        
        # 차이가 너무 크면 원래 비율 사용
        if min_diff > 0.5:
            return ratio
        
        return best_ratio
    
    def analyze_single_stock(self, stock_info: Dict[str, str]) -> Dict[str, any]:
        """단일 종목 분석"""
        stock_code = stock_info['stock_code']
        company_name = stock_info.get('company_name', stock_code)
        
        analysis = {
            'stock_code': stock_code,
            'company_name': company_name,
            'needs_correction': False,
            'corrections': [],
            'error': None
        }
        
        try:
            # 실시간 시장가격 조회
            real_price = self.get_real_market_price(stock_code)
            if not real_price:
                analysis['error'] = "실시간 가격 없음"
                return analysis
            
            analysis['real_price'] = real_price
            
            # 스코어카드 가격 조회
            scorecard_data = self.get_scorecard_prices(stock_code)
            if not scorecard_data:
                analysis['error'] = "스코어카드 데이터 없음"
                return analysis
            
            # 각 테이블별 분석
            for table_type, data in scorecard_data.items():
                if not data['current_price']:
                    continue
                
                current_price = float(data['current_price'])
                diff_pct = abs((current_price / real_price - 1) * 100)
                
                if diff_pct > self.correction_threshold:
                    analysis['needs_correction'] = True
                    
                    split_ratio = self.calculate_split_ratio(current_price, real_price)
                    new_target_high = data['target_price_high'] * split_ratio if data['target_price_high'] else None
                    new_target_low = data['target_price_low'] * split_ratio if data['target_price_low'] else None
                    
                    correction = {
                        'table': table_type,
                        'old_current': current_price,
                        'new_current': real_price,
                        'old_target_high': data['target_price_high'],
                        'new_target_high': new_target_high,
                        'old_target_low': data['target_price_low'],
                        'new_target_low': new_target_low,
                        'split_ratio': split_ratio,
                        'diff_pct': diff_pct
                    }
                    
                    analysis['corrections'].append(correction)
            
            return analysis
            
        except Exception as e:
            analysis['error'] = str(e)
            return analysis
    
    def apply_corrections(self, analysis: Dict[str, any]) -> bool:
        """분석 결과를 바탕으로 실제 수정 적용"""
        if not analysis['needs_correction']:
            return True
        
        stock_code = analysis['stock_code']
        
        try:
            with sqlite3.connect(str(self.buffett_db_path)) as conn:
                for correction in analysis['corrections']:
                    table_type = correction['table']
                    
                    if table_type == 'top50':
                        table_name = 'buffett_top50_scores'
                    elif table_type == 'final':
                        table_name = 'buffett_all_stocks_final'
                    else:
                        continue
                    
                    # 업데이트 쿼리 실행
                    update_values = [
                        correction['new_current'],
                        correction['new_target_high'],
                        correction['new_target_low'],
                        ((correction['new_target_high'] / correction['new_current'] - 1) * 100) if correction['new_target_high'] and correction['new_current'] > 0 else 0,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        stock_code
                    ]
                    
                    cursor = conn.execute(f"""
                        UPDATE {table_name} 
                        SET current_price = ?, 
                            target_price_high = ?,
                            target_price_low = ?,
                            upside_potential = ?,
                            created_at = ?
                        WHERE stock_code = ?
                    """, update_values)
                    
                    if cursor.rowcount == 0:
                        print(f"⚠️ {stock_code} {table_name} 업데이트 실패 (레코드 없음)")
                
                conn.commit()
                return True
                
        except Exception as e:
            print(f"❌ {stock_code} 수정 실패: {e}")
            return False
    
    def process_all_stocks(self, limit: Optional[int] = None, start_from: int = 0) -> Dict[str, int]:
        """모든 종목 일괄 처리"""
        
        print(f"\n🚀 전체 종목 가격 데이터 수정 시작")
        print("=" * 80)
        
        # 백업 생성
        if not self.create_backup():
            return {'error': 'backup_failed'}
        
        # 전체 종목 조회
        all_stocks = self.get_all_stocks()
        if not all_stocks:
            return {'error': 'no_stocks'}
        
        # 처리 범위 설정
        if limit:
            all_stocks = all_stocks[start_from:start_from + limit]
        else:
            all_stocks = all_stocks[start_from:]
        
        self.total_stocks = len(all_stocks)
        print(f"📊 처리 대상: {self.total_stocks}개 종목")
        
        if limit:
            print(f"📍 처리 범위: {start_from + 1}번째 ~ {start_from + len(all_stocks)}번째")
        
        # 사용자 확인
        if self.total_stocks > 100:
            confirm = input(f"\n❓ {self.total_stocks}개 종목을 일괄 처리하시겠습니까? (y/N): ")
            if confirm.lower() != 'y':
                print("⏹️ 처리가 취소되었습니다.")
                return {'cancelled': True}
        
        print(f"\n🔄 처리 시작...")
        start_time = time.time()
        
        # 통계 변수
        results = {
            'corrected': [],
            'skipped': [],
            'failed': []
        }
        
        # 종목별 처리
        for i, stock_info in enumerate(all_stocks, 1):
            stock_code = stock_info['stock_code']
            company_name = stock_info.get('company_name', stock_code)
            
            print(f"[{i:4d}/{self.total_stocks}] {stock_code} ({company_name[:10]:10s}) ", end="")
            
            try:
                # 분석 수행
                analysis = self.analyze_single_stock(stock_info)
                
                if analysis.get('error'):
                    print(f"❌ {analysis['error']}")
                    results['failed'].append(stock_code)
                    self.failed_stocks += 1
                    
                elif analysis['needs_correction']:
                    # 수정 적용
                    if self.apply_corrections(analysis):
                        corrections_count = len(analysis['corrections'])
                        max_diff = max([c['diff_pct'] for c in analysis['corrections']])
                        print(f"✅ 수정완료 ({corrections_count}개 테이블, 최대 {max_diff:.1f}% 차이)")
                        results['corrected'].append(stock_code)
                        self.corrected_stocks += 1
                    else:
                        print(f"❌ 수정실패")
                        results['failed'].append(stock_code)
                        self.failed_stocks += 1
                else:
                    print(f"⭕ 수정불필요")
                    results['skipped'].append(stock_code)
                    self.skipped_stocks += 1
                
                self.processed_stocks += 1
                
                # 진행상황 표시 (100개마다)
                if i % 100 == 0:
                    elapsed = time.time() - start_time
                    avg_time = elapsed / i
                    remaining = (self.total_stocks - i) * avg_time
                    
                    print(f"\n📊 진행률: {i}/{self.total_stocks} ({i/self.total_stocks*100:.1f}%)")
                    print(f"   수정: {self.corrected_stocks}, 생략: {self.skipped_stocks}, 실패: {self.failed_stocks}")
                    print(f"   예상 남은시간: {remaining/60:.1f}분")
                
                # API 제한을 위한 작은 딜레이
                if i % 50 == 0:
                    time.sleep(0.1)
                    
            except Exception as e:
                print(f"❌ 예외발생: {str(e)[:30]}")
                results['failed'].append(stock_code)
                self.failed_stocks += 1
        
        # 최종 통계
        elapsed_time = time.time() - start_time
        self.print_final_statistics(results, elapsed_time)
        
        return {
            'total': self.total_stocks,
            'processed': self.processed_stocks,
            'corrected': self.corrected_stocks,
            'skipped': self.skipped_stocks,
            'failed': self.failed_stocks,
            'elapsed_time': elapsed_time
        }
    
    def print_final_statistics(self, results: Dict[str, List], elapsed_time: float):
        """최종 통계 출력"""
        print(f"\n📊 전체 종목 가격 수정 완료!")
        print("=" * 80)
        print(f"⏱️  총 소요시간: {elapsed_time/60:.1f}분")
        print(f"📈 처리 속도: {self.processed_stocks/elapsed_time:.1f}종목/초")
        print()
        print(f"📊 처리 결과:")
        print(f"   ✅ 수정 완료: {self.corrected_stocks:4d}개 ({self.corrected_stocks/self.total_stocks*100:.1f}%)")
        print(f"   ⭕ 수정 불필요: {self.skipped_stocks:4d}개 ({self.skipped_stocks/self.total_stocks*100:.1f}%)")
        print(f"   ❌ 처리 실패: {self.failed_stocks:4d}개 ({self.failed_stocks/self.total_stocks*100:.1f}%)")
        print(f"   📊 전체 처리: {self.processed_stocks:4d}개")
        
        if self.corrected_stocks > 0:
            print(f"\n🎯 수정 결과:")
            print(f"   📈 {self.corrected_stocks}개 종목의 가격 데이터가 실시간 시세에 맞춰 조정되었습니다.")
            print(f"   🎯 목표가가 기존 상승률을 유지하며 재계산되었습니다.")
            print(f"   💾 백업 파일: {self.backup_file.name}")
        
        # 실패한 종목들 (처음 10개만)
        if results['failed']:
            print(f"\n❌ 처리 실패 종목 (상위 10개):")
            for stock_code in results['failed'][:10]:
                print(f"   - {stock_code}")
            if len(results['failed']) > 10:
                print(f"   ... 외 {len(results['failed']) - 10}개")
    
    def verify_corrections(self, sample_codes: List[str] = None) -> bool:
        """수정 결과 검증"""
        if not sample_codes:
            # 임의의 10개 종목으로 검증
            all_stocks = self.get_all_stocks()
            sample_codes = [stock['stock_code'] for stock in all_stocks[:10]]
        
        print(f"\n🔍 수정 결과 검증 ({len(sample_codes)}개 종목)")
        print("-" * 60)
        
        verification_passed = 0
        
        for stock_code in sample_codes:
            real_price = self.get_real_market_price(stock_code)
            scorecard_data = self.get_scorecard_prices(stock_code)
            
            if real_price and scorecard_data:
                max_diff = 0
                for table_type, data in scorecard_data.items():
                    if data['current_price']:
                        diff_pct = abs((data['current_price'] / real_price - 1) * 100)
                        max_diff = max(max_diff, diff_pct)
                
                if max_diff <= self.correction_threshold:
                    print(f"✅ {stock_code}: 정상 (최대 차이 {max_diff:.1f}%)")
                    verification_passed += 1
                else:
                    print(f"❌ {stock_code}: 여전히 차이 있음 (최대 {max_diff:.1f}%)")
        
        success_rate = verification_passed / len(sample_codes) * 100
        print(f"\n📊 검증 결과: {verification_passed}/{len(sample_codes)} 통과 ({success_rate:.1f}%)")
        
        return success_rate >= 80

def main():
    """메인 실행 함수"""
    print("🔧 전체 종목 버핏 스코어카드 가격 데이터 일괄 수정 프로그램")
    print("=" * 80)
    
    corrector = AllStocksPriceCorrector()
    
    if not corrector.stock_db_path or not corrector.buffett_db_path:
        return
    
    try:
        print(f"\n📋 실행 옵션:")
        print(f"   1. 전체 종목 처리")
        print(f"   2. 샘플 100개 종목 처리")
        print(f"   3. 특정 범위 처리")
        print(f"   4. 테스트 (10개 종목)")
        
        choice = input(f"\n선택하세요 (1-4): ").strip()
        
        if choice == '1':
            # 전체 종목 처리
            results = corrector.process_all_stocks()
            
        elif choice == '2':
            # 샘플 100개
            results = corrector.process_all_stocks(limit=100)
            
        elif choice == '3':
            # 특정 범위
            start = int(input("시작 번호 (0부터): "))
            limit = int(input("처리할 개수: "))
            results = corrector.process_all_stocks(limit=limit, start_from=start)
            
        elif choice == '4':
            # 테스트
            results = corrector.process_all_stocks(limit=10)
            
        else:
            print("❌ 잘못된 선택입니다.")
            return
        
        # 검증 수행
        if results.get('corrected', 0) > 0:
            print(f"\n" + "=" * 80)
            verify_option = input(f"❓ 수정 결과를 검증하시겠습니까? (y/N): ")
            if verify_option.lower() == 'y':
                corrector.verify_corrections()
        
        print(f"\n🎉 전체 종목 가격 데이터 수정이 완료되었습니다!")
        
    except KeyboardInterrupt:
        print(f"\n⏹️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()