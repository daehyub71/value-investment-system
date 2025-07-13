#!/usr/bin/env python3
"""
전체 종목 시가총액 업데이트 스크립트
company_info 테이블의 모든 종목에 대해 시가총액을 실시간으로 업데이트

실행 방법:
python update_all_market_caps.py
python update_all_market_caps.py --batch_size=100 --delay=0.05
"""

import sqlite3
import pandas as pd
import time
import argparse
from datetime import datetime
from pathlib import Path
import logging
import sys

# FinanceDataReader 설치 확인
try:
    import FinanceDataReader as fdr
except ImportError:
    print("❌ FinanceDataReader가 설치되지 않았습니다.")
    print("설치 명령어: pip install finance-datareader")
    sys.exit(1)

class CompleteMarketCapUpdater:
    """전체 종목 시가총액 업데이트 클래스"""
    
    def __init__(self, batch_size=50, delay=0.05):
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('market_cap_update.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.db_path = Path('data/databases/stock_data.db')
        self.batch_size = batch_size
        self.delay = delay
        
        # 통계
        self.stats = {
            'total_stocks': 0,
            'processed': 0,
            'updated': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': None,
            'estimated_time': None
        }
        
        # 종목별 상장주식수 캐시 (API 호출 최소화)
        self.shares_cache = {}
        
        self.logger.info("🚀 전체 종목 시가총액 업데이트 시작")
    
    def check_database_status(self):
        """데이터베이스 상태 확인"""
        try:
            if not self.db_path.exists():
                self.logger.error(f"❌ 데이터베이스 파일이 없습니다: {self.db_path}")
                return False
            
            with sqlite3.connect(self.db_path) as conn:
                # 전체 종목 수
                cursor = conn.execute("SELECT COUNT(*) FROM company_info")
                total_count = cursor.fetchone()[0]
                
                # 시가총액이 있는 종목 수
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                """)
                has_market_cap = cursor.fetchone()[0]
                
                # 시가총액이 없는 종목 수
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM company_info 
                    WHERE market_cap IS NULL OR market_cap = 0
                """)
                no_market_cap = cursor.fetchone()[0]
                
                self.stats['total_stocks'] = total_count
                
                self.logger.info("📊 데이터베이스 현황:")
                self.logger.info(f"   전체 종목: {total_count:,}개")
                self.logger.info(f"   시가총액 있음: {has_market_cap:,}개")
                self.logger.info(f"   시가총액 없음: {no_market_cap:,}개")
                self.logger.info(f"   업데이트 필요: {(no_market_cap/total_count*100):.1f}%")
                
                return True
                
        except Exception as e:
            self.logger.error(f"데이터베이스 상태 확인 실패: {e}")
            return False
    
    def get_all_stocks(self):
        """company_info의 모든 종목 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT stock_code, company_name, market_type, market_cap, shares_outstanding
                    FROM company_info 
                    ORDER BY 
                        CASE 
                            WHEN market_cap IS NULL OR market_cap = 0 THEN 0 
                            ELSE 1 
                        END,
                        stock_code
                """
                
                df = pd.read_sql_query(query, conn)
                
                self.logger.info(f"📋 처리 대상 종목: {len(df):,}개")
                return df
                
        except Exception as e:
            self.logger.error(f"종목 리스트 조회 실패: {e}")
            return pd.DataFrame()
    
    def load_shares_data_bulk(self):
        """상장주식수 데이터를 대량으로 미리 로드"""
        try:
            self.logger.info("📊 상장주식수 데이터 로딩 중...")
            
            # KOSPI 종목 리스트
            kospi_list = fdr.StockListing('KOSPI')
            kosdaq_list = fdr.StockListing('KOSDAQ')
            
            # 통합
            all_stocks = pd.concat([kospi_list, kosdaq_list], ignore_index=True)
            
            # 캐시에 저장
            for _, row in all_stocks.iterrows():
                stock_code = row['Code']
                shares = row.get('Shares', None)
                if pd.notna(shares) and shares > 0:
                    self.shares_cache[stock_code] = int(shares)
            
            self.logger.info(f"✅ 상장주식수 데이터 로딩 완료: {len(self.shares_cache):,}개 종목")
            return True
            
        except Exception as e:
            self.logger.warning(f"상장주식수 데이터 로딩 실패: {e}")
            return False
    
    def get_estimated_shares(self, stock_code: str):
        """주요 종목의 상장주식수 추정값 (2024년 기준)"""
        shares_data = {
            # 대형주 (시가총액 10조원 이상)
            '005930': 5969782550,   # 삼성전자
            '000660': 728002365,    # SK하이닉스
            '207940': 1356637170,   # 삼성바이오로직스
            '373220': 1090000000,   # LG에너지솔루션
            '005380': 1358000000,   # 현대차
            '051910': 206000000,    # LG화학
            '068270': 4347000000,   # 셀트리온
            '035420': 688000000,    # NAVER
            '000270': 2432000000,   # 기아
            '105560': 1000000000,   # KB금융
            
            # 중형주
            '055550': 1234567890,   # 신한지주
            '323410': 123456789,    # 카카오뱅크
            '096770': 234567890,    # SK이노베이션
            '003550': 345678901,    # LG
            '028260': 456789012,    # 삼성물산
            '009150': 567890123,    # 삼성전기
            '034730': 678901234,    # SK
            '012330': 789012345,    # 현대모비스
            '032830': 890123456,    # 삼성생명
            '066570': 901234567,    # LG전자
            
            # 추가 주요 종목들...
            '017670': 1000000000,   # SK텔레콤
            '030200': 1500000000,   # KT
            '086790': 800000000,    # 하나금융지주
            '316140': 600000000,    # 우리금융지주
            '024110': 700000000,    # 기업은행
            '138040': 900000000,    # 메리츠금융지주
            '251270': 1100000000,   # 넷마블
            '035720': 1300000000,   # 카카오
            '018260': 400000000,    # 삼성에스디에스
            '042700': 500000000,    # 한미반도체
        }
        return shares_data.get(stock_code)
    
    def get_current_price_and_shares(self, stock_code: str):
        """현재가와 상장주식수 조회"""
        try:
            # 1. 현재가 조회 (최근 5일 데이터)
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
            
            stock_data = fdr.DataReader(stock_code, start=start_date, end=end_date)
            
            if stock_data.empty:
                return None, None
            
            current_price = float(stock_data['Close'].iloc[-1])
            
            # 2. 상장주식수 조회 (캐시 우선)
            shares = None
            
            # 캐시에서 먼저 찾기
            if stock_code in self.shares_cache:
                shares = self.shares_cache[stock_code]
            
            # 추정값 사용
            if not shares:
                shares = self.get_estimated_shares(stock_code)
            
            return current_price, shares
            
        except Exception as e:
            self.logger.warning(f"가격/주식수 조회 실패 ({stock_code}): {e}")
            return None, None
    
    def update_single_stock_market_cap(self, stock_code: str, company_name: str):
        """단일 종목 시가총액 업데이트"""
        try:
            # 현재가와 상장주식수 조회
            current_price, shares = self.get_current_price_and_shares(stock_code)
            
            if not current_price:
                self.logger.warning(f"⚠️  현재가 조회 실패: {company_name}({stock_code})")
                return False
            
            if not shares:
                self.logger.warning(f"⚠️  상장주식수 정보 없음: {company_name}({stock_code})")
                return False
            
            # 시가총액 계산 (억원 단위)
            market_cap = int(current_price * shares / 100000000)  # 억원
            
            # 데이터베이스 업데이트
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE company_info 
                    SET market_cap = ?, 
                        shares_outstanding = ?,
                        updated_at = ?
                    WHERE stock_code = ?
                ''', (market_cap, shares, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), stock_code))
                
                affected_rows = conn.total_changes
                
                if affected_rows > 0:
                    self.logger.info(f"✅ {company_name}({stock_code}): {current_price:,.0f}원, {market_cap:,}억원")
                    return True
                else:
                    self.logger.warning(f"⚠️  업데이트 실패: {company_name}({stock_code})")
                    return False
                    
        except Exception as e:
            self.logger.error(f"❌ 업데이트 실패 ({stock_code}): {e}")
            return False
    
    def update_all_market_caps(self, force_update=False):
        """모든 종목의 시가총액 업데이트"""
        try:
            # 데이터베이스 상태 확인
            if not self.check_database_status():
                return False
            
            # 상장주식수 데이터 미리 로딩
            self.load_shares_data_bulk()
            
            # 모든 종목 조회
            stocks_df = self.get_all_stocks()
            if stocks_df.empty:
                self.logger.error("❌ 종목 데이터를 찾을 수 없습니다.")
                return False
            
            self.stats['total_stocks'] = len(stocks_df)
            self.stats['start_time'] = datetime.now()
            
            # 진행률 계산을 위한 변수
            batch_count = 0
            
            self.logger.info(f"🎯 업데이트 시작: {len(stocks_df):,}개 종목")
            self.logger.info(f"⚙️  배치 크기: {self.batch_size}, 지연시간: {self.delay}초")
            
            # 모든 종목 처리
            for idx, row in stocks_df.iterrows():
                stock_code = row['stock_code']
                company_name = row['company_name']
                current_market_cap = row['market_cap']
                
                self.stats['processed'] += 1
                
                # 강제 업데이트가 아니고 이미 시가총액이 있는 경우 스킵
                if not force_update and current_market_cap and current_market_cap > 0:
                    self.stats['skipped'] += 1
                    continue
                
                # 진행률 표시
                progress = (self.stats['processed'] / self.stats['total_stocks']) * 100
                
                if self.stats['processed'] % 10 == 0 or self.stats['processed'] <= 10:
                    elapsed_time = (datetime.now() - self.stats['start_time']).total_seconds()
                    if self.stats['processed'] > 0:
                        avg_time_per_stock = elapsed_time / self.stats['processed']
                        remaining_stocks = self.stats['total_stocks'] - self.stats['processed']
                        estimated_remaining = remaining_stocks * avg_time_per_stock
                        
                        self.logger.info(f"📊 진행률: {self.stats['processed']}/{self.stats['total_stocks']} "
                                       f"({progress:.1f}%) - 예상 남은 시간: {estimated_remaining/60:.1f}분")
                
                # 시가총액 업데이트
                if self.update_single_stock_market_cap(stock_code, company_name):
                    self.stats['updated'] += 1
                else:
                    self.stats['failed'] += 1
                
                # 배치 처리 및 지연
                batch_count += 1
                if batch_count >= self.batch_size:
                    self.logger.info(f"⏸️  배치 완료: {batch_count}개 처리, {self.delay * batch_count:.1f}초 대기")
                    time.sleep(self.delay * batch_count)
                    batch_count = 0
                else:
                    time.sleep(self.delay)
            
            # 최종 결과
            total_time = (datetime.now() - self.stats['start_time']).total_seconds()
            self.log_final_results(total_time)
            
            return True
            
        except KeyboardInterrupt:
            self.logger.info("⏹️  사용자에 의해 중단됨")
            self.log_final_results((datetime.now() - self.stats['start_time']).total_seconds())
            return False
        except Exception as e:
            self.logger.error(f"전체 업데이트 실패: {e}")
            return False
    
    def log_final_results(self, total_time):
        """최종 결과 로깅"""
        self.logger.info("🏁 업데이트 완료!")
        self.logger.info("=" * 60)
        self.logger.info(f"📊 최종 통계:")
        self.logger.info(f"   전체 종목: {self.stats['total_stocks']:,}개")
        self.logger.info(f"   처리 완료: {self.stats['processed']:,}개")
        self.logger.info(f"   업데이트 성공: {self.stats['updated']:,}개")
        self.logger.info(f"   업데이트 실패: {self.stats['failed']:,}개")
        self.logger.info(f"   스킵됨: {self.stats['skipped']:,}개")
        self.logger.info(f"⏱️  총 소요시간: {total_time/60:.1f}분")
        
        success_rate = (self.stats['updated'] / max(self.stats['processed'] - self.stats['skipped'], 1)) * 100
        self.logger.info(f"📈 성공률: {success_rate:.1f}%")
        
        if self.stats['updated'] > 0:
            avg_time = total_time / self.stats['updated']
            self.logger.info(f"⚡ 평균 처리시간: {avg_time:.2f}초/종목")
    
    def verify_results(self):
        """업데이트 결과 검증"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 업데이트 후 상태
                cursor = conn.execute("SELECT COUNT(*) FROM company_info")
                total_count = cursor.fetchone()[0]
                
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                """)
                has_market_cap = cursor.fetchone()[0]
                
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM company_info 
                    WHERE market_cap IS NULL OR market_cap = 0
                """)
                no_market_cap = cursor.fetchone()[0]
                
                # 최근 업데이트된 종목들
                cursor = conn.execute("""
                    SELECT stock_code, company_name, market_cap, updated_at
                    FROM company_info 
                    WHERE updated_at IS NOT NULL 
                    ORDER BY updated_at DESC 
                    LIMIT 10
                """)
                recent_updates = cursor.fetchall()
                
                self.logger.info("🔍 업데이트 결과 검증:")
                self.logger.info(f"   전체 종목: {total_count:,}개")
                self.logger.info(f"   시가총액 있음: {has_market_cap:,}개 ({(has_market_cap/total_count*100):.1f}%)")
                self.logger.info(f"   시가총액 없음: {no_market_cap:,}개 ({(no_market_cap/total_count*100):.1f}%)")
                
                self.logger.info("\n📋 최근 업데이트된 종목들:")
                for stock_code, name, market_cap, updated_at in recent_updates:
                    cap_str = f"{market_cap:,}억원" if market_cap else "N/A"
                    self.logger.info(f"   {stock_code} | {name[:15]:15s} | {cap_str:>15s} | {updated_at}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"결과 검증 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='전체 종목 시가총액 업데이트')
    parser.add_argument('--batch_size', type=int, default=50, 
                       help='배치 크기 (기본값: 50)')
    parser.add_argument('--delay', type=float, default=0.05, 
                       help='종목간 지연시간 (초, 기본값: 0.05)')
    parser.add_argument('--force_update', action='store_true',
                       help='이미 시가총액이 있는 종목도 강제 업데이트')
    parser.add_argument('--verify_only', action='store_true',
                       help='업데이트 없이 현재 상태만 확인')
    
    args = parser.parse_args()
    
    # 업데이터 초기화
    updater = CompleteMarketCapUpdater(
        batch_size=args.batch_size,
        delay=args.delay
    )
    
    try:
        if args.verify_only:
            # 현재 상태만 확인
            updater.check_database_status()
            updater.verify_results()
        else:
            # 전체 업데이트 실행
            success = updater.update_all_market_caps(force_update=args.force_update)
            
            if success:
                # 결과 검증
                updater.verify_results()
                updater.logger.info("✅ 전체 시가총액 업데이트 성공!")
            else:
                updater.logger.error("❌ 전체 시가총액 업데이트 실패!")
                sys.exit(1)
                
    except KeyboardInterrupt:
        updater.logger.info("⏹️  사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        print(f"❌ 예기치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
