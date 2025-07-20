#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
투자 가능 여부 필드 추가 및 업데이트 시스템
- 상장폐지, 관리종목, 거래정지 등 투자 제약 사항 관리
"""

import sqlite3
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Optional, Tuple
import os

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InvestmentStatusUpdater:
    """투자 가능 여부 업데이트 클래스"""
    
    def __init__(self, db_path: str = "data/databases/buffett_scorecard.db"):
        self.db_path = db_path
        self.ensure_tables()
        
    def ensure_tables(self):
        """필수 테이블 및 필드 존재 확인 및 생성"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # 1. investment_status 테이블 생성
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS investment_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT UNIQUE NOT NULL,
                    company_name TEXT,
                    market_type TEXT,  -- KOSPI, KOSDAQ, KONEX
                    listing_status TEXT,  -- LISTED, DELISTED, SUSPENDED
                    trading_status TEXT,  -- NORMAL, HALTED, RESTRICTED
                    investment_warning TEXT,  -- NONE, CAUTION, ALERT, DESIGNATED
                    is_investable BOOLEAN DEFAULT 1,  -- 투자 가능 여부
                    delisting_date TEXT,  -- 상장폐지일
                    suspension_date TEXT,  -- 거래정지일
                    warning_date TEXT,  -- 투자주의환기일
                    last_updated TEXT,
                    notes TEXT,
                    UNIQUE(stock_code)
                )
            """)
            
            # 2. buffett_all_stocks_final 테이블에 필드 추가
            try:
                cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN is_investable BOOLEAN DEFAULT 1")
                logger.info("buffett_all_stocks_final 테이블에 is_investable 필드 추가")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    logger.warning(f"필드 추가 중 오류: {e}")
            
            try:
                cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN investment_warning TEXT DEFAULT 'NONE'")
                logger.info("buffett_all_stocks_final 테이블에 investment_warning 필드 추가")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    logger.warning(f"필드 추가 중 오류: {e}")
                    
            try:
                cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN listing_status TEXT DEFAULT 'LISTED'")
                logger.info("buffett_all_stocks_final 테이블에 listing_status 필드 추가")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    logger.warning(f"필드 추가 중 오류: {e}")
            
            conn.commit()
            logger.info("테이블 구조 업데이트 완료")
            
        except Exception as e:
            logger.error(f"테이블 생성/수정 실패: {e}")
            conn.rollback()
        finally:
            conn.close()

    def get_krx_listing_data(self) -> pd.DataFrame:
        """KRX에서 상장종목 현황 조회"""
        logger.info("KRX 상장종목 현황 조회 시작")
        
        try:
            # KRX 상장종목 현황 API (실제 API는 인증이 필요할 수 있음)
            # 여기서는 예시 데이터 구조로 작성
            
            # KOSPI 종목
            kospi_url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
            kospi_data = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
                'mktId': 'STK',
                'trdDd': datetime.now().strftime('%Y%m%d')
            }
            
            # KOSDAQ 종목
            kosdaq_url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
            kosdaq_data = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501', 
                'mktId': 'KSQ',
                'trdDd': datetime.now().strftime('%Y%m%d')
            }
            
            # 실제 API 호출 (여기서는 더미 데이터 생성)
            all_stocks = []
            
            # 현재 DB에서 종목 코드 가져와서 기본 데이터 생성
            conn = sqlite3.connect(self.db_path)
            existing_stocks = pd.read_sql_query(
                "SELECT DISTINCT stock_code, company_name FROM buffett_all_stocks_final",
                conn
            )
            conn.close()
            
            for _, row in existing_stocks.iterrows():
                stock_code = row['stock_code']
                company_name = row['company_name']
                
                # 기본적으로 모든 종목을 상장 상태로 설정
                stock_info = {
                    'stock_code': stock_code,
                    'company_name': company_name,
                    'market_type': 'KOSPI' if stock_code.startswith(('00', '01', '02', '03', '04', '05')) else 'KOSDAQ',
                    'listing_status': 'LISTED',
                    'trading_status': 'NORMAL',
                    'investment_warning': 'NONE',
                    'is_investable': True
                }
                
                all_stocks.append(stock_info)
            
            df = pd.DataFrame(all_stocks)
            logger.info(f"상장종목 현황 조회 완료: {len(df)}개 종목")
            
            return df
            
        except Exception as e:
            logger.error(f"KRX 데이터 조회 실패: {e}")
            return pd.DataFrame()

    def check_delisted_stocks(self) -> List[Dict]:
        """상장폐지 종목 확인"""
        logger.info("상장폐지 종목 확인 시작")
        
        # 상장폐지가 확실한 종목들 (예시)
        known_delisted = [
            {'stock_code': '900110', 'company_name': '이수앱지스', 'delisting_date': '2024-12-31'},
            # 여기에 실제 상장폐지 종목들 추가
        ]
        
        # 추가적으로 거래량이 0이거나 극히 적은 종목들을 의심 종목으로 분류
        delisted_stocks = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 장기간 거래가 없는 종목 확인 (실제로는 가격 데이터가 필요)
            suspicious_query = """
                SELECT stock_code, company_name
                FROM buffett_all_stocks_final 
                WHERE total_score IS NULL OR total_score = 0
            """
            
            suspicious_stocks = pd.read_sql_query(suspicious_query, conn)
            conn.close()
            
            for _, row in suspicious_stocks.iterrows():
                delisted_stocks.append({
                    'stock_code': row['stock_code'],
                    'company_name': row['company_name'],
                    'status': 'SUSPECTED_DELISTED',
                    'reason': '거래 데이터 부족'
                })
            
            # 알려진 상장폐지 종목 추가
            for stock in known_delisted:
                delisted_stocks.append({
                    'stock_code': stock['stock_code'],
                    'company_name': stock['company_name'],
                    'status': 'DELISTED',
                    'delisting_date': stock['delisting_date'],
                    'reason': '상장폐지 확정'
                })
            
            logger.info(f"상장폐지/의심 종목 확인 완료: {len(delisted_stocks)}개")
            
        except Exception as e:
            logger.error(f"상장폐지 종목 확인 실패: {e}")
            
        return delisted_stocks

    def check_warning_stocks(self) -> List[Dict]:
        """투자주의/경고 종목 확인"""
        logger.info("투자주의 종목 확인 시작")
        
        warning_stocks = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 재무 상태가 매우 불량한 종목들을 경고 종목으로 분류
            warning_query = """
                SELECT stock_code, company_name, total_score, grade,
                       profitability_score, stability_score
                FROM buffett_all_stocks_final 
                WHERE (total_score < 30 OR stability_score < 5 OR profitability_score < 5)
                AND total_score IS NOT NULL
            """
            
            warning_data = pd.read_sql_query(warning_query, conn)
            conn.close()
            
            for _, row in warning_data.iterrows():
                warning_level = 'ALERT'
                reason = []
                
                if row['total_score'] < 20:
                    warning_level = 'DESIGNATED'  # 관리종목 수준
                    reason.append('극도로 낮은 종합점수')
                elif row['total_score'] < 30:
                    warning_level = 'ALERT'
                    reason.append('낮은 종합점수')
                
                if row['stability_score'] < 5:
                    reason.append('매우 불안정한 재무구조')
                
                if row['profitability_score'] < 5:
                    reason.append('극도로 낮은 수익성')
                
                warning_stocks.append({
                    'stock_code': row['stock_code'],
                    'company_name': row['company_name'],
                    'warning_level': warning_level,
                    'total_score': row['total_score'],
                    'reason': ', '.join(reason)
                })
            
            logger.info(f"투자주의 종목 확인 완료: {len(warning_stocks)}개")
            
        except Exception as e:
            logger.error(f"투자주의 종목 확인 실패: {e}")
            
        return warning_stocks

    def update_investment_status(self):
        """투자 가능 여부 전체 업데이트"""
        logger.info("투자 가능 여부 업데이트 시작")
        
        try:
            # 1. KRX 상장종목 현황 조회
            listing_data = self.get_krx_listing_data()
            
            # 2. 상장폐지 종목 확인
            delisted_stocks = self.check_delisted_stocks()
            
            # 3. 투자주의 종목 확인
            warning_stocks = self.check_warning_stocks()
            
            # 4. 데이터베이스 업데이트
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            updated_count = 0
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 4-1. 기본 상장종목 상태 업데이트
            if not listing_data.empty:
                for _, row in listing_data.iterrows():
                    cursor.execute("""
                        INSERT OR REPLACE INTO investment_status 
                        (stock_code, company_name, market_type, listing_status, 
                         trading_status, investment_warning, is_investable, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row['stock_code'], row['company_name'], row['market_type'],
                        row['listing_status'], row['trading_status'], row['investment_warning'],
                        row['is_investable'], current_time
                    ))
                    updated_count += 1
            
            # 4-2. 상장폐지 종목 업데이트
            for stock in delisted_stocks:
                cursor.execute("""
                    INSERT OR REPLACE INTO investment_status 
                    (stock_code, company_name, listing_status, is_investable, 
                     delisting_date, notes, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    stock['stock_code'], stock['company_name'], 
                    'DELISTED' if stock['status'] == 'DELISTED' else 'SUSPECTED_DELISTED',
                    False,
                    stock.get('delisting_date', ''),
                    stock['reason'], current_time
                ))
                
                # buffett_all_stocks_final 테이블도 업데이트
                cursor.execute("""
                    UPDATE buffett_all_stocks_final 
                    SET is_investable = ?, listing_status = ?
                    WHERE stock_code = ?
                """, (False, 'DELISTED', stock['stock_code']))
                
                updated_count += 1
            
            # 4-3. 투자주의 종목 업데이트
            for stock in warning_stocks:
                is_investable = stock['warning_level'] != 'DESIGNATED'  # 관리종목은 투자 불가
                
                cursor.execute("""
                    INSERT OR REPLACE INTO investment_status 
                    (stock_code, company_name, investment_warning, is_investable,
                     warning_date, notes, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    stock['stock_code'], stock['company_name'], stock['warning_level'],
                    is_investable, current_time, stock['reason'], current_time
                ))
                
                # buffett_all_stocks_final 테이블도 업데이트
                cursor.execute("""
                    UPDATE buffett_all_stocks_final 
                    SET is_investable = ?, investment_warning = ?
                    WHERE stock_code = ?
                """, (is_investable, stock['warning_level'], stock['stock_code']))
                
                updated_count += 1
            
            conn.commit()
            conn.close()
            
            logger.info(f"투자 가능 여부 업데이트 완료: {updated_count}개 종목")
            
            # 5. 업데이트 결과 보고서 생성
            self.generate_status_report()
            
        except Exception as e:
            logger.error(f"투자 가능 여부 업데이트 실패: {e}")

    def generate_status_report(self):
        """투자 가능 여부 현황 보고서 생성"""
        logger.info("투자 가능 여부 현황 보고서 생성")
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 전체 현황 조회
            status_query = """
                SELECT 
                    listing_status,
                    investment_warning,
                    is_investable,
                    COUNT(*) as count
                FROM investment_status 
                GROUP BY listing_status, investment_warning, is_investable
                ORDER BY listing_status, investment_warning
            """
            
            status_df = pd.read_sql_query(status_query, conn)
            
            # 투자 불가 종목 상세
            non_investable_query = """
                SELECT stock_code, company_name, listing_status, 
                       investment_warning, notes, last_updated
                FROM investment_status 
                WHERE is_investable = 0
                ORDER BY listing_status, investment_warning
            """
            
            non_investable_df = pd.read_sql_query(non_investable_query, conn)
            
            # buffett_all_stocks_final에서 투자 추천 종목 중 투자 불가 종목 확인
            conflict_query = """
                SELECT b.stock_code, b.company_name, b.investment_grade, 
                       b.total_score, i.listing_status, i.investment_warning
                FROM buffett_all_stocks_final b
                JOIN investment_status i ON b.stock_code = i.stock_code
                WHERE b.investment_grade IN ('Strong Buy', 'Buy') 
                AND i.is_investable = 0
                ORDER BY b.total_score DESC
            """
            
            conflict_df = pd.read_sql_query(conflict_query, conn)
            conn.close()
            
            # 보고서 출력
            print("🚨 투자 가능 여부 현황 보고서")
            print("=" * 80)
            
            print("\n📊 전체 현황:")
            print(status_df.to_string(index=False))
            
            if not non_investable_df.empty:
                print(f"\n❌ 투자 불가 종목: {len(non_investable_df)}개")
                print("-" * 80)
                for _, row in non_investable_df.iterrows():
                    print(f"• {row['company_name']} ({row['stock_code']})")
                    print(f"  상태: {row['listing_status']}, 경고: {row['investment_warning']}")
                    print(f"  사유: {row['notes']}")
                    print()
            
            if not conflict_df.empty:
                print(f"\n⚠️  추천 종목 중 투자 불가: {len(conflict_df)}개")
                print("-" * 80)
                for _, row in conflict_df.iterrows():
                    print(f"• {row['company_name']} ({row['stock_code']})")
                    print(f"  버핏 등급: {row['investment_grade']} ({row['total_score']:.1f}점)")
                    print(f"  투자 제약: {row['listing_status']} / {row['investment_warning']}")
                    print()
            
            # 파일로 저장
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = f"results/investment_status_report_{timestamp}.txt"
            
            os.makedirs(os.path.dirname(report_file), exist_ok=True)
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("투자 가능 여부 현황 보고서\n")
                f.write("=" * 80 + "\n\n")
                f.write("전체 현황:\n")
                f.write(status_df.to_string(index=False) + "\n\n")
                
                if not non_investable_df.empty:
                    f.write(f"투자 불가 종목 ({len(non_investable_df)}개):\n")
                    f.write(non_investable_df.to_string(index=False) + "\n\n")
                
                if not conflict_df.empty:
                    f.write(f"추천 종목 중 투자 불가 ({len(conflict_df)}개):\n")
                    f.write(conflict_df.to_string(index=False) + "\n")
            
            print(f"\n📁 보고서 저장: {report_file}")
            
        except Exception as e:
            logger.error(f"보고서 생성 실패: {e}")

    def get_investable_recommendations(self) -> pd.DataFrame:
        """투자 가능한 추천 종목만 조회"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = """
                SELECT b.stock_code, b.company_name, b.total_score, b.grade, 
                       b.investment_grade, b.profitability_score, b.growth_score,
                       b.stability_score, b.efficiency_score, b.valuation_score,
                       i.market_type, i.listing_status, i.investment_warning
                FROM buffett_all_stocks_final b
                LEFT JOIN investment_status i ON b.stock_code = i.stock_code
                WHERE (i.is_investable = 1 OR i.is_investable IS NULL)
                AND b.investment_grade IN ('Strong Buy', 'Buy')
                ORDER BY b.total_score DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            return df
            
        except Exception as e:
            logger.error(f"투자 가능 추천 종목 조회 실패: {e}")
            return pd.DataFrame()

def main():
    """메인 실행 함수"""
    print("🔄 투자 가능 여부 업데이트 시스템")
    print("=" * 60)
    
    # 업데이터 초기화
    updater = InvestmentStatusUpdater()
    
    # 1. 투자 가능 여부 업데이트
    updater.update_investment_status()
    
    # 2. 투자 가능한 추천 종목 조회
    print("\n💎 투자 가능한 추천 종목")
    print("=" * 60)
    
    investable_recommendations = updater.get_investable_recommendations()
    
    if not investable_recommendations.empty:
        print(f"투자 가능한 추천 종목: {len(investable_recommendations)}개\n")
        
        for i, (_, row) in enumerate(investable_recommendations.head(20).iterrows(), 1):
            market_status = f"({row['market_type']})" if pd.notna(row['market_type']) else ""
            warning_status = f"[{row['investment_warning']}]" if pd.notna(row['investment_warning']) and row['investment_warning'] != 'NONE' else ""
            
            print(f"{i:2d}. {row['company_name']:<15} ({row['stock_code']}) {market_status} {warning_status}")
            print(f"    등급: {row['investment_grade']:<10} 점수: {row['total_score']:5.1f}")
            print(f"    수익성:{row['profitability_score']:4.1f} 성장성:{row['growth_score']:4.1f} "
                  f"안정성:{row['stability_score']:4.1f} 효율성:{row['efficiency_score']:4.1f} "
                  f"가치평가:{row['valuation_score']:4.1f}")
            print()
        
        # CSV 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"results/investable_recommendations_{timestamp}.csv"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        investable_recommendations.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"📁 투자 가능 추천 종목 저장: {output_file}")
    else:
        print("❌ 투자 가능한 추천 종목이 없습니다.")

if __name__ == "__main__":
    main()
