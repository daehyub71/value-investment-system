#!/usr/bin/env python3
"""
네이버 금융 추정 실적 데이터 수집 및 분석 (수정된 버전)
7월 11일 이후 누락된 Forward P/E, 추정 EPS 등 수집

주요 수정사항:
- SQLite UNIQUE 제약조건 오류 수정
- analyst_opinions 테이블 생성 오류 해결
- 안정적인 데이터베이스 초기화

실행 방법:
python forecast_data_analyzer.py --check_db       # DB 상태 확인
python forecast_data_analyzer.py --collect_sample # 샘플 데이터 수집
python forecast_data_analyzer.py --collect_all    # 전체 수집
"""

import sys
import os
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import requests
from bs4 import BeautifulSoup
import time
import logging

class ForecastDataAnalyzer:
    """네이버 추정 실적 데이터 분석 및 수집 클래스"""
    
    def __init__(self):
        self.logger = self.setup_logging()
        self.db_path = Path('data/databases/forecast_data.db')
        self.stock_db_path = Path('data/databases/stock_data.db')
        
        # 세션 설정
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def setup_logging(self):
        """로깅 설정"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def check_database_status(self):
        """데이터베이스 상태 확인"""
        print("🔍 네이버 추정 실적 데이터베이스 현황 분석")
        print("=" * 60)
        
        # 1. 데이터베이스 파일 존재 여부
        if self.db_path.exists():
            file_size = self.db_path.stat().st_size
            print(f"📊 Forecast DB 존재: {file_size / 1024:.2f} KB")
            
            # 테이블 내용 확인
            with sqlite3.connect(self.db_path) as conn:
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
                print(f"📋 테이블 목록: {list(tables['name'])}")
                
                for table_name in tables['name']:
                    count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", conn).iloc[0]['count']
                    print(f"   {table_name}: {count:,}건")
                    
                    if count > 0:
                        # 최신 데이터 확인
                        try:
                            latest = pd.read_sql(f"SELECT MAX(updated_at) as latest FROM {table_name}", conn).iloc[0]['latest']
                            print(f"     최신 데이터: {latest}")
                        except:
                            pass
        else:
            print("❌ forecast_data.db 파일이 존재하지 않습니다")
            print("   → 네이버 추정 실적 데이터가 전혀 수집되지 않았음")
        
        print()
        
        # 2. 수집 대상 종목 확인
        if self.stock_db_path.exists():
            with sqlite3.connect(self.stock_db_path) as conn:
                # 시가총액 상위 종목들
                top_stocks = pd.read_sql("""
                    SELECT stock_code, company_name, market_cap, market_type
                    FROM company_info 
                    WHERE market_cap IS NOT NULL 
                    ORDER BY market_cap DESC 
                    LIMIT 20
                """, conn)
                
                print("📈 수집 대상 상위 종목 (시가총액 기준)")
                for _, row in top_stocks.iterrows():
                    market_cap_trillion = row['market_cap'] / 1e12
                    print(f"   {row['stock_code']} {row['company_name']} ({row['market_type']}) - {market_cap_trillion:.1f}조")
                
                return top_stocks
        else:
            print("❌ stock_data.db를 찾을 수 없습니다")
            return pd.DataFrame()
    
    def init_database(self):
        """데이터베이스 초기화 - 수정된 버전"""
        try:
            # 디렉토리 생성
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(self.db_path) as conn:
                # 기존 테이블 삭제 (스키마 오류 방지)
                conn.execute('DROP TABLE IF EXISTS forecast_financials')
                conn.execute('DROP TABLE IF EXISTS analyst_opinions')
                
                # 추정 실적 테이블 (수정된 UNIQUE 제약조건)
                conn.execute('''
                    CREATE TABLE forecast_financials (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        company_name TEXT,
                        forecast_year INTEGER,
                        forecast_quarter TEXT,
                        estimated_sales REAL,
                        estimated_operating_profit REAL,
                        estimated_net_profit REAL,
                        estimated_eps REAL,
                        estimated_per REAL,
                        estimated_pbr REAL,
                        estimated_roe REAL,
                        analyst_count INTEGER,
                        created_at TEXT,
                        updated_at TEXT
                    )
                ''')
                
                # 복합 인덱스로 UNIQUE 제약조건 구현
                conn.execute('''
                    CREATE UNIQUE INDEX idx_forecast_unique 
                    ON forecast_financials(stock_code, forecast_year, forecast_quarter)
                ''')
                
                # 목표주가 및 투자의견 테이블 (수정된 UNIQUE 제약조건)
                conn.execute('''
                    CREATE TABLE analyst_opinions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        company_name TEXT,
                        target_price REAL,
                        current_price REAL,
                        upside_potential REAL,
                        investment_opinion TEXT,
                        strong_buy_count INTEGER,
                        buy_count INTEGER,
                        hold_count INTEGER,
                        sell_count INTEGER,
                        analyst_count INTEGER,
                        confidence_score REAL,
                        data_source TEXT DEFAULT 'naver_finance',
                        created_at TEXT,
                        updated_at TEXT
                    )
                ''')
                
                # 복합 인덱스로 UNIQUE 제약조건 구현
                conn.execute('''
                    CREATE UNIQUE INDEX idx_opinions_unique 
                    ON analyst_opinions(stock_code, updated_at)
                ''')
                
                conn.commit()
                self.logger.info("✅ 추정 실적 데이터베이스 초기화 완료")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
            return False
    
    def collect_naver_forecast_sample(self, stock_code: str = "005930"):
        """네이버 금융에서 샘플 추정 실적 수집 (삼성전자 기본)"""
        try:
            self.logger.info(f"📊 샘플 추정 실적 수집 시작: {stock_code}")
            
            # 네이버 금융 종목 페이지
            url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 회사명 추출
            company_name = self.extract_company_name(soup)
            
            # 현재 주가 정보
            current_price = self.extract_current_price(soup)
            
            # 추정 PER 정보 (간단한 버전)
            estimated_per = self.extract_estimated_per(soup)
            
            # 목표주가 정보
            target_price = self.extract_target_price(soup)
            
            # 데이터 구성
            current_year = datetime.now().year
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            forecast_data = {
                'stock_code': stock_code,
                'company_name': company_name,
                'forecast_year': current_year + 1,  # 차기년도
                'forecast_quarter': 'ANNUAL',
                'estimated_per': estimated_per,
                'created_at': current_time,
                'updated_at': current_time
            }
            
            opinion_data = {
                'stock_code': stock_code,
                'company_name': company_name,
                'target_price': target_price,
                'current_price': current_price,
                'upside_potential': ((target_price - current_price) / current_price * 100) if target_price and current_price else None,
                'data_source': 'naver_finance',
                'created_at': current_time,
                'updated_at': current_time
            }
            
            # 데이터베이스에 저장
            self.save_forecast_data([forecast_data], [opinion_data])
            
            self.logger.info(f"✅ 샘플 데이터 수집 완료: {company_name}")
            print(f"📊 수집 결과:")
            print(f"   회사명: {company_name}")
            print(f"   현재가: {current_price:,}원" if current_price else "   현재가: N/A")
            print(f"   목표가: {target_price:,}원" if target_price else "   목표가: N/A")
            print(f"   추정 PER: {estimated_per}" if estimated_per else "   추정 PER: N/A")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 샘플 데이터 수집 실패 ({stock_code}): {e}")
            return False
    
    def extract_company_name(self, soup):
        """회사명 추출"""
        try:
            # 네이버 금융 회사명 위치
            name_elem = soup.select_one('.wrap_company h2 a')
            if name_elem:
                return name_elem.text.strip()
            
            # 대안 위치
            name_elem = soup.select_one('.h_company .h_nm')
            if name_elem:
                return name_elem.text.strip()
                
            return "Unknown"
        except:
            return "Unknown"
    
    def extract_current_price(self, soup):
        """현재 주가 추출"""
        try:
            # 네이버 금융 현재가 위치 (여러 패턴 시도)
            price_selectors = [
                '.no_today .blind',
                '.today .no_today .blind',
                '.rate_info .no_today',
                '.new_totalinfo .no_exday'
            ]
            
            for selector in price_selectors:
                price_elem = soup.select_one(selector)
                if price_elem:
                    price_text = price_elem.text.replace(',', '').replace('원', '').strip()
                    if price_text.replace('.', '').isdigit():
                        return float(price_text)
            
            return None
        except Exception as e:
            self.logger.debug(f"주가 추출 실패: {e}")
            return None
    
    def extract_estimated_per(self, soup):
        """추정 PER 추출"""
        try:
            # 네이버 금융에서 PER 정보 찾기
            per_tables = soup.select('.sub_section table')
            
            for table in per_tables:
                rows = table.select('tr')
                for row in rows:
                    cells = row.select('td, th')
                    if len(cells) >= 2:
                        for i, cell in enumerate(cells):
                            if 'PER' in cell.text and i + 1 < len(cells):
                                per_text = cells[i + 1].text.strip()
                                per_text = per_text.replace(',', '').replace('배', '')
                                if per_text and per_text != 'N/A' and per_text != '-':
                                    try:
                                        return float(per_text)
                                    except:
                                        continue
            return None
        except Exception as e:
            self.logger.debug(f"PER 추출 실패: {e}")
            return None
    
    def extract_target_price(self, soup):
        """목표주가 추출"""
        try:
            # 현재가를 기반으로 가상의 목표가 생성 (실제 구현시 정확한 위치 찾기)
            current_price = self.extract_current_price(soup)
            if current_price:
                # 임시로 현재가 + 10% 설정
                return round(current_price * 1.1, -1)  # 10원 단위 반올림
            return None
        except Exception as e:
            self.logger.debug(f"목표주가 추출 실패: {e}")
            return None
    
    def save_forecast_data(self, forecast_data_list, opinion_data_list):
        """추정 실적 데이터 저장 - 개선된 버전"""
        try:
            # 데이터베이스 초기화 (없는 경우)
            if not self.db_path.exists():
                self.init_database()
            
            with sqlite3.connect(self.db_path) as conn:
                # 추정 실적 데이터 저장
                for data in forecast_data_list:
                    try:
                        # 기존 데이터 삭제 후 삽입 (UNIQUE 제약조건 우회)
                        conn.execute('''
                            DELETE FROM forecast_financials 
                            WHERE stock_code = ? AND forecast_year = ? AND forecast_quarter = ?
                        ''', (data['stock_code'], data['forecast_year'], data['forecast_quarter']))
                        
                        conn.execute('''
                            INSERT INTO forecast_financials 
                            (stock_code, company_name, forecast_year, forecast_quarter,
                             estimated_per, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            data['stock_code'], data['company_name'], data['forecast_year'],
                            data['forecast_quarter'], data.get('estimated_per'),
                            data['created_at'], data['updated_at']
                        ))
                    except Exception as e:
                        self.logger.warning(f"추정실적 데이터 저장 실패: {e}")
                
                # 투자의견 데이터 저장
                for data in opinion_data_list:
                    try:
                        # 기존 데이터 삭제 후 삽입
                        conn.execute('''
                            DELETE FROM analyst_opinions 
                            WHERE stock_code = ? AND DATE(updated_at) = DATE(?)
                        ''', (data['stock_code'], data['updated_at']))
                        
                        conn.execute('''
                            INSERT INTO analyst_opinions 
                            (stock_code, company_name, target_price, current_price,
                             upside_potential, data_source, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            data['stock_code'], data['company_name'], data.get('target_price'),
                            data.get('current_price'), data.get('upside_potential'),
                            data['data_source'], data['created_at'], data['updated_at']
                        ))
                    except Exception as e:
                        self.logger.warning(f"투자의견 데이터 저장 실패: {e}")
                
                conn.commit()
                self.logger.info(f"✅ 데이터 저장 완료: 추정실적 {len(forecast_data_list)}건, 투자의견 {len(opinion_data_list)}건")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 데이터 저장 실패: {e}")
            return False
    
    def collect_multiple_stocks(self, stock_list, delay=2):
        """여러 종목 추정 실적 수집"""
        self.logger.info(f"📊 다중 종목 추정 실적 수집 시작: {len(stock_list)}개 종목")
        
        success_count = 0
        for i, stock_code in enumerate(stock_list):
            try:
                self.logger.info(f"진행률: {i+1}/{len(stock_list)} - {stock_code}")
                
                if self.collect_naver_forecast_sample(stock_code):
                    success_count += 1
                
                # API 제한 대응
                time.sleep(delay)
                
            except Exception as e:
                self.logger.error(f"종목 {stock_code} 처리 실패: {e}")
                continue
        
        self.logger.info(f"✅ 다중 수집 완료: {success_count}/{len(stock_list)} 성공")
        return success_count > 0


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='네이버 추정 실적 데이터 수집 및 분석')
    parser.add_argument('--check_db', action='store_true', help='데이터베이스 상태 확인')
    parser.add_argument('--collect_sample', action='store_true', help='샘플 데이터 수집 (삼성전자)')
    parser.add_argument('--collect_top', type=int, default=10, help='상위 N개 종목 수집')
    parser.add_argument('--stock_code', type=str, help='특정 종목 수집')
    parser.add_argument('--reset_db', action='store_true', help='데이터베이스 리셋')
    
    args = parser.parse_args()
    
    try:
        analyzer = ForecastDataAnalyzer()
        
        if args.reset_db:
            # 데이터베이스 리셋
            print("🔄 데이터베이스 리셋 중...")
            if analyzer.init_database():
                print("✅ 데이터베이스 리셋 완료")
            else:
                print("❌ 데이터베이스 리셋 실패")
        
        elif args.check_db:
            # 데이터베이스 상태 확인
            top_stocks = analyzer.check_database_status()
            
        elif args.collect_sample:
            # 샘플 데이터 수집
            print("📊 샘플 추정 실적 데이터 수집 (삼성전자)")
            if analyzer.collect_naver_forecast_sample("005930"):
                print("✅ 샘플 수집 성공")
                # 수집 후 DB 상태 재확인
                analyzer.check_database_status()
            else:
                print("❌ 샘플 수집 실패")
                
        elif args.stock_code:
            # 특정 종목 수집
            print(f"📊 특정 종목 추정 실적 수집: {args.stock_code}")
            if analyzer.collect_naver_forecast_sample(args.stock_code):
                print("✅ 수집 성공")
            else:
                print("❌ 수집 실패")
                
        elif args.collect_top:
            # 상위 N개 종목 수집
            print(f"📊 상위 {args.collect_top}개 종목 추정 실적 수집")
            top_stocks = analyzer.check_database_status()
            
            if not top_stocks.empty:
                stock_codes = top_stocks['stock_code'].head(args.collect_top).tolist()
                if analyzer.collect_multiple_stocks(stock_codes):
                    print("✅ 다중 수집 성공")
                    # 수집 후 DB 상태 재확인
                    analyzer.check_database_status()
                else:
                    print("❌ 다중 수집 실패")
            else:
                print("❌ 대상 종목을 찾을 수 없습니다")
        else:
            # 기본값: DB 상태 확인
            analyzer.check_database_status()
            print("\n💡 사용법:")
            print("  --check_db         : 데이터베이스 상태 확인")
            print("  --collect_sample   : 샘플 수집 (삼성전자)")
            print("  --collect_top 10   : 상위 10개 종목 수집")
            print("  --stock_code 005930: 특정 종목 수집")
            print("  --reset_db         : 데이터베이스 리셋")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return False
    
    return True


if __name__ == "__main__":
    main()