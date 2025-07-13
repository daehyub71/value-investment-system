#!/usr/bin/env python3
"""
네이버 금융 추정 실적 데이터 수집기
DART 시차 문제 해결을 위한 Forward P/E, 추정 EPS 수집

수집 데이터:
- 애널리스트 추정 EPS (당기, 차기)
- Forward P/E (예상 주가수익비율)
- 추정 매출, 영업이익, 순이익
- 목표주가 및 투자의견

실행 방법:
python scripts/data_collection/collect_forecast_data.py --stock_code=005930
"""

import sys
import os
import requests
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import sqlite3

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

class ForecastDataCollector:
    """추정 실적 데이터 수집 클래스"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_path = Path('data/databases/forecast_data.db')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 데이터베이스 초기화
        self._init_database()
    
    def _init_database(self):
        """데이터베이스 테이블 초기화"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 추정 실적 테이블
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS forecast_financials (
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
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(stock_code, forecast_year, forecast_quarter)
                    )
                ''')
                
                # 목표주가 및 투자의견 테이블
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS analyst_opinions (
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
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(stock_code, DATE(updated_at))
                    )
                ''')
                
                conn.commit()
                self.logger.info("추정 실적 데이터베이스 초기화 완료")
                
        except Exception as e:
            self.logger.error(f"데이터베이스 초기화 실패: {e}")
    
    def collect_naver_forecast_data(self, stock_code: str) -> Optional[Dict]:
        """네이버 금융에서 추정 실적 데이터 수집"""
        try:
            # 네이버 금융 추정실적 페이지
            url = f"https://finance.naver.com/item/fchart.naver?code={stock_code}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 회사명 추출
            company_name = self._extract_company_name(soup)
            
            # 추정 실적 테이블 찾기
            forecast_table = soup.select_one('#contentarea_left')
            if not forecast_table:
                self.logger.warning(f"추정 실적 데이터를 찾을 수 없습니다: {stock_code}")
                return None
            
            # 추정 실적 데이터 파싱
            forecast_data = self._parse_forecast_table(forecast_table, stock_code, company_name)
            
            if forecast_data:
                self.logger.info(f"✅ 추정 실적 수집 완료: {stock_code} ({company_name})")
                return forecast_data
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 네이버 금융 데이터 수집 실패 ({stock_code}): {e}")
            return None
    
    def collect_analyst_opinions(self, stock_code: str) -> Optional[Dict]:
        """네이버 금융에서 애널리스트 투자의견 수집"""
        try:
            # 네이버 금융 투자의견 페이지
            url = f"https://finance.naver.com/item/point.naver?code={stock_code}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 회사명 추출
            company_name = self._extract_company_name(soup)
            
            # 투자의견 데이터 파싱
            opinion_data = self._parse_analyst_opinions(soup, stock_code, company_name)
            
            if opinion_data:
                self.logger.info(f"✅ 투자의견 수집 완료: {stock_code} - 목표가 {opinion_data.get('target_price', 'N/A')}원")
                return opinion_data
            
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 투자의견 수집 실패 ({stock_code}): {e}")
            return None
    
    def _extract_company_name(self, soup) -> str:
        """회사명 추출"""
        try:
            name_elem = soup.select_one('.wrap_company h2 a')
            if name_elem:
                return name_elem.text.strip()
            return "Unknown"
        except:
            return "Unknown"
    
    def _parse_forecast_table(self, table_elem, stock_code: str, company_name: str) -> Dict:
        """추정 실적 테이블 파싱"""
        try:
            # 실제 구현에서는 네이버 금융의 HTML 구조에 맞게 조정 필요
            # 여기서는 예시 구조로 작성
            
            current_year = datetime.now().year
            
            forecast_data = {
                'stock_code': stock_code,
                'company_name': company_name,
                'forecasts': []
            }
            
            # 당기 및 차기 추정 실적 (예시)
            for year_offset in [0, 1]:  # 당기, 차기
                forecast_year = current_year + year_offset
                
                # 네이버 금융에서 실제 데이터 추출 로직
                # (실제 구현시 네이버 금융 HTML 구조 분석 필요)
                
                forecast_item = {
                    'forecast_year': forecast_year,
                    'forecast_quarter': 'ANNUAL',
                    'estimated_sales': None,  # 추정 매출
                    'estimated_operating_profit': None,  # 추정 영업이익
                    'estimated_net_profit': None,  # 추정 순이익
                    'estimated_eps': None,  # 추정 EPS
                    'estimated_per': None,  # 추정 PER
                    'estimated_pbr': None,  # 추정 PBR
                    'estimated_roe': None,  # 추정 ROE
                    'analyst_count': None  # 참여 애널리스트 수
                }
                
                forecast_data['forecasts'].append(forecast_item)
            
            return forecast_data
            
        except Exception as e:
            self.logger.error(f"추정 실적 테이블 파싱 실패: {e}")
            return {}
    
    def _parse_analyst_opinions(self, soup, stock_code: str, company_name: str) -> Dict:
        """애널리스트 투자의견 파싱"""
        try:
            opinion_data = {
                'stock_code': stock_code,
                'company_name': company_name,
                'target_price': None,
                'current_price': None,
                'upside_potential': None,
                'investment_opinion': None,
                'strong_buy_count': 0,
                'buy_count': 0,
                'hold_count': 0,
                'sell_count': 0,
                'analyst_count': 0
            }
            
            # 실제 네이버 금융 HTML 구조에 맞게 파싱 로직 구현
            # (구현 예정)
            
            return opinion_data
            
        except Exception as e:
            self.logger.error(f"투자의견 파싱 실패: {e}")
            return {}
    
    def save_forecast_data(self, forecast_data: Dict) -> bool:
        """추정 실적 데이터 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                for forecast in forecast_data.get('forecasts', []):
                    conn.execute('''
                        INSERT OR REPLACE INTO forecast_financials
                        (stock_code, company_name, forecast_year, forecast_quarter,
                         estimated_sales, estimated_operating_profit, estimated_net_profit,
                         estimated_eps, estimated_per, estimated_pbr, estimated_roe,
                         analyst_count, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (
                        forecast_data['stock_code'],
                        forecast_data['company_name'],
                        forecast['forecast_year'],
                        forecast['forecast_quarter'],
                        forecast['estimated_sales'],
                        forecast['estimated_operating_profit'],
                        forecast['estimated_net_profit'],
                        forecast['estimated_eps'],
                        forecast['estimated_per'],
                        forecast['estimated_pbr'],
                        forecast['estimated_roe'],
                        forecast['analyst_count']
                    ))
                
                conn.commit()
                self.logger.info(f"✅ 추정 실적 저장 완료: {forecast_data['stock_code']}")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 추정 실적 저장 실패: {e}")
            return False
    
    def save_analyst_opinions(self, opinion_data: Dict) -> bool:
        """투자의견 데이터 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO analyst_opinions
                    (stock_code, company_name, target_price, current_price,
                     upside_potential, investment_opinion, strong_buy_count,
                     buy_count, hold_count, sell_count, analyst_count, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    opinion_data['stock_code'],
                    opinion_data['company_name'],
                    opinion_data['target_price'],
                    opinion_data['current_price'],
                    opinion_data['upside_potential'],
                    opinion_data['investment_opinion'],
                    opinion_data['strong_buy_count'],
                    opinion_data['buy_count'],
                    opinion_data['hold_count'],
                    opinion_data['sell_count'],
                    opinion_data['analyst_count']
                ))
                
                conn.commit()
                self.logger.info(f"✅ 투자의견 저장 완료: {opinion_data['stock_code']}")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ 투자의견 저장 실패: {e}")
            return False
    
    def collect_stock_forecast(self, stock_code: str) -> bool:
        """종목별 전체 추정 데이터 수집"""
        try:
            self.logger.info(f"📊 추정 데이터 수집 시작: {stock_code}")
            
            success_count = 0
            
            # 1. 추정 실적 수집
            forecast_data = self.collect_naver_forecast_data(stock_code)
            if forecast_data and self.save_forecast_data(forecast_data):
                success_count += 1
            
            # 2. 투자의견 수집
            opinion_data = self.collect_analyst_opinions(stock_code)
            if opinion_data and self.save_analyst_opinions(opinion_data):
                success_count += 1
            
            # 요청 간격 (서버 부하 방지)
            time.sleep(1)
            
            if success_count > 0:
                self.logger.info(f"✅ 추정 데이터 수집 완료: {stock_code} ({success_count}/2)")
                return True
            else:
                self.logger.warning(f"⚠️ 추정 데이터 수집 부분 실패: {stock_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 추정 데이터 수집 실패 ({stock_code}): {e}")
            return False


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='추정 실적 데이터 수집 스크립트')
    parser.add_argument('--stock_code', type=str, help='수집할 종목코드')
    parser.add_argument('--stock_list', type=str, help='종목 리스트 파일 경로')
    parser.add_argument('--top_market_cap', type=int, help='시가총액 상위 N개 종목')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # 수집기 초기화
    collector = ForecastDataCollector()
    
    try:
        if args.stock_code:
            # 단일 종목 수집
            if collector.collect_stock_forecast(args.stock_code):
                print(f"✅ {args.stock_code} 추정 데이터 수집 성공")
            else:
                print(f"❌ {args.stock_code} 추정 데이터 수집 실패")
                
        elif args.top_market_cap:
            # 시가총액 상위 N개 종목 수집
            logger.info(f"시가총액 상위 {args.top_market_cap}개 종목 추정 데이터 수집 시작")
            
            # 주식 데이터베이스에서 상위 종목 조회
            stock_db_path = Path('data/databases/stock_data.db')
            if stock_db_path.exists():
                with sqlite3.connect(stock_db_path) as conn:
                    cursor = conn.execute(f"""
                        SELECT stock_code, company_name, market_cap
                        FROM company_info 
                        WHERE market_cap IS NOT NULL AND market_cap > 0
                        ORDER BY market_cap DESC 
                        LIMIT {args.top_market_cap}
                    """)
                    stock_list = cursor.fetchall()
                
                success_count = 0
                for idx, (stock_code, company_name, market_cap) in enumerate(stock_list):
                    logger.info(f"진행률: {idx+1}/{len(stock_list)} - {company_name}({stock_code})")
                    
                    if collector.collect_stock_forecast(stock_code):
                        success_count += 1
                
                print(f"✅ 추정 데이터 수집 완료: {success_count}/{len(stock_list)} 성공")
            else:
                print("❌ 주식 데이터베이스를 찾을 수 없습니다.")
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")


if __name__ == "__main__":
    main()
