#!/usr/bin/env python3
"""
전체 KOSPI/KOSDAQ 종목 워런 버핏 스코어카드 일괄 생성
Streamlit 웹앱 기초 자료 구축용

기능:
1. COMPANY_INFO 테이블에서 모든 종목 조회
2. Yahoo Finance 기반 워런 버핏 스코어카드 계산
3. 결과를 buffett_scorecard 테이블에 저장
4. 진행상황 실시간 모니터링
5. 에러 복구 및 재시도 로직

실행 방법:
python batch_buffett_scorecard.py --batch_size=50 --delay=1.0
python batch_buffett_scorecard.py --resume  # 중단된 작업 재개
"""

import sys
import os
import sqlite3
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Dict, Any, List, Optional
import pandas as pd

# yfinance 사용
try:
    import yfinance as yf
    print("✅ yfinance 라이브러리 사용 가능")
except ImportError:
    print("❌ yfinance가 필요합니다: pip install yfinance")
    sys.exit(1)

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

class BatchBuffettScorecard:
    """일괄 워런 버핏 스코어카드 생성 시스템"""
    
    def __init__(self, batch_size: int = 50, delay: float = 1.0):
        self.logger = logging.getLogger(__name__)
        self.batch_size = batch_size
        self.delay = delay  # API 호출 간 딜레이 (초)
        
        # 데이터베이스 경로
        self.stock_db = Path('data/databases/stock_data.db')
        self.scorecard_db = Path('data/databases/buffett_scorecard.db')
        
        # 점수 배점 (100점 만점)
        self.score_weights = {
            'valuation': 40,       # 가치평가 (확대)
            'profitability': 30,   # 수익성
            'growth': 20,         # 성장성
            'financial_health': 10 # 재무 건전성
        }
        
        # 워런 버핏 기준값
        self.criteria = {
            'forward_pe_max': 15,
            'trailing_pe_max': 20,
            'peg_ratio_max': 1.5,
            'pbr_max': 2.0,
            'roe_min': 10,
            'debt_equity_max': 0.5
        }
        
        # 스코어카드 데이터베이스 초기화
        self._init_scorecard_database()
    
    def _init_scorecard_database(self):
        """스코어카드 저장용 데이터베이스 초기화"""
        try:
            # 디렉토리 생성
            self.scorecard_db.parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(self.scorecard_db) as conn:
                # 워런 버핏 스코어카드 테이블
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS buffett_scorecard (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        company_name TEXT,
                        sector TEXT,
                        market_cap REAL,
                        
                        -- 현재가 및 목표가 정보
                        current_price REAL,
                        target_price REAL,
                        upside_potential REAL,
                        analyst_recommendation TEXT,
                        
                        -- 카테고리별 점수
                        valuation_score INTEGER DEFAULT 0,
                        profitability_score INTEGER DEFAULT 0,
                        growth_score INTEGER DEFAULT 0,
                        financial_health_score INTEGER DEFAULT 0,
                        
                        -- 총점 및 등급
                        total_score INTEGER DEFAULT 0,
                        max_score INTEGER DEFAULT 100,
                        percentage REAL DEFAULT 0,
                        investment_grade TEXT DEFAULT 'Avoid',
                        
                        -- 주요 지표들
                        forward_pe REAL,
                        trailing_pe REAL,
                        pbr REAL,
                        peg_ratio REAL,
                        roe REAL,
                        roa REAL,
                        debt_to_equity REAL,
                        current_ratio REAL,
                        operating_margin REAL,
                        revenue_growth REAL,
                        earnings_growth REAL,
                        
                        -- 메타 정보
                        data_source TEXT DEFAULT 'Yahoo Finance',
                        calculation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        
                        UNIQUE(stock_code)
                    )
                ''')
                
                # 진행상황 추적 테이블
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS batch_progress (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        status TEXT NOT NULL,  -- 'pending', 'processing', 'completed', 'failed'
                        error_message TEXT,
                        processing_time REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        
                        UNIQUE(stock_code)
                    )
                ''')
                
                # 배치 실행 로그 테이블
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS batch_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        batch_id TEXT NOT NULL,
                        total_stocks INTEGER,
                        completed_stocks INTEGER,
                        failed_stocks INTEGER,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        duration_minutes REAL,
                        success_rate REAL,
                        notes TEXT
                    )
                ''')
                
                conn.commit()
                self.logger.info("스코어카드 데이터베이스 초기화 완료")
                
        except Exception as e:
            self.logger.error(f"데이터베이스 초기화 실패: {e}")
            raise
    
    def get_all_stocks(self) -> List[tuple]:
        """COMPANY_INFO 테이블에서 모든 종목 조회"""
        try:
            if not self.stock_db.exists():
                raise FileNotFoundError("stock_data.db 파일이 없습니다")
            
            with sqlite3.connect(self.stock_db) as conn:
                cursor = conn.execute('''
                    SELECT stock_code, company_name, sector, market_cap, market_type
                    FROM company_info 
                    WHERE stock_code IS NOT NULL 
                    AND company_name IS NOT NULL
                    ORDER BY market_cap DESC NULLS LAST
                ''')
                
                stocks = cursor.fetchall()
                
                self.logger.info(f"📊 전체 {len(stocks)}개 종목 조회 완료")
                return stocks
                
        except Exception as e:
            self.logger.error(f"종목 조회 실패: {e}")
            return []
    
    def get_pending_stocks(self) -> List[tuple]:
        """아직 처리되지 않은 종목들 조회"""
        try:
            stocks = self.get_all_stocks()
            
            with sqlite3.connect(self.scorecard_db) as conn:
                # 이미 완료된 종목들 조회
                cursor = conn.execute('''
                    SELECT stock_code FROM batch_progress 
                    WHERE status = 'completed'
                ''')
                completed_stocks = {row[0] for row in cursor.fetchall()}
            
            # 미완료 종목들만 필터링
            pending_stocks = [
                stock for stock in stocks 
                if stock[0] not in completed_stocks
            ]
            
            self.logger.info(f"📋 미처리 종목: {len(pending_stocks)}개")
            return pending_stocks
            
        except Exception as e:
            self.logger.error(f"미처리 종목 조회 실패: {e}")
            return []
    
    def get_korean_ticker(self, stock_code: str) -> str:
        """한국 주식 코드를 Yahoo Finance 티커로 변환"""
        if len(stock_code) == 6 and stock_code.isdigit():
            # KOSPI/KOSDAQ 구분 (간단한 로직)
            if stock_code.startswith(('0', '1', '2', '3')):
                return f"{stock_code}.KS"  # KOSPI
            else:
                return f"{stock_code}.KQ"  # KOSDAQ
        return stock_code
    
    def collect_yahoo_data(self, stock_code: str) -> Dict[str, Any]:
        """Yahoo Finance에서 종합 데이터 수집"""
        try:
            ticker = self.get_korean_ticker(stock_code)
            stock = yf.Ticker(ticker)
            info = stock.info
            
            if not info or 'symbol' not in info:
                return {}
            
            # PBR 계산 로직 (개선된 방법)
            pbr = info.get('priceToBook')
            
            # PBR이 없는 경우 수동 계산 (확실한 방법)
            if not pbr:
                try:
                    market_cap = info.get('marketCap')
                    if market_cap:
                        balance_sheet = stock.balance_sheet
                        
                        if balance_sheet is not None and not balance_sheet.empty:
                            # 정확한 필드명들 시도
                            target_fields = [
                                'Stockholders Equity',
                                'Common Stock Equity',
                                'Total Equity Gross Minority Interest'
                            ]
                            
                            for field_name in target_fields:
                                if field_name in balance_sheet.index:
                                    equity_value = balance_sheet.loc[field_name].iloc[0]
                                    if pd.notna(equity_value) and equity_value > 0:
                                        calculated_pbr = market_cap / equity_value
                                        if 0.01 <= calculated_pbr <= 50:  # 합리적 범위 검증
                                            pbr = calculated_pbr
                                            self.logger.info(f"PBR 계산 성공 ({stock_code}): {pbr:.3f}")
                                            break
                                        else:
                                            pbr = None
                                    break
                except Exception as e:
                    self.logger.warning(f"PBR 계산 실패 ({stock_code}): {e}")
            
            # 재무 데이터 수집 (비율을 퍼센트로 변환)
            data = {
                # 기본 정보
                'company_name': info.get('longName', info.get('shortName', 'Unknown')),
                'sector': info.get('sector', 'Unknown'),
                'market_cap': info.get('marketCap'),
                
                # 가치평가 지표
                'forward_pe': info.get('forwardPE'),
                'trailing_pe': info.get('trailingPE'),
                'peg_ratio': info.get('pegRatio'),
                'price_to_book': pbr,
                'price_to_sales': info.get('priceToSalesTrailing12Months'),
                'ev_to_ebitda': info.get('enterpriseToEbitda'),
                
                # 수익성 지표 (퍼센트로 변환)
                'return_on_equity': info.get('returnOnEquity', 0) * 100 if info.get('returnOnEquity') else None,
                'return_on_assets': info.get('returnOnAssets', 0) * 100 if info.get('returnOnAssets') else None,
                'profit_margins': info.get('profitMargins', 0) * 100 if info.get('profitMargins') else None,
                'operating_margins': info.get('operatingMargins', 0) * 100 if info.get('operatingMargins') else None,
                
                # 성장성 지표 (퍼센트로 변환)
                'earnings_growth': info.get('earningsGrowth', 0) * 100 if info.get('earningsGrowth') else None,
                'revenue_growth': info.get('revenueGrowth', 0) * 100 if info.get('revenueGrowth') else None,
                
                # 재무 건전성
                'debt_to_equity': info.get('debtToEquity'),
                'current_ratio': info.get('currentRatio'),
                'quick_ratio': info.get('quickRatio'),
                
                # 기타
                'dividend_yield': info.get('dividendYield', 0) * 100 if info.get('dividendYield') else None,
                'target_mean_price': info.get('targetMeanPrice'),
                'recommendation_key': info.get('recommendationKey'),
                'current_price': info.get('currentPrice', info.get('regularMarketPrice'))
            }
            
            # None 값 제거
            cleaned_data = {k: v for k, v in data.items() if v is not None}
            return cleaned_data
            
        except Exception as e:
            self.logger.error(f"Yahoo Finance 데이터 수집 실패 ({stock_code}): {e}")
            return {}
    
    def calculate_category_scores(self, data: Dict[str, Any]) -> Dict[str, Dict]:
        """카테고리별 점수 계산"""
        scores = {}
        
        # 1. 가치평가 점수 (40점)
        val_score = 0
        val_details = {}
        
        # Forward P/E (12점)
        forward_pe = data.get('forward_pe')
        if forward_pe and forward_pe > 0:
            if forward_pe <= 10:
                pe_score = 12
            elif forward_pe <= self.criteria['forward_pe_max']:
                pe_score = 8
            elif forward_pe <= 20:
                pe_score = 4
            else:
                pe_score = 0
            val_score += pe_score
            val_details['forward_pe'] = pe_score
        
        # PBR (10점)
        pbr = data.get('price_to_book')
        if pbr and pbr > 0:
            if 0.8 <= pbr <= 1.5:
                pbr_score = 10
            elif pbr <= 2.0:
                pbr_score = 6
            elif pbr <= 3.0:
                pbr_score = 2
            else:
                pbr_score = 0
            val_score += pbr_score
            val_details['pbr'] = pbr_score
        
        # PEG Ratio (10점)
        peg_ratio = data.get('peg_ratio')
        if peg_ratio and peg_ratio > 0:
            if peg_ratio <= 1.0:
                peg_score = 10
            elif peg_ratio <= 1.5:
                peg_score = 6
            else:
                peg_score = 0
            val_score += peg_score
            val_details['peg_ratio'] = peg_score
        
        # EV/EBITDA (8점)
        ev_ebitda = data.get('ev_to_ebitda')
        if ev_ebitda and ev_ebitda > 0:
            if ev_ebitda <= 10:
                ev_score = 8
            elif ev_ebitda <= 15:
                ev_score = 4
            else:
                ev_score = 0
            val_score += ev_score
            val_details['ev_ebitda'] = ev_score
        
        scores['valuation'] = {'score': val_score, 'max': 40, 'details': val_details}
        
        # 2. 수익성 점수 (30점)
        prof_score = 0
        prof_details = {}
        
        # ROE (12점)
        roe = data.get('return_on_equity')
        if roe and roe > 0:
            if roe >= 20:
                roe_score = 12
            elif roe >= 15:
                roe_score = 8
            elif roe >= 10:
                roe_score = 4
            else:
                roe_score = 0
            prof_score += roe_score
            prof_details['roe'] = roe_score
        
        # ROA (10점)
        roa = data.get('return_on_assets')
        if roa and roa > 0:
            if roa >= 10:
                roa_score = 10
            elif roa >= 5:
                roa_score = 6
            elif roa > 0:
                roa_score = 2
            else:
                roa_score = 0
            prof_score += roa_score
            prof_details['roa'] = roa_score
        
        # 영업이익률 (8점)
        operating_margin = data.get('operating_margins')
        if operating_margin and operating_margin > 0:
            if operating_margin >= 20:
                margin_score = 8
            elif operating_margin >= 15:
                margin_score = 6
            elif operating_margin >= 10:
                margin_score = 3
            else:
                margin_score = 0
            prof_score += margin_score
            prof_details['operating_margin'] = margin_score
        
        scores['profitability'] = {'score': prof_score, 'max': 30, 'details': prof_details}
        
        # 3. 성장성 점수 (20점)
        growth_score = 0
        growth_details = {}
        
        # 매출 성장률 (10점)
        revenue_growth = data.get('revenue_growth')
        if revenue_growth and revenue_growth > 0:
            if revenue_growth >= 20:
                rev_score = 10
            elif revenue_growth >= 10:
                rev_score = 6
            elif revenue_growth >= 5:
                rev_score = 3
            else:
                rev_score = 0
            growth_score += rev_score
            growth_details['revenue_growth'] = rev_score
        
        # 이익 성장률 (10점)
        earnings_growth = data.get('earnings_growth')
        if earnings_growth and earnings_growth > 0:
            if earnings_growth >= 20:
                earn_score = 10
            elif earnings_growth >= 10:
                earn_score = 6
            elif earnings_growth >= 5:
                earn_score = 3
            else:
                earn_score = 0
            growth_score += earn_score
            growth_details['earnings_growth'] = earn_score
        
        scores['growth'] = {'score': growth_score, 'max': 20, 'details': growth_details}
        
        # 4. 재무 건전성 점수 (10점)
        health_score = 0
        health_details = {}
        
        # 부채비율 (6점)
        debt_equity = data.get('debt_to_equity')
        if debt_equity is not None:
            debt_ratio = debt_equity / 100 if debt_equity > 5 else debt_equity
            if debt_ratio <= 0.3:
                debt_score = 6
            elif debt_ratio <= 0.5:
                debt_score = 4
            elif debt_ratio <= 1.0:
                debt_score = 2
            else:
                debt_score = 0
            health_score += debt_score
            health_details['debt_equity'] = debt_score
        
        # 유동비율 (4점)
        current_ratio = data.get('current_ratio')
        if current_ratio and current_ratio > 0:
            if current_ratio >= 2.0:
                curr_score = 4
            elif current_ratio >= 1.5:
                curr_score = 3
            elif current_ratio >= 1.0:
                curr_score = 1
            else:
                curr_score = 0
            health_score += curr_score
            health_details['current_ratio'] = curr_score
        
        scores['financial_health'] = {'score': health_score, 'max': 10, 'details': health_details}
        
        return scores
    
    def calculate_scorecard(self, stock_code: str, stock_info: tuple) -> Optional[Dict[str, Any]]:
        """개별 종목 스코어카드 계산"""
        try:
            start_time = time.time()
            
            # Yahoo Finance 데이터 수집
            yahoo_data = self.collect_yahoo_data(stock_code)
            
            if not yahoo_data:
                return None
            
            # 카테고리별 점수 계산
            scores = self.calculate_category_scores(yahoo_data)
            
            # 총점 계산
            total_score = sum(score_data['score'] for score_data in scores.values())
            percentage = (total_score / 100) * 100
            
            # 투자 등급 판정
            if percentage >= 80:
                grade = "Strong Buy"
            elif percentage >= 65:
                grade = "Buy"
            elif percentage >= 50:
                grade = "Hold"
            elif percentage >= 35:
                grade = "Weak Hold"
            else:
                grade = "Avoid"
            
            # 업사이드 계산
            upside = None
            if yahoo_data.get('current_price') and yahoo_data.get('target_mean_price'):
                current = yahoo_data['current_price']
                target = yahoo_data['target_mean_price']
                upside = (target - current) / current * 100
            
            # 결과 구성
            scorecard = {
                'stock_code': stock_code,
                'company_name': yahoo_data.get('company_name', stock_info[1]),
                'sector': yahoo_data.get('sector', stock_info[2] if len(stock_info) > 2 else 'Unknown'),
                'market_cap': yahoo_data.get('market_cap', stock_info[3] if len(stock_info) > 3 else None),
                
                'current_price': yahoo_data.get('current_price'),
                'target_price': yahoo_data.get('target_mean_price'),
                'upside_potential': upside,
                'analyst_recommendation': yahoo_data.get('recommendation_key'),
                
                'valuation_score': scores['valuation']['score'],
                'profitability_score': scores['profitability']['score'],
                'growth_score': scores['growth']['score'],
                'financial_health_score': scores['financial_health']['score'],
                
                'total_score': total_score,
                'percentage': percentage,
                'investment_grade': grade,
                
                # 주요 지표들
                'forward_pe': yahoo_data.get('forward_pe'),
                'trailing_pe': yahoo_data.get('trailing_pe'),
                'pbr': yahoo_data.get('price_to_book'),
                'peg_ratio': yahoo_data.get('peg_ratio'),
                'roe': yahoo_data.get('return_on_equity'),
                'roa': yahoo_data.get('return_on_assets'),
                'debt_to_equity': yahoo_data.get('debt_to_equity'),
                'current_ratio': yahoo_data.get('current_ratio'),
                'operating_margin': yahoo_data.get('operating_margins'),
                'revenue_growth': yahoo_data.get('revenue_growth'),
                'earnings_growth': yahoo_data.get('earnings_growth'),
                
                'processing_time': time.time() - start_time
            }
            
            return scorecard
            
        except Exception as e:
            self.logger.error(f"스코어카드 계산 실패 ({stock_code}): {e}")
            return None
    
    def save_scorecard(self, scorecard: Dict[str, Any]) -> bool:
        """스코어카드 결과 저장"""
        try:
            with sqlite3.connect(self.scorecard_db) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO buffett_scorecard (
                        stock_code, company_name, sector, market_cap,
                        current_price, target_price, upside_potential, analyst_recommendation,
                        valuation_score, profitability_score, growth_score, financial_health_score,
                        total_score, percentage, investment_grade,
                        forward_pe, trailing_pe, pbr, peg_ratio, roe, roa,
                        debt_to_equity, current_ratio, operating_margin, revenue_growth, earnings_growth,
                        last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    scorecard['stock_code'], scorecard['company_name'], scorecard['sector'], scorecard['market_cap'],
                    scorecard['current_price'], scorecard['target_price'], scorecard['upside_potential'], 
                    scorecard['analyst_recommendation'],
                    scorecard['valuation_score'], scorecard['profitability_score'], 
                    scorecard['growth_score'], scorecard['financial_health_score'],
                    scorecard['total_score'], scorecard['percentage'], scorecard['investment_grade'],
                    scorecard['forward_pe'], scorecard['trailing_pe'], scorecard['pbr'], scorecard['peg_ratio'],
                    scorecard['roe'], scorecard['roa'], scorecard['debt_to_equity'], scorecard['current_ratio'],
                    scorecard['operating_margin'], scorecard['revenue_growth'], scorecard['earnings_growth']
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"스코어카드 저장 실패: {e}")
            return False
    
    def update_progress(self, stock_code: str, status: str, error_msg: str = None, processing_time: float = None):
        """진행상황 업데이트"""
        try:
            with sqlite3.connect(self.scorecard_db) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO batch_progress 
                    (stock_code, status, error_message, processing_time, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (stock_code, status, error_msg, processing_time))
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"진행상황 업데이트 실패: {e}")
    
    def run_batch_processing(self, resume: bool = False) -> Dict[str, Any]:
        """배치 처리 실행"""
        try:
            batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            start_time = datetime.now()
            
            # 처리할 종목 조회
            if resume:
                stocks = self.get_pending_stocks()
                print(f"🔄 중단된 작업 재개: {len(stocks)}개 종목")
            else:
                stocks = self.get_all_stocks()
                print(f"🚀 전체 배치 처리 시작: {len(stocks)}개 종목")
            
            if not stocks:
                print("❌ 처리할 종목이 없습니다.")
                return {'success': False, 'message': '처리할 종목이 없습니다'}
            
            # 진행상황 카운터
            total_stocks = len(stocks)
            completed = 0
            failed = 0
            
            print(f"📊 배치 ID: {batch_id}")
            print(f"📋 총 {total_stocks}개 종목 처리 예정")
            print(f"⚙️ 배치 크기: {self.batch_size}, 딜레이: {self.delay}초")
            print("=" * 60)
            
            for i, stock_info in enumerate(stocks, 1):
                stock_code = stock_info[0]
                company_name = stock_info[1] if len(stock_info) > 1 else 'Unknown'
                
                try:
                    # 진행상황 표시
                    progress = (i / total_stocks) * 100
                    print(f"[{i:4d}/{total_stocks}] ({progress:5.1f}%) {company_name}({stock_code}) 처리 중...", end=" ")
                    
                    # 진행상황 DB 업데이트
                    self.update_progress(stock_code, 'processing')
                    
                    # 스코어카드 계산
                    scorecard = self.calculate_scorecard(stock_code, stock_info)
                    
                    if scorecard and self.save_scorecard(scorecard):
                        self.update_progress(stock_code, 'completed', processing_time=scorecard['processing_time'])
                        print(f"✅ {scorecard['total_score']:2d}점 ({scorecard['investment_grade']})")
                        completed += 1
                    else:
                        self.update_progress(stock_code, 'failed', '데이터 없음')
                        print("❌ 실패")
                        failed += 1
                    
                    # 배치 크기마다 진행상황 요약
                    if i % self.batch_size == 0:
                        elapsed = (datetime.now() - start_time).total_seconds() / 60
                        success_rate = (completed / i) * 100
                        print(f"\n📊 중간 집계 ({i}/{total_stocks}): 성공 {completed}개, 실패 {failed}개, 성공률 {success_rate:.1f}%, 경과시간 {elapsed:.1f}분\n")
                    
                    # API 호출 제한 대응
                    time.sleep(self.delay)
                    
                except Exception as e:
                    self.update_progress(stock_code, 'failed', str(e))
                    print(f"❌ 오류: {e}")
                    failed += 1
                    
                    # 연속 실패 시 중단
                    if failed > 10 and completed == 0:
                        print("⚠️ 연속 실패가 많아 중단합니다. API 설정을 확인하세요.")
                        break
            
            # 최종 결과
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() / 60
            success_rate = (completed / total_stocks) * 100 if total_stocks > 0 else 0
            
            # 배치 로그 저장
            with sqlite3.connect(self.scorecard_db) as conn:
                conn.execute('''
                    INSERT INTO batch_logs 
                    (batch_id, total_stocks, completed_stocks, failed_stocks, 
                     start_time, end_time, duration_minutes, success_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (batch_id, total_stocks, completed, failed, 
                      start_time, end_time, duration, success_rate))
                conn.commit()
            
            print("\n" + "=" * 60)
            print(f"🏁 배치 처리 완료!")
            print(f"📊 총 {total_stocks}개 종목 중 {completed}개 성공, {failed}개 실패")
            print(f"📈 성공률: {success_rate:.1f}%")
            print(f"⏱️ 소요시간: {duration:.1f}분")
            print(f"💾 결과 저장: {self.scorecard_db}")
            
            return {
                'success': True,
                'batch_id': batch_id,
                'total_stocks': total_stocks,
                'completed': completed,
                'failed': failed,
                'success_rate': success_rate,
                'duration_minutes': duration
            }
            
        except Exception as e:
            self.logger.error(f"배치 처리 실패: {e}")
            return {'success': False, 'error': str(e)}


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='전체 KOSPI/KOSDAQ 종목 워런 버핏 스코어카드 일괄 생성')
    parser.add_argument('--batch_size', type=int, default=50, help='배치 크기 (기본: 50)')
    parser.add_argument('--delay', type=float, default=1.0, help='API 호출 간 딜레이 (초, 기본: 1.0)')
    parser.add_argument('--resume', action='store_true', help='중단된 작업 재개')
    parser.add_argument('--test', action='store_true', help='테스트 모드 (상위 10개 종목만)')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='로그 레벨')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 테스트 모드
        if args.test:
            args.batch_size = 10
            print("🧪 테스트 모드: 상위 10개 종목만 처리")
        
        # 배치 처리 시작
        batch_processor = BatchBuffettScorecard(
            batch_size=args.batch_size,
            delay=args.delay
        )
        
        result = batch_processor.run_batch_processing(resume=args.resume)
        
        if result['success']:
            print(f"\n🎉 성공적으로 완료되었습니다!")
            print(f"📂 다음 단계: streamlit run src/web/app.py")
        else:
            print(f"❌ 실패: {result.get('error', '알 수 없는 오류')}")
            
    except KeyboardInterrupt:
        print("\n⏸️ 사용자에 의해 중단됨")
        print("💡 재개하려면: python batch_buffett_scorecard.py --resume")
    except Exception as e:
        print(f"❌ 예기치 못한 오류: {e}")
        logging.exception("배치 처리 중 오류 발생")


if __name__ == "__main__":
    main()
