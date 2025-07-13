#!/usr/bin/env python3
"""
간소화된 워런 버핏 스코어카드 (Yahoo Finance 중심)
즉시 테스트 가능한 버전
"""

import sys
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, Any, Optional

# yfinance 사용
try:
    import yfinance as yf
except ImportError:
    print("❌ yfinance가 필요합니다: pip install yfinance")
    sys.exit(1)

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

class SimpleBuffettScorecard:
    """간소화된 워런 버핏 스코어카드 (Yahoo Finance 중심)"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 점수 배점 (100점 만점)
        self.score_weights = {
            'valuation': 40,       # 가치평가 (확대)
            'profitability': 30,   # 수익성 (Yahoo Finance 데이터)
            'growth': 20,         # 성장성 (예상 데이터)
            'financial_health': 10 # 재무 건전성 (기본 지표)
        }
        
        # 워런 버핏 기준값
        self.criteria = {
            'forward_pe_max': 15,
            'trailing_pe_max': 20,
            'peg_ratio_max': 1.5,
            'pbr_max': 2.0,
            'roe_min': 10,  # Yahoo Finance에서 계산 가능
            'debt_equity_max': 0.5
        }
    
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
                self.logger.warning(f"Yahoo Finance 데이터 없음: {ticker}")
                return {}
            
            # 재무 데이터 수집
            data = {
                # 기본 정보
                'company_name': info.get('longName', info.get('shortName', 'Unknown')),
                'sector': info.get('sector', 'Unknown'),
                'market_cap': info.get('marketCap'),
                
                # 가치평가 지표
                'forward_pe': info.get('forwardPE'),
                'trailing_pe': info.get('trailingPE'),
                'peg_ratio': info.get('pegRatio'),
                'price_to_book': info.get('priceToBook'),
                'price_to_sales': info.get('priceToSalesTrailing12Months'),
                'ev_to_ebitda': info.get('enterpriseToEbitda'),
                
                # 수익성 지표
                'return_on_equity': info.get('returnOnEquity'),
                'return_on_assets': info.get('returnOnAssets'),
                'profit_margins': info.get('profitMargins'),
                'operating_margins': info.get('operatingMargins'),
                
                # 성장성 지표
                'earnings_growth': info.get('earningsGrowth'),
                'revenue_growth': info.get('revenueGrowth'),
                'earnings_quarterly_growth': info.get('earningsQuarterlyGrowth'),
                
                # 재무 건전성
                'debt_to_equity': info.get('debtToEquity'),
                'current_ratio': info.get('currentRatio'),
                'quick_ratio': info.get('quickRatio'),
                
                # 기타
                'dividend_yield': info.get('dividendYield'),
                'target_mean_price': info.get('targetMeanPrice'),
                'recommendation_key': info.get('recommendationKey'),
                'current_price': info.get('currentPrice', info.get('regularMarketPrice'))
            }
            
            # None 값 제거 및 비율을 퍼센트로 변환
            cleaned_data = {}
            for key, value in data.items():
                if value is not None:
                    # 비율 데이터를 퍼센트로 변환
                    if key in ['return_on_equity', 'return_on_assets', 'profit_margins', 
                              'operating_margins', 'earnings_growth', 'revenue_growth', 
                              'dividend_yield']:
                        cleaned_data[key] = value * 100 if value else None
                    else:
                        cleaned_data[key] = value
            
            self.logger.info(f"✅ Yahoo Finance 데이터 수집 완료: {stock_code}")
            return cleaned_data
            
        except Exception as e:
            self.logger.error(f"❌ Yahoo Finance 데이터 수집 실패 ({stock_code}): {e}")
            return {}
    
    def calculate_valuation_score(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """가치평가 점수 계산 (40점)"""
        total_score = 0
        max_score = self.score_weights['valuation']
        details = {}
        
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
        else:
            pe_score = 0
        
        total_score += pe_score
        details['forward_pe'] = {'value': forward_pe, 'score': pe_score, 'max': 12}
        
        # PBR (10점)
        pbr = data.get('price_to_book')
        if pbr and pbr > 0:
            if 0.8 <= pbr <= 1.5:
                pbr_score = 10
            elif pbr <= self.criteria['pbr_max']:
                pbr_score = 6
            elif pbr <= 3.0:
                pbr_score = 2
            else:
                pbr_score = 0
        else:
            pbr_score = 0
        
        total_score += pbr_score
        details['pbr'] = {'value': pbr, 'score': pbr_score, 'max': 10}
        
        # PEG Ratio (10점)
        peg_ratio = data.get('peg_ratio')
        if peg_ratio and peg_ratio > 0:
            if peg_ratio <= 1.0:
                peg_score = 10
            elif peg_ratio <= self.criteria['peg_ratio_max']:
                peg_score = 6
            else:
                peg_score = 0
        else:
            peg_score = 0
        
        total_score += peg_score
        details['peg_ratio'] = {'value': peg_ratio, 'score': peg_score, 'max': 10}
        
        # EV/EBITDA (8점)
        ev_ebitda = data.get('ev_to_ebitda')
        if ev_ebitda and ev_ebitda > 0:
            if ev_ebitda <= 10:
                ev_score = 8
            elif ev_ebitda <= 15:
                ev_score = 4
            else:
                ev_score = 0
        else:
            ev_score = 0
        
        total_score += ev_score
        details['ev_ebitda'] = {'value': ev_ebitda, 'score': ev_score, 'max': 8}
        
        return {
            'category': 'valuation',
            'total_score': total_score,
            'max_score': max_score,
            'percentage': (total_score / max_score) * 100 if max_score > 0 else 0,
            'details': details
        }
    
    def calculate_profitability_score(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """수익성 점수 계산 (30점)"""
        total_score = 0
        max_score = self.score_weights['profitability']
        details = {}
        
        # ROE (12점)
        roe = data.get('return_on_equity')
        if roe and roe > 0:
            if roe >= 20:
                roe_score = 12
            elif roe >= 15:
                roe_score = 8
            elif roe >= self.criteria['roe_min']:
                roe_score = 4
            else:
                roe_score = 0
        else:
            roe_score = 0
        
        total_score += roe_score
        details['roe'] = {'value': roe, 'score': roe_score, 'max': 12}
        
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
        else:
            roa_score = 0
        
        total_score += roa_score
        details['roa'] = {'value': roa, 'score': roa_score, 'max': 10}
        
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
        else:
            margin_score = 0
        
        total_score += margin_score
        details['operating_margin'] = {'value': operating_margin, 'score': margin_score, 'max': 8}
        
        return {
            'category': 'profitability',
            'total_score': total_score,
            'max_score': max_score,
            'percentage': (total_score / max_score) * 100 if max_score > 0 else 0,
            'details': details
        }
    
    def calculate_growth_score(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """성장성 점수 계산 (20점)"""
        total_score = 0
        max_score = self.score_weights['growth']
        details = {}
        
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
        else:
            rev_score = 0
        
        total_score += rev_score
        details['revenue_growth'] = {'value': revenue_growth, 'score': rev_score, 'max': 10}
        
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
        else:
            earn_score = 0
        
        total_score += earn_score
        details['earnings_growth'] = {'value': earnings_growth, 'score': earn_score, 'max': 10}
        
        return {
            'category': 'growth',
            'total_score': total_score,
            'max_score': max_score,
            'percentage': (total_score / max_score) * 100 if max_score > 0 else 0,
            'details': details
        }
    
    def calculate_financial_health_score(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """재무 건전성 점수 계산 (10점)"""
        total_score = 0
        max_score = self.score_weights['financial_health']
        details = {}
        
        # 부채비율 (6점)
        debt_equity = data.get('debt_to_equity')
        if debt_equity is not None:
            debt_ratio = debt_equity / 100 if debt_equity > 5 else debt_equity  # 비율 정규화
            if debt_ratio <= 0.3:
                debt_score = 6
            elif debt_ratio <= self.criteria['debt_equity_max']:
                debt_score = 4
            elif debt_ratio <= 1.0:
                debt_score = 2
            else:
                debt_score = 0
        else:
            debt_score = 0
        
        total_score += debt_score
        details['debt_equity'] = {'value': debt_equity, 'score': debt_score, 'max': 6}
        
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
        else:
            curr_score = 0
        
        total_score += curr_score
        details['current_ratio'] = {'value': current_ratio, 'score': curr_score, 'max': 4}
        
        return {
            'category': 'financial_health',
            'total_score': total_score,
            'max_score': max_score,
            'percentage': (total_score / max_score) * 100 if max_score > 0 else 0,
            'details': details
        }
    
    def calculate_simple_scorecard(self, stock_code: str) -> Dict[str, Any]:
        """간소화된 워런 버핏 스코어카드 계산"""
        try:
            self.logger.info(f"📊 Simple 워런 버핏 스코어카드 계산 시작: {stock_code}")
            
            # Yahoo Finance 데이터 수집
            yahoo_data = self.collect_yahoo_data(stock_code)
            
            if not yahoo_data:
                self.logger.warning(f"데이터를 찾을 수 없습니다: {stock_code}")
                return None
            
            # 스코어카드 구성
            scorecard = {
                'stock_code': stock_code,
                'company_name': yahoo_data.get('company_name', 'Unknown'),
                'sector': yahoo_data.get('sector', 'Unknown'),
                'calculation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_source': 'Yahoo Finance',
                'current_price': yahoo_data.get('current_price'),
                'target_price': yahoo_data.get('target_mean_price'),
                'recommendation': yahoo_data.get('recommendation_key'),
                'scores': {}
            }
            
            # 각 카테고리별 점수 계산
            scorecard['scores']['valuation'] = self.calculate_valuation_score(yahoo_data)
            scorecard['scores']['profitability'] = self.calculate_profitability_score(yahoo_data)
            scorecard['scores']['growth'] = self.calculate_growth_score(yahoo_data)
            scorecard['scores']['financial_health'] = self.calculate_financial_health_score(yahoo_data)
            
            # 총점 계산
            total_score = sum(score['total_score'] for score in scorecard['scores'].values())
            max_total_score = sum(self.score_weights.values())
            
            scorecard['total_score'] = total_score
            scorecard['max_score'] = max_total_score
            scorecard['percentage'] = (total_score / max_total_score) * 100 if max_total_score > 0 else 0
            
            # 투자 등급 판정
            scorecard['investment_grade'] = self._determine_investment_grade(scorecard['percentage'])
            
            # 업사이드 계산
            if scorecard['current_price'] and scorecard['target_price']:
                upside = (scorecard['target_price'] - scorecard['current_price']) / scorecard['current_price'] * 100
                scorecard['upside_potential'] = upside
            
            self.logger.info(f"✅ Simple 스코어카드 계산 완료: {stock_code} - {total_score}/{max_total_score}점 ({scorecard['percentage']:.1f}%)")
            
            return scorecard
            
        except Exception as e:
            self.logger.error(f"❌ Simple 스코어카드 계산 실패 ({stock_code}): {e}")
            return None
    
    def _determine_investment_grade(self, percentage: float) -> str:
        """투자 등급 판정"""
        if percentage >= 80:
            return "Strong Buy"
        elif percentage >= 65:
            return "Buy"
        elif percentage >= 50:
            return "Hold"
        elif percentage >= 35:
            return "Weak Hold"
        else:
            return "Avoid"


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Simple 워런 버핏 스코어카드 (Yahoo Finance 기반)')
    parser.add_argument('--stock_code', type=str, required=True, help='분석할 종목코드')
    parser.add_argument('--save_result', action='store_true', help='결과를 JSON 파일로 저장')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 스코어카드 계산
    scorecard = SimpleBuffettScorecard()
    result = scorecard.calculate_simple_scorecard(args.stock_code)
    
    if result:
        print("\n" + "="*60)
        print(f"🏆 Simple 워런 버핏 스코어카드: {result['company_name']} ({args.stock_code})")
        print("="*60)
        print(f"📊 총점: {result['total_score']}/{result['max_score']}점 ({result['percentage']:.1f}%)")
        print(f"🎯 투자등급: {result['investment_grade']}")
        print(f"🏭 섹터: {result['sector']}")
        print(f"📅 계산일시: {result['calculation_date']}")
        print(f"📂 데이터소스: {result['data_source']}")
        
        # 가격 정보
        if result.get('current_price') and result.get('target_price'):
            print(f"💰 현재가: {result['current_price']:,.0f}원")
            print(f"🎯 목표가: {result['target_price']:,.0f}원")
            if result.get('upside_potential'):
                print(f"📈 업사이드: {result['upside_potential']:+.1f}%")
        
        if result.get('recommendation'):
            print(f"📊 애널리스트 추천: {result['recommendation'].upper()}")
        
        print("\n📋 카테고리별 점수:")
        for category, score_data in result['scores'].items():
            print(f"  {category.replace('_', ' ').title()}: {score_data['total_score']}/{score_data['max_score']}점 ({score_data['percentage']:.1f}%)")
        
        # 주요 지표 하이라이트
        print("\n🔍 주요 밸류에이션 지표:")
        val_details = result['scores']['valuation']['details']
        for key, detail in val_details.items():
            if detail['value'] is not None:
                print(f"  {key.replace('_', ' ').title()}: {detail['value']:.2f}")
        
        if args.save_result:
            import json
            output_file = f"results/simple_buffett_scorecard_{args.stock_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            os.makedirs('results', exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"\n💾 결과 저장: {output_file}")
        
        print("="*60)
    else:
        print(f"❌ {args.stock_code} 스코어카드 계산 실패")


if __name__ == "__main__":
    main()
