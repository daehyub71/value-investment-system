#!/usr/bin/env python3
"""
워런 버핏 스코어카드 시스템 데이터 품질 검증 스크립트
scripts/validation/data_quality_check.py

- 주가 데이터 품질 검증 (기술분석 30% 비중)
- 재무 데이터 무결성 검증 (기본분석 45% 비중)
- 뉴스 감정분석 품질 검증 (감정분석 25% 비중)
- 워런 버핏 스코어카드 정확성 검증
- 데이터 일관성 및 논리적 검증
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sqlite3
import logging
from typing import Dict, List, Optional, Tuple, Any
import argparse
import json

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from config.database_config import get_db_connection, get_database_path
except ImportError:
    print("⚠️ config 모듈을 찾을 수 없습니다. 경로를 확인해주세요.")
    # 기본 함수들을 여기서 정의
    def get_db_connection(db_name):
        db_path = Path(f'data/databases/{db_name}_data.db')
        return sqlite3.connect(str(db_path))

# 로깅 설정
log_dir = project_root / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'data_quality.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockDataQualityChecker:
    """주가 데이터 품질 검증 클래스 (기술분석 30% 비중)"""
    
    def __init__(self):
        self.quality_rules = {
            'price_positive': "모든 가격은 양수여야 함",
            'high_low_order': "고가 >= 저가",
            'ohlc_logic': "시가, 종가는 고가와 저가 사이에 있어야 함",
            'volume_non_negative': "거래량은 0 이상이어야 함",
            'price_reasonable': "가격 변동이 합리적 범위 내에 있어야 함",
            'consecutive_data': "연속된 영업일 데이터가 존재해야 함"
        }
    
    def check_stock_data_quality(self, target_date: str = None, stock_codes: List[str] = None) -> Dict:
        """주가 데이터 품질 종합 검증"""
        logger.info("📈 주가 데이터 품질 검증 시작...")
        
        try:
            with get_db_connection('stock') as conn:
                # 기본 쿼리 조건
                where_clause = "WHERE 1=1"
                params = []
                
                if target_date:
                    where_clause += " AND date = ?"
                    params.append(target_date)
                
                if stock_codes:
                    where_clause += f" AND stock_code IN ({','.join(['?' for _ in stock_codes])})"
                    params.extend(stock_codes)
                
                # 전체 데이터 통계
                total_query = f"SELECT COUNT(*) as total_records FROM stock_prices {where_clause}"
                total_records = pd.read_sql(total_query, conn, params=params).iloc[0]['total_records']
                
                if total_records == 0:
                    return {
                        'status': 'no_data',
                        'message': '검증할 데이터가 없습니다.',
                        'total_records': 0
                    }
                
                # 각 품질 규칙 검증
                quality_results = {}
                
                # 1. 가격 양수 검증
                quality_results['price_positive'] = self._check_positive_prices(conn, where_clause, params)
                
                # 2. 고가/저가 순서 검증
                quality_results['high_low_order'] = self._check_high_low_order(conn, where_clause, params)
                
                # 3. OHLC 논리 검증
                quality_results['ohlc_logic'] = self._check_ohlc_logic(conn, where_clause, params)
                
                # 4. 거래량 검증
                quality_results['volume_check'] = self._check_volume_data(conn, where_clause, params)
                
                # 5. 가격 변동 합리성 검증
                quality_results['price_variation'] = self._check_price_variation(conn, where_clause, params)
                
                # 6. 데이터 연속성 검증
                if not target_date:  # 특정 날짜가 아닌 경우만
                    quality_results['data_continuity'] = self._check_data_continuity(conn, stock_codes)
                
                # 종합 품질 점수 계산
                overall_score = self._calculate_overall_quality_score(quality_results, total_records)
                
                return {
                    'status': 'completed',
                    'timestamp': datetime.now().isoformat(),
                    'target_date': target_date,
                    'stock_codes': stock_codes,
                    'total_records': total_records,
                    'quality_results': quality_results,
                    'overall_score': overall_score,
                    'quality_grade': self._get_quality_grade(overall_score)
                }
                
        except Exception as e:
            logger.error(f"주가 데이터 품질 검증 실패: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _check_positive_prices(self, conn, where_clause, params) -> Dict:
        """양수 가격 검증"""
        try:
            invalid_query = f"""
                SELECT COUNT(*) as invalid_count,
                       COUNT(CASE WHEN open_price <= 0 THEN 1 END) as invalid_open,
                       COUNT(CASE WHEN high_price <= 0 THEN 1 END) as invalid_high,
                       COUNT(CASE WHEN low_price <= 0 THEN 1 END) as invalid_low,
                       COUNT(CASE WHEN close_price <= 0 THEN 1 END) as invalid_close
                FROM stock_prices {where_clause}
                AND (open_price <= 0 OR high_price <= 0 OR low_price <= 0 OR close_price <= 0)
            """
            
            result = pd.read_sql(invalid_query, conn, params=params).iloc[0]
            
            return {
                'rule': self.quality_rules['price_positive'],
                'invalid_count': result['invalid_count'],
                'details': {
                    'invalid_open': result['invalid_open'],
                    'invalid_high': result['invalid_high'],
                    'invalid_low': result['invalid_low'],
                    'invalid_close': result['invalid_close']
                },
                'passed': result['invalid_count'] == 0
            }
            
        except Exception as e:
            return {'rule': self.quality_rules['price_positive'], 'error': str(e), 'passed': False}
    
    def _check_high_low_order(self, conn, where_clause, params) -> Dict:
        """고가/저가 순서 검증"""
        try:
            invalid_query = f"""
                SELECT COUNT(*) as invalid_count
                FROM stock_prices {where_clause}
                AND high_price < low_price
            """
            
            invalid_count = pd.read_sql(invalid_query, conn, params=params).iloc[0]['invalid_count']
            
            return {
                'rule': self.quality_rules['high_low_order'],
                'invalid_count': invalid_count,
                'passed': invalid_count == 0
            }
            
        except Exception as e:
            return {'rule': self.quality_rules['high_low_order'], 'error': str(e), 'passed': False}
    
    def _check_ohlc_logic(self, conn, where_clause, params) -> Dict:
        """OHLC 논리 검증"""
        try:
            invalid_query = f"""
                SELECT COUNT(*) as invalid_count,
                       COUNT(CASE WHEN open_price NOT BETWEEN low_price AND high_price THEN 1 END) as invalid_open,
                       COUNT(CASE WHEN close_price NOT BETWEEN low_price AND high_price THEN 1 END) as invalid_close
                FROM stock_prices {where_clause}
                AND (open_price NOT BETWEEN low_price AND high_price 
                     OR close_price NOT BETWEEN low_price AND high_price)
            """
            
            result = pd.read_sql(invalid_query, conn, params=params).iloc[0]
            
            return {
                'rule': self.quality_rules['ohlc_logic'],
                'invalid_count': result['invalid_count'],
                'details': {
                    'invalid_open': result['invalid_open'],
                    'invalid_close': result['invalid_close']
                },
                'passed': result['invalid_count'] == 0
            }
            
        except Exception as e:
            return {'rule': self.quality_rules['ohlc_logic'], 'error': str(e), 'passed': False}
    
    def _check_volume_data(self, conn, where_clause, params) -> Dict:
        """거래량 데이터 검증"""
        try:
            volume_query = f"""
                SELECT COUNT(*) as total_records,
                       COUNT(CASE WHEN volume < 0 THEN 1 END) as negative_volume,
                       COUNT(CASE WHEN volume = 0 THEN 1 END) as zero_volume,
                       AVG(volume) as avg_volume,
                       MAX(volume) as max_volume
                FROM stock_prices {where_clause}
            """
            
            result = pd.read_sql(volume_query, conn, params=params).iloc[0]
            
            return {
                'rule': self.quality_rules['volume_non_negative'],
                'invalid_count': result['negative_volume'],
                'details': {
                    'negative_volume': result['negative_volume'],
                    'zero_volume': result['zero_volume'],
                    'avg_volume': int(result['avg_volume'] or 0),
                    'max_volume': int(result['max_volume'] or 0)
                },
                'passed': result['negative_volume'] == 0
            }
            
        except Exception as e:
            return {'rule': self.quality_rules['volume_non_negative'], 'error': str(e), 'passed': False}
    
    def _check_price_variation(self, conn, where_clause, params) -> Dict:
        """가격 변동 합리성 검증"""
        try:
            # 일일 변동률이 ±30%를 초과하는 경우를 비정상으로 간주
            variation_query = f"""
                SELECT stock_code, date, close_price,
                       LAG(close_price) OVER (PARTITION BY stock_code ORDER BY date) as prev_close,
                       ABS((close_price - LAG(close_price) OVER (PARTITION BY stock_code ORDER BY date)) 
                           / LAG(close_price) OVER (PARTITION BY stock_code ORDER BY date)) * 100 as daily_change_pct
                FROM stock_prices {where_clause}
            """
            
            variations = pd.read_sql(variation_query, conn, params=params)
            
            # 30% 초과 변동 검출
            extreme_variations = variations[variations['daily_change_pct'] > 30].dropna()
            
            return {
                'rule': self.quality_rules['price_reasonable'],
                'invalid_count': len(extreme_variations),
                'details': {
                    'extreme_variations': len(extreme_variations),
                    'max_variation': round(variations['daily_change_pct'].max() or 0, 2),
                    'avg_variation': round(variations['daily_change_pct'].mean() or 0, 2)
                },
                'passed': len(extreme_variations) < len(variations) * 0.01  # 1% 미만은 허용
            }
            
        except Exception as e:
            return {'rule': self.quality_rules['price_reasonable'], 'error': str(e), 'passed': False}
    
    def _check_data_continuity(self, conn, stock_codes: List[str] = None) -> Dict:
        """데이터 연속성 검증"""
        try:
            # 최근 30일간 영업일 기준 연속성 체크
            end_date = datetime.now()
            start_date = end_date - timedelta(days=50)  # 여유를 두고 50일
            
            # 영업일 생성 (주말 제외)
            business_days = pd.date_range(start=start_date, end=end_date, freq='B')
            expected_dates = [d.strftime('%Y-%m-%d') for d in business_days[-30:]]  # 최근 30 영업일
            
            # 종목별 데이터 연속성 체크
            stock_condition = ""
            params = []
            
            if stock_codes:
                stock_condition = f"AND stock_code IN ({','.join(['?' for _ in stock_codes])})"
                params = stock_codes
            
            # 각 예상 날짜별 데이터 존재 여부 확인
            missing_dates = []
            total_expected = len(expected_dates)
            
            for date in expected_dates:
                count_query = f"""
                    SELECT COUNT(DISTINCT stock_code) as stock_count 
                    FROM stock_prices 
                    WHERE date = ? {stock_condition}
                """
                count_result = pd.read_sql(count_query, conn, params=[date] + params)
                stock_count = count_result.iloc[0]['stock_count']
                
                if stock_count == 0:
                    missing_dates.append(date)
            
            continuity_score = (total_expected - len(missing_dates)) / total_expected * 100
            
            return {
                'rule': self.quality_rules['consecutive_data'],
                'missing_dates': missing_dates,
                'missing_count': len(missing_dates),
                'total_expected_dates': total_expected,
                'continuity_score': round(continuity_score, 2),
                'passed': continuity_score >= 90  # 90% 이상은 합격
            }
            
        except Exception as e:
            return {'rule': self.quality_rules['consecutive_data'], 'error': str(e), 'passed': False}
    
    def _calculate_overall_quality_score(self, quality_results: Dict, total_records: int) -> float:
        """종합 품질 점수 계산"""
        try:
            weights = {
                'price_positive': 25,
                'high_low_order': 20,
                'ohlc_logic': 20,
                'volume_check': 15,
                'price_variation': 15,
                'data_continuity': 5
            }
            
            total_score = 0
            total_weight = 0
            
            for rule_name, weight in weights.items():
                if rule_name in quality_results:
                    result = quality_results[rule_name]
                    if 'error' not in result:
                        # 각 규칙별 점수 계산
                        if result['passed']:
                            score = 100
                        else:
                            # 실패한 경우 실패 비율에 따라 점수 차등 적용
                            invalid_count = result.get('invalid_count', 0)
                            if total_records > 0:
                                error_rate = invalid_count / total_records
                                score = max(0, 100 - (error_rate * 100))
                            else:
                                score = 0
                        
                        total_score += score * weight
                        total_weight += weight
            
            return round(total_score / total_weight if total_weight > 0 else 0, 2)
            
        except Exception as e:
            logger.error(f"품질 점수 계산 실패: {e}")
            return 0
    
    def _get_quality_grade(self, score: float) -> str:
        """품질 점수에 따른 등급 반환"""
        if score >= 95:
            return 'Excellent'
        elif score >= 90:
            return 'Very Good'
        elif score >= 80:
            return 'Good'
        elif score >= 70:
            return 'Fair'
        else:
            return 'Poor'

class FinancialDataQualityChecker:
    """재무 데이터 품질 검증 클래스 (기본분석 45% 비중)"""
    
    def __init__(self):
        self.buffett_score_rules = {
            'score_range': "점수는 0-110 범위 내에 있어야 함",
            'component_sum': "구성 점수의 합이 총점과 일치해야 함",
            'logical_ratios': "재무비율이 논리적 범위 내에 있어야 함",
            'required_fields': "필수 재무 지표가 모두 존재해야 함"
        }
    
    def check_buffett_scorecard_quality(self, year: int = None) -> Dict:
        """워런 버핏 스코어카드 품질 검증"""
        logger.info("🏆 워런 버핏 스코어카드 품질 검증 시작...")
        
        try:
            with get_db_connection('stock') as conn:
                # 기본 쿼리 조건
                where_clause = "WHERE quarter IS NULL"  # 연간 데이터만
                params = []
                
                if year:
                    where_clause += " AND year = ?"
                    params.append(year)
                
                # 전체 스코어카드 데이터 통계
                total_query = f"""
                    SELECT COUNT(*) as total_records,
                           COUNT(CASE WHEN total_buffett_score IS NOT NULL THEN 1 END) as scored_records
                    FROM financial_ratios {where_clause}
                """
                total_result = pd.read_sql(total_query, conn, params=params).iloc[0]
                
                if total_result['total_records'] == 0:
                    return {
                        'status': 'no_data',
                        'message': '검증할 재무데이터가 없습니다.',
                        'total_records': 0
                    }
                
                # 각 품질 규칙 검증
                quality_results = {}
                
                # 1. 점수 범위 검증
                quality_results['score_range'] = self._check_score_ranges(conn, where_clause, params)
                
                # 2. 구성 점수 합계 검증
                quality_results['component_sum'] = self._check_component_sum(conn, where_clause, params)
                
                # 3. 재무비율 논리성 검증
                quality_results['logical_ratios'] = self._check_ratio_logic(conn, where_clause, params)
                
                # 4. 필수 필드 존재 검증
                quality_results['required_fields'] = self._check_required_fields(conn, where_clause, params)
                
                # 종합 품질 점수 계산
                overall_score = self._calculate_financial_quality_score(quality_results, total_result['scored_records'])
                
                return {
                    'status': 'completed',
                    'timestamp': datetime.now().isoformat(),
                    'year': year,
                    'total_records': total_result['total_records'],
                    'scored_records': total_result['scored_records'],
                    'coverage_rate': round(total_result['scored_records'] / total_result['total_records'] * 100, 2),
                    'quality_results': quality_results,
                    'overall_score': overall_score,
                    'quality_grade': self._get_quality_grade(overall_score)
                }
                
        except Exception as e:
            logger.error(f"재무 데이터 품질 검증 실패: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _check_score_ranges(self, conn, where_clause, params) -> Dict:
        """점수 범위 검증"""
        try:
            range_query = f"""
                SELECT 
                    COUNT(CASE WHEN total_buffett_score < 0 OR total_buffett_score > 110 THEN 1 END) as invalid_total,
                    COUNT(CASE WHEN profitability_score < 0 OR profitability_score > 30 THEN 1 END) as invalid_profitability,
                    COUNT(CASE WHEN growth_score < 0 OR growth_score > 25 THEN 1 END) as invalid_growth,
                    COUNT(CASE WHEN stability_score < 0 OR stability_score > 25 THEN 1 END) as invalid_stability,
                    COUNT(CASE WHEN efficiency_score < 0 OR efficiency_score > 10 THEN 1 END) as invalid_efficiency,
                    COUNT(CASE WHEN valuation_score < 0 OR valuation_score > 20 THEN 1 END) as invalid_valuation
                FROM financial_ratios {where_clause}
                AND total_buffett_score IS NOT NULL
            """
            
            result = pd.read_sql(range_query, conn, params=params).iloc[0]
            total_invalid = result['invalid_total']
            
            return {
                'rule': self.buffett_score_rules['score_range'],
                'invalid_count': total_invalid,
                'details': {
                    'invalid_total': result['invalid_total'],
                    'invalid_profitability': result['invalid_profitability'],
                    'invalid_growth': result['invalid_growth'],
                    'invalid_stability': result['invalid_stability'],
                    'invalid_efficiency': result['invalid_efficiency'],
                    'invalid_valuation': result['invalid_valuation']
                },
                'passed': total_invalid == 0
            }
            
        except Exception as e:
            return {'rule': self.buffett_score_rules['score_range'], 'error': str(e), 'passed': False}
    
    def _check_component_sum(self, conn, where_clause, params) -> Dict:
        """구성 점수 합계 검증"""
        try:
            sum_query = f"""
                SELECT COUNT(*) as invalid_count
                FROM financial_ratios {where_clause}
                AND total_buffett_score IS NOT NULL
                AND ABS(total_buffett_score - (
                    COALESCE(profitability_score, 0) + 
                    COALESCE(growth_score, 0) + 
                    COALESCE(stability_score, 0) + 
                    COALESCE(efficiency_score, 0) + 
                    COALESCE(valuation_score, 0)
                )) > 0.1
            """
            
            invalid_count = pd.read_sql(sum_query, conn, params=params).iloc[0]['invalid_count']
            
            return {
                'rule': self.buffett_score_rules['component_sum'],
                'invalid_count': invalid_count,
                'passed': invalid_count == 0
            }
            
        except Exception as e:
            return {'rule': self.buffett_score_rules['component_sum'], 'error': str(e), 'passed': False}
    
    def _check_ratio_logic(self, conn, where_clause, params) -> Dict:
        """재무비율 논리성 검증"""
        try:
            logic_query = f"""
                SELECT 
                    COUNT(CASE WHEN roe < -100 OR roe > 100 THEN 1 END) as invalid_roe,
                    COUNT(CASE WHEN debt_ratio < 0 OR debt_ratio > 1000 THEN 1 END) as invalid_debt,
                    COUNT(CASE WHEN current_ratio < 0 OR current_ratio > 50 THEN 1 END) as invalid_current,
                    COUNT(CASE WHEN per < 0 OR per > 1000 THEN 1 END) as invalid_per,
                    COUNT(CASE WHEN pbr < 0 OR pbr > 100 THEN 1 END) as invalid_pbr
                FROM financial_ratios {where_clause}
                AND total_buffett_score IS NOT NULL
            """
            
            result = pd.read_sql(logic_query, conn, params=params).iloc[0]
            total_invalid = sum(result.values())
            
            return {
                'rule': self.buffett_score_rules['logical_ratios'],
                'invalid_count': total_invalid,
                'details': dict(result),
                'passed': total_invalid == 0
            }
            
        except Exception as e:
            return {'rule': self.buffett_score_rules['logical_ratios'], 'error': str(e), 'passed': False}
    
    def _check_required_fields(self, conn, where_clause, params) -> Dict:
        """필수 필드 존재 검증"""
        try:
            required_fields = ['roe', 'debt_ratio', 'per', 'pbr', 'revenue', 'net_income']
            
            missing_query = f"""
                SELECT 
                    COUNT(CASE WHEN roe IS NULL THEN 1 END) as missing_roe,
                    COUNT(CASE WHEN debt_ratio IS NULL THEN 1 END) as missing_debt_ratio,
                    COUNT(CASE WHEN per IS NULL THEN 1 END) as missing_per,
                    COUNT(CASE WHEN pbr IS NULL THEN 1 END) as missing_pbr,
                    COUNT(CASE WHEN revenue IS NULL THEN 1 END) as missing_revenue,
                    COUNT(CASE WHEN net_income IS NULL THEN 1 END) as missing_net_income
                FROM financial_ratios {where_clause}
                AND total_buffett_score IS NOT NULL
            """
            
            result = pd.read_sql(missing_query, conn, params=params).iloc[0]
            total_missing = sum(result.values())
            
            return {
                'rule': self.buffett_score_rules['required_fields'],
                'missing_count': total_missing,
                'details': dict(result),
                'passed': total_missing == 0
            }
            
        except Exception as e:
            return {'rule': self.buffett_score_rules['required_fields'], 'error': str(e), 'passed': False}
    
    def _calculate_financial_quality_score(self, quality_results: Dict, total_records: int) -> float:
        """재무 데이터 품질 점수 계산"""
        try:
            weights = {
                'score_range': 30,
                'component_sum': 25,
                'logical_ratios': 25,
                'required_fields': 20
            }
            
            total_score = 0
            total_weight = 0
            
            for rule_name, weight in weights.items():
                if rule_name in quality_results:
                    result = quality_results[rule_name]
                    if 'error' not in result:
                        if result['passed']:
                            score = 100
                        else:
                            invalid_count = result.get('invalid_count', 0) or result.get('missing_count', 0)
                            if total_records > 0:
                                error_rate = invalid_count / total_records
                                score = max(0, 100 - (error_rate * 100))
                            else:
                                score = 0
                        
                        total_score += score * weight
                        total_weight += weight
            
            return round(total_score / total_weight if total_weight > 0 else 0, 2)
            
        except Exception as e:
            logger.error(f"재무 품질 점수 계산 실패: {e}")
            return 0
    
    def _get_quality_grade(self, score: float) -> str:
        """품질 점수에 따른 등급 반환"""
        if score >= 95:
            return 'Excellent'
        elif score >= 90:
            return 'Very Good'
        elif score >= 80:
            return 'Good'
        elif score >= 70:
            return 'Fair'
        else:
            return 'Poor'

class NewsDataQualityChecker:
    """뉴스 감정분석 데이터 품질 검증 클래스 (감정분석 25% 비중)"""
    
    def __init__(self):
        self.sentiment_rules = {
            'sentiment_range': "감정 점수는 -1 ~ 1 범위 내에 있어야 함",
            'confidence_range': "신뢰도는 0 ~ 1 범위 내에 있어야 함",
            'required_content': "제목과 내용이 존재해야 함",
            'date_validity': "발행일이 유효해야 함"
        }
    
    def check_news_data_quality(self, days: int = 7) -> Dict:
        """뉴스 감정분석 데이터 품질 검증"""
        logger.info("📰 뉴스 감정분석 데이터 품질 검증 시작...")
        
        try:
            with get_db_connection('news') as conn:
                # 최근 N일 데이터 검증
                cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
                
                # 전체 뉴스 데이터 통계
                total_query = f"""
                    SELECT COUNT(*) as total_news,
                           COUNT(CASE WHEN sentiment_score IS NOT NULL THEN 1 END) as analyzed_news
                    FROM news_articles
                    WHERE DATE(created_at) >= ?
                """
                total_result = pd.read_sql(total_query, conn, params=[cutoff_date]).iloc[0]
                
                if total_result['total_news'] == 0:
                    return {
                        'status': 'no_data',
                        'message': f'최근 {days}일간 뉴스 데이터가 없습니다.',
                        'total_news': 0
                    }
                
                # 각 품질 규칙 검증
                quality_results = {}
                
                # 1. 감정 점수 범위 검증
                quality_results['sentiment_range'] = self._check_sentiment_ranges(conn, cutoff_date)
                
                # 2. 신뢰도 범위 검증
                quality_results['confidence_range'] = self._check_confidence_ranges(conn, cutoff_date)
                
                # 3. 필수 콘텐츠 존재 검증
                quality_results['required_content'] = self._check_required_content(conn, cutoff_date)
                
                # 4. 날짜 유효성 검증
                quality_results['date_validity'] = self._check_date_validity(conn, cutoff_date)
                
                # 종합 품질 점수 계산
                overall_score = self._calculate_news_quality_score(quality_results, total_result['analyzed_news'])
                
                return {
                    'status': 'completed',
                    'timestamp': datetime.now().isoformat(),
                    'days': days,
                    'cutoff_date': cutoff_date,
                    'total_news': total_result['total_news'],
                    'analyzed_news': total_result['analyzed_news'],
                    'analysis_rate': round(total_result['analyzed_news'] / total_result['total_news'] * 100, 2),
                    'quality_results': quality_results,
                    'overall_score': overall_score,
                    'quality_grade': self._get_quality_grade(overall_score)
                }
                
        except Exception as e:
            logger.error(f"뉴스 데이터 품질 검증 실패: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _check_sentiment_ranges(self, conn, cutoff_date) -> Dict:
        """감정 점수 범위 검증"""
        try:
            range_query = f"""
                SELECT COUNT(*) as invalid_count
                FROM news_articles
                WHERE DATE(created_at) >= ?
                AND sentiment_score IS NOT NULL
                AND (sentiment_score < -1 OR sentiment_score > 1)
            """
            
            invalid_count = pd.read_sql(range_query, conn, params=[cutoff_date]).iloc[0]['invalid_count']
            
            return {
                'rule': self.sentiment_rules['sentiment_range'],
                'invalid_count': invalid_count,
                'passed': invalid_count == 0
            }
            
        except Exception as e:
            return {'rule': self.sentiment_rules['sentiment_range'], 'error': str(e), 'passed': False}
    
    def _check_confidence_ranges(self, conn, cutoff_date) -> Dict:
        """신뢰도 범위 검증"""
        try:
            range_query = f"""
                SELECT COUNT(*) as invalid_count
                FROM news_articles
                WHERE DATE(created_at) >= ?
                AND confidence_score IS NOT NULL
                AND (confidence_score < 0 OR confidence_score > 1)
            """
            
            invalid_count = pd.read_sql(range_query, conn, params=[cutoff_date]).iloc[0]['invalid_count']
            
            return {
                'rule': self.sentiment_rules['confidence_range'],
                'invalid_count': invalid_count,
                'passed': invalid_count == 0
            }
            
        except Exception as e:
            return {'rule': self.sentiment_rules['confidence_range'], 'error': str(e), 'passed': False}
    
    def _check_required_content(self, conn, cutoff_date) -> Dict:
        """필수 콘텐츠 존재 검증"""
        try:
            content_query = f"""
                SELECT 
                    COUNT(CASE WHEN title IS NULL OR TRIM(title) = '' THEN 1 END) as missing_title,
                    COUNT(CASE WHEN description IS NULL OR TRIM(description) = '' THEN 1 END) as missing_description,
                    COUNT(CASE WHEN stock_code IS NULL OR TRIM(stock_code) = '' THEN 1 END) as missing_stock_code
                FROM news_articles
                WHERE DATE(created_at) >= ?
            """
            
            result = pd.read_sql(content_query, conn, params=[cutoff_date]).iloc[0]
            total_missing = sum(result.values())
            
            return {
                'rule': self.sentiment_rules['required_content'],
                'missing_count': total_missing,
                'details': dict(result),
                'passed': total_missing == 0
            }
            
        except Exception as e:
            return {'rule': self.sentiment_rules['required_content'], 'error': str(e), 'passed': False}
    
    def _check_date_validity(self, conn, cutoff_date) -> Dict:
        """날짜 유효성 검증"""
        try:
            # 미래 날짜나 너무 과거 날짜 검증
            future_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
            past_limit = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            
            date_query = f"""
                SELECT COUNT(*) as invalid_count
                FROM news_articles
                WHERE DATE(created_at) >= ?
                AND (DATE(pubDate) > ? OR DATE(pubDate) < ?)
            """
            
            invalid_count = pd.read_sql(date_query, conn, params=[cutoff_date, future_date, past_limit]).iloc[0]['invalid_count']
            
            return {
                'rule': self.sentiment_rules['date_validity'],
                'invalid_count': invalid_count,
                'passed': invalid_count == 0
            }
            
        except Exception as e:
            return {'rule': self.sentiment_rules['date_validity'], 'error': str(e), 'passed': False}
    
    def _calculate_news_quality_score(self, quality_results: Dict, total_records: int) -> float:
        """뉴스 데이터 품질 점수 계산"""
        try:
            weights = {
                'sentiment_range': 30,
                'confidence_range': 25,
                'required_content': 30,
                'date_validity': 15
            }
            
            total_score = 0
            total_weight = 0
            
            for rule_name, weight in weights.items():
                if rule_name in quality_results:
                    result = quality_results[rule_name]
                    if 'error' not in result:
                        if result['passed']:
                            score = 100
                        else:
                            invalid_count = result.get('invalid_count', 0) or result.get('missing_count', 0)
                            if total_records > 0:
                                error_rate = invalid_count / total_records
                                score = max(0, 100 - (error_rate * 100))
                            else:
                                score = 0
                        
                        total_score += score * weight
                        total_weight += weight
            
            return round(total_score / total_weight if total_weight > 0 else 0, 2)
            
        except Exception as e:
            logger.error(f"뉴스 품질 점수 계산 실패: {e}")
            return 0
    
    def _get_quality_grade(self, score: float) -> str:
        """품질 점수에 따른 등급 반환"""
        if score >= 95:
            return 'Excellent'
        elif score >= 90:
            return 'Very Good'
        elif score >= 80:
            return 'Good'
        elif score >= 70:
            return 'Fair'
        else:
            return 'Poor'

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='워런 버핏 시스템 데이터 품질 검증')
    parser.add_argument('--database', choices=['stock', 'financial', 'news', 'all'], 
                       default='all', help='검증할 데이터베이스')
    parser.add_argument('--target-date', help='대상 날짜 (YYYY-MM-DD)')
    parser.add_argument('--stock-codes', nargs='+', help='특정 종목 코드들')
    parser.add_argument('--year', type=int, help='재무데이터 검증 연도')
    parser.add_argument('--days', type=int, default=7, help='뉴스 데이터 검증 일수')
    parser.add_argument('--output', help='결과 저장 파일 (JSON)')
    parser.add_argument('--verbose', '-v', action='store_true', help='상세 로그 출력')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        results = {
            'timestamp': datetime.now().isoformat(),
            'validation_type': args.database,
            'target_date': args.target_date,
            'stock_codes': args.stock_codes,
            'results': {}
        }
        
        print(f"🔍 워런 버핏 시스템 데이터 품질 검증 시작 ({args.database})")
        print("=" * 60)
        
        if args.database in ['stock', 'all']:
            print("📈 주가 데이터 품질 검증 중...")
            stock_checker = StockDataQualityChecker()
            stock_result = stock_checker.check_stock_data_quality(args.target_date, args.stock_codes)
            results['results']['stock'] = stock_result
            
            if stock_result['status'] == 'completed':
                print(f"   ✅ 품질 점수: {stock_result['overall_score']}/100 ({stock_result['quality_grade']})")
                print(f"   📊 총 레코드: {stock_result['total_records']:,}개")
            else:
                print(f"   ❌ 검증 실패: {stock_result.get('message', stock_result.get('error', '알 수 없는 오류'))}")
        
        if args.database in ['financial', 'all']:
            print("\n🏆 워런 버핏 스코어카드 품질 검증 중...")
            financial_checker = FinancialDataQualityChecker()
            financial_result = financial_checker.check_buffett_scorecard_quality(args.year)
            results['results']['financial'] = financial_result
            
            if financial_result['status'] == 'completed':
                print(f"   ✅ 품질 점수: {financial_result['overall_score']}/100 ({financial_result['quality_grade']})")
                print(f"   📊 스코어카드 적용: {financial_result['scored_records']:,}/{financial_result['total_records']:,}개 ({financial_result['coverage_rate']:.1f}%)")
            else:
                print(f"   ❌ 검증 실패: {financial_result.get('message', financial_result.get('error', '알 수 없는 오류'))}")
        
        if args.database in ['news', 'all']:
            print("\n📰 뉴스 감정분석 데이터 품질 검증 중...")
            news_checker = NewsDataQualityChecker()
            news_result = news_checker.check_news_data_quality(args.days)
            results['results']['news'] = news_result
            
            if news_result['status'] == 'completed':
                print(f"   ✅ 품질 점수: {news_result['overall_score']}/100 ({news_result['quality_grade']})")
                print(f"   📊 감정분석 적용: {news_result['analyzed_news']:,}/{news_result['total_news']:,}개 ({news_result['analysis_rate']:.1f}%)")
            else:
                print(f"   ❌ 검증 실패: {news_result.get('message', news_result.get('error', '알 수 없는 오류'))}")
        
        # 전체 요약
        if args.database == 'all':
            print("\n" + "=" * 60)
            print("📊 전체 품질 검증 요약")
            print("=" * 60)
            
            total_scores = []
            for db_name, result in results['results'].items():
                if result['status'] == 'completed':
                    score = result['overall_score']
                    grade = result['quality_grade']
                    total_scores.append(score)
                    print(f"   {db_name.upper()}: {score}/100 ({grade})")
            
            if total_scores:
                avg_score = sum(total_scores) / len(total_scores)
                print(f"\n🎯 전체 평균 품질 점수: {avg_score:.1f}/100")
                
                if avg_score >= 90:
                    print("✅ 전체 시스템 데이터 품질이 우수합니다!")
                elif avg_score >= 80:
                    print("👍 전체 시스템 데이터 품질이 양호합니다.")
                else:
                    print("⚠️ 데이터 품질 개선이 필요합니다.")
        
        # 결과 파일 저장
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"\n💾 결과가 {args.output}에 저장되었습니다.")
        
        print("\n✅ 데이터 품질 검증이 완료되었습니다.")
        return True
        
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 중단되었습니다.")
        return True
    except Exception as e:
        logger.error(f"품질 검증 실행 실패: {e}")
        print(f"❌ 실행 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)