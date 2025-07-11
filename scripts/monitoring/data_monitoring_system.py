#!/usr/bin/env python3
"""
워런 버핏 스코어카드 시스템 데이터 모니터링 및 백업 시스템
scripts/monitoring/data_monitoring_system.py

- 실시간 데이터 상태 모니터링
- 자동 백업 및 복구 시스템
- 시스템 헬스 체크
- 이메일 알림 시스템
- 데이터 품질 검증
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import shutil
import json
import logging
import smtplib
import gzip
from typing import Dict, List, Optional, Tuple, Any
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from email.mime.base import MimeBase
from email import encoders
import argparse

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from config.database_config import get_db_connection, get_database_path
    from config.settings import get_database_info
except ImportError:
    print("⚠️ config 모듈을 찾을 수 없습니다. 경로를 확인해주세요.")
    # 기본 함수들을 여기서 정의
    def get_db_connection(db_name):
        db_path = Path(f'data/databases/{db_name}_data.db')
        return sqlite3.connect(str(db_path))
    
    def get_database_path(db_name):
        return Path(f'data/databases/{db_name}_data.db')

# 로깅 설정
log_dir = project_root / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'monitoring.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataMonitor:
    """워런 버핏 시스템 데이터 모니터링 클래스"""
    
    def __init__(self):
        self.databases = ['stock', 'dart', 'news', 'kis']
        self.critical_tables = {
            'stock': ['stock_prices', 'company_info', 'financial_ratios', 'technical_indicators', 'investment_scores'],
            'dart': ['corp_codes', 'financial_statements', 'disclosures', 'company_outlines'],
            'news': ['news_articles', 'sentiment_scores', 'market_sentiment'],
            'kis': ['realtime_quotes', 'account_balance', 'order_history', 'market_indicators']
        }
    
    def check_data_freshness(self) -> Dict[str, Dict]:
        """데이터 최신성 체크 - 워런 버핏 시스템 특화"""
        logger.info("🔍 데이터 최신성 체크 시작...")
        results = {}
        
        for db_name in self.databases:
            try:
                results[db_name] = self._check_database_freshness(db_name)
                logger.debug(f"{db_name} 데이터베이스 체크 완료")
            except Exception as e:
                logger.error(f"{db_name} 데이터베이스 체크 실패: {e}")
                results[db_name] = {'error': str(e)}
        
        return results
    
    def _check_database_freshness(self, db_name: str) -> Dict:
        """개별 데이터베이스 최신성 체크"""
        try:
            with get_db_connection(db_name) as conn:
                if db_name == 'stock':
                    return self._check_stock_freshness(conn)
                elif db_name == 'news':
                    return self._check_news_freshness(conn)
                elif db_name == 'dart':
                    return self._check_dart_freshness(conn)
                elif db_name == 'kis':
                    return self._check_kis_freshness(conn)
                else:
                    return {'status': 'not_implemented'}
                    
        except Exception as e:
            logger.error(f"데이터베이스 체크 실패 ({db_name}): {e}")
            return {'error': str(e)}
    
    def _check_stock_freshness(self, conn: sqlite3.Connection) -> Dict:
        """주가 데이터 최신성 체크 (기술분석 30% + 기본분석 45%)"""
        try:
            # 1. 주가 데이터 최신성 (기술분석 30% 비중)
            stock_query = """
                SELECT 
                    MAX(date) as latest_date, 
                    COUNT(DISTINCT stock_code) as stock_count,
                    COUNT(*) as total_records
                FROM stock_prices
                WHERE date >= date('now', '-7 days')
            """
            stock_result = pd.read_sql(stock_query, conn).iloc[0]
            
            # 2. 워런 버핏 스코어카드 최신성 (기본분석 45% 비중)
            buffett_query = """
                SELECT 
                    COUNT(*) as scored_companies,
                    AVG(total_buffett_score) as avg_score,
                    MAX(updated_at) as latest_update
                FROM financial_ratios
                WHERE total_buffett_score IS NOT NULL
            """
            
            try:
                buffett_result = pd.read_sql(buffett_query, conn).iloc[0]
            except:
                buffett_result = {'scored_companies': 0, 'avg_score': 0, 'latest_update': None}
            
            # 3. 기술적 지표 최신성
            technical_query = """
                SELECT 
                    MAX(date) as latest_tech_date,
                    COUNT(DISTINCT stock_code) as tech_stock_count,
                    AVG(technical_score) as avg_tech_score
                FROM technical_indicators
                WHERE date >= date('now', '-7 days')
            """
            
            try:
                tech_result = pd.read_sql(technical_query, conn).iloc[0]
            except:
                tech_result = {'latest_tech_date': None, 'tech_stock_count': 0, 'avg_tech_score': 0}
            
            # 영업일 계산
            today = datetime.now()
            if today.weekday() >= 5:  # 주말
                expected_date = today - timedelta(days=today.weekday()-4)
            else:
                expected_date = today - timedelta(days=1) if today.hour >= 18 else today - timedelta(days=2)
            
            latest_date = stock_result['latest_date']
            is_fresh = latest_date == expected_date.strftime('%Y-%m-%d')
            
            return {
                'type': 'stock_analysis',
                'latest_date': latest_date,
                'expected_date': expected_date.strftime('%Y-%m-%d'),
                'stock_count': stock_result['stock_count'],
                'total_records': stock_result['total_records'],
                'is_fresh': is_fresh,
                
                # 워런 버핏 스코어카드 정보
                'buffett_scorecard': {
                    'scored_companies': buffett_result['scored_companies'],
                    'avg_score': round(buffett_result['avg_score'] or 0, 2),
                    'latest_update': buffett_result['latest_update']
                },
                
                # 기술적 분석 정보
                'technical_analysis': {
                    'latest_date': tech_result['latest_tech_date'],
                    'stock_count': tech_result['tech_stock_count'],
                    'avg_score': round(tech_result['avg_tech_score'] or 0, 2)
                },
                
                'health_score': self._calculate_stock_health_score(stock_result, buffett_result, tech_result, is_fresh)
            }
            
        except Exception as e:
            logger.error(f"주가 데이터 체크 실패: {e}")
            return {'error': str(e)}
    
    def _check_news_freshness(self, conn: sqlite3.Connection) -> Dict:
        """뉴스 데이터 최신성 체크 (감정분석 25% 비중)"""
        try:
            # 뉴스 데이터 최신성
            news_query = """
                SELECT 
                    DATE(MAX(created_at)) as latest_date, 
                    COUNT(*) as news_count,
                    COUNT(DISTINCT stock_code) as covered_stocks,
                    AVG(sentiment_score) as avg_sentiment
                FROM news_articles
                WHERE created_at >= datetime('now', '-1 day')
            """
            news_result = pd.read_sql(news_query, conn).iloc[0]
            
            # 감정분석 점수 최신성
            sentiment_query = """
                SELECT 
                    COUNT(*) as analyzed_stocks,
                    AVG(sentiment_final_score) as avg_final_score,
                    MAX(updated_at) as latest_analysis
                FROM sentiment_scores
                WHERE date >= date('now', '-1 day')
            """
            
            try:
                sentiment_result = pd.read_sql(sentiment_query, conn).iloc[0]
            except:
                sentiment_result = {'analyzed_stocks': 0, 'avg_final_score': 0, 'latest_analysis': None}
            
            return {
                'type': 'sentiment_analysis',
                'latest_date': news_result['latest_date'],
                'news_count': news_result['news_count'],
                'covered_stocks': news_result['covered_stocks'],
                'avg_sentiment': round(news_result['avg_sentiment'] or 0, 3),
                'is_fresh': news_result['news_count'] > 0,
                
                'sentiment_analysis': {
                    'analyzed_stocks': sentiment_result['analyzed_stocks'],
                    'avg_final_score': round(sentiment_result['avg_final_score'] or 0, 2),
                    'latest_analysis': sentiment_result['latest_analysis']
                }
            }
            
        except Exception as e:
            logger.error(f"뉴스 데이터 체크 실패: {e}")
            return {'error': str(e)}
    
    def _check_dart_freshness(self, conn: sqlite3.Connection) -> Dict:
        """DART 데이터 최신성 체크 (기본분석 45% 비중 지원)"""
        try:
            disclosure_query = """
                SELECT 
                    DATE(MAX(created_at)) as latest_date, 
                    COUNT(*) as disclosure_count,
                    COUNT(DISTINCT corp_code) as corp_count
                FROM disclosures
                WHERE created_at >= datetime('now', '-7 days')
            """
            disclosure_result = pd.read_sql(disclosure_query, conn).iloc[0]
            
            financial_query = """
                SELECT 
                    COUNT(DISTINCT corp_code) as financial_corps,
                    MAX(created_at) as latest_financial
                FROM financial_statements
                WHERE created_at >= datetime('now', '-30 days')
            """
            
            try:
                financial_result = pd.read_sql(financial_query, conn).iloc[0]
            except:
                financial_result = {'financial_corps': 0, 'latest_financial': None}
            
            return {
                'type': 'fundamental_data',
                'latest_date': disclosure_result['latest_date'],
                'disclosure_count': disclosure_result['disclosure_count'],
                'corp_count': disclosure_result['corp_count'],
                'is_fresh': disclosure_result['disclosure_count'] > 0,
                
                'financial_data': {
                    'corps_with_data': financial_result['financial_corps'],
                    'latest_update': financial_result['latest_financial']
                }
            }
            
        except Exception as e:
            logger.error(f"DART 데이터 체크 실패: {e}")
            return {'error': str(e)}
    
    def _check_kis_freshness(self, conn: sqlite3.Connection) -> Dict:
        """KIS 데이터 최신성 체크"""
        try:
            kis_query = """
                SELECT 
                    MAX(timestamp) as latest_timestamp,
                    COUNT(DISTINCT stock_code) as stock_count,
                    COUNT(*) as quote_count
                FROM realtime_quotes
                WHERE timestamp >= datetime('now', '-1 hour')
            """
            
            try:
                kis_result = pd.read_sql(kis_query, conn).iloc[0]
                return {
                    'type': 'realtime_data',
                    'latest_timestamp': kis_result['latest_timestamp'],
                    'stock_count': kis_result['stock_count'],
                    'quote_count': kis_result['quote_count'],
                    'is_fresh': kis_result['quote_count'] > 0
                }
            except:
                return {
                    'type': 'realtime_data',
                    'latest_timestamp': None,
                    'stock_count': 0,
                    'quote_count': 0,
                    'is_fresh': False
                }
                
        except Exception as e:
            logger.error(f"KIS 데이터 체크 실패: {e}")
            return {'error': str(e)}
    
    def _calculate_stock_health_score(self, stock_result, buffett_result, tech_result, is_fresh) -> int:
        """주식 데이터 종합 건강도 점수 계산 (0-100)"""
        score = 0
        
        # 데이터 최신성 (30점)
        if is_fresh:
            score += 30
        
        # 데이터 완성도 (40점)
        if stock_result['stock_count'] > 2000:  # 최소 종목 수
            score += 20
        if buffett_result['scored_companies'] > 1000:  # 스코어카드 적용 종목
            score += 20
        
        # 데이터 품질 (30점)
        if buffett_result['avg_score'] > 0:  # 평균 스코어 존재
            score += 15
        if tech_result['avg_tech_score'] > 0:  # 기술적 분석 점수 존재
            score += 15
        
        return min(score, 100)
    
    def check_data_quality(self) -> Dict[str, Dict]:
        """워런 버핏 시스템 데이터 품질 체크"""
        logger.info("📊 데이터 품질 체크 시작...")
        results = {}
        
        # 주가 데이터 품질 체크
        results['stock_quality'] = self._check_stock_data_quality()
        
        # 워런 버핏 스코어카드 품질 체크
        results['buffett_quality'] = self._check_buffett_scorecard_quality()
        
        # 중복 데이터 체크
        results['duplicates'] = self._check_duplicates()
        
        # 누락 데이터 체크
        results['missing_data'] = self._check_missing_data()
        
        return results
    
    def _check_stock_data_quality(self) -> Dict:
        """주가 데이터 품질 체크"""
        try:
            with get_db_connection('stock') as conn:
                # 비정상적인 가격 데이터 체크
                invalid_query = """
                    SELECT COUNT(*) as invalid_count
                    FROM stock_prices
                    WHERE open_price <= 0 OR high_price <= 0 OR low_price <= 0 OR close_price <= 0
                       OR high_price < low_price
                       OR open_price NOT BETWEEN low_price AND high_price
                       OR close_price NOT BETWEEN low_price AND high_price
                """
                invalid_count = pd.read_sql(invalid_query, conn).iloc[0]['invalid_count']
                
                # 전체 레코드 수
                total_query = "SELECT COUNT(*) as total FROM stock_prices"
                total_count = pd.read_sql(total_query, conn).iloc[0]['total']
                
                # 최근 7일 데이터 완성도
                recent_query = """
                    SELECT 
                        COUNT(DISTINCT stock_code) as recent_stocks,
                        COUNT(DISTINCT date) as recent_dates
                    FROM stock_prices 
                    WHERE date >= date('now', '-7 days')
                """
                recent_result = pd.read_sql(recent_query, conn).iloc[0]
                
                quality_score = (total_count - invalid_count) / total_count * 100 if total_count > 0 else 0
                
                return {
                    'total_records': total_count,
                    'invalid_records': invalid_count,
                    'quality_score': round(quality_score, 2),
                    'recent_stocks': recent_result['recent_stocks'],
                    'recent_dates': recent_result['recent_dates'],
                    'status': 'excellent' if quality_score >= 99 else 'good' if quality_score >= 95 else 'poor'
                }
                
        except Exception as e:
            logger.error(f"주가 데이터 품질 체크 실패: {e}")
            return {'error': str(e)}
    
    def _check_buffett_scorecard_quality(self) -> Dict:
        """워런 버핏 스코어카드 품질 체크"""
        try:
            with get_db_connection('stock') as conn:
                scorecard_query = """
                    SELECT 
                        COUNT(*) as total_companies,
                        COUNT(CASE WHEN total_buffett_score IS NOT NULL THEN 1 END) as scored_companies,
                        AVG(total_buffett_score) as avg_total_score,
                        AVG(profitability_score) as avg_profitability,
                        AVG(growth_score) as avg_growth,
                        AVG(stability_score) as avg_stability,
                        AVG(efficiency_score) as avg_efficiency,
                        AVG(valuation_score) as avg_valuation,
                        MIN(total_buffett_score) as min_score,
                        MAX(total_buffett_score) as max_score
                    FROM financial_ratios
                    WHERE quarter IS NULL  -- 연간 데이터만
                """
                
                result = pd.read_sql(scorecard_query, conn).iloc[0]
                
                # 스코어카드 커버리지 계산
                coverage = result['scored_companies'] / result['total_companies'] * 100 if result['total_companies'] > 0 else 0
                
                # 점수 분포 체크
                distribution_query = """
                    SELECT 
                        COUNT(CASE WHEN total_buffett_score >= 90 THEN 1 END) as excellent,
                        COUNT(CASE WHEN total_buffett_score >= 80 AND total_buffett_score < 90 THEN 1 END) as very_good,
                        COUNT(CASE WHEN total_buffett_score >= 70 AND total_buffett_score < 80 THEN 1 END) as good,
                        COUNT(CASE WHEN total_buffett_score >= 60 AND total_buffett_score < 70 THEN 1 END) as fair,
                        COUNT(CASE WHEN total_buffett_score < 60 THEN 1 END) as poor
                    FROM financial_ratios
                    WHERE total_buffett_score IS NOT NULL AND quarter IS NULL
                """
                
                distribution = pd.read_sql(distribution_query, conn).iloc[0]
                
                return {
                    'total_companies': result['total_companies'],
                    'scored_companies': result['scored_companies'],
                    'coverage_percentage': round(coverage, 2),
                    'avg_scores': {
                        'total': round(result['avg_total_score'] or 0, 2),
                        'profitability': round(result['avg_profitability'] or 0, 2),
                        'growth': round(result['avg_growth'] or 0, 2),
                        'stability': round(result['avg_stability'] or 0, 2),
                        'efficiency': round(result['avg_efficiency'] or 0, 2),
                        'valuation': round(result['avg_valuation'] or 0, 2)
                    },
                    'score_range': {
                        'min': result['min_score'],
                        'max': result['max_score']
                    },
                    'grade_distribution': {
                        'excellent': distribution['excellent'],
                        'very_good': distribution['very_good'],
                        'good': distribution['good'],
                        'fair': distribution['fair'],
                        'poor': distribution['poor']
                    },
                    'status': 'excellent' if coverage >= 90 else 'good' if coverage >= 70 else 'poor'
                }
                
        except Exception as e:
            logger.error(f"워런 버핏 스코어카드 품질 체크 실패: {e}")
            return {'error': str(e)}
    
    def _check_duplicates(self) -> Dict:
        """중복 데이터 체크"""
        results = {}
        
        try:
            # 주가 데이터 중복 체크
            with get_db_connection('stock') as conn:
                stock_dup_query = """
                    SELECT stock_code, date, COUNT(*) as dup_count
                    FROM stock_prices
                    GROUP BY stock_code, date
                    HAVING COUNT(*) > 1
                """
                stock_duplicates = pd.read_sql(stock_dup_query, conn)
                results['stock_duplicates'] = len(stock_duplicates)
            
            # 뉴스 데이터 중복 체크
            with get_db_connection('news') as conn:
                news_dup_query = """
                    SELECT title, pubDate, COUNT(*) as dup_count
                    FROM news_articles
                    GROUP BY title, pubDate
                    HAVING COUNT(*) > 1
                """
                news_duplicates = pd.read_sql(news_dup_query, conn)
                results['news_duplicates'] = len(news_duplicates)
                
        except Exception as e:
            logger.error(f"중복 데이터 체크 실패: {e}")
            results['error'] = str(e)
        
        return results
    
    def _check_missing_data(self) -> Dict:
        """누락 데이터 체크"""
        try:
            # 최근 5일간 영업일 중 누락된 날짜 체크
            with get_db_connection('stock') as conn:
                # 영업일 생성 (주말 제외)
                end_date = datetime.now()
                start_date = end_date - timedelta(days=10)
                date_range = pd.date_range(start=start_date, end=end_date, freq='B')
                
                missing_dates = []
                for date in date_range:
                    date_str = date.strftime('%Y-%m-%d')
                    count_query = f"SELECT COUNT(DISTINCT stock_code) as count FROM stock_prices WHERE date = '{date_str}'"
                    count_result = pd.read_sql(count_query, conn).iloc[0]['count']
                    
                    if count_result == 0:
                        missing_dates.append(date_str)
                
                return {
                    'missing_dates': missing_dates,
                    'missing_count': len(missing_dates),
                    'status': 'good' if len(missing_dates) == 0 else 'warning' if len(missing_dates) <= 2 else 'critical'
                }
                
        except Exception as e:
            logger.error(f"누락 데이터 체크 실패: {e}")
            return {'error': str(e)}
    
    def generate_health_report(self) -> Dict:
        """시스템 종합 헬스 리포트 생성"""
        try:
            logger.info("🏥 워런 버핏 시스템 헬스 체크 시작...")
            
            report = {
                'timestamp': datetime.now().isoformat(),
                'system_name': 'Warren Buffett Investment System',
                'version': '1.0.0',
                'data_freshness': self.check_data_freshness(),
                'data_quality': self.check_data_quality(),
                'database_sizes': self._get_database_sizes(),
                'system_status': 'healthy',
                'issues': [],
                'recommendations': []
            }
            
            # 전체 상태 평가
            issues = []
            recommendations = []
            
            # 데이터 최신성 문제 체크
            for db_name, freshness in report['data_freshness'].items():
                if 'error' in freshness:
                    issues.append(f"❌ {db_name} 데이터베이스 연결 오류: {freshness['error']}")
                elif not freshness.get('is_fresh', False):
                    issues.append(f"⚠️ {db_name} 데이터가 최신이 아님")
                    recommendations.append(f"📅 {db_name} 데이터 업데이트 실행 필요")
            
            # 워런 버핏 스코어카드 품질 체크
            buffett_quality = report['data_quality'].get('buffett_quality', {})
            if buffett_quality.get('coverage_percentage', 0) < 70:
                issues.append(f"📊 워런 버핏 스코어카드 커버리지 부족 ({buffett_quality.get('coverage_percentage', 0)}%)")
                recommendations.append("🔄 재무데이터 수집 및 스코어카드 계산 재실행 필요")
            
            # 데이터 품질 문제 체크
            stock_quality = report['data_quality'].get('stock_quality', {})
            if stock_quality.get('quality_score', 100) < 95:
                issues.append(f"📈 주가 데이터 품질 저하 ({stock_quality.get('quality_score', 0)}%)")
                recommendations.append("🧹 데이터 정리 및 검증 필요")
            
            # 중복 데이터 문제 체크
            duplicates = report['data_quality'].get('duplicates', {})
            if duplicates.get('stock_duplicates', 0) > 0:
                issues.append(f"🔄 주가 데이터 중복 {duplicates['stock_duplicates']}건")
                recommendations.append("🧹 중복 데이터 제거 필요")
            
            # 누락 데이터 문제 체크
            missing_data = report['data_quality'].get('missing_data', {})
            if missing_data.get('missing_count', 0) > 0:
                issues.append(f"📅 누락된 날짜 {missing_data['missing_count']}개")
                recommendations.append("🔄 누락 데이터 보완 실행 필요")
            
            # 시스템 상태 결정
            if len(issues) == 0:
                report['system_status'] = 'excellent'
            elif len(issues) <= 2:
                report['system_status'] = 'good'
            elif len(issues) <= 5:
                report['system_status'] = 'warning'
            else:
                report['system_status'] = 'critical'
            
            report['issues'] = issues
            report['recommendations'] = recommendations
            report['summary'] = self._generate_summary(report)
            
            logger.info(f"✅ 시스템 상태: {report['system_status']} ({len(issues)}개 이슈)")
            return report
            
        except Exception as e:
            logger.error(f"헬스 리포트 생성 실패: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'system_name': 'Warren Buffett Investment System',
                'system_status': 'error',
                'error': str(e)
            }
    
    def _generate_summary(self, report: Dict) -> Dict:
        """리포트 요약 생성"""
        try:
            stock_data = report['data_freshness'].get('stock', {})
            buffett_data = report['data_quality'].get('buffett_quality', {})
            
            return {
                'total_companies': buffett_data.get('total_companies', 0),
                'buffett_scored': buffett_data.get('scored_companies', 0),
                'avg_buffett_score': buffett_data.get('avg_scores', {}).get('total', 0),
                'latest_stock_date': stock_data.get('latest_date', 'N/A'),
                'stock_coverage': stock_data.get('stock_count', 0),
                'health_score': stock_data.get('health_score', 0),
                'issue_count': len(report.get('issues', [])),
                'recommendation_count': len(report.get('recommendations', []))
            }
        except Exception as e:
            logger.error(f"요약 생성 실패: {e}")
            return {}
    
    def _get_database_sizes(self) -> Dict:
        """데이터베이스 크기 정보"""
        sizes = {}
        
        for db_name in self.databases:
            try:
                db_path = get_database_path(db_name)
                if db_path.exists():
                    size_mb = db_path.stat().st_size / (1024 * 1024)
                    sizes[db_name] = round(size_mb, 2)
                else:
                    sizes[db_name] = 0
            except Exception as e:
                sizes[db_name] = f"error: {e}"
        
        return sizes

class DataBackupManager:
    """데이터 백업 관리 클래스"""
    
    def __init__(self):
        self.backup_dir = project_root / 'backups'
        self.backup_dir.mkdir(exist_ok=True)
        self.databases = ['stock', 'dart', 'news', 'kis']
    
    def create_backup(self, backup_type: str = 'daily') -> Dict[str, str]:
        """백업 생성"""
        try:
            logger.info(f"💾 {backup_type} 백업 생성 시작...")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_results = {}
            
            for db_name in self.databases:
                try:
                    source_path = get_database_path(db_name)
                    
                    if source_path.exists():
                        backup_filename = f"{db_name}_{backup_type}_{timestamp}.db"
                        backup_path = self.backup_dir / backup_filename
                        
                        # 백업 실행
                        shutil.copy2(source_path, backup_path)
                        
                        # 주간 백업은 압축
                        if backup_type == 'weekly':
                            with open(backup_path, 'rb') as f_in:
                                with gzip.open(f"{backup_path}.gz", 'wb') as f_out:
                                    shutil.copyfileobj(f_in, f_out)
                            backup_path.unlink()  # 원본 삭제
                            backup_path = f"{backup_path}.gz"
                        
                        backup_results[db_name] = str(backup_path)
                        logger.info(f"✅ 백업 완료: {db_name} -> {backup_path.name}")
                    else:
                        backup_results[db_name] = "database_not_found"
                        logger.warning(f"⚠️ 데이터베이스 파일 없음: {db_name}")
                        
                except Exception as e:
                    backup_results[db_name] = f"error: {str(e)}"
                    logger.error(f"❌ 백업 실패 ({db_name}): {e}")
            
            logger.info(f"📦 백업 작업 완료: {len([r for r in backup_results.values() if not r.startswith('error')])}/{len(self.databases)}개 성공")
            return backup_results
            
        except Exception as e:
            logger.error(f"백업 생성 실패: {e}")
            return {'error': str(e)}
    
    def cleanup_old_backups(self, keep_days: int = 30) -> int:
        """오래된 백업 정리"""
        try:
            logger.info(f"🧹 {keep_days}일 이상된 백업 파일 정리...")
            cutoff_date = datetime.now() - timedelta(days=keep_days)
            deleted_count = 0
            
            for backup_file in self.backup_dir.glob("*.db*"):
                if backup_file.stat().st_mtime < cutoff_date.timestamp():
                    backup_file.unlink()
                    deleted_count += 1
                    logger.debug(f"🗑️ 오래된 백업 삭제: {backup_file.name}")
            
            logger.info(f"✅ {deleted_count}개 백업 파일 삭제 완료")
            return deleted_count
            
        except Exception as e:
            logger.error(f"백업 정리 실패: {e}")
            return 0
    
    def restore_backup(self, backup_file: str, target_db: str) -> bool:
        """백업 복원"""
        try:
            logger.info(f"🔄 백업 복원: {backup_file} -> {target_db}")
            backup_path = Path(backup_file)
            target_path = get_database_path(target_db)
            
            if not backup_path.exists():
                logger.error(f"❌ 백업 파일이 존재하지 않음: {backup_path}")
                return False
            
            # 현재 데이터베이스 백업
            current_backup = f"{target_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            if target_path.exists():
                shutil.copy2(target_path, current_backup)
                logger.info(f"💾 기존 데이터베이스 백업: {current_backup}")
            
            # 백업 복원
            if backup_path.suffix == '.gz':
                with gzip.open(backup_path, 'rb') as f_in:
                    with open(target_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy2(backup_path, target_path)
            
            logger.info(f"✅ 백업 복원 완료: {backup_file} -> {target_path}")
            return True
            
        except Exception as e:
            logger.error(f"백업 복원 실패: {e}")
            return False
    
    def list_backups(self) -> List[Dict]:
        """백업 파일 목록 조회"""
        backups = []
        
        try:
            for backup_file in sorted(self.backup_dir.glob("*.db*"), key=lambda x: x.stat().st_mtime, reverse=True):
                stat = backup_file.stat()
                backups.append({
                    'filename': backup_file.name,
                    'path': str(backup_file),
                    'size_mb': round(stat.st_size / (1024 * 1024), 2),
                    'created': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                    'database': backup_file.name.split('_')[0],
                    'type': backup_file.name.split('_')[1] if '_' in backup_file.name else 'unknown'
                })
        except Exception as e:
            logger.error(f"백업 목록 조회 실패: {e}")
        
        return backups

class AlertManager:
    """알림 관리 클래스"""
    
    def __init__(self):
        # 환경변수에서 이메일 설정 가져오기
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.email_user = os.getenv('EMAIL_USER')
        self.email_password = os.getenv('EMAIL_PASSWORD')
        self.alert_recipients = [email.strip() for email in os.getenv('ALERT_RECIPIENTS', '').split(',') if email.strip()]
        
        # 이메일 설정 검증
        self.email_enabled = bool(self.email_user and self.email_password and self.alert_recipients)
        
        if not self.email_enabled:
            logger.warning("⚠️ 이메일 설정이 불완전합니다. 알림 기능이 제한됩니다.")
    
    def send_health_alert(self, health_report: Dict):
        """헬스 체크 알림 전송"""
        try:
            if health_report['system_status'] in ['warning', 'critical', 'error']:
                status_icons = {
                    'warning': '⚠️',
                    'critical': '🚨',
                    'error': '❌'
                }
                
                icon = status_icons.get(health_report['system_status'], '⚠️')
                subject = f"{icon} 워런 버핏 시스템 알림: {health_report['system_status'].upper()}"
                
                body = self._format_health_report_email(health_report)
                
                if self._send_email(subject, body):
                    logger.info(f"📧 헬스 알림 전송 완료: {health_report['system_status']}")
                else:
                    logger.error("📧 헬스 알림 전송 실패")
                    
        except Exception as e:
            logger.error(f"헬스 알림 생성 실패: {e}")
    
    def send_backup_notification(self, backup_results: Dict):
        """백업 완료 알림"""
        try:
            if backup_results and 'error' not in backup_results:
                successful_backups = [db for db, result in backup_results.items() if not result.startswith('error')]
                failed_backups = [db for db, result in backup_results.items() if result.startswith('error')]
                
                if successful_backups:
                    subject = f"✅ 워런 버핏 시스템 백업 완료 ({len(successful_backups)}/{len(backup_results)})"
                    
                    body = f"""
🎯 워런 버핏 투자 시스템 백업 리포트

🕐 백업 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 성공/전체: {len(successful_backups)}/{len(backup_results)}

✅ 성공한 백업:
"""
                    
                    for db_name in successful_backups:
                        backup_path = backup_results[db_name]
                        body += f"  • {db_name}: {Path(backup_path).name}\n"
                    
                    if failed_backups:
                        body += f"\n❌ 실패한 백업:\n"
                        for db_name in failed_backups:
                            body += f"  • {db_name}: {backup_results[db_name]}\n"
                    
                    body += f"\n💾 백업 위치: {Path('backups').absolute()}"
                    
                    if self._send_email(subject, body):
                        logger.info("📧 백업 알림 전송 완료")
            else:
                # 백업 실패 알림
                subject = "❌ 워런 버핏 시스템 백업 실패"
                body = f"""
백업 작업이 실패했습니다.

🕐 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
❌ 오류: {backup_results.get('error', '알 수 없는 오류')}

📞 시스템 관리자에게 문의해주세요.
"""
                self._send_email(subject, body)
                
        except Exception as e:
            logger.error(f"백업 알림 생성 실패: {e}")
    
    def send_daily_summary(self, health_report: Dict):
        """일일 요약 보고서 전송"""
        try:
            subject = f"📊 워런 버핏 시스템 일일 리포트 - {datetime.now().strftime('%Y-%m-%d')}"
            body = self._format_daily_summary_email(health_report)
            
            if self._send_email(subject, body):
                logger.info("📧 일일 요약 리포트 전송 완료")
                
        except Exception as e:
            logger.error(f"일일 요약 리포트 생성 실패: {e}")
    
    def _format_health_report_email(self, health_report: Dict) -> str:
        """헬스 리포트 이메일 포맷"""
        try:
            summary = health_report.get('summary', {})
            
            body = f"""
🎯 워런 버핏 투자 시스템 헬스 리포트

🕐 체크 시간: {health_report['timestamp']}
🚨 시스템 상태: {health_report['system_status'].upper()}
📊 시스템 건강도: {summary.get('health_score', 0)}/100

📈 시스템 요약:
• 총 상장기업: {summary.get('total_companies', 0):,}개
• 워런 버핏 스코어 적용: {summary.get('buffett_scored', 0):,}개
• 평균 버핏 점수: {summary.get('avg_buffett_score', 0):.1f}/110점
• 최신 주가 날짜: {summary.get('latest_stock_date', 'N/A')}
• 주가 데이터 종목 수: {summary.get('stock_coverage', 0):,}개

"""
            
            if health_report.get('issues'):
                body += "🚨 발견된 문제점:\n"
                for issue in health_report['issues']:
                    body += f"  {issue}\n"
                body += "\n"
            
            if health_report.get('recommendations'):
                body += "💡 권장 조치사항:\n"
                for rec in health_report['recommendations']:
                    body += f"  {rec}\n"
                body += "\n"
            
            # 데이터베이스 크기 정보
            if 'database_sizes' in health_report:
                body += "💾 데이터베이스 크기:\n"
                for db_name, size in health_report['database_sizes'].items():
                    if isinstance(size, (int, float)):
                        body += f"  • {db_name}: {size:.1f} MB\n"
                    else:
                        body += f"  • {db_name}: {size}\n"
                body += "\n"
            
            body += "📞 문제가 지속되면 시스템 관리자에게 문의해주세요."
            
            return body
            
        except Exception as e:
            logger.error(f"헬스 리포트 이메일 포맷 실패: {e}")
            return f"헬스 리포트 포맷 오류: {str(e)}"
    
    def _format_daily_summary_email(self, health_report: Dict) -> str:
        """일일 요약 이메일 포맷"""
        try:
            summary = health_report.get('summary', {})
            
            return f"""
📊 워런 버핏 투자 시스템 일일 요약

📅 날짜: {datetime.now().strftime('%Y년 %m월 %d일')}
🏥 시스템 상태: {health_report['system_status'].upper()}

🎯 오늘의 주요 지표:
• 📈 분석된 종목: {summary.get('stock_coverage', 0):,}개
• 🏆 워런 버핏 스코어 평균: {summary.get('avg_buffett_score', 0):.1f}/110점
• 📊 시스템 건강도: {summary.get('health_score', 0)}/100점
• 🔍 발견된 이슈: {summary.get('issue_count', 0)}개

💼 투자 분석 현황:
• 기본분석 (45%): 재무제표 기반 워런 버핏 스코어카드
• 기술분석 (30%): 차트 패턴 및 기술적 지표 
• 감정분석 (25%): 뉴스 및 시장 심리 분석

📈 시스템이 정상 작동 중이며, 50-60대 투자자를 위한 
   맞춤형 분석 서비스를 제공하고 있습니다.

🌐 웹 대시보드에서 상세 분석 결과를 확인하세요.
"""
            
        except Exception as e:
            logger.error(f"일일 요약 이메일 포맷 실패: {e}")
            return f"일일 요약 포맷 오류: {str(e)}"
    
    def _send_email(self, subject: str, body: str, attachments: List[str] = None) -> bool:
        """이메일 전송"""
        if not self.email_enabled:
            logger.warning("📧 이메일 설정이 없어 알림을 전송할 수 없습니다.")
            return False
        
        try:
            msg = MimeMultipart()
            msg['From'] = self.email_user
            msg['To'] = ', '.join(self.alert_recipients)
            msg['Subject'] = subject
            
            msg.attach(MimeText(body, 'plain', 'utf-8'))
            
            # 첨부파일 처리
            if attachments:
                for file_path in attachments:
                    if Path(file_path).exists():
                        with open(file_path, 'rb') as attachment:
                            part = MimeBase('application', 'octet-stream')
                            part.set_payload(attachment.read())
                        
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {Path(file_path).name}'
                        )
                        msg.attach(part)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_user, self.email_password)
                server.send_message(msg)
            
            logger.debug(f"📧 이메일 전송 완료: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 전송 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='워런 버핏 시스템 모니터링 및 백업')
    parser.add_argument('--action', choices=['monitor', 'backup', 'restore', 'cleanup', 'list'], 
                       default='monitor', help='실행할 작업')
    parser.add_argument('--backup-type', choices=['daily', 'weekly'], 
                       default='daily', help='백업 유형')
    parser.add_argument('--backup-file', help='복원할 백업 파일')
    parser.add_argument('--target-db', help='복원 대상 데이터베이스')
    parser.add_argument('--keep-days', type=int, default=30, help='백업 보관 일수')
    parser.add_argument('--send-email', action='store_true', help='이메일 알림 전송')
    parser.add_argument('--verbose', '-v', action='store_true', help='상세 로그 출력')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        if args.action == 'monitor':
            print("🔍 워런 버핏 시스템 모니터링을 시작합니다...")
            
            monitor = DataMonitor()
            health_report = monitor.generate_health_report()
            
            # 결과 출력
            print(f"\n📊 시스템 상태: {health_report['system_status']}")
            print(f"📅 체크 시간: {health_report['timestamp']}")
            
            if 'summary' in health_report:
                summary = health_report['summary']
                print(f"\n💼 시스템 요약:")
                print(f"  • 📈 총 상장기업: {summary.get('total_companies', 0):,}개")
                print(f"  • 🏆 워런 버핏 스코어 적용: {summary.get('buffett_scored', 0):,}개")
                print(f"  • 📊 평균 버핏 점수: {summary.get('avg_buffett_score', 0):.1f}/110점")
                print(f"  • 🎯 시스템 건강도: {summary.get('health_score', 0)}/100점")
            
            if health_report.get('issues'):
                print(f"\n⚠️ 발견된 문제 ({len(health_report['issues'])}개):")
                for issue in health_report['issues']:
                    print(f"  {issue}")
            
            if health_report.get('recommendations'):
                print(f"\n💡 권장 조치사항 ({len(health_report['recommendations'])}개):")
                for rec in health_report['recommendations']:
                    print(f"  {rec}")
            
            # 이메일 알림 전송
            if args.send_email:
                alert_manager = AlertManager()
                alert_manager.send_health_alert(health_report)
                if health_report['system_status'] in ['excellent', 'good']:
                    alert_manager.send_daily_summary(health_report)
            
        elif args.action == 'backup':
            print(f"💾 {args.backup_type} 백업을 시작합니다...")
            
            backup_manager = DataBackupManager()
            backup_results = backup_manager.create_backup(args.backup_type)
            
            if 'error' not in backup_results:
                successful = [db for db, result in backup_results.items() if not result.startswith('error')]
                failed = [db for db, result in backup_results.items() if result.startswith('error')]
                
                print(f"✅ 백업 완료: {len(successful)}/{len(backup_results)}개 성공")
                for db_name in successful:
                    print(f"  • {db_name}: {Path(backup_results[db_name]).name}")
                
                if failed:
                    print(f"\n❌ 백업 실패:")
                    for db_name in failed:
                        print(f"  • {db_name}: {backup_results[db_name]}")
                
                # 이메일 알림 전송
                if args.send_email:
                    alert_manager = AlertManager()
                    alert_manager.send_backup_notification(backup_results)
            else:
                print(f"❌ 백업 실패: {backup_results['error']}")
        
        elif args.action == 'restore':
            if not args.backup_file or not args.target_db:
                print("❌ 복원에는 --backup-file과 --target-db가 필요합니다.")
                return False
            
            print(f"🔄 백업 복원: {args.backup_file} -> {args.target_db}")
            
            backup_manager = DataBackupManager()
            success = backup_manager.restore_backup(args.backup_file, args.target_db)
            
            if success:
                print("✅ 복원 완료!")
            else:
                print("❌ 복원 실패!")
        
        elif args.action == 'cleanup':
            print(f"🧹 {args.keep_days}일 이상된 백업을 정리합니다...")
            
            backup_manager = DataBackupManager()
            deleted_count = backup_manager.cleanup_old_backups(args.keep_days)
            
            print(f"✅ {deleted_count}개 백업 파일을 삭제했습니다.")
            
        elif args.action == 'list':
            print("📋 백업 파일 목록:")
            
            backup_manager = DataBackupManager()
            backups = backup_manager.list_backups()
            
            if backups:
                print(f"\n{'파일명':<40} {'데이터베이스':<10} {'크기(MB)':<10} {'생성일시':<20}")
                print("-" * 85)
                for backup in backups:
                    print(f"{backup['filename']:<40} {backup['database']:<10} {backup['size_mb']:<10} {backup['created']:<20}")
            else:
                print("백업 파일이 없습니다.")
        
        return True
        
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 중단되었습니다.")
        return True
    except Exception as e:
        logger.error(f"실행 실패: {e}")
        print(f"❌ 실행 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)