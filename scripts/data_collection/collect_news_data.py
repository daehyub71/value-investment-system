#!/usr/bin/env python3
"""
뉴스 데이터 수집 스크립트
네이버 뉴스 API를 활용한 금융 뉴스 데이터 수집 및 감정분석

실행 방법:
python scripts/data_collection/collect_news_data.py --stock_code=005930 --days=30
python scripts/data_collection/collect_news_data.py --keyword="삼성전자" --days=7
python scripts/data_collection/collect_news_data.py --all_stocks --days=3
"""

import sys
import os
import argparse
import sqlite3
import pandas as pd
import requests
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
import logging
import time
from urllib.parse import quote

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.database_config import DatabaseConfig
from config.api_config import APIConfig
from config.logging_config import setup_logging

class NewsDataCollector:
    """뉴스 데이터 수집 클래스"""
    
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.api_config = APIConfig()
        self.naver_client_id = self.api_config.naver_client_id
        self.naver_client_secret = self.api_config.naver_client_secret
        self.base_url = "https://openapi.naver.com/v1/search/news.json"
        self.logger = logging.getLogger(__name__)
        
        if not self.naver_client_id or not self.naver_client_secret:
            raise ValueError("네이버 API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
        
        # 금융 관련 키워드 사전
        self.financial_keywords = [
            '실적', '매출', '영업이익', '순이익', '배당', '투자', '인수합병', 'M&A',
            '신제품', '출시', '계약', '수주', '특허', '기술개발', '연구개발',
            '증설', '투자', '공장', '시설', '생산', '공급',
            '주가', '상승', '하락', '목표가', '투자의견', '매수', '매도',
            '분할', '합병', '유상증자', '무상증자', '자사주',
            'CEO', '대표이사', '임원', '인사', '조직개편'
        ]
        
        # 감정분석 사전 (간단한 버전)
        self.positive_words = [
            '성장', '증가', '상승', '개선', '확대', '호조', '좋은', '긍정', '성공',
            '달성', '돌파', '최고', '우수', '강세', '기대', '전망', '혁신'
        ]
        
        self.negative_words = [
            '감소', '하락', '부진', '악화', '축소', '우려', '나쁜', '부정', '실패',
            '부족', '손실', '적자', '최저', '부실', '불안', '위험', '하향'
        ]
    
    def get_company_name_by_stock_code(self, stock_code):
        """주식코드로 회사명 조회"""
        try:
            # stock DB에서 회사명 조회
            with self.db_config.get_connection('stock') as conn:
                result = conn.execute(
                    "SELECT company_name FROM company_info WHERE stock_code = ?",
                    (stock_code,)
                ).fetchone()
                
                if result:
                    return result[0]
            
            # dart DB에서도 조회 시도
            with self.db_config.get_connection('dart') as conn:
                result = conn.execute(
                    "SELECT corp_name FROM corp_codes WHERE stock_code = ?",
                    (stock_code,)
                ).fetchone()
                
                if result:
                    return result[0]
            
            return None
            
        except Exception as e:
            self.logger.error(f"회사명 조회 실패 ({stock_code}): {e}")
            return None
    
    def search_news(self, keyword, start_date=None, sort='date', display=100):
        """네이버 뉴스 검색"""
        try:
            headers = {
                'X-Naver-Client-Id': self.naver_client_id,
                'X-Naver-Client-Secret': self.naver_client_secret
            }
            
            params = {
                'query': keyword,
                'display': display,  # 최대 100개
                'start': 1,
                'sort': sort  # date: 최신순, sim: 정확도순
            }
            
            response = requests.get(self.base_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            news_items = data.get('items', [])
            
            self.logger.info(f"뉴스 검색 완료 (키워드: {keyword}): {len(news_items)}건")
            return news_items
            
        except Exception as e:
            self.logger.error(f"뉴스 검색 실패 (키워드: {keyword}): {e}")
            return []
    
    def filter_financial_news(self, news_items, company_name=None):
        """금융/펀더멘털 관련 뉴스 필터링"""
        filtered_news = []
        
        for item in news_items:
            title = item.get('title', '').lower()
            description = item.get('description', '').lower()
            content = f"{title} {description}"
            
            # HTML 태그 제거
            content = re.sub(r'<[^>]+>', '', content)
            
            # 금융 키워드 포함 여부 확인
            financial_score = 0
            for keyword in self.financial_keywords:
                if keyword.lower() in content:
                    financial_score += 1
            
            # 회사명 포함 여부 확인
            company_score = 0
            if company_name:
                if company_name.lower() in content:
                    company_score += 2
            
            # 총 점수가 일정 이상이면 포함
            total_score = financial_score + company_score
            if total_score >= 1:  # 최소 1개의 금융 키워드 또는 회사명 포함
                item['relevance_score'] = total_score
                filtered_news.append(item)
        
        self.logger.info(f"금융 뉴스 필터링 완료: {len(filtered_news)}/{len(news_items)}건 선택")
        return filtered_news
    
    def analyze_sentiment(self, text):
        """간단한 감정분석"""
        try:
            # HTML 태그 제거
            clean_text = re.sub(r'<[^>]+>', '', text).lower()
            
            # 긍정/부정 단어 카운트
            positive_count = sum(1 for word in self.positive_words if word in clean_text)
            negative_count = sum(1 for word in self.negative_words if word in clean_text)
            
            # 감정 점수 계산 (-1 ~ 1)
            total_words = positive_count + negative_count
            if total_words == 0:
                sentiment_score = 0.0  # 중립
                confidence = 0.1
            else:
                sentiment_score = (positive_count - negative_count) / total_words
                confidence = min(total_words / 10, 1.0)  # 키워드가 많을수록 신뢰도 높음
            
            return {
                'sentiment_score': sentiment_score,
                'positive_score': positive_count / max(total_words, 1),
                'negative_score': negative_count / max(total_words, 1),
                'neutral_score': 1.0 - abs(sentiment_score),
                'confidence': confidence,
                'keywords': f"긍정:{positive_count},부정:{negative_count}"
            }
            
        except Exception as e:
            self.logger.error(f"감정분석 실패: {e}")
            return {
                'sentiment_score': 0.0,
                'positive_score': 0.0,
                'negative_score': 0.0,
                'neutral_score': 1.0,
                'confidence': 0.0,
                'keywords': ''
            }
    
    def process_news_data(self, news_items, stock_code=None, company_name=None):
        """뉴스 데이터 처리"""
        news_data = []
        sentiment_data = []
        
        for item in news_items:
            # HTML 태그 제거
            title = re.sub(r'<[^>]+>', '', item.get('title', ''))
            description = re.sub(r'<[^>]+>', '', item.get('description', ''))
            
            # 뉴스 기사 데이터
            news_info = {
                'title': title,
                'originallink': item.get('originallink', ''),
                'link': item.get('link', ''),
                'description': description,
                'pubDate': item.get('pubDate', ''),
                'stock_code': stock_code,
                'company_name': company_name,
                'category': '금융',
                'source': '네이버뉴스',
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            news_data.append(news_info)
            
            # 감정분석
            sentiment_result = self.analyze_sentiment(f"{title} {description}")
            sentiment_info = {
                'news_id': None,  # DB 저장 후 업데이트
                'stock_code': stock_code,
                'sentiment_score': sentiment_result['sentiment_score'],
                'positive_score': sentiment_result['positive_score'],
                'negative_score': sentiment_result['negative_score'],
                'neutral_score': sentiment_result['neutral_score'],
                'confidence': sentiment_result['confidence'],
                'keywords': sentiment_result['keywords'],
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            sentiment_data.append(sentiment_info)
        
        return pd.DataFrame(news_data), pd.DataFrame(sentiment_data)
    
    def calculate_market_sentiment(self, stock_code, date):
        """시장 감정지수 계산"""
        try:
            with self.db_config.get_connection('news') as conn:
                # 해당 날짜의 뉴스 감정점수 조회
                query = """
                SELECT sentiment_score, confidence 
                FROM sentiment_scores 
                WHERE stock_code = ? AND date(created_at) = ?
                """
                
                df = pd.read_sql(query, conn, params=(stock_code, date))
                
                if df.empty:
                    return None
                
                # 신뢰도 가중 평균으로 일일 감정점수 계산
                weighted_sentiment = (df['sentiment_score'] * df['confidence']).sum() / df['confidence'].sum()
                
                # 주별, 월별 감정점수도 계산 (7일, 30일 이동평균)
                week_ago = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
                month_ago = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
                
                week_query = """
                SELECT sentiment_score, confidence 
                FROM sentiment_scores 
                WHERE stock_code = ? AND date(created_at) >= ? AND date(created_at) <= ?
                """
                
                week_df = pd.read_sql(week_query, conn, params=(stock_code, week_ago, date))
                month_df = pd.read_sql(week_query, conn, params=(stock_code, month_ago, date))
                
                weekly_sentiment = None
                monthly_sentiment = None
                
                if not week_df.empty:
                    weekly_sentiment = (week_df['sentiment_score'] * week_df['confidence']).sum() / week_df['confidence'].sum()
                
                if not month_df.empty:
                    monthly_sentiment = (month_df['sentiment_score'] * month_df['confidence']).sum() / month_df['confidence'].sum()
                
                return {
                    'date': date,
                    'stock_code': stock_code,
                    'daily_sentiment': weighted_sentiment,
                    'weekly_sentiment': weekly_sentiment,
                    'monthly_sentiment': monthly_sentiment,
                    'news_count': len(df),
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
        except Exception as e:
            self.logger.error(f"시장감정지수 계산 실패 ({stock_code}, {date}): {e}")
            return None
    
    def save_to_database(self, news_data=None, sentiment_data=None, market_sentiment_data=None):
        """데이터베이스에 저장"""
        try:
            with self.db_config.get_connection('news') as conn:
                # 뉴스 기사 저장
                if news_data is not None and not news_data.empty:
                    news_data.to_sql('news_articles', conn, if_exists='append', index=False, method='ignore')
                    self.logger.info(f"뉴스 기사 저장 완료: {len(news_data)}건")
                    
                    # 저장된 뉴스 ID 조회하여 감정분석 데이터에 연결
                    if sentiment_data is not None and not sentiment_data.empty:
                        for idx, row in news_data.iterrows():
                            news_id = conn.execute(
                                "SELECT id FROM news_articles WHERE link = ? ORDER BY id DESC LIMIT 1",
                                (row['link'],)
                            ).fetchone()
                            
                            if news_id and idx < len(sentiment_data):
                                sentiment_data.iloc[idx, sentiment_data.columns.get_loc('news_id')] = news_id[0]
                
                # 감정분석 데이터 저장
                if sentiment_data is not None and not sentiment_data.empty:
                    sentiment_data.to_sql('sentiment_scores', conn, if_exists='append', index=False)
                    self.logger.info(f"감정분석 데이터 저장 완료: {len(sentiment_data)}건")
                
                # 시장감정지수 저장
                if market_sentiment_data is not None:
                    conn.execute('''
                        INSERT OR REPLACE INTO market_sentiment
                        (date, stock_code, daily_sentiment, weekly_sentiment, monthly_sentiment, 
                         news_count, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        market_sentiment_data['date'],
                        market_sentiment_data['stock_code'],
                        market_sentiment_data['daily_sentiment'],
                        market_sentiment_data['weekly_sentiment'],
                        market_sentiment_data['monthly_sentiment'],
                        market_sentiment_data['news_count'],
                        market_sentiment_data['created_at'],
                        market_sentiment_data['updated_at']
                    ))
                    self.logger.info("시장감정지수 저장 완료")
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"데이터베이스 저장 실패: {e}")
            return False
    
    def collect_news_for_stock(self, stock_code, days=30):
        """특정 종목 뉴스 수집"""
        try:
            # 회사명 조회
            company_name = self.get_company_name_by_stock_code(stock_code)
            if not company_name:
                self.logger.warning(f"회사명을 찾을 수 없습니다: {stock_code}")
                return False
            
            self.logger.info(f"뉴스 수집 시작: {company_name}({stock_code})")
            
            # 회사명으로 뉴스 검색
            news_items = self.search_news(company_name)
            
            if not news_items:
                self.logger.warning(f"검색된 뉴스가 없습니다: {company_name}")
                return False
            
            # 금융 뉴스 필터링
            filtered_news = self.filter_financial_news(news_items, company_name)
            
            if not filtered_news:
                self.logger.warning(f"필터링된 금융 뉴스가 없습니다: {company_name}")
                return False
            
            # 뉴스 데이터 처리
            news_data, sentiment_data = self.process_news_data(filtered_news, stock_code, company_name)
            
            # 데이터베이스 저장
            success = self.save_to_database(news_data, sentiment_data)
            
            if success:
                # 시장감정지수 계산 및 저장
                today = datetime.now().strftime('%Y-%m-%d')
                market_sentiment = self.calculate_market_sentiment(stock_code, today)
                if market_sentiment:
                    self.save_to_database(market_sentiment_data=market_sentiment)
                
                self.logger.info(f"뉴스 수집 완료: {company_name}({stock_code})")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"뉴스 수집 실패 ({stock_code}): {e}")
            return False
    
    def collect_all_stocks_news(self, days=7, limit=50):
        """전체 종목 뉴스 수집"""
        try:
            # 주요 종목 리스트 조회 (시가총액 상위)
            with self.db_config.get_connection('stock') as conn:
                stock_df = pd.read_sql(f"""
                    SELECT stock_code, company_name, market_cap
                    FROM company_info 
                    WHERE market_cap IS NOT NULL 
                    ORDER BY market_cap DESC 
                    LIMIT {limit}
                """, conn)
            
            if stock_df.empty:
                self.logger.error("종목 리스트를 찾을 수 없습니다.")
                return False
            
            self.logger.info(f"전체 종목 뉴스 수집 시작: {len(stock_df)}개 종목")
            
            success_count = 0
            for idx, row in stock_df.iterrows():
                stock_code = row['stock_code']
                company_name = row['company_name']
                
                self.logger.info(f"진행률: {idx+1}/{len(stock_df)} - {company_name}({stock_code})")
                
                if self.collect_news_for_stock(stock_code, days):
                    success_count += 1
                
                # API 호출 제한 대응
                time.sleep(0.1)
            
            self.logger.info(f"전체 뉴스 수집 완료: {success_count}/{len(stock_df)} 성공")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"전체 뉴스 수집 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='뉴스 데이터 수집 스크립트')
    parser.add_argument('--stock_code', type=str, help='수집할 종목코드 (예: 005930)')
    parser.add_argument('--keyword', type=str, help='검색할 키워드')
    parser.add_argument('--all_stocks', action='store_true', help='전체 종목 뉴스 수집')
    parser.add_argument('--days', type=int, default=30, help='수집 기간 (일수)')
    parser.add_argument('--limit', type=int, default=50, help='전체 수집시 종목 수 제한')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 로깅 설정
    setup_logging(level=args.log_level)
    logger = logging.getLogger(__name__)
    
    # 수집기 초기화
    try:
        collector = NewsDataCollector()
    except ValueError as e:
        logger.error(f"초기화 실패: {e}")
        sys.exit(1)
    
    try:
        if args.stock_code:
            # 특정 종목 뉴스 수집
            if collector.collect_news_for_stock(args.stock_code, args.days):
                logger.info("✅ 뉴스 데이터 수집 성공")
            else:
                logger.error("❌ 뉴스 데이터 수집 실패")
                sys.exit(1)
                
        elif args.keyword:
            # 키워드 뉴스 검색
            news_items = collector.search_news(args.keyword)
            if news_items:
                news_data, sentiment_data = collector.process_news_data(news_items)
                if collector.save_to_database(news_data, sentiment_data):
                    logger.info("✅ 키워드 뉴스 수집 성공")
                else:
                    logger.error("❌ 키워드 뉴스 저장 실패")
                    sys.exit(1)
            else:
                logger.error("❌ 키워드 뉴스 검색 실패")
                sys.exit(1)
                
        elif args.all_stocks:
            # 전체 종목 뉴스 수집
            if collector.collect_all_stocks_news(args.days, args.limit):
                logger.info("✅ 전체 뉴스 데이터 수집 성공")
            else:
                logger.error("❌ 전체 뉴스 데이터 수집 실패")
                sys.exit(1)
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()