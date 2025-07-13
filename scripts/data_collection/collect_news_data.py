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
import requests
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
import logging
import time
from urllib.parse import quote
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from config import ConfigManager
except ImportError:
    print("⚠️  ConfigManager를 찾을 수 없습니다. 기본 설정으로 진행합니다.")
    ConfigManager = None

class NewsDataCollector:
    """뉴스 데이터 수집 클래스"""
    
    def __init__(self):
        # ConfigManager를 통한 통합 설정 관리
        if ConfigManager:
            self.config_manager = ConfigManager()
            self.logger = self.config_manager.get_logger('NewsDataCollector')
            
            # 네이버 API 설정 가져오기
            naver_config = self.config_manager.get_naver_news_config()
            self.naver_client_id = naver_config.get('client_id')
            self.naver_client_secret = naver_config.get('client_secret')
        else:
            # 기본 설정
            self.logger = logging.getLogger(__name__)
            self.naver_client_id = os.getenv('NAVER_CLIENT_ID')
            self.naver_client_secret = os.getenv('NAVER_CLIENT_SECRET')
        
        self.base_url = "https://openapi.naver.com/v1/search/news.json"
        self.db_path = Path('data/databases/news_data.db')
        
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
            stock_db_path = Path('data/databases/stock_data.db')
            if stock_db_path.exists():
                with sqlite3.connect(stock_db_path) as conn:
                    result = conn.execute(
                        "SELECT company_name FROM company_info WHERE stock_code = ?",
                        (stock_code,)
                    ).fetchone()
                    
                    if result:
                        return result[0]
            
            # dart DB에서도 조회 시도
            dart_db_path = Path('data/databases/dart_data.db')
            if dart_db_path.exists():
                with sqlite3.connect(dart_db_path) as conn:
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
        
        for item in news_items:
            # HTML 태그 제거
            title = re.sub(r'<[^>]+>', '', item.get('title', ''))
            description = re.sub(r'<[^>]+>', '', item.get('description', ''))
            
            # 감정분석 수행
            sentiment_result = self.analyze_sentiment(f"{title} {description}")
            
            # 감정 라벨 결정
            if sentiment_result['sentiment_score'] > 0.1:
                sentiment_label = 'positive'
            elif sentiment_result['sentiment_score'] < -0.1:
                sentiment_label = 'negative'
            else:
                sentiment_label = 'neutral'
            
            # 뉴스 기사 데이터 (감정분석 결과 포함)
            news_info = {
                'title': title,
                'description': description,
                'originallink': item.get('originallink', ''),
                'link': item.get('link', ''),
                'pubDate': item.get('pubDate', ''),
                'stock_code': stock_code,
                'category': '금융',
                'source': '네이버뉴스',
                'sentiment_score': sentiment_result['sentiment_score'],
                'sentiment_label': sentiment_label,
                'confidence_score': sentiment_result['confidence'],
                'keywords': sentiment_result['keywords']
            }
            news_data.append(news_info)
        
        return news_data, []  # sentiment_data는 비워두고 다른 곳에서 집계
    
    def calculate_market_sentiment(self, stock_code, date):
        """시장 감정지수 계산 - 새로운 스키마에 맞게 수정"""
        try:
            # 뉴스 DB 연결
            with sqlite3.connect(self.db_path) as conn:
                # 새로운 스키마에 맞게 수정된 쿼리
                # news_articles 테이블에서 해당 날짜의 뉴스 감정점수 조회
                query = """
                SELECT sentiment_score, confidence_score
                FROM news_articles 
                WHERE stock_code = ? AND date(created_at) = ?
                """
                
                cursor = conn.execute(query, (stock_code, date))
                results = cursor.fetchall()
                
                if not results:
                    # 뉴스가 없는 경우 기본값으로 저장
                    return {
                        'date': date,
                        'stock_code': stock_code,
                        'daily_sentiment': 0.0,
                        'weekly_sentiment': None,
                        'monthly_sentiment': None,
                        'total_news_count': 0,
                        'positive_news_count': 0,
                        'negative_news_count': 0,
                        'neutral_news_count': 0,
                        'fundamental_news_count': 0,
                        'fundamental_sentiment': 0.0,
                        'sentiment_momentum': 0.0,
                        'sentiment_volatility': 0.0,
                        'sentiment_final_score': 50.0,  # 중립 점수
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                
                # 감정 점수 통계 계산
                sentiment_scores = [row[0] for row in results]
                confidence_scores = [row[1] for row in results]
                
                # 신뢰도 가중 평균으로 일일 감정점수 계산
                total_weighted = sum(score * conf for score, conf in results)
                total_confidence = sum(confidence_scores)
                
                if total_confidence == 0:
                    weighted_sentiment = 0.0
                else:
                    weighted_sentiment = total_weighted / total_confidence
                
                # 뉴스 개수별 분류
                positive_count = sum(1 for score in sentiment_scores if score > 0.1)
                negative_count = sum(1 for score in sentiment_scores if score < -0.1)
                neutral_count = len(sentiment_scores) - positive_count - negative_count
                
                # 펀더멘털 뉴스 개수 (category가 '금융'인 뉴스)
                fundamental_query = """
                SELECT COUNT(*) FROM news_articles 
                WHERE stock_code = ? AND date(created_at) = ? AND category = '금융'
                """
                cursor = conn.execute(fundamental_query, (stock_code, date))
                fundamental_count = cursor.fetchone()[0]
                
                # 감정 점수를 0-100 스케일로 변환
                sentiment_final_score = (weighted_sentiment + 1) * 50  # -1~1 → 0~100
                
                return {
                    'date': date,
                    'stock_code': stock_code,
                    'daily_sentiment': weighted_sentiment,
                    'weekly_sentiment': None,  # 향후 구현
                    'monthly_sentiment': None,  # 향후 구현
                    'total_news_count': len(results),
                    'positive_news_count': positive_count,
                    'negative_news_count': negative_count,
                    'neutral_news_count': neutral_count,
                    'fundamental_news_count': fundamental_count,
                    'fundamental_sentiment': weighted_sentiment,  # 단순화
                    'sentiment_momentum': 0.0,  # 향후 구현
                    'sentiment_volatility': 0.0,  # 향후 구현
                    'sentiment_final_score': sentiment_final_score,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
        except Exception as e:
            self.logger.error(f"시장감정지수 계산 실패 ({stock_code}, {date}): {e}")
            return None
    
    def save_to_database(self, news_data=None, sentiment_data=None, market_sentiment_data=None):
        """데이터베이스에 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 뉴스 기사 저장
                if news_data:
                    for news_item in news_data:
                        try:
                            conn.execute('''
                                INSERT OR IGNORE INTO news_articles 
                                (stock_code, title, description, originallink, link, pubDate, 
                                 source, category, sentiment_score, sentiment_label, confidence_score, keywords)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                news_item['stock_code'],
                                news_item['title'],
                                news_item['description'],
                                news_item['originallink'],
                                news_item['link'],
                                news_item['pubDate'],
                                news_item['source'],
                                news_item['category'],
                                news_item.get('sentiment_score', 0.0),
                                news_item.get('sentiment_label', 'neutral'),
                                news_item.get('confidence_score', 0.0),
                                news_item.get('keywords', '')
                            ))
                        except sqlite3.Error as e:
                            self.logger.warning(f"뉴스 저장 실패: {e}")
                            continue
                    
                    self.logger.info(f"뉴스 기사 저장 완료: {len(news_data)}건")
                
                # 감정분석 데이터 저장
                if sentiment_data:
                    for sentiment_item in sentiment_data:
                        try:
                            conn.execute('''
                                INSERT INTO sentiment_scores 
                                (stock_code, score, confidence, keywords, created_at)
                                VALUES (?, ?, ?, ?, ?)
                            ''', (
                                sentiment_item['stock_code'],
                                sentiment_item['sentiment_score'],
                                sentiment_item['confidence'],
                                sentiment_item['keywords'],
                                sentiment_item['created_at']
                            ))
                        except sqlite3.Error as e:
                            self.logger.warning(f"감정분석 데이터 저장 실패: {e}")
                            continue
                    
                    self.logger.info(f"감정분석 데이터 저장 완료: {len(sentiment_data)}건")
                
                # 시장감정지수 저장 (새로운 스키마)
                if market_sentiment_data:
                    try:
                        conn.execute('''
                            INSERT OR REPLACE INTO sentiment_scores
                            (stock_code, date, daily_sentiment, weekly_sentiment, monthly_sentiment, 
                             total_news_count, positive_news_count, negative_news_count, neutral_news_count,
                             fundamental_news_count, fundamental_sentiment, sentiment_momentum, 
                             sentiment_volatility, sentiment_final_score, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            market_sentiment_data['stock_code'],
                            market_sentiment_data['date'],
                            market_sentiment_data['daily_sentiment'],
                            market_sentiment_data['weekly_sentiment'],
                            market_sentiment_data['monthly_sentiment'],
                            market_sentiment_data['total_news_count'],
                            market_sentiment_data['positive_news_count'],
                            market_sentiment_data['negative_news_count'],
                            market_sentiment_data['neutral_news_count'],
                            market_sentiment_data['fundamental_news_count'],
                            market_sentiment_data['fundamental_sentiment'],
                            market_sentiment_data['sentiment_momentum'],
                            market_sentiment_data['sentiment_volatility'],
                            market_sentiment_data['sentiment_final_score'],
                            market_sentiment_data['created_at'],
                            market_sentiment_data['updated_at']
                        ))
                        self.logger.info("시장감정지수 저장 완료")
                    except sqlite3.Error as e:
                        self.logger.warning(f"시장감정지수 저장 실패: {e}")
                
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
            stock_db_path = Path('data/databases/stock_data.db')
            if not stock_db_path.exists():
                self.logger.error("주식 데이터베이스를 찾을 수 없습니다.")
                return False
                
            with sqlite3.connect(stock_db_path) as conn:
                cursor = conn.execute(f"""
                    SELECT stock_code, company_name, market_cap
                    FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                    ORDER BY market_cap DESC 
                    LIMIT {limit}
                """)
                stock_list = cursor.fetchall()
            
            if not stock_list:
                self.logger.error("종목 리스트를 찾을 수 없습니다.")
                return False
            
            self.logger.info(f"전체 종목 뉴스 수집 시작: {len(stock_list)}개 종목")
            
            success_count = 0
            for idx, (stock_code, company_name, market_cap) in enumerate(stock_list):
                self.logger.info(f"진행률: {idx+1}/{len(stock_list)} - {company_name}({stock_code})")
                
                if self.collect_news_for_stock(stock_code, days):
                    success_count += 1
                
                # API 호출 제한 대응
                time.sleep(0.1)
            
            self.logger.info(f"전체 뉴스 수집 완료: {success_count}/{len(stock_list)} 성공")
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
    
    # 기본 로깅 설정
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
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