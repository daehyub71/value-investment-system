#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
증분 뉴스 수집 스크립트
마지막 수집일 이후의 뉴스만 효율적으로 수집

실행 방법:
python incremental_news_collector.py --from_date=2025-07-09 --to_date=2025-07-13
python incremental_news_collector.py --auto  # 자동으로 마지막 수집일 이후 수집
python incremental_news_collector.py --stock_code=000660 --from_date=2025-07-09
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

class IncrementalNewsCollector:
    """증분 뉴스 수집 클래스"""
    
    def __init__(self):
        # 네이버 API 설정
        self.naver_client_id = os.getenv('NAVER_CLIENT_ID')
        self.naver_client_secret = os.getenv('NAVER_CLIENT_SECRET')
        
        if not self.naver_client_id or not self.naver_client_secret:
            raise ValueError("네이버 API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
        
        self.base_url = "https://openapi.naver.com/v1/search/news.json"
        self.db_path = Path('data/databases/news_data.db')
        
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # 금융 관련 키워드
        self.financial_keywords = [
            '실적', '매출', '영업이익', '순이익', '배당', '투자', '인수합병', 'M&A',
            '신제품', '출시', '계약', '수주', '특허', '기술개발', '연구개발',
            '증설', '투자', '공장', '시설', '생산', '공급',
            '주가', '상승', '하락', '목표가', '투자의견', '매수', '매도',
            '분할', '합병', '유상증자', '무상증자', '자사주',
            'CEO', '대표이사', '임원', '인사', '조직개편'
        ]
        
        # 감정분석 키워드
        self.positive_words = [
            '성장', '증가', '상승', '개선', '확대', '호조', '좋은', '긍정', '성공',
            '달성', '돌파', '최고', '우수', '강세', '기대', '전망', '혁신'
        ]
        
        self.negative_words = [
            '감소', '하락', '부진', '악화', '축소', '우려', '나쁜', '부정', '실패',
            '부족', '손실', '적자', '최저', '부실', '불안', '위험', '하향'
        ]
    
    def get_last_collection_date(self, stock_code=None):
        """마지막 뉴스 수집 날짜 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if stock_code:
                    # 특정 종목의 마지막 수집일
                    query = """
                        SELECT MAX(date(created_at)) 
                        FROM news_articles 
                        WHERE stock_code = ?
                    """
                    result = conn.execute(query, (stock_code,)).fetchone()
                else:
                    # 전체 뉴스의 마지막 수집일
                    query = """
                        SELECT MAX(date(created_at)) 
                        FROM news_articles
                    """
                    result = conn.execute(query).fetchone()
                
                if result and result[0]:
                    return result[0]
                else:
                    # 데이터가 없으면 7일 전부터
                    return (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                    
        except Exception as e:
            self.logger.error(f"마지막 수집일 조회 실패: {e}")
            return (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    def get_company_list(self, limit=50):
        """회사 목록 조회"""
        try:
            stock_db_path = Path('data/databases/stock_data.db')
            if not stock_db_path.exists():
                self.logger.error("주식 데이터베이스를 찾을 수 없습니다.")
                return []
                
            with sqlite3.connect(stock_db_path) as conn:
                cursor = conn.execute(f"""
                    SELECT stock_code, company_name, market_cap
                    FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                    ORDER BY market_cap DESC 
                    LIMIT {limit}
                """)
                return cursor.fetchall()
                
        except Exception as e:
            self.logger.error(f"회사 목록 조회 실패: {e}")
            return []
    
    def search_news_by_date(self, keyword, start_date, end_date, max_results=100):
        """날짜 범위로 뉴스 검색"""
        try:
            headers = {
                'X-Naver-Client-Id': self.naver_client_id,
                'X-Naver-Client-Secret': self.naver_client_secret
            }
            
            all_news = []
            
            # 네이버 뉴스 API는 날짜 필터가 제한적이므로 전체 검색 후 필터링
            for page in range(1, 6):  # 최대 5페이지 (500개 뉴스)
                params = {
                    'query': keyword,
                    'display': 100,
                    'start': ((page - 1) * 100) + 1,
                    'sort': 'date'
                }
                
                response = requests.get(self.base_url, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                news_items = data.get('items', [])
                
                if not news_items:
                    break
                
                # 날짜 필터링
                filtered_items = self.filter_by_date_range(news_items, start_date, end_date)
                all_news.extend(filtered_items)
                
                # 충분한 뉴스를 수집했으면 중단
                if len(all_news) >= max_results:
                    break
                
                # API 호출 제한 대응
                time.sleep(0.1)
            
            # 중복 제거
            unique_news = []
            seen_links = set()
            
            for news in all_news:
                link = news.get('link', '')
                if link not in seen_links:
                    unique_news.append(news)
                    seen_links.add(link)
            
            self.logger.info(f"뉴스 검색 완료 (키워드: {keyword}): {len(unique_news)}건")
            return unique_news[:max_results]
            
        except Exception as e:
            self.logger.error(f"뉴스 검색 실패 (키워드: {keyword}): {e}")
            return []
    
    def filter_by_date_range(self, news_items, start_date, end_date):
        """날짜 범위로 뉴스 필터링"""
        filtered_news = []
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)  # 포함
        
        for item in news_items:
            pub_date_str = item.get('pubDate', '')
            if not pub_date_str:
                continue
            
            try:
                # RFC 2822 형식 파싱: Wed, 25 Jun 2025 18:16:00 +0900
                if '+' in pub_date_str:
                    pub_date_str = pub_date_str.split(' +')[0]
                
                pub_date = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S')
                
                if start_dt <= pub_date < end_dt:
                    filtered_news.append(item)
                    
            except Exception as e:
                # 날짜 파싱 실패시 최신 뉴스로 간주
                filtered_news.append(item)
                continue
        
        return filtered_news
    
    def filter_financial_news(self, news_items, company_name=None):
        """금융 관련 뉴스 필터링"""
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
            if total_score >= 1:
                item['relevance_score'] = total_score
                filtered_news.append(item)
        
        return filtered_news
    
    def analyze_sentiment(self, text):
        """감정분석"""
        clean_text = re.sub(r'<[^>]+>', '', text).lower()
        
        positive_count = sum(1 for word in self.positive_words if word in clean_text)
        negative_count = sum(1 for word in self.negative_words if word in clean_text)
        
        total_words = positive_count + negative_count
        if total_words == 0:
            sentiment_score = 0.0
            confidence = 0.1
        else:
            sentiment_score = (positive_count - negative_count) / total_words
            confidence = min(total_words / 10, 1.0)
        
        # 감정 라벨
        if sentiment_score > 0.1:
            sentiment_label = 'positive'
        elif sentiment_score < -0.1:
            sentiment_label = 'negative'
        else:
            sentiment_label = 'neutral'
        
        return {
            'sentiment_score': sentiment_score,
            'sentiment_label': sentiment_label,
            'confidence': confidence,
            'keywords': f"긍정:{positive_count},부정:{negative_count}"
        }
    
    def save_news_to_db(self, news_items, stock_code, company_name):
        """뉴스를 데이터베이스에 저장"""
        try:
            saved_count = 0
            
            with sqlite3.connect(self.db_path) as conn:
                for item in news_items:
                    # HTML 태그 제거
                    title = re.sub(r'<[^>]+>', '', item.get('title', ''))
                    description = re.sub(r'<[^>]+>', '', item.get('description', ''))
                    
                    # 감정분석
                    sentiment = self.analyze_sentiment(f"{title} {description}")
                    
                    try:
                        # 중복 확인 (링크 기준)
                        existing = conn.execute(
                            "SELECT id FROM news_articles WHERE link = ?",
                            (item.get('link', ''),)
                        ).fetchone()
                        
                        if existing:
                            continue  # 이미 존재하는 뉴스
                        
                        # 새 뉴스 저장
                        conn.execute('''
                            INSERT INTO news_articles 
                            (stock_code, title, description, originallink, link, pubDate, 
                             source, category, sentiment_score, sentiment_label, confidence_score, keywords, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            stock_code,
                            title,
                            description,
                            item.get('originallink', ''),
                            item.get('link', ''),
                            item.get('pubDate', ''),
                            '네이버뉴스',
                            '금융',
                            sentiment['sentiment_score'],
                            sentiment['sentiment_label'],
                            sentiment['confidence'],
                            sentiment['keywords'],
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        ))
                        
                        saved_count += 1
                        
                    except sqlite3.Error as e:
                        self.logger.warning(f"뉴스 저장 실패: {e}")
                        continue
                
                conn.commit()
            
            return saved_count
            
        except Exception as e:
            self.logger.error(f"데이터베이스 저장 실패: {e}")
            return 0
    
    def collect_incremental_news(self, from_date, to_date, stock_code=None, limit=50):
        """증분 뉴스 수집"""
        self.logger.info(f"증분 뉴스 수집 시작: {from_date} ~ {to_date}")
        
        if stock_code:
            # 특정 종목만 수집
            company_list = [(stock_code, self.get_company_name(stock_code), 0)]
        else:
            # 주요 종목들 수집
            company_list = self.get_company_list(limit)
        
        if not company_list:
            self.logger.error("수집할 종목이 없습니다.")
            return False
        
        total_saved = 0
        success_count = 0
        
        for idx, (stock_code, company_name, market_cap) in enumerate(company_list):
            self.logger.info(f"진행률: {idx+1}/{len(company_list)} - {company_name}({stock_code})")
            
            try:
                # 해당 기간의 뉴스 검색
                news_items = self.search_news_by_date(company_name, from_date, to_date)
                
                if not news_items:
                    self.logger.info(f"새 뉴스 없음: {company_name}")
                    continue
                
                # 금융 뉴스 필터링
                filtered_news = self.filter_financial_news(news_items, company_name)
                
                if not filtered_news:
                    self.logger.info(f"금융 뉴스 없음: {company_name}")
                    continue
                
                # 데이터베이스 저장
                saved_count = self.save_news_to_db(filtered_news, stock_code, company_name)
                
                if saved_count > 0:
                    total_saved += saved_count
                    success_count += 1
                    self.logger.info(f"저장 완료: {company_name} - {saved_count}건")
                
                # API 호출 제한 대응
                time.sleep(0.2)
                
            except Exception as e:
                self.logger.error(f"수집 실패: {company_name} - {e}")
                continue
        
        self.logger.info(f"증분 뉴스 수집 완료: {success_count}/{len(company_list)} 종목, 총 {total_saved}건 저장")
        return total_saved > 0
    
    def get_company_name(self, stock_code):
        """종목코드로 회사명 조회"""
        try:
            stock_db_path = Path('data/databases/stock_data.db')
            if stock_db_path.exists():
                with sqlite3.connect(stock_db_path) as conn:
                    result = conn.execute(
                        "SELECT company_name FROM company_info WHERE stock_code = ?",
                        (stock_code,)
                    ).fetchone()
                    
                    if result:
                        return result[0]
            
            return f"종목{stock_code}"
            
        except Exception as e:
            self.logger.error(f"회사명 조회 실패: {e}")
            return f"종목{stock_code}"

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='증분 뉴스 수집 스크립트')
    parser.add_argument('--from_date', type=str, help='시작 날짜 (YYYY-MM-DD)')
    parser.add_argument('--to_date', type=str, help='종료 날짜 (YYYY-MM-DD)')
    parser.add_argument('--stock_code', type=str, help='특정 종목코드')
    parser.add_argument('--auto', action='store_true', help='자동으로 마지막 수집일 이후 수집')
    parser.add_argument('--limit', type=int, default=50, help='수집할 종목 수')
    
    args = parser.parse_args()
    
    try:
        collector = IncrementalNewsCollector()
        
        if args.auto:
            # 자동 모드: 마지막 수집일 이후부터 오늘까지
            last_date = collector.get_last_collection_date(args.stock_code)
            today = datetime.now().strftime('%Y-%m-%d')
            
            print(f"🔍 자동 증분 수집")
            print(f"📅 기간: {last_date} ~ {today}")
            
            if collector.collect_incremental_news(last_date, today, args.stock_code, args.limit):
                print("✅ 증분 뉴스 수집 성공!")
            else:
                print("❌ 증분 뉴스 수집 실패")
                
        elif args.from_date and args.to_date:
            # 수동 모드: 지정된 날짜 범위
            print(f"🔍 수동 증분 수집")
            print(f"📅 기간: {args.from_date} ~ {args.to_date}")
            
            if collector.collect_incremental_news(args.from_date, args.to_date, args.stock_code, args.limit):
                print("✅ 증분 뉴스 수집 성공!")
            else:
                print("❌ 증분 뉴스 수집 실패")
        else:
            parser.print_help()
            
    except ValueError as e:
        print(f"❌ 초기화 실패: {e}")
        print("💡 .env 파일에서 네이버 API 키를 확인하세요.")
    except KeyboardInterrupt:
        print("\n⏹️ 사용자에 의해 중단됨")
    except Exception as e:
        print(f"❌ 예기치 못한 오류: {e}")

if __name__ == "__main__":
    main()
