# =============================================================================
# 4. scripts/data_collection/collect_news_data.py
# =============================================================================

#!/usr/bin/env python3
"""
뉴스 데이터 수집 스크립트
실행: python scripts/data_collection/collect_news_data.py
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import requests
import pandas as pd
import sqlite3
import time
import urllib.parse
from datetime import datetime, timedelta
import logging
from config import get_naver_news_config, get_db_connection

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NewsCollector:
    def __init__(self):
        self.naver_config = get_naver_news_config()
        self.client_id = self.naver_config['client_id']
        self.client_secret = self.naver_config['client_secret']
        self.base_url = "https://openapi.naver.com/v1/search/news.json"
        
        if not self.client_id or not self.client_secret:
            raise ValueError("네이버 뉴스 API 키가 설정되지 않았습니다. .env 파일을 확인해주세요.")
    
    def collect_news_data(self, keyword, display=100, start=1, sort='date'):
        """뉴스 데이터 수집"""
        try:
            encoded_keyword = urllib.parse.quote(keyword)
            
            params = {
                'query': encoded_keyword,
                'display': display,
                'start': start,
                'sort': sort
            }
            
            headers = {
                'X-Naver-Client-Id': self.client_id,
                'X-Naver-Client-Secret': self.client_secret
            }
            
            response = requests.get(self.base_url, params=params, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            if 'items' not in data:
                logger.warning(f"뉴스 데이터 없음: {keyword}")
                return pd.DataFrame()
            
            news_data = []
            for item in data['items']:
                news_data.append({
                    'stock_code': self._extract_stock_code(keyword),
                    'title': self._clean_html(item['title']),
                    'description': self._clean_html(item['description']),
                    'originallink': item['originallink'],
                    'link': item['link'],
                    'pubDate': self._parse_date(item['pubDate']),
                    'source': self._extract_source(item['link']),
                    'category': self._classify_news_category(item['title'], item['description']),
                    'news_type': self._classify_news_type(item['title'], item['description'])
                })
            
            return pd.DataFrame(news_data)
            
        except Exception as e:
            logger.error(f"뉴스 수집 실패 ({keyword}): {e}")
            return pd.DataFrame()
    
    def _clean_html(self, text):
        """HTML 태그 제거"""
        import re
        return re.sub('<[^<]+?>', '', text)
    
    def _parse_date(self, date_str):
        """날짜 형식 변환"""
        try:
            dt = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return date_str
    
    def _extract_source(self, link):
        """뉴스 소스 추출"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(link)
            return parsed.netloc
        except:
            return 'unknown'
    
    def _extract_stock_code(self, keyword):
        """키워드에서 종목코드 추출"""
        if keyword.isdigit() and len(keyword) == 6:
            return keyword
        return None
    
    def _classify_news_category(self, title, description):
        """뉴스 카테고리 분류"""
        content = f"{title} {description}".lower()
        
        fundamental_keywords = [
            '실적', '매출', '영업익', '순이익', '손익', '재무제표',
            '배당', '증자', '감자', '사업', '투자', '인수', '합병',
            '공시', '지배구조', '경영진', '이사회'
        ]
        
        for keyword in fundamental_keywords:
            if keyword in content:
                return 'fundamental'
        
        return 'general'
    
    def _classify_news_type(self, title, description):
        """뉴스 유형 분류"""
        content = f"{title} {description}".lower()
        
        if any(keyword in content for keyword in ['실적', '매출', '영업익', '순이익']):
            return 'earnings'
        
        if any(keyword in content for keyword in ['투자', '확장', '진출', '설립']):
            return 'expansion'
        
        if any(keyword in content for keyword in ['배당', '배당금', '주주환원']):
            return 'dividend'
        
        if any(keyword in content for keyword in ['CEO', '사장', '경영진', '이사회']):
            return 'management'
        
        if any(keyword in content for keyword in ['산업', '업계', '시장', '동향']):
            return 'industry'
        
        return 'general'
    
    def collect_stock_news(self, stock_code, company_name, days=30):
        """개별 종목 뉴스 수집"""
        try:
            keywords = [stock_code, company_name]
            all_news = []
            
            for keyword in keywords:
                for page in range(1, 3):  # 최대 2페이지
                    start_idx = (page - 1) * 100 + 1
                    
                    news_data = self.collect_news_data(keyword, display=100, start=start_idx)
                    
                    if not news_data.empty:
                        news_data['stock_code'] = stock_code
                        all_news.append(news_data)
                    
                    time.sleep(0.1)
            
            if all_news:
                combined_news = pd.concat(all_news, ignore_index=True)
                combined_news = combined_news.drop_duplicates(subset=['title', 'pubDate'])
                
                # 날짜 필터링
                cutoff_date = datetime.now() - timedelta(days=days)
                combined_news = combined_news[
                    pd.to_datetime(combined_news['pubDate']) > cutoff_date
                ]
                
                return combined_news
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"종목 뉴스 수집 실패 ({stock_code}): {e}")
            return pd.DataFrame()
    
    def collect_all_stock_news(self, days=30, limit=None):
        """전종목 뉴스 데이터 수집"""
        try:
            with get_db_connection('stock') as conn:
                query = "SELECT stock_code, company_name FROM company_info"
                if limit:
                    query += f" LIMIT {limit}"
                
                stocks = pd.read_sql(query, conn)
            
            if stocks.empty:
                logger.error("종목 정보가 없습니다. 먼저 종목 기본정보를 수집해주세요.")
                return False
            
            total_count = len(stocks)
            success_count = 0
            
            for i, row in stocks.iterrows():
                stock_code = row['stock_code']
                company_name = row['company_name']
                
                logger.info(f"뉴스 수집: {i+1}/{total_count} - {stock_code} ({company_name})")
                
                news_data = self.collect_stock_news(stock_code, company_name, days)
                
                if not news_data.empty:
                    with get_db_connection('news') as conn:
                        conn.execute(
                            "DELETE FROM news_articles WHERE stock_code = ?", (stock_code,)
                        )
                        news_data.to_sql('news_articles', conn, if_exists='append', index=False)
                    
                    success_count += 1
                    logger.info(f"저장 완료: {stock_code} - {len(news_data)}개 뉴스")
                
                time.sleep(0.1)
            
            logger.info(f"전종목 뉴스 수집 완료: {success_count}/{total_count}")
            return True
            
        except Exception as e:
            logger.error(f"전종목 뉴스 수집 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    try:
        collector = NewsCollector()
        
        print("📰 뉴스 데이터 수집을 시작합니다...")
        
        # 테스트 모드 여부 확인
        test_mode = input("테스트 모드로 실행하시겠습니까? (10개 종목만 수집) (y/N): ")
        limit = 10 if test_mode.lower() == 'y' else None
        
        # 수집 기간 설정
        days = 30
        print(f"수집 기간: 최근 {days}일")
        
        if not test_mode.lower() == 'y':
            print("⚠️  전종목 뉴스 수집은 시간이 오래 걸릴 수 있습니다...")
            user_input = input("계속 진행하시겠습니까? (y/N): ")
            if user_input.lower() != 'y':
                print("뉴스 데이터 수집을 취소합니다.")
                return False
        
        # 뉴스 데이터 수집
        success = collector.collect_all_stock_news(days, limit)
        
        if success:
            print("✅ 뉴스 데이터 수집 완료!")
        else:
            print("❌ 뉴스 데이터 수집 실패!")
            
        return success
        
    except Exception as e:
        logger.error(f"뉴스 데이터 수집 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)