#!/usr/bin/env python3
"""
전체 종목 뉴스 데이터 복구 스크립트 (수정된 버전)
7월 11일 이후 누락된 뉴스 데이터를 체계적으로 복구

실행 방법:
python news_recovery_fixed.py --quick_recovery --top_stocks=10 --start_date=2025-07-11
python news_recovery_fixed.py --full_recovery --start_date=2025-07-11
"""

import sys
import os
import argparse
import sqlite3
import requests
import json
import re
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import time
from urllib.parse import quote
from dotenv import load_dotenv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 환경변수 로드
load_dotenv()

class NewsRecoveryManager:
    """뉴스 데이터 복구 매니저"""
    
    def __init__(self, log_level='INFO'):
        # 로깅 설정
        self.setup_logging(log_level)
        
        # API 설정
        self.naver_client_id = os.getenv('NAVER_CLIENT_ID')
        self.naver_client_secret = os.getenv('NAVER_CLIENT_SECRET')
        
        if not self.naver_client_id or not self.naver_client_secret:
            raise ValueError("네이버 API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
        
        self.base_url = "https://openapi.naver.com/v1/search/news.json"
        
        # 데이터베이스 경로
        self.stock_db_path = Path('data/databases/stock_data.db')
        self.news_db_path = Path('data/databases/news_data.db')
        self.dart_db_path = Path('data/databases/dart_data.db')
        
        # 통계
        self.stats = {
            'total_companies': 0,
            'processed_companies': 0,
            'successful_companies': 0,
            'total_news_collected': 0,
            'failed_companies': [],
            'start_time': datetime.now(),
            'target_start_date': None
        }
        
        # 금융 키워드 사전
        self.financial_keywords = [
            '실적', '매출', '영업이익', '순이익', '배당', '투자', '인수합병', 'M&A',
            '신제품', '출시', '계약', '수주', '특허', '기술개발', '연구개발',
            '증설', '투자', '공장', '시설', '생산', '공급',
            '주가', '상승', '하락', '목표가', '투자의견', '매수', '매도',
            '분할', '합병', '유상증자', '무상증자', '자사주',
            'CEO', '대표이사', '임원', '인사', '조직개편'
        ]
        
        # 감정분석 사전
        self.positive_words = [
            '성장', '증가', '상승', '개선', '확대', '호조', '좋은', '긍정', '성공',
            '달성', '돌파', '최고', '우수', '강세', '기대', '전망', '혁신'
        ]
        
        self.negative_words = [
            '감소', '하락', '부진', '악화', '축소', '우려', '나쁜', '부정', '실패',
            '부족', '손실', '적자', '최저', '부실', '불안', '위험', '하향'
        ]
    
    def setup_logging(self, log_level):
        """로깅 설정"""
        # 로그 파일명에 타임스탬프 추가
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f'news_recovery_{timestamp}.log'
        
        # 로깅 설정
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"🔧 뉴스 복구 매니저 초기화 - 로그파일: {log_filename}")
    
    def get_company_list(self, top_n=None, min_market_cap=None):
        """회사 리스트 조회 - DEBUG 로깅 추가"""
        try:
            self.logger.debug(f"📋 회사 리스트 조회 시작 - top_n: {top_n}, min_market_cap: {min_market_cap}")
            companies = []
            
            # 1. stock_data.db에서 회사 정보 조회
            if self.stock_db_path.exists():
                self.logger.debug(f"📊 Stock DB 발견: {self.stock_db_path}")
                with sqlite3.connect(self.stock_db_path) as conn:
                    query = """
                        SELECT stock_code, company_name, market_cap, market_type
                        FROM company_info 
                        WHERE company_name IS NOT NULL AND company_name != ''
                    """
                    
                    if min_market_cap:
                        query += f" AND market_cap >= {min_market_cap}"
                        self.logger.debug(f"💰 최소 시가총액 필터: {min_market_cap:,}")
                    
                    query += " ORDER BY market_cap DESC"
                    
                    if top_n:
                        query += f" LIMIT {top_n}"
                        self.logger.debug(f"🔢 상위 {top_n}개 종목으로 제한")
                    
                    self.logger.debug(f"🔍 실행할 쿼리: {query}")
                    
                    stock_companies = pd.read_sql(query, conn)
                    companies.extend(stock_companies.to_dict('records'))
                    self.logger.debug(f"📈 Stock DB에서 {len(stock_companies)}개 회사 조회")
            else:
                self.logger.warning(f"⚠️ Stock DB 없음: {self.stock_db_path}")
            
            # 2. dart_data.db에서 추가 회사 정보 조회 (필요시)
            if len(companies) < (top_n or 999999) and self.dart_db_path.exists():
                self.logger.debug(f"📋 DART DB에서 추가 회사 조회")
                with sqlite3.connect(self.dart_db_path) as conn:
                    existing_codes = [c['stock_code'] for c in companies]
                    if existing_codes:
                        placeholder = ','.join(['?' for _ in existing_codes])
                        query = f"""
                            SELECT stock_code, corp_name as company_name, NULL as market_cap, 'DART' as market_type
                            FROM corp_codes 
                            WHERE stock_code IS NOT NULL AND stock_code != ''
                            AND stock_code NOT IN ({placeholder})
                        """
                        remaining_limit = (top_n or 999999) - len(companies)
                        query += f" LIMIT {remaining_limit}"
                        dart_companies = pd.read_sql(query, conn, params=existing_codes)
                    else:
                        query = """
                            SELECT stock_code, corp_name as company_name, NULL as market_cap, 'DART' as market_type
                            FROM corp_codes 
                            WHERE stock_code IS NOT NULL AND stock_code != ''
                        """
                        if top_n:
                            query += f" LIMIT {top_n}"
                        dart_companies = pd.read_sql(query, conn)
                    
                    companies.extend(dart_companies.to_dict('records'))
                    self.logger.debug(f"📋 DART DB에서 {len(dart_companies)}개 추가 회사 조회")
            
            # 중복 제거 및 최종 제한
            seen_codes = set()
            unique_companies = []
            for company in companies:
                if company['stock_code'] not in seen_codes:
                    seen_codes.add(company['stock_code'])
                    unique_companies.append(company)
                    if top_n and len(unique_companies) >= top_n:
                        break
            
            self.logger.info(f"✅ 회사 리스트 조회 완료: {len(unique_companies)}개 회사")
            
            # DEBUG: 상위 5개 회사명 출력
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("📋 상위 회사들:")
                for i, company in enumerate(unique_companies[:5]):
                    self.logger.debug(f"  {i+1}. {company['company_name']}({company['stock_code']}) - 시총: {company.get('market_cap', 'N/A')}")
            
            return unique_companies
            
        except Exception as e:
            self.logger.error(f"❌ 회사 리스트 조회 실패: {e}")
            return []
    
    def filter_news_by_date(self, news_items, start_date_str):
        """날짜별 뉴스 필터링"""
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            filtered_items = []
            
            self.logger.debug(f"📅 날짜 필터링: {start_date_str} 이후, 총 {len(news_items)}개 뉴스 검사")
            
            for item in news_items:
                pub_date_str = item.get('pubDate', '')
                if pub_date_str:
                    try:
                        # 네이버 뉴스 날짜 형식: "Mon, 15 Jul 2025 14:30:00 +0900"
                        pub_date = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z')
                        pub_date_naive = pub_date.replace(tzinfo=None)  # timezone 제거
                        
                        if pub_date_naive >= start_date:
                            filtered_items.append(item)
                    except ValueError as e:
                        self.logger.debug(f"⚠️ 날짜 파싱 실패, 포함처리: {pub_date_str} - {e}")
                        # 날짜 파싱 실패시 포함 (안전장치)
                        filtered_items.append(item)
                else:
                    # 날짜 정보 없으면 포함
                    filtered_items.append(item)
            
            self.logger.debug(f"✅ 날짜 필터링 완료: {len(filtered_items)}/{len(news_items)}개 뉴스 선택")
            return filtered_items
            
        except Exception as e:
            self.logger.error(f"❌ 날짜 필터링 실패: {e}")
            return news_items  # 실패시 원본 반환
    
    def search_news_with_retry(self, keyword, start_date=None, max_retries=3, delay=1):
        """재시도 로직이 포함된 뉴스 검색"""
        for attempt in range(max_retries):
            try:
                headers = {
                    'X-Naver-Client-Id': self.naver_client_id,
                    'X-Naver-Client-Secret': self.naver_client_secret,
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                params = {
                    'query': keyword,
                    'display': 100,  # 최대 100개
                    'start': 1,
                    'sort': 'date'  # 최신순
                }
                
                self.logger.debug(f"🔍 뉴스 검색 시도 {attempt+1}/{max_retries}: {keyword}")
                
                response = requests.get(
                    self.base_url, 
                    headers=headers, 
                    params=params, 
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                news_items = data.get('items', [])
                
                # 날짜 필터링 (start_date 이후만)
                if start_date:
                    news_items = self.filter_news_by_date(news_items, start_date)
                
                self.logger.debug(f"✅ 뉴스 검색 성공 (키워드: {keyword}): {len(news_items)}건")
                return news_items
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"⚠️ 뉴스 검색 실패 (시도 {attempt+1}/{max_retries}, 키워드: {keyword}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay * (attempt + 1))  # 지수적 백오프
                else:
                    self.logger.error(f"❌ 뉴스 검색 최종 실패 (키워드: {keyword})")
                    return []
            except Exception as e:
                self.logger.error(f"❌ 뉴스 검색 예외 (키워드: {keyword}): {e}")
                return []
        
        return []
    
    def filter_financial_news(self, news_items, company_name=None):
        """금융/펀더멘털 관련 뉴스 필터링"""
        filtered_news = []
        
        for item in news_items:
            title = re.sub(r'<[^>]+>', '', item.get('title', '')).lower()
            description = re.sub(r'<[^>]+>', '', item.get('description', '')).lower()
            content = f"{title} {description}"
            
            # 점수 계산
            relevance_score = 0
            
            # 1. 금융 키워드 점수
            financial_matches = sum(1 for keyword in self.financial_keywords if keyword.lower() in content)
            relevance_score += financial_matches * 2
            
            # 2. 회사명 점수
            if company_name:
                company_matches = content.count(company_name.lower())
                relevance_score += company_matches * 3
            
            # 3. 제목에 회사명이 포함된 경우 보너스
            if company_name and company_name.lower() in title:
                relevance_score += 5
            
            # 4. 최소 점수 기준
            if relevance_score >= 3:  # 최소 3점 이상
                item['relevance_score'] = relevance_score
                filtered_news.append(item)
        
        self.logger.debug(f"🔍 금융 뉴스 필터링: {len(filtered_news)}/{len(news_items)}건 선택")
        return filtered_news
    
    def analyze_sentiment(self, text):
        """개선된 감정분석"""
        try:
            clean_text = re.sub(r'<[^>]+>', '', text).lower()
            
            # 긍정/부정 단어 카운트
            positive_count = sum(1 for word in self.positive_words if word in clean_text)
            negative_count = sum(1 for word in self.negative_words if word in clean_text)
            
            # 감정 점수 계산
            total_sentiment_words = positive_count + negative_count
            if total_sentiment_words == 0:
                sentiment_score = 0.0
                confidence = 0.1
            else:
                sentiment_score = (positive_count - negative_count) / total_sentiment_words
                confidence = min(total_sentiment_words / 5, 1.0)
            
            # 감정 라벨 결정
            if sentiment_score > 0.2:
                sentiment_label = 'positive'
            elif sentiment_score < -0.2:
                sentiment_label = 'negative'
            else:
                sentiment_label = 'neutral'
            
            return {
                'sentiment_score': sentiment_score,
                'sentiment_label': sentiment_label,
                'confidence': confidence,
                'positive_count': positive_count,
                'negative_count': negative_count,
                'keywords': f"긍정:{positive_count},부정:{negative_count}"
            }
            
        except Exception as e:
            self.logger.error(f"❌ 감정분석 실패: {e}")
            return {
                'sentiment_score': 0.0,
                'sentiment_label': 'neutral',
                'confidence': 0.0,
                'positive_count': 0,
                'negative_count': 0,
                'keywords': ''
            }
    
    def save_news_to_database(self, news_data):
        """뉴스 데이터를 데이터베이스에 저장"""
        try:
            # 데이터베이스 디렉토리 생성
            self.news_db_path.parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(self.news_db_path) as conn:
                # 테이블 생성 (없는 경우)
                self.create_news_tables(conn)
                
                saved_count = 0
                for news_item in news_data:
                    try:
                        # 중복 확인 (title + stock_code 기준)
                        existing = conn.execute("""
                            SELECT COUNT(*) FROM news_articles 
                            WHERE title = ? AND stock_code = ?
                        """, (news_item['title'], news_item['stock_code'])).fetchone()[0]
                        
                        if existing == 0:
                            conn.execute('''
                                INSERT INTO news_articles 
                                (stock_code, title, description, originallink, link, pubDate, 
                                 source, category, sentiment_score, sentiment_label, confidence_score, keywords, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                news_item['stock_code'],
                                news_item['title'],
                                news_item['description'],
                                news_item['originallink'],
                                news_item['link'],
                                news_item['pubDate'],
                                news_item['source'],
                                news_item['category'],
                                news_item['sentiment_score'],
                                news_item['sentiment_label'],
                                news_item['confidence_score'],
                                news_item['keywords'],
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            ))
                            saved_count += 1
                        else:
                            self.logger.debug(f"⏭️ 중복 뉴스 스킵: {news_item['title'][:50]}...")
                    except sqlite3.Error as e:
                        self.logger.warning(f"⚠️ 뉴스 저장 실패: {e}")
                        continue
                
                conn.commit()
                return saved_count
                
        except Exception as e:
            self.logger.error(f"❌ 뉴스 데이터베이스 저장 실패: {e}")
            return 0
    
    def create_news_tables(self, conn):
        """뉴스 테이블 생성"""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS news_articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                originallink TEXT,
                link TEXT,
                pubDate TEXT,
                source TEXT DEFAULT '네이버뉴스',
                category TEXT DEFAULT '금융',
                sentiment_score REAL DEFAULT 0.0,
                sentiment_label TEXT DEFAULT 'neutral',
                confidence_score REAL DEFAULT 0.0,
                keywords TEXT,
                created_at TEXT,
                UNIQUE(title, stock_code)
            )
        ''')
        
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_news_stock_code ON news_articles(stock_code)
        ''')
        
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_news_created_at ON news_articles(created_at)
        ''')
    
    def collect_news_for_company(self, company_info, start_date=None):
        """단일 회사 뉴스 수집"""
        stock_code = company_info['stock_code']
        company_name = company_info['company_name']
        
        try:
            self.logger.debug(f"📰 뉴스 수집 시작: {company_name}({stock_code})")
            
            # 회사명으로 뉴스 검색
            news_items = self.search_news_with_retry(company_name, start_date)
            
            if not news_items:
                self.logger.debug(f"🔍 뉴스 없음: {company_name}")
                return 0
            
            # 금융 뉴스 필터링
            filtered_news = self.filter_financial_news(news_items, company_name)
            
            if not filtered_news:
                self.logger.debug(f"💼 금융 뉴스 없음: {company_name}")
                return 0
            
            # 뉴스 데이터 처리
            processed_news = []
            for item in filtered_news:
                # HTML 태그 제거
                title = re.sub(r'<[^>]+>', '', item.get('title', ''))
                description = re.sub(r'<[^>]+>', '', item.get('description', ''))
                
                # 감정분석
                sentiment = self.analyze_sentiment(f"{title} {description}")
                
                news_data = {
                    'stock_code': stock_code,
                    'title': title,
                    'description': description,
                    'originallink': item.get('originallink', ''),
                    'link': item.get('link', ''),
                    'pubDate': item.get('pubDate', ''),
                    'source': '네이버뉴스',
                    'category': '금융',
                    'sentiment_score': sentiment['sentiment_score'],
                    'sentiment_label': sentiment['sentiment_label'],
                    'confidence_score': sentiment['confidence'],
                    'keywords': sentiment['keywords']
                }
                processed_news.append(news_data)
            
            # 데이터베이스 저장
            saved_count = self.save_news_to_database(processed_news)
            
            if saved_count > 0:
                self.logger.info(f"✅ {company_name}({stock_code}): {saved_count}건 저장")
            else:
                self.logger.debug(f"⏭️ {company_name}({stock_code}): 새로운 뉴스 없음")
            return saved_count
            
        except Exception as e:
            self.logger.error(f"❌ {company_name}({stock_code}) 뉴스 수집 실패: {e}")
            self.stats['failed_companies'].append({
                'stock_code': stock_code,
                'company_name': company_name,
                'error': str(e)
            })
            return 0
    
    def sequential_news_collection(self, companies, start_date=None, delay_between_requests=0.3):
        """순차적 뉴스 수집 (안정적)"""
        self.logger.info(f"🐌 순차적 뉴스 수집 시작: {len(companies)}개 회사")
        
        total_news = 0
        successful_companies = 0
        
        for i, company in enumerate(companies):
            try:
                news_count = self.collect_news_for_company(company, start_date)
                if news_count > 0:
                    successful_companies += 1
                    total_news += news_count
                
                self.stats['processed_companies'] += 1
                
                # 진행률 표시 (매번)
                progress = (i + 1) / len(companies) * 100
                elapsed = datetime.now() - self.stats['start_time']
                self.logger.info(f"📊 진행률: {i+1}/{len(companies)} ({progress:.1f}%) - 성공: {successful_companies}, 뉴스: {total_news:,}건, 경과: {elapsed}")
                
                # API 제한 대응
                time.sleep(delay_between_requests)
                
            except Exception as e:
                self.logger.error(f"❌ 순차 처리 오류 ({company['company_name']}): {e}")
                continue
        
        self.stats['successful_companies'] = successful_companies
        self.stats['total_news_collected'] = total_news
        
        return total_news, successful_companies
    
    def print_final_stats(self):
        """최종 통계 출력"""
        elapsed = datetime.now() - self.stats['start_time']
        
        self.logger.info("="*60)
        self.logger.info("📊 뉴스 데이터 복구 완료 - 최종 통계")
        self.logger.info("="*60)
        self.logger.info(f"🏢 전체 회사 수: {self.stats['total_companies']:,}개")
        self.logger.info(f"✅ 처리 완료: {self.stats['processed_companies']:,}개")
        self.logger.info(f"🎯 성공한 회사: {self.stats['successful_companies']:,}개")
        self.logger.info(f"📰 수집된 뉴스: {self.stats['total_news_collected']:,}건")
        self.logger.info(f"⏱️ 총 소요시간: {elapsed}")
        
        if self.stats['target_start_date']:
            self.logger.info(f"📅 수집 대상 기간: {self.stats['target_start_date']} 이후")
        
        if self.stats['failed_companies']:
            self.logger.info(f"❌ 실패한 회사: {len(self.stats['failed_companies'])}개")
            for failed in self.stats['failed_companies'][:5]:  # 상위 5개만 표시
                self.logger.info(f"   - {failed['company_name']}({failed['stock_code']}): {failed['error']}")
        
        # 성공률 계산
        if self.stats['processed_companies'] > 0:
            success_rate = (self.stats['successful_companies'] / self.stats['processed_companies']) * 100
            self.logger.info(f"📈 성공률: {success_rate:.1f}%")
        
        self.logger.info("="*60)
    
    def quick_recovery(self, top_stocks=100, start_date=None, parallel=False):
        """빠른 복구 (주요 종목만)"""
        self.logger.info(f"⚡ 빠른 복구 시작 (상위 {top_stocks}개 종목)")
        
        if start_date:
            self.logger.info(f"📅 수집 대상: {start_date} 이후 뉴스")
            self.stats['target_start_date'] = start_date
        
        # 시가총액 상위 종목들 조회
        companies = self.get_company_list(top_n=top_stocks, min_market_cap=1000000000)  # 10억 이상
        
        if not companies:
            self.logger.error("❌ 회사 리스트를 가져올 수 없습니다")
            return False
        
        self.stats['total_companies'] = len(companies)
        
        # 순차 처리만 사용 (안정성 우선)
        total_news, successful = self.sequential_news_collection(companies, start_date)
        
        self.print_final_stats()
        return total_news > 0
    
    def full_recovery(self, start_date=None, parallel=False, batch_size=100):
        """전체 복구 (모든 종목)"""
        self.logger.info("🏭 전체 복구 시작 (모든 종목)")
        
        if start_date:
            self.logger.info(f"📅 수집 대상: {start_date} 이후 뉴스")
            self.stats['target_start_date'] = start_date
        
        # 전체 종목 조회
        companies = self.get_company_list()
        
        if not companies:
            self.logger.error("❌ 회사 리스트를 가져올 수 없습니다")
            return False
        
        self.stats['total_companies'] = len(companies)
        
        # 배치 단위로 처리
        total_news_all = 0
        total_successful_all = 0
        
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(companies) + batch_size - 1) // batch_size
            
            self.logger.info(f"📦 배치 {batch_num}/{total_batches} 처리 중 ({len(batch)}개 회사)")
            
            total_news, successful = self.sequential_news_collection(batch, start_date)
            
            total_news_all += total_news
            total_successful_all += successful
            
            # 배치 간 대기 (API 안정성)
            if i + batch_size < len(companies):
                self.logger.info("⏸️ 배치 간 대기 중...")
                time.sleep(5)
        
        self.stats['total_news_collected'] = total_news_all
        self.stats['successful_companies'] = total_successful_all
        
        self.print_final_stats()
        return total_news_all > 0


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='전체 종목 뉴스 데이터 복구 스크립트 (수정된 버전)')
    parser.add_argument('--quick_recovery', action='store_true', help='빠른 복구 (주요 종목만)')
    parser.add_argument('--full_recovery', action='store_true', help='전체 복구 (모든 종목)')
    parser.add_argument('--start_date', type=str, help='수집 시작일 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, help='수집 기간 (일수)')
    parser.add_argument('--top_stocks', type=int, default=100, help='빠른 복구시 대상 종목 수')
    parser.add_argument('--batch_size', type=int, default=50, help='배치 크기')
    parser.add_argument('--parallel', action='store_true', help='병렬 처리 사용 (현재 비활성화)')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    try:
        # 복구 매니저 초기화
        recovery_manager = NewsRecoveryManager(log_level=args.log_level)
        
        # 인수 확인 로그
        recovery_manager.logger.info(f"🔧 실행 인수: {vars(args)}")
        
        # 날짜 처리
        start_date = None
        if args.start_date:
            start_date = args.start_date
            recovery_manager.logger.info(f"📅 지정된 시작 날짜: {start_date}")
        elif args.days:
            # days가 지정된 경우 현재 날짜에서 역산
            target_date = datetime.now() - timedelta(days=args.days)
            start_date = target_date.strftime('%Y-%m-%d')
            recovery_manager.logger.info(f"📅 계산된 시작 날짜 (최근 {args.days}일): {start_date}")
        
        if args.quick_recovery:
            # 빠른 복구
            recovery_manager.logger.info(f"⚡ 빠른 복구 모드 - 상위 {args.top_stocks}개 종목")
            success = recovery_manager.quick_recovery(
                top_stocks=args.top_stocks,
                start_date=start_date,
                parallel=args.parallel
            )
        elif args.full_recovery:
            # 전체 복구
            recovery_manager.logger.info("🏭 전체 복구 모드")
            success = recovery_manager.full_recovery(
                start_date=start_date,
                parallel=args.parallel,
                batch_size=args.batch_size
            )
        else:
            # 기본값: 빠른 복구
            recovery_manager.logger.info(f"🔧 기본 모드: 빠른 복구 실행 - 상위 {args.top_stocks}개 종목")
            success = recovery_manager.quick_recovery(
                top_stocks=args.top_stocks,
                start_date=start_date,
                parallel=False  # 기본값은 순차 처리
            )
        
        if success:
            recovery_manager.logger.info("✅ 뉴스 데이터 복구 성공")
            sys.exit(0)
        else:
            recovery_manager.logger.error("❌ 뉴스 데이터 복구 실패")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        print(f"❌ 예기치 못한 오류: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
