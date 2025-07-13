#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수정된 전체 회사 뉴스 수집 스크립트 (스키마 호환 버전)
기존 news_articles 테이블 구조에 맞게 수정
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
import traceback

# 환경변수 로드
load_dotenv()

class FixedCompanyNewsCollector:
    """수정된 전체 회사 뉴스 수집 클래스"""
    
    def __init__(self, delay=0.2, batch_size=50):
        # 네이버 API 설정
        self.naver_client_id = os.getenv('NAVER_CLIENT_ID')
        self.naver_client_secret = os.getenv('NAVER_CLIENT_SECRET')
        
        if not self.naver_client_id or not self.naver_client_secret:
            raise ValueError("네이버 API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
        
        self.base_url = "https://openapi.naver.com/v1/search/news.json"
        self.stock_db_path = Path('data/databases/stock_data.db')
        self.news_db_path = Path('data/databases/news_data.db')
        
        # 수집 설정
        self.delay = delay
        self.batch_size = batch_size
        self.max_news_per_company = 100
        
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('news_collection_fixed.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # 진행 상황 추적용 파일
        self.progress_file = Path('news_collection_progress_fixed.txt')
        
        # 테이블 스키마 확인 및 설정
        self.available_columns = self.check_table_schema()
        
        # 금융 관련 키워드
        self.financial_keywords = [
            '실적', '매출', '영업이익', '순이익', '배당', '투자', '인수합병', 'M&A',
            '신제품', '출시', '계약', '수주', '특허', '기술개발', '연구개발',
            '증설', '투자', '공장', '시설', '생산', '공급', '확장',
            '주가', '상승', '하락', '목표가', '투자의견', '매수', '매도',
            '분할', '합병', '유상증자', '무상증자', '자사주', '배당금',
            'CEO', '대표이사', '임원', '인사', '조직개편', '경영진',
            '업적', '성과', '전망', '계획', '전략', '사업', '부문'
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
        
        # 통계 추적
        self.stats = {
            'total_companies': 0,
            'processed_companies': 0,
            'successful_companies': 0,
            'total_news_collected': 0,
            'financial_news_saved': 0,
            'api_calls': 0,
            'errors': 0,
            'start_time': None
        }
    
    def check_table_schema(self):
        """테이블 스키마 확인 및 사용 가능한 컬럼 반환"""
        try:
            with sqlite3.connect(self.news_db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(news_articles)")
                columns = cursor.fetchall()
                
                column_names = [col[1] for col in columns]
                self.logger.info(f"사용 가능한 컬럼: {column_names}")
                
                return column_names
                
        except Exception as e:
            self.logger.error(f"테이블 스키마 확인 실패: {e}")
            # 기본 컬럼들 반환
            return ['stock_code', 'title', 'description', 'originallink', 'link', 
                   'pubDate', 'source', 'category', 'created_at']
    
    def get_insert_query_and_data(self, news_item, stock_code, company_name, sentiment):
        """사용 가능한 컬럼에 맞는 INSERT 쿼리와 데이터 생성"""
        
        # 모든 가능한 데이터 준비
        all_data = {
            'stock_code': stock_code,
            'company_name': company_name,  # 컬럼이 있으면 사용
            'title': re.sub(r'<[^>]+>', '', news_item.get('title', ''))[:500],
            'description': re.sub(r'<[^>]+>', '', news_item.get('description', ''))[:1000],
            'originallink': news_item.get('originallink', ''),
            'link': news_item.get('link', ''),
            'pubDate': news_item.get('pubDate', ''),
            'source': '네이버뉴스',
            'category': '금융',
            'sentiment_score': sentiment.get('sentiment_score', 0.0),
            'sentiment_label': sentiment.get('sentiment_label', 'neutral'),
            'confidence_score': sentiment.get('confidence', 0.0),
            'keywords': f"긍정:{sentiment.get('positive_count', 0)},부정:{sentiment.get('negative_count', 0)}",
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 사용 가능한 컬럼만 선택
        available_data = {}
        for col in self.available_columns:
            if col in all_data:
                available_data[col] = all_data[col]
        
        # INSERT 쿼리 생성
        columns_str = ', '.join(available_data.keys())
        placeholders = ', '.join(['?' for _ in available_data])
        query = f"INSERT INTO news_articles ({columns_str}) VALUES ({placeholders})"
        
        return query, list(available_data.values())
    
    def get_all_companies(self):
        """stock_data.db에서 모든 회사 정보 조회"""
        try:
            if not self.stock_db_path.exists():
                raise FileNotFoundError("stock_data.db 파일을 찾을 수 없습니다.")
            
            with sqlite3.connect(self.stock_db_path) as conn:
                query = """
                    SELECT stock_code, company_name, market_cap, market_type
                    FROM company_info 
                    WHERE company_name IS NOT NULL 
                        AND company_name != ''
                        AND LENGTH(company_name) >= 2
                    ORDER BY 
                        CASE WHEN market_cap IS NOT NULL THEN market_cap ELSE 0 END DESC,
                        company_name
                """
                
                cursor = conn.execute(query)
                companies = cursor.fetchall()
                
                self.logger.info(f"전체 회사 조회 완료: {len(companies)}개")
                return companies
                
        except Exception as e:
            self.logger.error(f"회사 목록 조회 실패: {e}")
            return []
    
    def load_progress(self):
        """진행 상황 로드"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if lines:
                        last_line = lines[-1].strip()
                        if ',' in last_line:
                            parts = last_line.split(',')
                            if len(parts) >= 2:
                                return int(parts[0]), parts[1]
                return 0, None
            except Exception as e:
                self.logger.warning(f"진행 상황 로드 실패: {e}")
                return 0, None
        return 0, None
    
    def save_progress(self, index, company_name, stock_code):
        """진행 상황 저장"""
        try:
            with open(self.progress_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                f.write(f"{index},{company_name},{stock_code},{timestamp}\n")
        except Exception as e:
            self.logger.warning(f"진행 상황 저장 실패: {e}")
    
    def search_company_news(self, company_name, max_results=100):
        """특정 회사의 뉴스 검색"""
        try:
            headers = {
                'X-Naver-Client-Id': self.naver_client_id,
                'X-Naver-Client-Secret': self.naver_client_secret
            }
            
            all_news = []
            
            # 회사명 검색
            search_queries = [
                f'"{company_name}"',  # 정확한 회사명
                company_name,         # 일반 검색
            ]
            
            for query in search_queries:
                for page in range(1, 4):  # 최대 3페이지
                    params = {
                        'query': query,
                        'display': 100,
                        'start': ((page - 1) * 100) + 1,
                        'sort': 'date'
                    }
                    
                    response = requests.get(self.base_url, headers=headers, params=params, timeout=30)
                    response.raise_for_status()
                    
                    self.stats['api_calls'] += 1
                    
                    data = response.json()
                    news_items = data.get('items', [])
                    
                    if not news_items:
                        break
                    
                    all_news.extend(news_items)
                    
                    if len(all_news) >= max_results:
                        break
                    
                    time.sleep(0.05)
                
                if len(all_news) >= max_results:
                    break
            
            # 중복 제거
            unique_news = []
            seen_links = set()
            
            for news in all_news:
                link = news.get('link', '')
                if link and link not in seen_links:
                    unique_news.append(news)
                    seen_links.add(link)
            
            self.stats['total_news_collected'] += len(unique_news)
            return unique_news[:max_results]
            
        except Exception as e:
            self.logger.error(f"뉴스 검색 실패: {company_name} - {e}")
            self.stats['errors'] += 1
            return []
    
    def filter_financial_news(self, news_items, company_name):
        """금융 관련 뉴스 필터링"""
        filtered_news = []
        
        for item in news_items:
            title = item.get('title', '').lower()
            description = item.get('description', '').lower()
            content = f"{title} {description}"
            
            # HTML 태그 제거
            content = re.sub(r'<[^>]+>', '', content)
            
            # 회사명 포함 확인
            company_variations = [
                company_name.lower(),
                company_name.replace('(주)', '').replace('㈜', '').lower(),
                company_name.replace('주식회사', '').lower()
            ]
            
            company_mentioned = any(variation in content for variation in company_variations)
            
            if not company_mentioned:
                continue
            
            # 금융 키워드 점수 계산
            financial_score = sum(1 for keyword in self.financial_keywords if keyword.lower() in content)
            
            if financial_score >= 1:
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
            'positive_count': positive_count,
            'negative_count': negative_count
        }
    
    def save_news_to_db(self, news_items, stock_code, company_name):
        """뉴스를 데이터베이스에 저장 (스키마 호환)"""
        try:
            saved_count = 0
            
            with sqlite3.connect(self.news_db_path) as conn:
                for item in news_items:
                    try:
                        # 감정분석
                        title = re.sub(r'<[^>]+>', '', item.get('title', ''))
                        description = re.sub(r'<[^>]+>', '', item.get('description', ''))
                        sentiment = self.analyze_sentiment(f"{title} {description}")
                        
                        # 중복 확인
                        existing = conn.execute(
                            "SELECT id FROM news_articles WHERE link = ?",
                            (item.get('link', ''),)
                        ).fetchone()
                        
                        if existing:
                            continue
                        
                        # 스키마에 맞는 쿼리와 데이터 생성
                        query, data = self.get_insert_query_and_data(item, stock_code, company_name, sentiment)
                        
                        # 데이터 저장
                        conn.execute(query, data)
                        saved_count += 1
                        
                    except sqlite3.Error as e:
                        self.logger.warning(f"개별 뉴스 저장 실패: {e}")
                        continue
                
                conn.commit()
            
            self.stats['financial_news_saved'] += saved_count
            return saved_count
            
        except Exception as e:
            self.logger.error(f"데이터베이스 저장 실패: {e}")
            self.stats['errors'] += 1
            return 0
    
    def print_progress(self, current, total, company_name, news_count=0):
        """진행 상황 출력"""
        percentage = (current / total) * 100
        elapsed_time = time.time() - self.stats['start_time']
        
        if current > 0:
            avg_time_per_company = elapsed_time / current
            remaining_companies = total - current
            eta_seconds = avg_time_per_company * remaining_companies
            eta_str = str(timedelta(seconds=int(eta_seconds)))
        else:
            eta_str = "계산 중..."
        
        print(f"\r진행률: {current:,}/{total:,} ({percentage:.1f}%) | "
              f"현재: {company_name[:20]} | 뉴스: {news_count}건 | "
              f"경과: {str(timedelta(seconds=int(elapsed_time)))} | "
              f"예상 완료: {eta_str}", end='', flush=True)
    
    def collect_all_company_news(self, resume=False):
        """모든 회사의 뉴스 수집"""
        self.stats['start_time'] = time.time()
        
        # 회사 목록 조회
        companies = self.get_all_companies()
        if not companies:
            self.logger.error("수집할 회사가 없습니다.")
            return False
        
        self.stats['total_companies'] = len(companies)
        self.logger.info(f"뉴스 수집 시작: {len(companies):,}개 회사")
        self.logger.info(f"사용 가능한 컬럼: {self.available_columns}")
        
        # 진행 상황 로드
        start_index = 0
        if resume:
            start_index, last_company = self.load_progress()
            if start_index > 0:
                self.logger.info(f"수집 재개: {start_index}번째부터")
        
        # 회사별 처리
        for i in range(start_index, len(companies)):
            stock_code, company_name, market_cap, market_type = companies[i]
            
            try:
                self.stats['processed_companies'] += 1
                
                # 진행 상황 출력
                self.print_progress(i + 1, len(companies), company_name)
                
                # 뉴스 검색
                news_items = self.search_company_news(company_name, self.max_news_per_company)
                
                if not news_items:
                    self.save_progress(i, company_name, stock_code)
                    continue
                
                # 금융 뉴스 필터링
                filtered_news = self.filter_financial_news(news_items, company_name)
                
                if not filtered_news:
                    self.save_progress(i, company_name, stock_code)
                    continue
                
                # 데이터베이스 저장
                saved_count = self.save_news_to_db(filtered_news, stock_code, company_name)
                
                if saved_count > 0:
                    self.stats['successful_companies'] += 1
                
                self.save_progress(i, company_name, stock_code)
                time.sleep(self.delay)
                
                # 배치 단위 로그
                if (i + 1) % self.batch_size == 0:
                    self.logger.info(f"\n배치 완료: {i + 1:,}/{len(companies):,} "
                                   f"(성공: {self.stats['successful_companies']:,}, "
                                   f"저장: {self.stats['financial_news_saved']:,}건)")
                
            except KeyboardInterrupt:
                print(f"\n\n⏹️ 사용자에 의해 중단됨")
                print(f"📍 중단 지점: {i + 1}/{len(companies)} - {company_name}")
                return False
                
            except Exception as e:
                self.logger.error(f"회사 처리 실패: {company_name} - {e}")
                self.stats['errors'] += 1
                self.save_progress(i, company_name, stock_code)
                continue
        
        # 최종 통계
        elapsed_time = time.time() - self.stats['start_time']
        print(f"\n\n{'='*80}")
        print(f"📊 뉴스 수집 완료!")
        print(f"🏢 전체 회사: {self.stats['total_companies']:,}개")
        print(f"✅ 처리 완료: {self.stats['processed_companies']:,}개")  
        print(f"🎯 수집 성공: {self.stats['successful_companies']:,}개")
        print(f"💰 뉴스 저장: {self.stats['financial_news_saved']:,}건")
        print(f"⏱️ 소요 시간: {str(timedelta(seconds=int(elapsed_time)))}")
        
        if self.progress_file.exists():
            self.progress_file.unlink()
        
        return True

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='수정된 전체 회사 뉴스 수집 스크립트')
    parser.add_argument('--all', action='store_true', help='모든 회사 뉴스 수집')
    parser.add_argument('--resume', action='store_true', help='중단된 지점부터 재개')
    parser.add_argument('--delay', type=float, default=0.2, help='API 호출 간격 (초)')
    parser.add_argument('--batch_size', type=int, default=50, help='배치 크기')
    parser.add_argument('--max_news', type=int, default=100, help='회사당 최대 뉴스 수')
    
    args = parser.parse_args()
    
    try:
        collector = FixedCompanyNewsCollector(
            delay=args.delay,
            batch_size=args.batch_size
        )
        collector.max_news_per_company = args.max_news
        
        if args.all or args.resume:
            print(f"🚀 수정된 뉴스 수집 시작 (스키마 호환 버전)")
            print(f"⚙️ 설정: API 간격 {args.delay}초, 배치 크기 {args.batch_size}")
            print(f"{'='*60}")
            
            if collector.collect_all_company_news(resume=args.resume):
                print(f"\n✅ 전체 뉴스 수집 완료!")
            else:
                print(f"\n❌ 뉴스 수집 중단됨")
        else:
            parser.print_help()
            
    except ValueError as e:
        print(f"❌ 초기화 실패: {e}")
    except KeyboardInterrupt:
        print("\n⏹️ 프로그램 종료")
    except Exception as e:
        print(f"❌ 예기치 못한 오류: {e}")

if __name__ == "__main__":
    main()
