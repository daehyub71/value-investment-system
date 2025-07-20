#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite 호환 전체 종목 뉴스 수집 (오류 수정 버전)
"""

import sqlite3
import requests
import re
import time
import os
from datetime import datetime, timedelta
from dateutil import parser as date_parser
from dotenv import load_dotenv

load_dotenv()

class CompatibleStocksNewsCollector:
    def __init__(self):
        self.client_id = os.getenv('NAVER_CLIENT_ID')
        self.client_secret = os.getenv('NAVER_CLIENT_SECRET')
        self.stock_db = 'data/databases/stock_data.db'
        self.news_db = 'data/databases/news_data.db'
        
        if not self.client_id or not self.client_secret:
            raise ValueError("네이버 API 키가 설정되지 않았습니다.")
        
        print(f"🔧 API 키 확인 완료")
    
    def find_stock_column_and_data(self):
        """테이블에서 종목 컬럼과 데이터 자동 탐지"""
        try:
            with sqlite3.connect(self.stock_db) as conn:
                cursor = conn.cursor()
                
                # 테이블 목록
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [table[0] for table in cursor.fetchall()]
                print(f"📊 사용 가능한 테이블: {tables}")
                
                # 우선순위 테이블 목록
                priority_tables = ['daily_prices', 'stock_prices', 'company_info']
                
                for table_name in priority_tables:
                    if table_name in tables:
                        print(f"\n🔍 {table_name} 테이블 분석...")
                        
                        # 컬럼 정보
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        columns = cursor.fetchall()
                        column_names = [col[1] for col in columns]
                        
                        print(f"   컬럼: {column_names}")
                        
                        # 종목 관련 컬럼 탐지
                        stock_candidates = []
                        for col in column_names:
                            if any(keyword in col.lower() for keyword in ['code', 'symbol', 'ticker']):
                                stock_candidates.append(col)
                        
                        print(f"   종목 후보 컬럼: {stock_candidates}")
                        
                        # 각 후보 컬럼 검증
                        for col in stock_candidates:
                            try:
                                # 샘플 데이터 확인
                                cursor.execute(f"SELECT DISTINCT {col} FROM {table_name} WHERE {col} IS NOT NULL LIMIT 10")
                                samples = [str(row[0]) for row in cursor.fetchall()]
                                
                                # 6자리 숫자 종목코드 필터링
                                valid_samples = [s for s in samples if len(s) == 6 and s.isdigit()]
                                
                                if valid_samples:
                                    print(f"   ✅ {col}: {valid_samples}")
                                    
                                    # 전체 유효 종목 수 확인
                                    cursor.execute(f"""
                                        SELECT COUNT(DISTINCT {col}) 
                                        FROM {table_name} 
                                        WHERE {col} IS NOT NULL 
                                          AND LENGTH({col}) = 6
                                    """)
                                    
                                    total_count = cursor.fetchone()[0]
                                    print(f"   📈 총 {total_count}개 종목 (6자리)")
                                    
                                    if total_count > 0:
                                        # 실제 종목 목록 추출
                                        cursor.execute(f"""
                                            SELECT DISTINCT {col}
                                            FROM {table_name} 
                                            WHERE {col} IS NOT NULL 
                                              AND LENGTH({col}) = 6
                                            ORDER BY {col}
                                        """)
                                        
                                        all_stocks = [str(row[0]) for row in cursor.fetchall()]
                                        
                                        # 숫자로만 구성된 것만 필터링
                                        numeric_stocks = [s for s in all_stocks if s.isdigit()]
                                        
                                        if numeric_stocks:
                                            print(f"   🎯 {table_name}.{col}에서 {len(numeric_stocks)}개 유효 종목 발견")
                                            return table_name, col, numeric_stocks
                                
                            except Exception as e:
                                print(f"   ❌ {col} 검증 실패: {e}")
                                continue
                
                print("❌ 모든 테이블에서 종목 데이터를 찾을 수 없음")
                return None, None, []
                
        except Exception as e:
            print(f"❌ 테이블 분석 실패: {e}")
            return None, None, []
    
    def get_company_name_mapping(self):
        """종목코드-회사명 매핑"""
        mapping = {
            '005930': '삼성전자', '000660': 'SK하이닉스', '035420': 'NAVER', '035720': '카카오',
            '051910': 'LG화학', '006400': '삼성SDI', '090430': '아모레퍼시픽', '068270': '셀트리온',
            '207940': '삼성바이오로직스', '028260': '삼성물산', '066570': 'LG전자', '096770': 'SK이노베이션',
            '003550': 'LG', '017670': 'SK텔레콤', '030200': 'KT', '009150': '삼성전기',
            '032830': '삼성생명', '018260': '삼성에스디에스', '010950': 'S-Oil', '011070': 'LG이노텍',
            '012330': '현대모비스', '000270': '기아', '005380': '현대차', '373220': 'LG에너지솔루션',
            '000720': '현대건설', '034730': 'SK', '011780': '금고석유화학', '047810': '한국항공우주',
            '036570': '엔씨소프트', '251270': '넷마블', '018880': '한온시스템', '003490': '대한항공'
        }
        
        # 데이터베이스에서 추가 매핑 시도
        try:
            with sqlite3.connect(self.stock_db) as conn:
                cursor = conn.execute("SELECT stock_code, company_name FROM company_info")
                for row in cursor.fetchall():
                    if row[0] and row[1]:
                        mapping[str(row[0])] = str(row[1])
        except:
            pass
        
        return mapping
    
    def search_latest_news(self, company_name, stock_code, days_back=30):
        """최신 뉴스 검색"""
        try:
            headers = {
                'X-Naver-Client-Id': self.client_id,
                'X-Naver-Client-Secret': self.client_secret
            }
            
            all_news = []
            cutoff_date = datetime.now().date() - timedelta(days=days_back)
            
            print(f"   🔍 '{company_name}' 뉴스 검색...")
            
            # 3페이지까지 검색
            for page in range(1, 4):
                start_index = (page - 1) * 100 + 1
                
                params = {
                    'query': company_name,
                    'display': 100,
                    'start': start_index,
                    'sort': 'date'
                }
                
                response = requests.get(
                    "https://openapi.naver.com/v1/search/news.json",
                    headers=headers,
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                news_items = data.get('items', [])
                
                if not news_items:
                    break
                
                # 날짜 필터링
                recent_news = []
                old_count = 0
                
                for item in news_items:
                    pub_date_str = item.get('pubDate', '')
                    try:
                        if pub_date_str:
                            pub_date = date_parser.parse(pub_date_str).date()
                            if pub_date >= cutoff_date:
                                recent_news.append(item)
                            else:
                                old_count += 1
                        else:
                            recent_news.append(item)
                    except:
                        recent_news.append(item)
                
                all_news.extend(recent_news)
                
                # 오래된 뉴스가 많으면 중단
                if old_count > len(recent_news):
                    break
                
                time.sleep(0.1)
            
            # 중복 제거
            seen_urls = set()
            unique_news = []
            
            for item in all_news:
                url = item.get('originallink', item.get('link', ''))
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_news.append(item)
            
            print(f"   📰 {len(unique_news)}개 뉴스 발견")
            return unique_news
            
        except Exception as e:
            print(f"   ❌ 검색 실패: {e}")
            return []
    
    def save_news_to_database(self, stock_code, company_name, news_items):
        """뉴스 저장"""
        if not news_items:
            return 0
        
        saved_count = 0
        
        try:
            with sqlite3.connect(self.news_db) as conn:
                for item in news_items:
                    try:
                        title = re.sub(r'<[^>]+>', '', item.get('title', ''))
                        description = re.sub(r'<[^>]+>', '', item.get('description', ''))
                        
                        # 중복 체크
                        cursor = conn.execute(
                            "SELECT COUNT(*) FROM news_articles WHERE originallink = ?",
                            (item.get('originallink', ''),)
                        )
                        
                        if cursor.fetchone()[0] > 0:
                            continue
                        
                        # 감정분석
                        content = f"{title} {description}".lower()
                        pos_words = ['성장', '증가', '상승', '개선', '호조']
                        neg_words = ['감소', '하락', '부진', '악화', '우려']
                        
                        pos_count = sum(1 for word in pos_words if word in content)
                        neg_count = sum(1 for word in neg_words if word in content)
                        
                        if pos_count > neg_count:
                            sentiment_score = 0.3
                            sentiment_label = 'positive'
                        elif neg_count > pos_count:
                            sentiment_score = -0.3
                            sentiment_label = 'negative'
                        else:
                            sentiment_score = 0.0
                            sentiment_label = 'neutral'
                        
                        # 저장
                        conn.execute('''
                            INSERT INTO news_articles 
                            (stock_code, title, description, originallink, link, pubDate, 
                             source, category, sentiment_score, sentiment_label, confidence_score, 
                             keywords, created_at, company_name)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            stock_code, title, description,
                            item.get('originallink', ''), item.get('link', ''),
                            item.get('pubDate', ''), '네이버뉴스', '금융',
                            sentiment_score, sentiment_label, 0.5,
                            f"pos:{pos_count},neg:{neg_count}",
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            company_name
                        ))
                        
                        saved_count += 1
                        
                    except Exception as e:
                        continue
                
                conn.commit()
                
        except Exception as e:
            print(f"   ❌ 저장 실패: {e}")
        
        return saved_count
    
    def collect_all_news(self, days_back=30, max_stocks=100):
        """전체 종목 뉴스 수집"""
        print(f"🚀 호환성 개선된 전체 종목 뉴스 수집")
        print(f"📅 수집 기간: 최근 {days_back}일")
        print("=" * 60)
        
        # 1. 종목 데이터 탐지
        table_name, column_name, stock_list = self.find_stock_column_and_data()
        
        if not stock_list:
            print("❌ 종목 데이터를 찾을 수 없습니다.")
            print("📋 하드코딩된 주요 종목으로 진행합니다.")
            stock_list = ['005930', '000660', '035420', '035720', '090430', '068270', 
                         '051910', '006400', '207940', '028260', '066570', '096770']
        
        if max_stocks:
            stock_list = stock_list[:max_stocks]
        
        print(f"📊 처리 대상: {len(stock_list)}개 종목")
        
        # 2. 회사명 매핑
        name_mapping = self.get_company_name_mapping()
        
        # 3. 뉴스 수집
        total_success = 0
        total_news = 0
        
        for idx, stock_code in enumerate(stock_list):
            company_name = name_mapping.get(stock_code, stock_code)
            
            print(f"\\n[{idx+1:3d}/{len(stock_list)}] {company_name}({stock_code})")
            
            try:
                news_items = self.search_latest_news(company_name, stock_code, days_back)
                
                if news_items:
                    saved_count = self.save_news_to_database(stock_code, company_name, news_items)
                    if saved_count > 0:
                        total_success += 1
                        total_news += saved_count
                        print(f"   ✅ {saved_count}개 저장")
                    else:
                        print(f"   ⚠️ 모두 중복")
                else:
                    print(f"   ❌ 뉴스 없음")
                
                time.sleep(0.1)
                
                # 중간 보고
                if (idx + 1) % 25 == 0:
                    print(f"\\n📊 중간 결과: {total_success}개 종목, {total_news}개 뉴스")
                
            except Exception as e:
                print(f"   ❌ 오류: {e}")
                continue
        
        print(f"\\n🎉 수집 완료!")
        print(f"📊 최종: {total_success}개 종목에서 {total_news}개 뉴스 수집")

def main():
    try:
        os.chdir('C:/data_analysis/value-investment-system/value-investment-system')
        collector = CompatibleStocksNewsCollector()
        collector.collect_all_news(days_back=30, max_stocks=50)  # 테스트: 50개
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
