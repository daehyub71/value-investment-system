#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 종목 최신 뉴스 수집 - stock_prices 기반
daily_prices 테이블의 모든 종목에 대해 최근 30일 뉴스 수집
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

class TotalStocksNewsCollector:
    def __init__(self):
        self.client_id = os.getenv('NAVER_CLIENT_ID')
        self.client_secret = os.getenv('NAVER_CLIENT_SECRET')
        self.stock_db = 'data/databases/stock_data.db'
        self.news_db = 'data/databases/news_data.db'
        
        if not self.client_id or not self.client_secret:
            raise ValueError("네이버 API 키가 설정되지 않았습니다.")
        
        print(f"🔧 API 키 확인 완료")
    
    def get_all_stock_symbols(self):
        """daily_prices 테이블에서 모든 종목 가져오기"""
        try:
            with sqlite3.connect(self.stock_db) as conn:
                cursor = conn.cursor()
                
                # 테이블 확인
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [table[0] for table in cursor.fetchall()]
                print(f"📊 사용 가능한 테이블: {tables}")
                
                if 'daily_prices' in tables:
                    print("✅ daily_prices 테이블 사용")
                    
                    # 모든 종목 심볼 추출
                    cursor.execute("""
                        SELECT DISTINCT symbol, 
                               COUNT(*) as data_count, 
                               MAX(date) as latest_date,
                               MIN(date) as earliest_date
                        FROM daily_prices 
                        WHERE symbol IS NOT NULL 
                          AND symbol != ''
                          AND LENGTH(symbol) = 6
                          AND symbol NOT LIKE '%-%'
                          AND symbol ~ '^[0-9]{6}$'
                        GROUP BY symbol
                        HAVING data_count >= 10  -- 최소 10일 이상 데이터
                        ORDER BY latest_date DESC, data_count DESC
                    """)
                    
                    stocks = cursor.fetchall()
                    print(f"📈 daily_prices에서 {len(stocks)}개 종목 발견")
                    
                    # 샘플 출력
                    for i, (symbol, count, latest, earliest) in enumerate(stocks[:10]):
                        print(f"   {i+1:2d}. {symbol}: {count}개 데이터 ({earliest} ~ {latest})")
                    
                    if len(stocks) > 10:
                        print(f"   ... 외 {len(stocks)-10}개 종목")
                    
                    return [stock[0] for stock in stocks]  # 심볼만 반환
                    
                elif 'company_info' in tables:
                    print("⚠️ daily_prices 없음, company_info 사용")
                    cursor.execute("SELECT DISTINCT stock_code FROM company_info WHERE stock_code IS NOT NULL")
                    stocks = [row[0] for row in cursor.fetchall()]
                    print(f"📊 company_info에서 {len(stocks)}개 종목 발견")
                    return stocks
                    
                else:
                    print("❌ 사용 가능한 종목 테이블이 없습니다.")
                    return []
                    
        except Exception as e:
            print(f"❌ 종목 목록 조회 실패: {e}")
            # SQLite 정규식 문제 해결을 위한 대안
            try:
                with sqlite3.connect(self.stock_db) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT DISTINCT symbol, COUNT(*) as data_count
                        FROM daily_prices 
                        WHERE symbol IS NOT NULL AND symbol != '' AND LENGTH(symbol) = 6
                        GROUP BY symbol
                        ORDER BY data_count DESC
                    """)
                    stocks = [row[0] for row in cursor.fetchall() if row[0].isdigit()]
                    print(f"📈 대안 방법으로 {len(stocks)}개 종목 발견")
                    return stocks
            except Exception as e2:
                print(f"❌ 대안 방법도 실패: {e2}")
                return []
    
    def get_company_name_mapping(self):
        """종목코드-회사명 매핑 생성"""
        # 주요 종목 하드코딩
        mapping = {
            '005930': '삼성전자', '000660': 'SK하이닉스', '035420': 'NAVER', '035720': '카카오',
            '051910': 'LG화학', '006400': '삼성SDI', '090430': '아모레퍼시픽', '068270': '셀트리온',
            '207940': '삼성바이오로직스', '028260': '삼성물산', '066570': 'LG전자', '096770': 'SK이노베이션',
            '003550': 'LG', '017670': 'SK텔레콤', '030200': 'KT', '009150': '삼성전기',
            '032830': '삼성생명', '018260': '삼성에스디에스', '010950': 'S-Oil', '011070': 'LG이노텍',
            '012330': '현대모비스', '000270': '기아', '005380': '현대차', '373220': 'LG에너지솔루션',
            '000720': '현대건설', '034730': 'SK', '011780': '금호석유화학', '047810': '한국항공우주산업'
        }
        
        # 데이터베이스에서 추가 정보 수집
        try:
            with sqlite3.connect(self.stock_db) as conn:
                cursor = conn.execute("SELECT stock_code, company_name FROM company_info WHERE stock_code IS NOT NULL")
                for row in cursor.fetchall():
                    if row[0] and row[1]:
                        mapping[row[0]] = row[1]
        except:
            pass
        
        # DART 데이터베이스에서도 시도
        try:
            dart_db = 'data/databases/dart_data.db'
            if os.path.exists(dart_db):
                with sqlite3.connect(dart_db) as conn:
                    cursor = conn.execute("SELECT stock_code, corp_name FROM corp_codes WHERE stock_code IS NOT NULL")
                    for row in cursor.fetchall():
                        if row[0] and row[1]:
                            mapping[row[0]] = row[1]
        except:
            pass
        
        print(f"📋 종목명 매핑: {len(mapping)}개")
        return mapping
    
    def search_latest_news(self, company_name, stock_code, days_back=30):
        """최신 뉴스 검색 (강화된 날짜 필터링)"""
        try:
            headers = {
                'X-Naver-Client-Id': self.client_id,
                'X-Naver-Client-Secret': self.client_secret
            }
            
            all_news = []
            cutoff_date = datetime.now().date() - timedelta(days=days_back)
            
            print(f"   🔍 '{company_name}' 뉴스 검색 (최근 {days_back}일, {cutoff_date} 이후)")
            
            # 최대 3페이지 검색
            for page in range(1, 4):
                start_index = (page - 1) * 100 + 1
                
                params = {
                    'query': company_name,
                    'display': 100,
                    'start': start_index,
                    'sort': 'date'  # 최신순 정렬
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
                
                # 날짜 필터링 강화
                filtered_news = []
                old_news_count = 0
                
                for item in news_items:
                    pub_date_str = item.get('pubDate', '')
                    
                    # 날짜 파싱 시도
                    is_recent = False
                    try:
                        if pub_date_str:
                            # 네이버 API 날짜 형식: "Wed, 28 Aug 2024 10:26:00 +0900"
                            pub_date = date_parser.parse(pub_date_str).date()
                            
                            if pub_date >= cutoff_date:
                                is_recent = True
                            else:
                                old_news_count += 1
                        else:
                            # 날짜 정보가 없으면 최신으로 간주
                            is_recent = True
                    except Exception as date_error:
                        # 날짜 파싱 실패시 최신으로 간주
                        is_recent = True
                    
                    if is_recent:
                        filtered_news.append(item)
                
                all_news.extend(filtered_news)
                
                print(f"     페이지 {page}: {len(filtered_news)}개 최신, {old_news_count}개 오래된 뉴스")
                
                # 오래된 뉴스가 많으면 중단
                if old_news_count > len(filtered_news) and len(filtered_news) > 0:
                    print(f"     → 오래된 뉴스 비율이 높아 검색 중단")
                    break
                
                time.sleep(0.1)  # API 제한 준수
            
            # 중복 제거 (URL 기준)
            seen_urls = set()
            unique_news = []
            
            for item in all_news:
                url = item.get('originallink', item.get('link', ''))
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_news.append(item)
            
            print(f"   📰 최종 결과: {len(unique_news)}개 고유 뉴스")
            return unique_news
            
        except Exception as e:
            print(f"   ❌ {company_name} 뉴스 검색 실패: {e}")
            return []
    
    def save_news_to_database(self, stock_code, company_name, news_items):
        """뉴스를 데이터베이스에 저장"""
        if not news_items:
            return 0
        
        saved_count = 0
        duplicate_count = 0
        
        try:
            with sqlite3.connect(self.news_db) as conn:
                for item in news_items:
                    try:
                        # 기본 정보 정리
                        title = re.sub(r'<[^>]+>', '', item.get('title', ''))
                        description = re.sub(r'<[^>]+>', '', item.get('description', ''))
                        originallink = item.get('originallink', '')
                        link = item.get('link', '')
                        pub_date = item.get('pubDate', '')
                        
                        # 중복 확인
                        cursor = conn.execute(
                            "SELECT COUNT(*) FROM news_articles WHERE originallink = ? OR link = ?",
                            (originallink, link)
                        )
                        
                        if cursor.fetchone()[0] > 0:
                            duplicate_count += 1
                            continue
                        
                        # 간단한 감정분석
                        content = f"{title} {description}".lower()
                        positive_words = ['성장', '증가', '상승', '개선', '확대', '호조', '좋은', '긍정', '성공', '달성', '돌파']
                        negative_words = ['감소', '하락', '부진', '악화', '축소', '우려', '나쁜', '부정', '실패', '손실', '적자']
                        
                        pos_count = sum(1 for word in positive_words if word in content)
                        neg_count = sum(1 for word in negative_words if word in content)
                        
                        if pos_count > neg_count:
                            sentiment_score = min(pos_count * 0.1, 1.0)
                            sentiment_label = 'positive'
                        elif neg_count > pos_count:
                            sentiment_score = max(-neg_count * 0.1, -1.0)
                            sentiment_label = 'negative'
                        else:
                            sentiment_score = 0.0
                            sentiment_label = 'neutral'
                        
                        # 데이터베이스 저장
                        conn.execute('''
                            INSERT INTO news_articles 
                            (stock_code, title, description, originallink, link, pubDate, 
                             source, category, sentiment_score, sentiment_label, confidence_score, 
                             keywords, created_at, company_name)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            stock_code, title, description, originallink, link, pub_date,
                            '네이버뉴스', '금융', sentiment_score, sentiment_label, 0.6,
                            f"pos:{pos_count},neg:{neg_count}",
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            company_name
                        ))
                        
                        saved_count += 1
                        
                    except sqlite3.IntegrityError:
                        duplicate_count += 1
                        continue
                    except Exception as e:
                        print(f"     ⚠️ 뉴스 저장 오류: {e}")
                        continue
                
                conn.commit()
                
        except Exception as e:
            print(f"   ❌ 데이터베이스 저장 실패: {e}")
        
        if duplicate_count > 0:
            print(f"   📝 저장: {saved_count}개 신규, {duplicate_count}개 중복")
        else:
            print(f"   📝 저장: {saved_count}개")
        
        return saved_count
    
    def collect_all_stocks_news(self, days_back=30, max_stocks=None, start_from=0):
        """전체 종목 뉴스 수집 실행"""
        print(f"🚀 전체 종목 최신 뉴스 수집 시작")
        print(f"📅 수집 기간: 최근 {days_back}일")
        print(f"🕒 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # 1. 전체 종목 목록 가져오기
        all_stocks = self.get_all_stock_symbols()
        
        if not all_stocks:
            print("❌ 처리할 종목이 없습니다.")
            return
        
        # 시작점과 최대 개수 적용
        if start_from > 0:
            all_stocks = all_stocks[start_from:]
            print(f"📊 {start_from}번째부터 시작")
        
        if max_stocks:
            all_stocks = all_stocks[:max_stocks]
        
        print(f"📊 처리 대상: {len(all_stocks)}개 종목")
        
        # 2. 종목명 매핑 준비
        company_mapping = self.get_company_name_mapping()
        
        # 3. 각 종목별 뉴스 수집
        total_success = 0
        total_news = 0
        total_failed = 0
        
        for idx, stock_code in enumerate(all_stocks):
            try:
                company_name = company_mapping.get(stock_code, stock_code)
                
                print(f"\n📈 [{idx+1:3d}/{len(all_stocks)}] {company_name}({stock_code})")
                
                # 뉴스 검색
                news_items = self.search_latest_news(company_name, stock_code, days_back)
                
                if news_items:
                    # 뉴스 저장
                    saved_count = self.save_news_to_database(stock_code, company_name, news_items)
                    
                    if saved_count > 0:
                        total_success += 1
                        total_news += saved_count
                        print(f"   ✅ 성공: {saved_count}개 뉴스 저장")
                    else:
                        print(f"   ⚠️ 모두 중복 뉴스")
                else:
                    total_failed += 1
                    print(f"   ❌ 뉴스 없음")
                
                # API 제한 준수 (초당 10회 제한)
                time.sleep(0.12)
                
                # 중간 결과 출력 (매 50개마다)
                if (idx + 1) % 50 == 0:
                    success_rate = (total_success / (idx + 1)) * 100
                    print(f"\n📊 중간 결과 [{idx+1}/{len(all_stocks)}]:")
                    print(f"   성공: {total_success}개 종목 ({success_rate:.1f}%)")
                    print(f"   뉴스: 총 {total_news}개 수집")
                    print(f"   실패: {total_failed}개 종목")
                
            except KeyboardInterrupt:
                print(f"\n⚠️ 사용자에 의해 중단됨 (진행률: {idx+1}/{len(all_stocks)})")
                break
            except Exception as e:
                total_failed += 1
                print(f"   ❌ 오류: {e}")
                continue
        
        # 최종 결과
        print(f"\n" + "=" * 70)
        print(f"🎉 전체 수집 완료!")
        print(f"🕒 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 최종 결과:")
        print(f"   • 처리 종목: {len(all_stocks)}개")
        print(f"   • 성공 종목: {total_success}개 ({(total_success/len(all_stocks)*100):.1f}%)")
        print(f"   • 실패 종목: {total_failed}개")
        print(f"   • 수집 뉴스: 총 {total_news}개")
        print(f"   • 평균 뉴스: {(total_news/total_success):.1f}개/종목" if total_success > 0 else "")

def main():
    """메인 실행 함수"""
    try:
        # 작업 디렉토리 설정
        os.chdir('C:/data_analysis/value-investment-system/value-investment-system')
        print(f"📁 작업 디렉토리: {os.getcwd()}")
        
        # 수집기 초기화
        collector = TotalStocksNewsCollector()
        
        # 전체 종목 뉴스 수집 실행
        # 테스트: 첫 100개 종목만
        collector.collect_all_stocks_news(
            days_back=30,      # 최근 30일
            max_stocks=100,    # 상위 100개 종목만 (테스트)
            start_from=0       # 처음부터 시작
        )
        
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
