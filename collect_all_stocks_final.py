#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 3967개 종목 뉴스 수집 - company_info 기반
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

class FullStocksNewsCollector:
    def __init__(self):
        self.client_id = os.getenv('NAVER_CLIENT_ID')
        self.client_secret = os.getenv('NAVER_CLIENT_SECRET')
        self.stock_db = 'data/databases/stock_data.db'
        self.news_db = 'data/databases/news_data.db'
        
        if not self.client_id or not self.client_secret:
            raise ValueError("네이버 API 키가 설정되지 않았습니다.")
        
        print(f"🔧 API 키 확인 완료")
    
    def get_all_stocks_from_company_info(self):
        """company_info 테이블에서 전체 종목 및 회사명 가져오기"""
        try:
            with sqlite3.connect(self.stock_db) as conn:
                cursor = conn.cursor()
                
                print("📊 company_info 테이블에서 종목 정보 로딩...")
                
                # 종목코드와 회사명 함께 조회
                cursor.execute("""
                    SELECT stock_code, company_name 
                    FROM company_info 
                    WHERE stock_code IS NOT NULL 
                      AND LENGTH(stock_code) = 6
                      AND company_name IS NOT NULL
                      AND company_name != ''
                    ORDER BY stock_code
                """)
                
                stocks_data = cursor.fetchall()
                
                # 유효한 종목만 필터링 (숫자로만 구성)
                valid_stocks = []
                for stock_code, company_name in stocks_data:
                    if isinstance(stock_code, str) and stock_code.isdigit() and len(stock_code) == 6:
                        valid_stocks.append((stock_code, company_name))
                
                print(f"✅ {len(valid_stocks)}개 유효 종목 로딩 완료")
                
                # 샘플 출력
                print(f"📋 샘플 종목:")
                for i, (code, name) in enumerate(valid_stocks[:10]):
                    print(f"   {i+1:2d}. {name}({code})")
                
                if len(valid_stocks) > 10:
                    print(f"   ... 외 {len(valid_stocks)-10}개")
                
                return valid_stocks
                
        except Exception as e:
            print(f"❌ 종목 정보 로딩 실패: {e}")
            return []
    
    def search_latest_news(self, company_name, stock_code, days_back=30):
        """최신 뉴스 검색 (최적화된 버전)"""
        try:
            headers = {
                'X-Naver-Client-Id': self.client_id,
                'X-Naver-Client-Secret': self.client_secret
            }
            
            all_news = []
            cutoff_date = datetime.now().date() - timedelta(days=days_back)
            
            # 회사명이 너무 길면 줄여서 검색
            search_name = company_name[:10] if len(company_name) > 10 else company_name
            
            # 최대 2페이지만 검색 (API 효율성)
            for page in range(1, 3):
                start_index = (page - 1) * 100 + 1
                
                params = {
                    'query': search_name,
                    'display': 100,
                    'start': start_index,
                    'sort': 'date'
                }
                
                response = requests.get(
                    "https://openapi.naver.com/v1/search/news.json",
                    headers=headers,
                    params=params,
                    timeout=10  # 타임아웃 단축
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
                if old_count > len(recent_news) and len(recent_news) > 0:
                    break
                
                time.sleep(0.05)  # 짧은 대기
            
            # 중복 제거
            seen_urls = set()
            unique_news = []
            
            for item in all_news:
                url = item.get('originallink', item.get('link', ''))
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_news.append(item)
            
            return unique_news
            
        except Exception as e:
            # 오류 발생시 조용히 넘어감 (로그 최소화)
            return []
    
    def save_news_to_database(self, stock_code, company_name, news_items):
        """뉴스 저장 (배치 처리)"""
        if not news_items:
            return 0
        
        saved_count = 0
        
        try:
            with sqlite3.connect(self.news_db) as conn:
                # 기존 URL 목록 조회 (중복 방지)
                existing_urls = set()
                cursor = conn.execute("SELECT originallink FROM news_articles WHERE stock_code = ?", (stock_code,))
                existing_urls.update(row[0] for row in cursor.fetchall() if row[0])
                
                for item in news_items:
                    try:
                        originallink = item.get('originallink', '')
                        
                        # 중복 체크
                        if originallink in existing_urls:
                            continue
                        
                        title = re.sub(r'<[^>]+>', '', item.get('title', ''))
                        description = re.sub(r'<[^>]+>', '', item.get('description', ''))
                        
                        # 빠른 감정분석
                        content = f"{title} {description}".lower()
                        pos_count = sum(1 for word in ['성장', '증가', '상승', '개선', '호조', '긍정'] if word in content)
                        neg_count = sum(1 for word in ['감소', '하락', '부진', '악화', '우려', '부정'] if word in content)
                        
                        sentiment_score = (pos_count - neg_count) * 0.1
                        sentiment_label = 'positive' if sentiment_score > 0 else ('negative' if sentiment_score < 0 else 'neutral')
                        
                        # 저장
                        conn.execute('''
                            INSERT OR IGNORE INTO news_articles 
                            (stock_code, title, description, originallink, link, pubDate, 
                             source, category, sentiment_score, sentiment_label, confidence_score, 
                             keywords, created_at, company_name)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            stock_code, title, description, originallink, item.get('link', ''),
                            item.get('pubDate', ''), '네이버뉴스', '금융',
                            sentiment_score, sentiment_label, 0.5,
                            f"pos:{pos_count},neg:{neg_count}",
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            company_name
                        ))
                        
                        saved_count += 1
                        existing_urls.add(originallink)  # 중복 방지용 업데이트
                        
                    except Exception:
                        continue
                
                conn.commit()
                
        except Exception:
            pass
        
        return saved_count
    
    def collect_all_news_optimized(self, days_back=30, max_stocks=None, start_from=0, batch_size=100):
        """최적화된 전체 종목 뉴스 수집"""
        print(f"🚀 전체 종목 뉴스 수집 (최적화 버전)")
        print(f"📅 수집 기간: 최근 {days_back}일")
        print(f"🕒 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # 1. 전체 종목 로딩
        all_stocks = self.get_all_stocks_from_company_info()
        
        if not all_stocks:
            print("❌ 종목 데이터를 로딩할 수 없습니다.")
            return
        
        # 2. 범위 설정
        if start_from > 0:
            all_stocks = all_stocks[start_from:]
            print(f"📊 {start_from}번째부터 시작")
        
        if max_stocks:
            all_stocks = all_stocks[:max_stocks]
        
        print(f"📊 처리 대상: {len(all_stocks)}개 종목")
        
        # 3. 배치별 처리
        total_success = 0
        total_news = 0
        total_failed = 0
        
        for idx, (stock_code, company_name) in enumerate(all_stocks):
            try:
                # 간단한 진행률 표시 (매 종목마다 출력하지 않음)
                if idx % 50 == 0 or idx < 10:
                    progress = f"[{idx+1:4d}/{len(all_stocks)}]"
                    print(f"\\n{progress} {company_name}({stock_code})")
                elif idx % 10 == 0:
                    print(".", end="", flush=True)
                
                # 뉴스 검색
                news_items = self.search_latest_news(company_name, stock_code, days_back)
                
                if news_items:
                    saved_count = self.save_news_to_database(stock_code, company_name, news_items)
                    if saved_count > 0:
                        total_success += 1
                        total_news += saved_count
                        if idx % 50 == 0 or idx < 10:
                            print(f"   ✅ {saved_count}개 뉴스 저장")
                    else:
                        if idx % 50 == 0 or idx < 10:
                            print(f"   ⚠️ 모두 중복")
                else:
                    total_failed += 1
                    if idx % 50 == 0 or idx < 10:
                        print(f"   ❌ 뉴스 없음")
                
                # API 제한 준수 (네이버 초당 10회)
                time.sleep(0.12)
                
                # 배치별 중간 보고
                if (idx + 1) % batch_size == 0:
                    elapsed = (idx + 1) / len(all_stocks) * 100
                    success_rate = (total_success / (idx + 1)) * 100
                    
                    print(f"\\n📊 배치 완료 [{idx+1}/{len(all_stocks)}] ({elapsed:.1f}%)")
                    print(f"   성공: {total_success}개 종목 ({success_rate:.1f}%)")
                    print(f"   뉴스: {total_news}개 수집")
                    print(f"   실패: {total_failed}개")
                    print(f"   예상 완료: {datetime.now() + timedelta(seconds=(len(all_stocks)-(idx+1))*0.12)}")
                
            except KeyboardInterrupt:
                print(f"\\n⚠️ 사용자 중단 (진행률: {idx+1}/{len(all_stocks)})")
                break
            except Exception as e:
                total_failed += 1
                if idx < 10:  # 처음 10개만 오류 출력
                    print(f"   ❌ 오류: {e}")
                continue
        
        # 최종 결과
        print(f"\\n" + "=" * 70)
        print(f"🎉 전체 수집 완료!")
        print(f"🕒 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 최종 결과:")
        print(f"   • 처리 종목: {len(all_stocks)}개")
        print(f"   • 성공 종목: {total_success}개 ({(total_success/len(all_stocks)*100):.1f}%)")
        print(f"   • 실패 종목: {total_failed}개")
        print(f"   • 수집 뉴스: 총 {total_news}개")
        print(f"   • 평균 뉴스: {(total_news/total_success):.1f}개/종목" if total_success > 0 else "")
        
        # 아모레퍼시픽 확인
        amore_news = self.check_amorepacific_news()
        print(f"\\n🎯 아모레퍼시픽 뉴스: {amore_news}개")
    
    def check_amorepacific_news(self):
        """아모레퍼시픽 뉴스 확인"""
        try:
            with sqlite3.connect(self.news_db) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM news_articles 
                    WHERE stock_code = '090430' 
                       OR company_name LIKE '%아모레퍼시픽%'
                """)
                return cursor.fetchone()[0]
        except:
            return 0

def main():
    try:
        os.chdir('C:/data_analysis/value-investment-system/value-investment-system')
        collector = FullStocksNewsCollector()
        
        print("🔧 수집 옵션 선택:")
        print("1. 테스트 (100개 종목)")
        print("2. 중간 규모 (500개 종목)")  
        print("3. 전체 (3967개 종목) - 약 2시간 소요")
        print("4. 아모레퍼시픽만")
        
        choice = input("선택 (1-4, 기본값 1): ").strip() or "1"
        
        if choice == "1":
            collector.collect_all_news_optimized(days_back=30, max_stocks=100)
        elif choice == "2":
            collector.collect_all_news_optimized(days_back=30, max_stocks=500)
        elif choice == "3":
            confirm = input("전체 3967개 종목 수집 (약 2시간 소요)? (y/N): ").strip().lower()
            if confirm == 'y':
                collector.collect_all_news_optimized(days_back=30)
            else:
                print("취소되었습니다.")
        elif choice == "4":
            # 아모레퍼시픽만 수집
            print("🎯 아모레퍼시픽 뉴스 수집 시작")
            stocks = [('090430', '아모레퍼시픽')]
            
            total_success = 0
            total_news = 0
            
            for stock_code, company_name in stocks:
                print(f"\n📈 {company_name}({stock_code}) 처리 중...")
                
                news_items = collector.search_latest_news(company_name, stock_code, 30)
                
                if news_items:
                    saved_count = collector.save_news_to_database(stock_code, company_name, news_items)
                    if saved_count > 0:
                        total_success += 1
                        total_news += saved_count
                        print(f"✅ {saved_count}개 뉴스 저장 완료")
                    else:
                        print(f"⚠️ 모두 중복 뉴스")
                else:
                    print(f"❌ 뉴스를 찾을 수 없음")
            
            print(f"\n🎉 아모레퍼시픽 수집 완료: {total_news}개 뉴스")
            
            # 결과 확인
            amore_total = collector.check_amorepacific_news()
            print(f"📊 아모레퍼시픽 총 뉴스: {amore_total}개")
        else:
            print("기본값으로 100개 종목 테스트를 실행합니다.")
            collector.collect_all_news_optimized(days_back=30, max_stocks=100)
            
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
