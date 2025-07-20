#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
아모레퍼시픽 뉴스 중복 문제 디버깅
"""

import sqlite3
import requests
import os
from datetime import datetime, timedelta
from dateutil import parser as date_parser
from dotenv import load_dotenv

load_dotenv()

def debug_amorepacific_news():
    """아모레퍼시픽 뉴스 중복 문제 분석"""
    
    os.chdir('C:/data_analysis/value-investment-system/value-investment-system')
    news_db = 'data/databases/news_data.db'
    
    print("🔍 아모레퍼시픽 뉴스 중복 문제 디버깅")
    print("=" * 60)
    
    # 1. 현재 저장된 뉴스 분석
    try:
        with sqlite3.connect(news_db) as conn:
            cursor = conn.cursor()
            
            print("📊 현재 저장된 아모레퍼시픽 뉴스 분석:")
            
            # 총 개수
            cursor.execute("""
                SELECT COUNT(*) FROM news_articles 
                WHERE stock_code = '090430' OR company_name LIKE '%아모레퍼시픽%'
            """)
            total_count = cursor.fetchone()[0]
            print(f"   총 뉴스: {total_count}개")
            
            # 날짜별 분포
            cursor.execute("""
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as count
                FROM news_articles 
                WHERE stock_code = '090430' OR company_name LIKE '%아모레퍼시픽%'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
                LIMIT 10
            """)
            
            date_dist = cursor.fetchall()
            print(f"   최근 저장일별 분포:")
            for date, count in date_dist:
                print(f"     {date}: {count}개")
            
            # 최신 뉴스 확인
            cursor.execute("""
                SELECT pubDate, title, originallink, created_at
                FROM news_articles 
                WHERE stock_code = '090430' OR company_name LIKE '%아모레퍼시픽%'
                ORDER BY created_at DESC
                LIMIT 5
            """)
            
            recent_news = cursor.fetchall()
            print(f"   최근 저장된 뉴스:")
            for pub_date, title, link, created in recent_news:
                print(f"     발행: {pub_date}")
                print(f"     제목: {title[:50]}...")
                print(f"     저장: {created}")
                print(f"     링크: {link[:50]}...")
                print()
            
            # URL 중복 확인
            cursor.execute("""
                SELECT originallink FROM news_articles 
                WHERE stock_code = '090430' 
                AND originallink IS NOT NULL 
                AND originallink != ''
                ORDER BY created_at DESC
                LIMIT 10
            """)
            
            existing_urls = [row[0] for row in cursor.fetchall()]
            print(f"   기존 URL 샘플 (최근 10개):")
            for i, url in enumerate(existing_urls, 1):
                print(f"     {i}. {url}")
                
    except Exception as e:
        print(f"❌ 저장된 뉴스 분석 실패: {e}")
    
    # 2. 네이버 뉴스 API 실시간 테스트
    print(f"\n🔍 네이버 뉴스 API 실시간 테스트:")
    
    client_id = os.getenv('NAVER_CLIENT_ID')
    client_secret = os.getenv('NAVER_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        print("❌ 네이버 API 키가 없습니다.")
        return
    
    try:
        headers = {
            'X-Naver-Client-Id': client_id,
            'X-Naver-Client-Secret': client_secret
        }
        
        params = {
            'query': '아모레퍼시픽',
            'display': 20,
            'start': 1,
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
        
        print(f"   API 응답: {len(news_items)}개 뉴스")
        
        if news_items:
            print(f"   최신 뉴스 분석:")
            
            cutoff_date = datetime.now().date() - timedelta(days=30)
            recent_count = 0
            
            for i, item in enumerate(news_items[:10]):
                pub_date_str = item.get('pubDate', '')
                title = item.get('title', '').replace('<b>', '').replace('</b>', '')
                link = item.get('originallink', item.get('link', ''))
                
                try:
                    if pub_date_str:
                        pub_date = date_parser.parse(pub_date_str).date()
                        is_recent = pub_date >= cutoff_date
                        if is_recent:
                            recent_count += 1
                    else:
                        is_recent = "날짜없음"
                except:
                    is_recent = "파싱실패"
                
                print(f"     {i+1:2d}. {pub_date_str}")
                print(f"         제목: {title[:60]}...")
                print(f"         링크: {link[:60]}...")
                print(f"         최근: {is_recent}")
                print()
            
            print(f"   최근 30일 뉴스: {recent_count}개")
            
            # 3. 중복 체크 시뮬레이션
            print(f"\n🔍 중복 체크 시뮬레이션:")
            
            try:
                with sqlite3.connect(news_db) as conn:
                    existing_urls = set()
                    cursor = conn.execute("SELECT originallink FROM news_articles WHERE stock_code = '090430'")
                    existing_urls.update(row[0] for row in cursor.fetchall() if row[0])
                    
                    print(f"   기존 URL 수: {len(existing_urls)}개")
                    
                    new_count = 0
                    duplicate_count = 0
                    
                    for item in news_items[:10]:
                        url = item.get('originallink', '')
                        if url:
                            if url in existing_urls:
                                duplicate_count += 1
                            else:
                                new_count += 1
                                print(f"   신규 URL: {url[:60]}...")
                    
                    print(f"   신규: {new_count}개, 중복: {duplicate_count}개")
                    
                    if new_count == 0:
                        print("   ⚠️ 모든 뉴스가 이미 저장되어 있습니다!")
                        print("   💡 더 많은 뉴스를 가져오거나 다른 검색어를 시도해보세요.")
                    
            except Exception as e:
                print(f"   ❌ 중복 체크 실패: {e}")
        
    except Exception as e:
        print(f"❌ API 테스트 실패: {e}")

def force_collect_latest_news():
    """강제로 최신 뉴스 수집 시도"""
    
    print(f"\n🚀 강제 최신 뉴스 수집 시도")
    print("-" * 40)
    
    client_id = os.getenv('NAVER_CLIENT_ID')
    client_secret = os.getenv('NAVER_CLIENT_SECRET')
    news_db = 'data/databases/news_data.db'
    
    if not client_id or not client_secret:
        print("❌ API 키 없음")
        return
    
    try:
        headers = {
            'X-Naver-Client-Id': client_id,
            'X-Naver-Client-Secret': client_secret
        }
        
        # 더 많은 페이지 수집
        all_news = []
        
        for page in range(1, 6):  # 5페이지까지
            start_index = (page - 1) * 100 + 1
            
            params = {
                'query': '아모레퍼시픽',
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
            
            all_news.extend(news_items)
            print(f"   페이지 {page}: {len(news_items)}개 수집")
        
        print(f"✅ 총 {len(all_news)}개 뉴스 수집")
        
        # 중복 제거 후 저장
        with sqlite3.connect(news_db) as conn:
            existing_urls = set()
            cursor = conn.execute("SELECT originallink FROM news_articles WHERE stock_code = '090430'")
            existing_urls.update(row[0] for row in cursor.fetchall() if row[0])
            
            new_count = 0
            
            for item in all_news:
                url = item.get('originallink', '')
                if url and url not in existing_urls:
                    try:
                        title = item.get('title', '').replace('<b>', '').replace('</b>', '')
                        description = item.get('description', '').replace('<b>', '').replace('</b>', '')
                        
                        conn.execute('''
                            INSERT INTO news_articles 
                            (stock_code, title, description, originallink, link, pubDate, 
                             source, category, sentiment_score, sentiment_label, confidence_score, 
                             keywords, created_at, company_name)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            '090430', title, description, url, item.get('link', ''),
                            item.get('pubDate', ''), '네이버뉴스', '금융', 0.0, 'neutral', 0.5,
                            '', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '아모레퍼시픽'
                        ))
                        
                        new_count += 1
                        existing_urls.add(url)
                        
                    except Exception as e:
                        continue
            
            conn.commit()
            
            print(f"✅ {new_count}개 신규 뉴스 저장")
            
            # 최종 확인
            cursor = conn.execute("SELECT COUNT(*) FROM news_articles WHERE stock_code = '090430'")
            total = cursor.fetchone()[0]
            print(f"📊 아모레퍼시픽 총 뉴스: {total}개")
        
    except Exception as e:
        print(f"❌ 강제 수집 실패: {e}")

def main():
    """메인 실행"""
    debug_amorepacific_news()
    
    print(f"\n" + "=" * 60)
    proceed = input("강제로 최신 뉴스 수집을 시도하시겠습니까? (y/N): ").strip().lower()
    
    if proceed == 'y':
        force_collect_latest_news()

if __name__ == "__main__":
    main()
