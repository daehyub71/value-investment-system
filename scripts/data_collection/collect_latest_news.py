#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수정된 뉴스 수집 스크립트 - 아모레퍼시픽용 최신 뉴스 수집
"""

import sqlite3
import requests
import re
from datetime import datetime, timedelta
from dateutil import parser as date_parser
import time
import os
from dotenv import load_dotenv

load_dotenv()

def collect_amorepacific_latest_news():
    """아모레퍼시픽 최신 뉴스 수집 (2025년 포함)"""
    
    # 네이버 API 설정
    client_id = os.getenv('NAVER_CLIENT_ID')
    client_secret = os.getenv('NAVER_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        print("❌ 네이버 API 키가 설정되지 않았습니다.")
        return False
    
    db_path = 'data/databases/news_data.db'
    company_name = '아모레퍼시픽'
    stock_code = '090430'
    
    headers = {
        'X-Naver-Client-Id': client_id,
        'X-Naver-Client-Secret': client_secret
    }
    
    print(f"🔍 {company_name} 최신 뉴스 수집 시작...")
    
    # 기존 뉴스 URL 조회 (중복 방지)
    existing_urls = set()
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("""
                SELECT DISTINCT originallink FROM news_articles 
                WHERE stock_code = ? AND originallink IS NOT NULL AND originallink != ''
            """, (stock_code,))
            existing_urls = {row[0] for row in cursor.fetchall()}
    except:
        pass
    
    print(f"📊 기존 뉴스 URL: {len(existing_urls)}개")
    
    # 여러 페이지에서 뉴스 수집
    all_new_news = []
    cutoff_date = datetime.now().date() - timedelta(days=30)  # 최근 30일
    
    for page in range(1, 6):  # 5페이지까지 수집
        start_index = (page - 1) * 100 + 1
        
        params = {
            'query': company_name,
            'display': 100,
            'start': start_index,
            'sort': 'date'  # 최신순
        }
        
        try:
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
            
            new_count = 0
            old_count = 0
            
            for item in news_items:
                # URL 중복 체크
                url = item.get('originallink', item.get('link', ''))
                if url in existing_urls:
                    continue
                
                # 날짜 확인
                pub_date_str = item.get('pubDate', '')
                try:
                    if pub_date_str:
                        pub_date = date_parser.parse(pub_date_str).date()
                        if pub_date >= cutoff_date:
                            all_new_news.append(item)
                            new_count += 1
                        else:
                            old_count += 1
                    else:
                        all_new_news.append(item)  # 날짜 없으면 일단 포함
                        new_count += 1
                except:
                    all_new_news.append(item)  # 파싱 실패하면 일단 포함
                    new_count += 1
            
            print(f"📄 페이지 {page}: 신규 {new_count}개, 오래된 뉴스 {old_count}개")
            
            # 오래된 뉴스가 많으면 중단
            if old_count > new_count:
                break
                
            time.sleep(0.1)  # API 제한 준수
            
        except Exception as e:
            print(f"❌ 페이지 {page} 수집 실패: {e}")
            break
    
    print(f"✅ 총 {len(all_new_news)}개 신규 뉴스 발견")
    
    if not all_new_news:
        print("📰 신규 뉴스가 없습니다.")
        return True
    
    # 뉴스 저장
    saved_count = 0
    
    with sqlite3.connect(db_path) as conn:
        for item in all_new_news:
            try:
                # HTML 태그 제거
                title = re.sub(r'<[^>]+>', '', item.get('title', ''))
                description = re.sub(r'<[^>]+>', '', item.get('description', ''))
                
                # 간단한 감정분석
                content = f"{title} {description}".lower()
                positive_words = ['성장', '증가', '상승', '개선', '호조', '긍정', '성공']
                negative_words = ['감소', '하락', '부진', '악화', '우려', '부정', '실패']
                
                pos_count = sum(1 for word in positive_words if word in content)
                neg_count = sum(1 for word in negative_words if word in content)
                
                if pos_count > neg_count:
                    sentiment_score = 0.3
                    sentiment_label = 'positive'
                elif neg_count > pos_count:
                    sentiment_score = -0.3
                    sentiment_label = 'negative'
                else:
                    sentiment_score = 0.0
                    sentiment_label = 'neutral'
                
                # 데이터베이스에 저장
                conn.execute('''
                    INSERT OR IGNORE INTO news_articles 
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
                    sentiment_score,
                    sentiment_label,
                    0.5,
                    f"pos:{pos_count},neg:{neg_count}",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
                
                saved_count += 1
                
            except Exception as e:
                print(f"⚠️ 뉴스 저장 실패: {e}")
                continue
    
    print(f"💾 {saved_count}개 뉴스 저장 완료!")
    
    # 데이터베이스 상태 확인
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("""
            SELECT MIN(pubDate), MAX(pubDate), COUNT(*) 
            FROM news_articles 
            WHERE stock_code = ?
        """, (stock_code,))
        
        result = cursor.fetchone()
        if result:
            min_date, max_date, total_count = result
            print(f"📊 업데이트 후 상태:")
            print(f"   기간: {min_date} ~ {max_date}")
            print(f"   총 뉴스: {total_count}개")
    
    return True

if __name__ == "__main__":
    collect_amorepacific_latest_news()
