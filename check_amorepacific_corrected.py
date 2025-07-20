#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수정된 아모레퍼시픽 데이터 검토 프로그램 (날짜 파싱 오류 수정)
"""

import sqlite3
import os
from datetime import datetime

def check_amorepacific_corrected():
    """수정된 아모레퍼시픽 데이터 검토"""
    
    os.chdir('C:/data_analysis/value-investment-system/value-investment-system')
    
    stock_db = 'data/databases/stock_data.db'
    dart_db = 'data/databases/dart_data.db'
    news_db = 'data/databases/news_data.db'
    
    target_stock = "090430"
    stock_name = "아모레퍼시픽"
    
    print(f"🔍 {stock_name}({target_stock}) 데이터 현황 정확한 검사")
    print(f"검사 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 1. 주가 데이터 확인
    print(f"\n📈 주가 데이터 검사:")
    try:
        with sqlite3.connect(stock_db) as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) as count, MIN(date) as start_date, MAX(date) as end_date
                FROM stock_prices 
                WHERE stock_code = ?
            """, (target_stock,))
            
            result = cursor.fetchone()
            if result and result[0] > 0:
                count, start_date, end_date = result
                print(f"   ✅ {count}개 데이터 ({start_date} ~ {end_date})")
            else:
                print(f"   ❌ 주가 데이터 없음")
                
    except Exception as e:
        print(f"   ❌ 주가 데이터 확인 실패: {e}")
    
    # 2. 재무 데이터 확인
    print(f"\n📊 재무 데이터 검사:")
    try:
        with sqlite3.connect(dart_db) as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM disclosures 
                WHERE stock_code = ? OR corp_name LIKE ?
            """, (target_stock, f"%{stock_name}%"))
            
            count = cursor.fetchone()[0]
            print(f"   ✅ {count}개 공시 데이터")
            
    except Exception as e:
        print(f"   ❌ 재무 데이터 확인 실패: {e}")
    
    # 3. 뉴스 데이터 확인 (수정된 버전)
    print(f"\n📰 뉴스 데이터 검사 (수정된 버전):")
    try:
        with sqlite3.connect(news_db) as conn:
            # 총 뉴스 수
            cursor = conn.execute("""
                SELECT COUNT(*) FROM news_articles 
                WHERE stock_code = ? OR company_name LIKE ?
            """, (target_stock, f"%{stock_name}%"))
            
            total_count = cursor.fetchone()[0]
            print(f"   📊 총 뉴스: {total_count}개")
            
            if total_count > 0:
                # 저장 날짜 기준 분석 (created_at)
                cursor = conn.execute("""
                    SELECT 
                        MIN(created_at) as first_saved,
                        MAX(created_at) as last_saved,
                        COUNT(DISTINCT DATE(created_at)) as save_days
                    FROM news_articles 
                    WHERE stock_code = ? OR company_name LIKE ?
                """, (target_stock, f"%{stock_name}%"))
                
                save_result = cursor.fetchone()
                if save_result:
                    first_saved, last_saved, save_days = save_result
                    print(f"   📅 저장 기간: {first_saved} ~ {last_saved}")
                    print(f"   📆 저장된 날 수: {save_days}일")
                
                # 발행 날짜 분석 (pubDate) - 다양한 형식 처리
                cursor = conn.execute("""
                    SELECT pubDate, title, created_at
                    FROM news_articles 
                    WHERE stock_code = ? OR company_name LIKE ?
                    ORDER BY created_at DESC
                    LIMIT 10
                """, (target_stock, f"%{stock_name}%"))
                
                recent_news = cursor.fetchall()
                print(f"   📋 최근 10개 뉴스:")
                
                for i, (pub_date, title, created) in enumerate(recent_news, 1):
                    # 제목 길이 제한
                    short_title = title[:40] + "..." if len(title) > 40 else title
                    
                    # 발행일 파싱 시도
                    try:
                        if pub_date and 'Jul 2025' in pub_date:
                            # 2025년 7월 뉴스 확인
                            parsed_date = "2025-07"
                        elif pub_date:
                            # 간단한 연도 추출
                            if '2025' in pub_date:
                                parsed_date = "2025년"
                            elif '2024' in pub_date:
                                parsed_date = "2024년"
                            else:
                                parsed_date = pub_date[:20]
                        else:
                            parsed_date = "날짜없음"
                    except:
                        parsed_date = "파싱실패"
                    
                    print(f"     {i:2d}. {short_title}")
                    print(f"         발행: {parsed_date}")
                    print(f"         저장: {created[:10]}")
                
                # 월별 뉴스 분포 (저장 기준)
                cursor = conn.execute("""
                    SELECT 
                        strftime('%Y-%m', created_at) as month,
                        COUNT(*) as count
                    FROM news_articles 
                    WHERE stock_code = ? OR company_name LIKE ?
                    GROUP BY strftime('%Y-%m', created_at)
                    ORDER BY month DESC
                    LIMIT 6
                """, (target_stock, f"%{stock_name}%"))
                
                monthly_dist = cursor.fetchall()
                print(f"   📊 월별 저장 분포:")
                for month, count in monthly_dist:
                    print(f"     {month}: {count}개")
                
                # 2025년 뉴스 확인
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM news_articles 
                    WHERE (stock_code = ? OR company_name LIKE ?)
                      AND (pubDate LIKE '%2025%' OR created_at LIKE '2025%')
                """, (target_stock, f"%{stock_name}%"))
                
                news_2025 = cursor.fetchone()[0]
                print(f"   🎯 2025년 뉴스: {news_2025}개")
                
                # 최근 7일 뉴스
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM news_articles 
                    WHERE (stock_code = ? OR company_name LIKE ?)
                      AND created_at >= date('now', '-7 days')
                """, (target_stock, f"%{stock_name}%"))
                
                recent_7days = cursor.fetchone()[0]
                print(f"   📅 최근 7일: {recent_7days}개")
                
            else:
                print(f"   ❌ 뉴스 데이터 없음")
                
    except Exception as e:
        print(f"   ❌ 뉴스 데이터 확인 실패: {e}")
    
    # 4. 종합 평가
    print(f"\n" + "=" * 60)
    print(f"📊 {stock_name}({target_stock}) 데이터 수집 현황 종합")
    print(f"=" * 60)
    
    try:
        with sqlite3.connect(stock_db) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM stock_prices WHERE stock_code = ?", (target_stock,))
            stock_count = cursor.fetchone()[0]
        
        with sqlite3.connect(dart_db) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM disclosures WHERE stock_code = ?", (target_stock,))
            dart_count = cursor.fetchone()[0]
        
        with sqlite3.connect(news_db) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM news_articles WHERE stock_code = ?", (target_stock,))
            news_count = cursor.fetchone()[0]
            
            cursor = conn.execute("""
                SELECT COUNT(*) FROM news_articles 
                WHERE stock_code = ? AND created_at >= date('now', '-30 days')
            """, (target_stock,))
            recent_news_count = cursor.fetchone()[0]
        
        print(f"✅ 주가 데이터: {stock_count}개")
        print(f"✅ 재무 데이터: {dart_count}개") 
        print(f"✅ 뉴스 데이터: {news_count}개 (최근 30일: {recent_news_count}개)")
        
        # 데이터 품질 평가
        quality_score = 0
        if stock_count > 500:
            quality_score += 30
        if dart_count > 10:
            quality_score += 30
        if news_count > 100:
            quality_score += 40
        
        print(f"\n📈 데이터 품질 점수: {quality_score}/100점")
        
        if quality_score >= 80:
            print("🎉 우수: 투자 분석에 충분한 데이터")
        elif quality_score >= 60:
            print("👍 양호: 기본적인 분석 가능")
        else:
            print("⚠️ 부족: 추가 데이터 수집 필요")
            
    except Exception as e:
        print(f"❌ 종합 평가 실패: {e}")

if __name__ == "__main__":
    check_amorepacific_corrected()
