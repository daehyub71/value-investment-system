#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
실시간 뉴스 수집 모니터링 도구
collect_all_stocks_final.py 실행 중 뉴스 저장 상황을 실시간으로 체크
"""

import sqlite3
import os
import time
from datetime import datetime, timedelta

class NewsMonitor:
    def __init__(self):
        self.news_db = 'data/databases/news_data.db'
        self.last_count = 0
        
    def get_current_stats(self):
        """현재 뉴스 통계 조회"""
        try:
            with sqlite3.connect(self.news_db) as conn:
                cursor = conn.cursor()
                
                # 전체 뉴스 수
                cursor.execute("SELECT COUNT(*) FROM news_articles")
                total_count = cursor.fetchone()[0]
                
                # 오늘 저장된 뉴스 수
                cursor.execute("""
                    SELECT COUNT(*) FROM news_articles 
                    WHERE DATE(created_at) = DATE('now', 'localtime')
                """)
                today_count = cursor.fetchone()[0]
                
                # 최근 10분 저장된 뉴스 수
                cursor.execute("""
                    SELECT COUNT(*) FROM news_articles 
                    WHERE created_at >= datetime('now', '-10 minutes', 'localtime')
                """)
                recent_count = cursor.fetchone()[0]
                
                # 최근 저장된 종목 (상위 5개)
                cursor.execute("""
                    SELECT 
                        stock_code, 
                        company_name,
                        COUNT(*) as news_count,
                        MAX(created_at) as last_saved
                    FROM news_articles 
                    WHERE DATE(created_at) = DATE('now', 'localtime')
                    GROUP BY stock_code
                    ORDER BY last_saved DESC
                    LIMIT 5
                """)
                
                latest_stocks = cursor.fetchall()
                
                # 아모레퍼시픽 상태
                cursor.execute("""
                    SELECT COUNT(*), MAX(created_at)
                    FROM news_articles 
                    WHERE stock_code = '090430'
                """)
                
                amore_result = cursor.fetchone()
                amore_count, amore_last = amore_result if amore_result else (0, None)
                
                return {
                    'total_count': total_count,
                    'today_count': today_count,
                    'recent_count': recent_count,
                    'latest_stocks': latest_stocks,
                    'amore_count': amore_count,
                    'amore_last': amore_last
                }
                
        except Exception as e:
            return {'error': str(e)}
    
    def display_status(self, stats):
        """상태 표시"""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"⏰ {current_time}")
        print("=" * 60)
        
        if 'error' in stats:
            print(f"❌ 오류: {stats['error']}")
            return
        
        # 기본 통계
        print(f"📊 전체 뉴스: {stats['total_count']:,}개")
        print(f"📅 오늘 저장: {stats['today_count']:,}개")
        print(f"⚡ 최근 10분: {stats['recent_count']:,}개")
        
        # 증가량
        if self.last_count > 0:
            increase = stats['total_count'] - self.last_count
            print(f"📈 증가량: +{increase}개")
        
        self.last_count = stats['total_count']
        
        # 아모레퍼시픽
        print(f"🎯 아모레퍼시픽: {stats['amore_count']}개")
        if stats['amore_last']:
            print(f"   최근 업데이트: {stats['amore_last']}")
        
        # 최근 종목들
        print(f"\n📈 최근 저장 종목:")
        if stats['latest_stocks']:
            for stock_code, company_name, count, last_saved in stats['latest_stocks']:
                name = company_name if company_name else stock_code
                print(f"   {name}({stock_code}): {count}개 - {last_saved}")
        else:
            print("   오늘 저장된 뉴스 없음")
        
        # 상태 판단
        if stats['recent_count'] > 0:
            print(f"\n✅ 활발히 수집 중...")
        else:
            print(f"\n⏸️ 수집 일시 정지 또는 완료")
        
        print("-" * 60)

def quick_check():
    """빠른 체크"""
    os.chdir('C:/data_analysis/value-investment-system/value-investment-system')
    
    monitor = NewsMonitor()
    stats = monitor.get_current_stats()
    monitor.display_status(stats)

def continuous_monitor():
    """연속 모니터링"""
    os.chdir('C:/data_analysis/value-investment-system/value-investment-system')
    
    monitor = NewsMonitor()
    
    print("🚀 뉴스 수집 연속 모니터링 시작")
    print("   Ctrl+C로 중단")
    print()
    
    try:
        while True:
            stats = monitor.get_current_stats()
            monitor.display_status(stats)
            time.sleep(30)  # 30초마다 체크
            
    except KeyboardInterrupt:
        print("\n🛑 모니터링 중단")

def main():
    print("🔍 뉴스 수집 모니터링")
    print("1. 빠른 체크")
    print("2. 연속 모니터링 (30초마다)")
    
    choice = input("선택 (1-2): ").strip()
    
    if choice == "1":
        quick_check()
    elif choice == "2":
        continuous_monitor()
    else:
        print("빠른 체크를 실행합니다.")
        quick_check()

if __name__ == "__main__":
    main()
