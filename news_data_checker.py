#!/usr/bin/env python3
"""
뉴스 데이터 확인 및 분석 도구
수집된 뉴스 데이터의 현황을 확인하고 기본 분석을 수행합니다.
"""

import sqlite3
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os

class NewsDataChecker:
    """뉴스 데이터 확인 및 분석 클래스"""
    
    def __init__(self, db_path: str = None):
        """
        초기화
        
        Args:
            db_path: 데이터베이스 파일 경로 (기본값: 자동 탐지)
        """
        if db_path is None:
            # 기본 경로들에서 데이터베이스 파일 찾기
            possible_paths = [
                "data/databases/news_data.db",
                "C:/data_analysis/value-investment-system/value-investment-system/data/databases/news_data.db",
                "./news_data.db"
            ]
            
            for path in possible_paths:
                if Path(path).exists():
                    self.db_path = path
                    break
            else:
                raise FileNotFoundError("뉴스 데이터베이스 파일을 찾을 수 없습니다.")
        else:
            self.db_path = db_path
        
        print(f"📍 데이터베이스 위치: {self.db_path}")
    
    def get_connection(self):
        """데이터베이스 연결 반환"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def check_database_info(self):
        """데이터베이스 기본 정보 확인"""
        print("\n" + "="*60)
        print("🔍 뉴스 데이터베이스 기본 정보")
        print("="*60)
        
        with self.get_connection() as conn:
            # 파일 크기 확인
            file_size = os.path.getsize(self.db_path) / (1024 * 1024)  # MB
            print(f"📁 파일 크기: {file_size:.2f} MB")
            
            # 테이블 목록 확인
            tables = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """).fetchall()
            
            print(f"📋 테이블 수: {len(tables)}")
            print("📋 테이블 목록:")
            for table in tables:
                print(f"   - {table['name']}")
    
    def check_news_articles(self):
        """뉴스 기사 데이터 확인"""
        print("\n" + "="*60)
        print("📰 뉴스 기사 데이터 현황")
        print("="*60)
        
        with self.get_connection() as conn:
            # 전체 기사 수
            total_count = conn.execute("SELECT COUNT(*) as count FROM news_articles").fetchone()['count']
            print(f"📊 총 뉴스 기사 수: {total_count:,}개")
            
            if total_count == 0:
                print("⚠️  저장된 뉴스 기사가 없습니다.")
                return
            
            # 최신/최오래된 기사 날짜
            date_info = conn.execute("""
                SELECT 
                    MIN(pubDate) as earliest,
                    MAX(pubDate) as latest,
                    COUNT(DISTINCT pubDate) as unique_dates
                FROM news_articles
            """).fetchone()
            
            print(f"📅 기사 수집 기간: {date_info['earliest']} ~ {date_info['latest']}")
            print(f"📅 수집된 날짜 수: {date_info['unique_dates']}일")
            
            # 종목별 기사 수
            stock_stats = conn.execute("""
                SELECT 
                    stock_code,
                    company_name,
                    COUNT(*) as article_count
                FROM news_articles 
                WHERE stock_code IS NOT NULL
                GROUP BY stock_code, company_name
                ORDER BY article_count DESC
                LIMIT 10
            """).fetchall()
            
            if stock_stats:
                print(f"\n📈 종목별 뉴스 기사 수 (상위 10개):")
                for stock in stock_stats:
                    print(f"   {stock['stock_code']} ({stock['company_name']}): {stock['article_count']}개")
            
            # 뉴스 소스별 통계
            source_stats = conn.execute("""
                SELECT 
                    source,
                    COUNT(*) as count
                FROM news_articles
                GROUP BY source
                ORDER BY count DESC
            """).fetchall()
            
            print(f"\n📺 뉴스 소스별 기사 수:")
            for source in source_stats:
                print(f"   {source['source']}: {source['count']}개")
    
    def check_sentiment_scores(self):
        """감정분석 점수 확인"""
        print("\n" + "="*60)
        print("😊 감정분석 데이터 현황")
        print("="*60)
        
        with self.get_connection() as conn:
            # 감정분석 점수 통계
            sentiment_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_scores,
                    AVG(sentiment_score) as avg_sentiment,
                    AVG(positive_score) as avg_positive,
                    AVG(negative_score) as avg_negative,
                    AVG(neutral_score) as avg_neutral,
                    AVG(confidence) as avg_confidence
                FROM sentiment_scores
            """).fetchone()
            
            if sentiment_stats['total_scores'] == 0:
                print("⚠️  감정분석 데이터가 없습니다.")
                return
            
            print(f"📊 총 감정분석 점수: {sentiment_stats['total_scores']:,}개")
            print(f"📊 평균 감정 점수: {sentiment_stats['avg_sentiment']:.3f}")
            print(f"📊 평균 긍정 점수: {sentiment_stats['avg_positive']:.3f}")
            print(f"📊 평균 부정 점수: {sentiment_stats['avg_negative']:.3f}")
            print(f"📊 평균 중립 점수: {sentiment_stats['avg_neutral']:.3f}")
            print(f"📊 평균 신뢰도: {sentiment_stats['avg_confidence']:.3f}")
            
            # 종목별 감정 점수
            stock_sentiment = conn.execute("""
                SELECT 
                    stock_code,
                    COUNT(*) as score_count,
                    AVG(sentiment_score) as avg_sentiment,
                    AVG(confidence) as avg_confidence
                FROM sentiment_scores
                WHERE stock_code IS NOT NULL
                GROUP BY stock_code
                ORDER BY avg_sentiment DESC
                LIMIT 10
            """).fetchall()
            
            if stock_sentiment:
                print(f"\n📈 종목별 평균 감정점수 (상위 10개):")
                for stock in stock_sentiment:
                    sentiment_emoji = "😊" if stock['avg_sentiment'] > 0.1 else "😐" if stock['avg_sentiment'] > -0.1 else "😔"
                    print(f"   {stock['stock_code']}: {stock['avg_sentiment']:.3f} {sentiment_emoji} ({stock['score_count']}개)")
    
    def show_sample_data(self, limit: int = 5):
        """샘플 데이터 보기"""
        print("\n" + "="*60)
        print("📋 샘플 데이터")
        print("="*60)
        
        with self.get_connection() as conn:
            # 최신 뉴스 기사 샘플
            print("📰 최신 뉴스 기사 샘플:")
            news_samples = conn.execute(f"""
                SELECT title, company_name, pubDate, source
                FROM news_articles
                ORDER BY created_at DESC
                LIMIT {limit}
            """).fetchall()
            
            for i, news in enumerate(news_samples, 1):
                print(f"   {i}. [{news['source']}] {news['title'][:60]}...")
                print(f"      회사: {news['company_name']}, 날짜: {news['pubDate']}")
            
            # 감정분석 샘플
            print(f"\n😊 감정분석 결과 샘플:")
            sentiment_samples = conn.execute(f"""
                SELECT s.sentiment_score, s.positive_score, s.negative_score, 
                       s.confidence, n.title
                FROM sentiment_scores s
                JOIN news_articles n ON s.news_id = n.id
                ORDER BY s.created_at DESC
                LIMIT {limit}
            """).fetchall()
            
            for i, sentiment in enumerate(sentiment_samples, 1):
                sentiment_emoji = "😊" if sentiment['sentiment_score'] > 0.1 else "😐" if sentiment['sentiment_score'] > -0.1 else "😔"
                print(f"   {i}. {sentiment_emoji} 감정점수: {sentiment['sentiment_score']:.3f} (신뢰도: {sentiment['confidence']:.3f})")
                print(f"      제목: {sentiment['title'][:60]}...")
    
    def export_to_excel(self, output_path: str = "news_analysis_report.xlsx"):
        """엑셀 파일로 데이터 내보내기"""
        print(f"\n📊 엑셀 리포트 생성 중: {output_path}")
        
        with self.get_connection() as conn:
            # 뉴스 기사 데이터
            news_df = pd.read_sql_query("""
                SELECT title, company_name, stock_code, pubDate, source, description
                FROM news_articles
                ORDER BY pubDate DESC
            """, conn)
            
            # 감정분석 데이터
            sentiment_df = pd.read_sql_query("""
                SELECT n.title, n.company_name, n.stock_code, s.sentiment_score,
                       s.positive_score, s.negative_score, s.neutral_score, s.confidence
                FROM sentiment_scores s
                JOIN news_articles n ON s.news_id = n.id
                ORDER BY s.created_at DESC
            """, conn)
            
            # 종목별 통계
            stock_stats_df = pd.read_sql_query("""
                SELECT 
                    stock_code,
                    company_name,
                    COUNT(*) as article_count,
                    AVG(s.sentiment_score) as avg_sentiment,
                    AVG(s.confidence) as avg_confidence
                FROM news_articles n
                LEFT JOIN sentiment_scores s ON n.id = s.news_id
                WHERE stock_code IS NOT NULL
                GROUP BY stock_code, company_name
                ORDER BY article_count DESC
            """, conn)
        
        # 엑셀 파일로 저장
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            news_df.to_excel(writer, sheet_name='뉴스기사', index=False)
            sentiment_df.to_excel(writer, sheet_name='감정분석', index=False)
            stock_stats_df.to_excel(writer, sheet_name='종목별통계', index=False)
        
        print(f"✅ 엑셀 리포트가 생성되었습니다: {output_path}")
    
    def generate_daily_report(self):
        """일별 수집 현황 리포트"""
        print("\n" + "="*60)
        print("📅 일별 뉴스 수집 현황")
        print("="*60)
        
        with self.get_connection() as conn:
            daily_stats = conn.execute("""
                SELECT 
                    DATE(pubDate) as date,
                    COUNT(*) as article_count,
                    COUNT(DISTINCT stock_code) as unique_stocks,
                    AVG(s.sentiment_score) as avg_sentiment
                FROM news_articles n
                LEFT JOIN sentiment_scores s ON n.id = s.news_id
                WHERE pubDate IS NOT NULL
                GROUP BY DATE(pubDate)
                ORDER BY date DESC
                LIMIT 10
            """).fetchall()
            
            for stat in daily_stats:
                sentiment_trend = "📈" if stat['avg_sentiment'] and stat['avg_sentiment'] > 0 else "📉" if stat['avg_sentiment'] and stat['avg_sentiment'] < 0 else "➡️"
                print(f"📅 {stat['date']}: {stat['article_count']}개 기사, {stat['unique_stocks']}개 종목 {sentiment_trend}")

def main():
    """메인 실행 함수"""
    try:
        # 뉴스 데이터 체커 초기화
        checker = NewsDataChecker()
        
        # 전체 분석 실행
        checker.check_database_info()
        checker.check_news_articles()
        checker.check_sentiment_scores()
        checker.show_sample_data()
        checker.generate_daily_report()
        
        # 엑셀 리포트 생성 (선택사항)
        print("\n" + "="*60)
        export_choice = input("📊 엑셀 리포트를 생성하시겠습니까? (y/n): ").lower()
        if export_choice == 'y':
            checker.export_to_excel()
        
        print("\n✅ 뉴스 데이터 확인이 완료되었습니다!")
        
    except Exception as e:
        print(f"❌ 오류가 발생했습니다: {e}")
        print("📍 데이터베이스 파일 경로를 확인해주세요.")

if __name__ == "__main__":
    main()