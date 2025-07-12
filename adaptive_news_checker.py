#!/usr/bin/env python3
"""
적응형 뉴스 데이터 확인 도구
실제 데이터베이스 구조를 확인하고 그에 맞춰 분석을 수행
"""

import sqlite3
import os
from pathlib import Path
from datetime import datetime

class AdaptiveNewsChecker:
    """실제 테이블 구조에 맞춰 동작하는 뉴스 데이터 확인 클래스"""
    
    def __init__(self, db_path: str = None):
        """초기화"""
        if db_path is None:
            # 기본 경로들에서 데이터베이스 파일 찾기
            possible_paths = [
                "data/databases/news_data.db",
                "C:/data_analysis/value-investment-system/value-investment-system/data/databases/news_data.db",
                "./news_data.db",
                "../data/databases/news_data.db"
            ]
            
            for path in possible_paths:
                if Path(path).exists():
                    self.db_path = path
                    break
            else:
                print("❌ 뉴스 데이터베이스 파일을 찾을 수 없습니다.")
                print("🔍 다음 경로들을 확인했습니다:")
                for path in possible_paths:
                    print(f"   - {path}")
                exit(1)
        else:
            self.db_path = db_path
        
        print(f"📍 데이터베이스 위치: {self.db_path}")
        
        # 테이블 스키마 확인
        self.table_schemas = self._get_table_schemas()
    
    def _get_table_schemas(self) -> dict:
        """실제 테이블 구조 확인"""
        schemas = {}
        
        try:
            with self.get_connection() as conn:
                # 모든 테이블 목록 가져오기
                tables = conn.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """).fetchall()
                
                for table in tables:
                    table_name = table['name']
                    
                    # 각 테이블의 컬럼 정보 가져오기
                    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
                    
                    schemas[table_name] = {
                        'columns': [col['name'] for col in columns],
                        'column_info': {col['name']: {
                            'type': col['type'],
                            'not_null': col['notnull'],
                            'default': col['dflt_value']
                        } for col in columns}
                    }
                    
        except Exception as e:
            print(f"❌ 테이블 스키마 확인 실패: {e}")
            schemas = {}
        
        return schemas
    
    def get_connection(self):
        """데이터베이스 연결 반환"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def print_separator(self, title: str):
        """구분선 출력"""
        print("\n" + "="*60)
        print(f"🔍 {title}")
        print("="*60)
    
    def check_table_structures(self):
        """테이블 구조 상세 확인"""
        self.print_separator("테이블 구조 분석")
        
        for table_name, schema in self.table_schemas.items():
            print(f"\n📋 {table_name} 테이블:")
            print(f"   컬럼 수: {len(schema['columns'])}")
            print("   컬럼 목록:")
            
            for col_name in schema['columns']:
                col_info = schema['column_info'][col_name]
                not_null = " NOT NULL" if col_info['not_null'] else ""
                default = f" DEFAULT {col_info['default']}" if col_info['default'] else ""
                print(f"     - {col_name}: {col_info['type']}{not_null}{default}")
    
    def check_database_info(self):
        """데이터베이스 기본 정보 확인"""
        self.print_separator("데이터베이스 기본 정보")
        
        try:
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
                    # 각 테이블의 레코드 수 확인
                    try:
                        count = conn.execute(f"SELECT COUNT(*) as count FROM {table['name']}").fetchone()['count']
                        print(f"   - {table['name']}: {count:,}개 레코드")
                    except Exception as e:
                        print(f"   - {table['name']}: 확인 실패 ({e})")
                        
        except Exception as e:
            print(f"❌ 데이터베이스 연결 실패: {e}")
    
    def check_news_articles(self):
        """뉴스 기사 데이터 확인 (실제 스키마 기반)"""
        self.print_separator("뉴스 기사 데이터 현황")
        
        if 'news_articles' not in self.table_schemas:
            print("❌ news_articles 테이블이 없습니다.")
            return
            
        try:
            with self.get_connection() as conn:
                # 전체 기사 수
                total_count = conn.execute("SELECT COUNT(*) as count FROM news_articles").fetchone()['count']
                print(f"📊 총 뉴스 기사 수: {total_count:,}개")
                
                if total_count == 0:
                    print("⚠️  저장된 뉴스 기사가 없습니다.")
                    return
                
                # 날짜 정보 확인 (pubDate 컬럼이 있는지 확인)
                columns = self.table_schemas['news_articles']['columns']
                
                if 'pubDate' in columns:
                    try:
                        date_info = conn.execute("""
                            SELECT 
                                MIN(pubDate) as earliest,
                                MAX(pubDate) as latest,
                                COUNT(DISTINCT DATE(pubDate)) as unique_dates
                            FROM news_articles
                            WHERE pubDate IS NOT NULL
                        """).fetchone()
                        
                        if date_info['earliest']:
                            print(f"📅 기사 발행 기간: {date_info['earliest']} ~ {date_info['latest']}")
                            print(f"📅 발행된 날짜 수: {date_info['unique_dates']}일")
                    except Exception as e:
                        print(f"⚠️  날짜 정보 확인 실패: {e}")
                
                # 종목코드별 통계 (stock_code 컬럼이 있는지 확인)
                if 'stock_code' in columns:
                    print(f"\n📈 종목별 뉴스 기사 수 (상위 10개):")
                    try:
                        # company_name이 있는지 확인
                        if 'company_name' in columns:
                            query = """
                                SELECT 
                                    stock_code,
                                    company_name,
                                    COUNT(*) as article_count
                                FROM news_articles 
                                WHERE stock_code IS NOT NULL AND stock_code != ''
                                GROUP BY stock_code, company_name
                                ORDER BY article_count DESC
                                LIMIT 10
                            """
                        else:
                            query = """
                                SELECT 
                                    stock_code,
                                    COUNT(*) as article_count
                                FROM news_articles 
                                WHERE stock_code IS NOT NULL AND stock_code != ''
                                GROUP BY stock_code
                                ORDER BY article_count DESC
                                LIMIT 10
                            """
                        
                        stock_stats = conn.execute(query).fetchall()
                        
                        if stock_stats:
                            for i, stock in enumerate(stock_stats, 1):
                                if 'company_name' in columns and stock.get('company_name'):
                                    company = stock['company_name']
                                    print(f"   {i:2d}. {stock['stock_code']} ({company}): {stock['article_count']:,}개")
                                else:
                                    print(f"   {i:2d}. {stock['stock_code']}: {stock['article_count']:,}개")
                        else:
                            print("   종목 정보가 있는 뉴스가 없습니다.")
                    except Exception as e:
                        print(f"   ⚠️  종목별 통계 확인 실패: {e}")
                
                # 뉴스 소스별 통계 (source 컬럼이 있는지 확인)
                if 'source' in columns:
                    print(f"\n📺 뉴스 소스별 기사 수:")
                    try:
                        source_stats = conn.execute("""
                            SELECT 
                                COALESCE(source, '알 수 없음') as source,
                                COUNT(*) as count
                            FROM news_articles
                            GROUP BY source
                            ORDER BY count DESC
                            LIMIT 10
                        """).fetchall()
                        
                        for i, source in enumerate(source_stats, 1):
                            print(f"   {i:2d}. {source['source']}: {source['count']:,}개")
                    except Exception as e:
                        print(f"   ⚠️  소스별 통계 확인 실패: {e}")
                
                # 카테고리별 통계 (category 컬럼이 있는지 확인)
                if 'category' in columns:
                    print(f"\n📂 카테고리별 기사 수:")
                    try:
                        category_stats = conn.execute("""
                            SELECT 
                                COALESCE(category, '미분류') as category,
                                COUNT(*) as count
                            FROM news_articles
                            GROUP BY category
                            ORDER BY count DESC
                        """).fetchall()
                        
                        for i, cat in enumerate(category_stats, 1):
                            print(f"   {i:2d}. {cat['category']}: {cat['count']:,}개")
                    except Exception as e:
                        print(f"   ⚠️  카테고리별 통계 확인 실패: {e}")
                        
        except Exception as e:
            print(f"❌ 뉴스 기사 확인 실패: {e}")
    
    def check_sentiment_scores(self):
        """감정분석 점수 확인 (실제 스키마 기반)"""
        self.print_separator("감정분석 데이터 현황")
        
        if 'sentiment_scores' not in self.table_schemas:
            print("❌ sentiment_scores 테이블이 없습니다.")
            return
            
        try:
            with self.get_connection() as conn:
                # 전체 감정분석 수
                total_count = conn.execute("SELECT COUNT(*) as count FROM sentiment_scores").fetchone()['count']
                print(f"📊 총 감정분석 점수: {total_count:,}개")
                
                if total_count == 0:
                    print("⚠️  감정분석 데이터가 없습니다.")
                    return
                
                # 사용 가능한 컬럼 확인
                columns = self.table_schemas['sentiment_scores']['columns']
                
                # 기본 통계
                stats_query = "SELECT COUNT(*) as total_scores"
                
                if 'sentiment_score' in columns:
                    stats_query += ", AVG(sentiment_score) as avg_sentiment"
                if 'positive_score' in columns:
                    stats_query += ", AVG(positive_score) as avg_positive"
                if 'negative_score' in columns:
                    stats_query += ", AVG(negative_score) as avg_negative"
                if 'neutral_score' in columns:
                    stats_query += ", AVG(neutral_score) as avg_neutral"
                if 'confidence' in columns:
                    stats_query += ", AVG(confidence) as avg_confidence"
                
                stats_query += " FROM sentiment_scores"
                
                sentiment_stats = conn.execute(stats_query).fetchone()
                
                if 'sentiment_score' in columns and sentiment_stats.get('avg_sentiment') is not None:
                    print(f"📊 평균 감정 점수: {sentiment_stats['avg_sentiment']:.3f}")
                if 'positive_score' in columns and sentiment_stats.get('avg_positive') is not None:
                    print(f"📊 평균 긍정 점수: {sentiment_stats['avg_positive']:.3f}")
                if 'negative_score' in columns and sentiment_stats.get('avg_negative') is not None:
                    print(f"📊 평균 부정 점수: {sentiment_stats['avg_negative']:.3f}")
                if 'neutral_score' in columns and sentiment_stats.get('avg_neutral') is not None:
                    print(f"📊 평균 중립 점수: {sentiment_stats['avg_neutral']:.3f}")
                if 'confidence' in columns and sentiment_stats.get('avg_confidence') is not None:
                    print(f"📊 평균 신뢰도: {sentiment_stats['avg_confidence']:.3f}")
                
                # 종목별 감정 점수 (stock_code가 있는 경우)
                if 'stock_code' in columns and 'sentiment_score' in columns:
                    print(f"\n📈 종목별 평균 감정점수 (상위 10개):")
                    try:
                        stock_sentiment = conn.execute("""
                            SELECT 
                                stock_code,
                                COUNT(*) as score_count,
                                AVG(sentiment_score) as avg_sentiment
                            FROM sentiment_scores
                            WHERE stock_code IS NOT NULL AND stock_code != ''
                            GROUP BY stock_code
                            ORDER BY avg_sentiment DESC
                            LIMIT 10
                        """).fetchall()
                        
                        if stock_sentiment:
                            for i, stock in enumerate(stock_sentiment, 1):
                                sentiment_emoji = "😊" if stock['avg_sentiment'] > 0.1 else "😐" if stock['avg_sentiment'] > -0.1 else "😔"
                                print(f"   {i:2d}. {stock['stock_code']}: {stock['avg_sentiment']:.3f} {sentiment_emoji} ({stock['score_count']}개)")
                        else:
                            print("   종목별 감정분석 데이터가 없습니다.")
                    except Exception as e:
                        print(f"   ⚠️  종목별 감정분석 확인 실패: {e}")
                        
        except Exception as e:
            print(f"❌ 감정분석 확인 실패: {e}")
    
    def show_sample_data(self, limit: int = 5):
        """샘플 데이터 보기 (실제 스키마 기반)"""
        self.print_separator("샘플 데이터")
        
        try:
            with self.get_connection() as conn:
                # 뉴스 기사 샘플
                if 'news_articles' in self.table_schemas:
                    print("📰 최신 뉴스 기사 샘플:")
                    try:
                        columns = self.table_schemas['news_articles']['columns']
                        
                        # 기본 컬럼들
                        select_columns = ['title']
                        if 'company_name' in columns:
                            select_columns.append('company_name')
                        if 'stock_code' in columns:
                            select_columns.append('stock_code')
                        if 'pubDate' in columns:
                            select_columns.append('pubDate')
                        if 'source' in columns:
                            select_columns.append('source')
                        
                        query = f"SELECT {', '.join(select_columns)} FROM news_articles ORDER BY "
                        
                        # 정렬 기준 결정
                        if 'created_at' in columns:
                            query += "created_at DESC"
                        elif 'pubDate' in columns:
                            query += "pubDate DESC"
                        else:
                            query += "id DESC"
                        
                        query += f" LIMIT {limit}"
                        
                        news_samples = conn.execute(query).fetchall()
                        
                        for i, news in enumerate(news_samples, 1):
                            title = news['title'][:60] + "..." if len(news['title']) > 60 else news['title']
                            
                            extra_info = []
                            if 'company_name' in columns and news.get('company_name'):
                                extra_info.append(f"회사: {news['company_name']}")
                            elif 'stock_code' in columns and news.get('stock_code'):
                                extra_info.append(f"종목: {news['stock_code']}")
                            if 'source' in columns and news.get('source'):
                                extra_info.append(f"출처: {news['source']}")
                            if 'pubDate' in columns and news.get('pubDate'):
                                extra_info.append(f"발행일: {news['pubDate']}")
                            
                            print(f"   {i}. {title}")
                            if extra_info:
                                print(f"      {', '.join(extra_info)}")
                                
                    except Exception as e:
                        print(f"   ⚠️  뉴스 샘플 조회 실패: {e}")
                
                # 감정분석 샘플
                if 'sentiment_scores' in self.table_schemas:
                    print(f"\n😊 감정분석 결과 샘플:")
                    try:
                        columns = self.table_schemas['sentiment_scores']['columns']
                        
                        # 기본 컬럼들
                        select_columns = []
                        if 'sentiment_score' in columns:
                            select_columns.append('sentiment_score')
                        if 'positive_score' in columns:
                            select_columns.append('positive_score')
                        if 'negative_score' in columns:
                            select_columns.append('negative_score')
                        if 'confidence' in columns:
                            select_columns.append('confidence')
                        if 'stock_code' in columns:
                            select_columns.append('stock_code')
                        
                        if select_columns:
                            query = f"SELECT {', '.join(select_columns)} FROM sentiment_scores ORDER BY "
                            
                            if 'created_at' in columns:
                                query += "created_at DESC"
                            else:
                                query += "id DESC"
                            
                            query += f" LIMIT {limit}"
                            
                            sentiment_samples = conn.execute(query).fetchall()
                            
                            for i, sentiment in enumerate(sentiment_samples, 1):
                                info_parts = []
                                
                                if 'sentiment_score' in columns and sentiment.get('sentiment_score') is not None:
                                    score = sentiment['sentiment_score']
                                    sentiment_emoji = "😊" if score > 0.1 else "😐" if score > -0.1 else "😔"
                                    info_parts.append(f"감정점수: {score:.3f} {sentiment_emoji}")
                                
                                if 'confidence' in columns and sentiment.get('confidence') is not None:
                                    info_parts.append(f"신뢰도: {sentiment['confidence']:.3f}")
                                
                                if 'stock_code' in columns and sentiment.get('stock_code'):
                                    info_parts.append(f"종목: {sentiment['stock_code']}")
                                
                                if info_parts:
                                    print(f"   {i}. {', '.join(info_parts)}")
                        else:
                            print("   감정분석 데이터 구조를 파악할 수 없습니다.")
                            
                    except Exception as e:
                        print(f"   ⚠️  감정분석 샘플 조회 실패: {e}")
                        
        except Exception as e:
            print(f"❌ 샘플 데이터 확인 실패: {e}")
    
    def generate_daily_report(self):
        """일별 수집 현황 리포트 (실제 스키마 기반)"""
        self.print_separator("일별 뉴스 수집 현황")
        
        if 'news_articles' not in self.table_schemas:
            print("❌ news_articles 테이블이 없습니다.")
            return
            
        try:
            with self.get_connection() as conn:
                columns = self.table_schemas['news_articles']['columns']
                
                if 'pubDate' in columns:
                    query = """
                        SELECT 
                            DATE(pubDate) as date,
                            COUNT(*) as article_count
                    """
                    
                    if 'stock_code' in columns:
                        query += ", COUNT(DISTINCT stock_code) as unique_stocks"
                    
                    query += """
                        FROM news_articles
                        WHERE pubDate IS NOT NULL
                        GROUP BY DATE(pubDate)
                        ORDER BY date DESC
                        LIMIT 10
                    """
                    
                    daily_stats = conn.execute(query).fetchall()
                    
                    if daily_stats:
                        for stat in daily_stats:
                            info = f"📅 {stat['date']}: {stat['article_count']:,}개 기사"
                            if 'unique_stocks' in stat.keys():
                                info += f", {stat['unique_stocks']}개 종목"
                            print(info)
                    else:
                        print("   일별 통계 데이터가 없습니다.")
                else:
                    print("   pubDate 컬럼이 없어 일별 통계를 생성할 수 없습니다.")
                    
        except Exception as e:
            print(f"❌ 일별 리포트 생성 실패: {e}")
    
    def check_data_quality(self):
        """데이터 품질 확인 (실제 스키마 기반)"""
        self.print_separator("데이터 품질 확인")
        
        try:
            with self.get_connection() as conn:
                # news_articles 테이블 품질 확인
                if 'news_articles' in self.table_schemas:
                    print("📋 뉴스 기사 NULL 값 확인:")
                    columns = self.table_schemas['news_articles']['columns']
                    
                    null_check_parts = []
                    if 'title' in columns:
                        null_check_parts.append("SUM(CASE WHEN title IS NULL OR title = '' THEN 1 ELSE 0 END) as NULL_title")
                    if 'stock_code' in columns:
                        null_check_parts.append("SUM(CASE WHEN stock_code IS NULL OR stock_code = '' THEN 1 ELSE 0 END) as NULL_stock_code")
                    if 'pubDate' in columns:
                        null_check_parts.append("SUM(CASE WHEN pubDate IS NULL OR pubDate = '' THEN 1 ELSE 0 END) as NULL_pubDate")
                    if 'source' in columns:
                        null_check_parts.append("SUM(CASE WHEN source IS NULL OR source = '' THEN 1 ELSE 0 END) as NULL_source")
                    
                    if null_check_parts:
                        query = f"SELECT {', '.join(null_check_parts)} FROM news_articles"
                        null_checks = conn.execute(query).fetchone()
                        
                        for key in null_checks.keys():
                            field_name = key.replace('NULL_', '')
                            print(f"   - {field_name} 누락: {null_checks[key]}개")
                    
                    # 중복 확인
                    if 'title' in columns:
                        duplicates = conn.execute("""
                            SELECT COUNT(*) as duplicate_count
                            FROM (
                                SELECT title, COUNT(*) as cnt
                                FROM news_articles
                                GROUP BY title
                                HAVING COUNT(*) > 1
                            )
                        """).fetchone()['duplicate_count']
                        
                        print(f"📋 중복 제목: {duplicates}개")
                
                # sentiment_scores 테이블 품질 확인
                if 'sentiment_scores' in self.table_schemas:
                    print("\n📋 감정분석 NULL 값 확인:")
                    columns = self.table_schemas['sentiment_scores']['columns']
                    
                    null_check_parts = []
                    if 'sentiment_score' in columns:
                        null_check_parts.append("SUM(CASE WHEN sentiment_score IS NULL THEN 1 ELSE 0 END) as NULL_sentiment")
                    if 'confidence' in columns:
                        null_check_parts.append("SUM(CASE WHEN confidence IS NULL THEN 1 ELSE 0 END) as NULL_confidence")
                    if 'stock_code' in columns:
                        null_check_parts.append("SUM(CASE WHEN stock_code IS NULL OR stock_code = '' THEN 1 ELSE 0 END) as NULL_stock_code")
                    
                    if null_check_parts:
                        query = f"SELECT {', '.join(null_check_parts)} FROM sentiment_scores"
                        null_checks = conn.execute(query).fetchone()
                        
                        for key in null_checks.keys():
                            field_name = key.replace('NULL_', '')
                            print(f"   - {field_name} 누락: {null_checks[key]}개")
                
        except Exception as e:
            print(f"❌ 데이터 품질 확인 실패: {e}")

def main():
    """메인 실행 함수"""
    print("🚀 적응형 뉴스 데이터 확인 도구 시작")
    print("=" * 60)
    
    try:
        # 뉴스 데이터 체커 초기화
        checker = AdaptiveNewsChecker()
        
        # 전체 분석 실행
        checker.check_database_info()
        checker.check_table_structures()
        checker.check_news_articles()
        checker.check_sentiment_scores()
        checker.show_sample_data()
        checker.generate_daily_report()
        checker.check_data_quality()
        
        print("\n" + "="*60)
        print("✅ 뉴스 데이터 확인이 완료되었습니다!")
        print("💡 더 자세한 분석을 위해서는 다음 라이브러리를 설치하세요:")
        print("   pip install pandas matplotlib seaborn openpyxl")
        
    except Exception as e:
        print(f"❌ 전체 오류가 발생했습니다: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()