#!/usr/bin/env python3
"""
데이터베이스 현황 분석 스크립트 (간단 버전)
로컬 DB 파일들의 현재 상태를 분석하고 수집이 필요한 데이터를 추천
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

class SimpleDatabaseAnalyzer:
    """간단한 데이터베이스 분석 클래스"""
    
    def __init__(self):
        self.db_path = Path("data/databases")
        
    def analyze_all_databases(self):
        """모든 데이터베이스 분석"""
        print("🔍 데이터베이스 현황 분석")
        print("=" * 60)
        
        db_files = {
            'stock_data.db': '주식 데이터',
            'dart_data.db': 'DART 재무데이터', 
            'news_data.db': '뉴스 데이터',
            'kis_data.db': 'KIS API 데이터'
        }
        
        recommendations = []
        
        for db_file, description in db_files.items():
            print(f"\n📊 {description} ({db_file})")
            print("-" * 40)
            
            db_path = self.db_path / db_file
            if not db_path.exists():
                print(f"❌ 파일 없음: {db_path}")
                recommendations.append(f"전체 {description} 수집 필요")
                continue
            
            file_size = db_path.stat().st_size / (1024 * 1024)  # MB
            print(f"📦 파일 크기: {file_size:.2f} MB")
            
            try:
                if db_file == 'stock_data.db':
                    recs = self.analyze_stock_data(db_path)
                elif db_file == 'dart_data.db':
                    recs = self.analyze_dart_data(db_path)
                elif db_file == 'news_data.db':
                    recs = self.analyze_news_data(db_path)
                elif db_file == 'kis_data.db':
                    recs = self.analyze_kis_data(db_path)
                
                recommendations.extend(recs)
                
            except Exception as e:
                print(f"❌ 분석 실패: {e}")
                recommendations.append(f"{description} 재수집 필요")
        
        self.print_recommendations(recommendations)
    
    def analyze_stock_data(self, db_path):
        """주식 데이터 분석"""
        recommendations = []
        
        with sqlite3.connect(db_path) as conn:
            # 테이블 목록 확인
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"📋 테이블: {', '.join(tables)}")
            
            # 주가 데이터 확인
            if 'stock_prices' in tables:
                # 최신 데이터 확인
                cursor = conn.execute("""
                    SELECT MAX(date) as latest_date, COUNT(DISTINCT stock_code) as stock_count
                    FROM stock_prices
                """)
                latest_data = cursor.fetchone()
                latest_date, stock_count = latest_data
                
                print(f"📅 최신 주가 데이터: {latest_date}")
                print(f"📈 종목 수: {stock_count}개")
                
                # 최신 날짜가 3일 전보다 오래되었으면 업데이트 필요
                if latest_date:
                    latest = datetime.strptime(latest_date, '%Y-%m-%d')
                    if (datetime.now() - latest).days > 3:
                        recommendations.append("최신 주가 데이터 업데이트 필요")
                else:
                    recommendations.append("주가 데이터 수집 필요")
            else:
                recommendations.append("주가 데이터 테이블 생성 및 수집 필요")
            
            # 기업 정보 확인
            if 'company_info' in tables:
                cursor = conn.execute("SELECT COUNT(*) FROM company_info")
                company_count = cursor.fetchone()[0]
                print(f"🏢 기업 정보: {company_count}개")
                
                if company_count < 100:
                    recommendations.append("기업 정보 데이터 보완 필요")
            else:
                recommendations.append("기업 정보 테이블 생성 및 수집 필요")
        
        return recommendations
    
    def analyze_dart_data(self, db_path):
        """DART 데이터 분석"""
        recommendations = []
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"📋 테이블: {', '.join(tables)}")
            
            # 기업코드 확인
            if 'corp_codes' in tables:
                cursor = conn.execute("SELECT COUNT(*) FROM corp_codes")
                corp_count = cursor.fetchone()[0]
                print(f"🏢 등록 기업: {corp_count}개")
                
                if corp_count < 2000:
                    recommendations.append("기업코드 업데이트 필요")
            else:
                recommendations.append("기업코드 수집 필요")
            
            # 재무제표 확인
            if 'financial_statements' in tables:
                # 최신 재무제표 확인
                cursor = conn.execute("""
                    SELECT MAX(bsns_year) as latest_year, COUNT(DISTINCT corp_code) as corp_count
                    FROM financial_statements
                """)
                latest_fs = cursor.fetchone()
                latest_year, corp_count = latest_fs
                
                print(f"📊 최신 재무제표: {latest_year}년")
                print(f"📈 재무제표 보유 기업: {corp_count}개")
                
                current_year = datetime.now().year
                if latest_year and latest_year < current_year - 1:
                    recommendations.append(f"{current_year-1}년, {current_year}년 재무제표 수집 필요")
            else:
                recommendations.append("재무제표 데이터 수집 필요")
        
        return recommendations
    
    def analyze_news_data(self, db_path):
        """뉴스 데이터 분석"""
        recommendations = []
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"📋 테이블: {', '.join(tables)}")
            
            if 'news_articles' in tables:
                # 최신 뉴스 확인
                cursor = conn.execute("""
                    SELECT MAX(pubDate) as latest_date, COUNT(*) as total_count,
                           COUNT(DISTINCT stock_code) as stock_count
                    FROM news_articles
                """)
                latest_news = cursor.fetchone()
                latest_date, total_count, stock_count = latest_news
                
                print(f"📰 최신 뉴스: {latest_date}")
                print(f"📊 총 뉴스: {total_count}개")
                print(f"📈 대상 종목: {stock_count}개")
                
                # 최신 뉴스가 7일 전보다 오래되었으면 업데이트 필요
                if latest_date:
                    try:
                        latest = datetime.strptime(latest_date[:10], '%Y-%m-%d')
                        if (datetime.now() - latest).days > 7:
                            recommendations.append("최신 뉴스 데이터 수집 필요")
                    except:
                        recommendations.append("뉴스 날짜 형식 확인 필요")
                else:
                    recommendations.append("뉴스 데이터 수집 필요")
            else:
                recommendations.append("뉴스 데이터 테이블 생성 및 수집 필요")
        
        return recommendations
    
    def analyze_kis_data(self, db_path):
        """KIS 데이터 분석"""
        recommendations = []
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            if not tables:
                print("📋 테이블: 없음")
                recommendations.append("KIS API 데이터 수집 필요")
            else:
                print(f"📋 테이블: {', '.join(tables)}")
                # KIS 데이터는 실시간성이 중요하므로 매일 업데이트 권장
                recommendations.append("KIS 실시간 데이터 정기 업데이트 권장")
        
        return recommendations
    
    def print_recommendations(self, recommendations):
        """수집 권장사항 출력"""
        print("\n" + "=" * 60)
        print("🎯 데이터 수집 권장사항")
        print("=" * 60)
        
        if not recommendations:
            print("✅ 모든 데이터가 최신 상태입니다!")
            return
        
        # 우선순위별 분류
        critical = []  # 필수
        update = []    # 업데이트
        optional = []  # 선택
        
        for rec in recommendations:
            if "필요" in rec and ("수집" in rec or "생성" in rec):
                critical.append(rec)
            elif "업데이트" in rec or "최신" in rec:
                update.append(rec)
            else:
                optional.append(rec)
        
        if critical:
            print("\n🚨 필수 수집 항목:")
            for i, rec in enumerate(critical, 1):
                print(f"  {i}. {rec}")
        
        if update:
            print("\n⚡ 업데이트 권장 항목:")
            for i, rec in enumerate(update, 1):
                print(f"  {i}. {rec}")
        
        if optional:
            print("\n💡 선택 항목:")
            for i, rec in enumerate(optional, 1):
                print(f"  {i}. {rec}")
        
        print("\n📋 추천 수집 명령어:")
        self.generate_collection_commands(recommendations)
    
    def generate_collection_commands(self, recommendations):
        """수집 명령어 생성"""
        commands = []
        
        # 기업코드 수집
        if any("기업코드" in rec for rec in recommendations):
            commands.append("# 1. 기업코드 수집")
            commands.append("python scripts/data_collection/collect_dart_data.py --corp_codes")
        
        # 최신 주가 데이터
        if any("주가" in rec for rec in recommendations):
            commands.append("\n# 2. 최신 주가 데이터 수집 (최근 30일)")
            today = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            commands.append(f"python scripts/data_collection/collect_stock_data.py --start_date={start_date} --end_date={today}")
        
        # 재무제표 데이터
        if any("재무제표" in rec for rec in recommendations):
            current_year = datetime.now().year
            commands.append(f"\n# 3. 최신 재무제표 수집 ({current_year-1}년, {current_year}년)")
            commands.append(f"python scripts/data_collection/collect_dart_data.py --year={current_year-1}")
            commands.append(f"python scripts/data_collection/collect_dart_data.py --year={current_year}")
        
        # 뉴스 데이터
        if any("뉴스" in rec for rec in recommendations):
            commands.append("\n# 4. 최신 뉴스 수집 (최근 7일)")
            commands.append("python scripts/data_collection/collect_news_data.py --days=7 --update_all")
        
        # 전체 데이터 수집 (새 프로젝트인 경우)
        if len([r for r in recommendations if "필요" in r and "수집" in r]) >= 3:
            commands.append("\n# 📦 전체 데이터 초기 수집 (새 프로젝트)")
            commands.append("python scripts/setup_project.py --init_all_data")
        
        for cmd in commands:
            print(cmd)

if __name__ == "__main__":
    analyzer = SimpleDatabaseAnalyzer()
    analyzer.analyze_all_databases()
