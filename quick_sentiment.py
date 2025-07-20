#!/usr/bin/env python3
"""
빠른 감정분석 실행기
복잡한 import 없이 77,729건 뉴스 데이터 감정분석
"""

import sqlite3
import pandas as pd
import re
from datetime import datetime, timedelta
from pathlib import Path

def quick_sentiment_analysis(stock_code='005930', days=7):
    """빠른 감정분석 실행"""
    
    print("🚀 빠른 감정분석 시작!")
    print("=" * 60)
    print(f"📊 분석 대상: {stock_code}")
    print(f"📅 분석 기간: 최근 {days}일")
    print()
    
    # 데이터베이스 경로
    db_path = Path('data/databases/news_data.db')
    
    if not db_path.exists():
        print(f"❌ 뉴스 데이터베이스가 없습니다: {db_path}")
        print("💡 데이터베이스 경로를 확인해주세요.")
        return
    
    # 한국어 감정 키워드 사전
    positive_words = {
        '성장', '상승', '증가', '개선', '호실적', '성공', '확장', '투자',
        '수익', '이익', '매출', '순이익', '배당', '실적', '호조', '신고가',
        '긍정', '전망', '기대', '목표가', '상향', '추천', '매수', '급등',
        '강세', '회복', '반등', '최고', '우수', '선도', '돌파', '상한가'
    }
    
    negative_words = {
        '하락', '감소', '악화', '적자', '손실', '부진', '침체', '위험',
        '우려', '불안', '하향', '매도', '하한가', '급락', '약세', '폭락',
        '최저', '최악', '위기', '파산', '부도', '문제', '논란', '실망',
        '충격', '타격', '피해', '손해', '악재', '부정', '취소', '중단'
    }
    
    # 회사명 매핑
    company_mapping = {
        '005930': '삼성전자',
        '000660': 'SK하이닉스', 
        '005380': '현대차',
        '035420': 'NAVER',
        '005490': 'POSCO',
        '051910': 'LG화학',
        '006400': '삼성SDI',
        '035720': '카카오',
        '000270': '기아',
        '207940': '삼성바이오로직스'
    }
    
    company_name = company_mapping.get(stock_code, '')
    
    try:
        # 데이터베이스 연결
        conn = sqlite3.connect(db_path)
        
        # 1. 전체 데이터 현황 확인
        total_count_query = "SELECT COUNT(*) as count FROM news_articles"
        total_count = pd.read_sql_query(total_count_query, conn)['count'][0]
        print(f"📰 전체 뉴스 데이터: {total_count:,}건")
        
        # 2. 종목 관련 뉴스 검색
        if company_name:
            news_query = """
                SELECT title, description, pubDate, company_name, stock_code, source
                FROM news_articles 
                WHERE (company_name LIKE ? OR title LIKE ? OR description LIKE ? OR stock_code = ?)
                ORDER BY pubDate DESC 
                LIMIT 500
            """
            search_terms = [f'%{company_name}%', f'%{company_name}%', f'%{company_name}%', stock_code]
        else:
            news_query = """
                SELECT title, description, pubDate, company_name, stock_code, source
                FROM news_articles 
                WHERE (title LIKE ? OR description LIKE ? OR stock_code = ?)
                ORDER BY pubDate DESC 
                LIMIT 500
            """
            search_terms = [f'%{stock_code}%', f'%{stock_code}%', stock_code]
        
        df_news = pd.read_sql_query(news_query, conn, params=search_terms)
        
        print(f"🔍 {stock_code} 관련 뉴스: {len(df_news)}건 발견")
        
        if df_news.empty:
            print(f"❌ {stock_code} 관련 뉴스가 없습니다.")
            print("💡 다른 종목 코드를 시도해보세요.")
            conn.close()
            return
        
        # 3. 최근 뉴스만 필터링 (시간대 문제 해결)
        df_news['pubDate'] = pd.to_datetime(df_news['pubDate'], errors='coerce')
        
        # 시간대 정보 제거하여 비교 가능하게 만들기
        if df_news['pubDate'].dt.tz is not None:
            df_news['pubDate'] = df_news['pubDate'].dt.tz_localize(None)
        
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_news = df_news[df_news['pubDate'] >= cutoff_date]
        
        if recent_news.empty:
            recent_news = df_news.head(50)  # 최근 뉴스가 없으면 최신 50건 사용
            print(f"⚠️ 최근 {days}일 뉴스가 없어 최신 {len(recent_news)}건으로 분석합니다.")
        else:
            print(f"📅 최근 {days}일 뉴스: {len(recent_news)}건")
        
        # 4. 감정분석 실행
        print(f"\n🔬 감정분석 실행 중...")
        
        sentiment_results = []
        
        for _, row in recent_news.iterrows():
            title = str(row.get('title', '')).lower()
            description = str(row.get('description', '')).lower()
            full_text = f"{title} {description}"
            
            # 긍정/부정 키워드 개수 계산
            positive_count = sum(1 for word in positive_words if word in full_text)
            negative_count = sum(1 for word in negative_words if word in full_text)
            
            # 전체 한글 단어 수
            korean_words = re.findall(r'[가-힣]+', full_text)
            total_words = len(korean_words)
            
            # 감정점수 계산 (-1 ~ +1)
            if total_words > 0:
                sentiment_score = (positive_count - negative_count) / max(total_words, 1) * 10
                sentiment_score = max(-1, min(1, sentiment_score))
            else:
                sentiment_score = 0
            
            sentiment_results.append({
                'date': row.get('pubDate'),
                'title': str(row.get('title', ''))[:80] + '...' if len(str(row.get('title', ''))) > 80 else str(row.get('title', '')),
                'sentiment': round(sentiment_score, 3),
                'positive_words': positive_count,
                'negative_words': negative_count,
                'source': row.get('source', '')
            })
        
        # 5. 결과 분석
        df_sentiment = pd.DataFrame(sentiment_results)
        
        # 종합 감정지수
        overall_sentiment = df_sentiment['sentiment'].mean()
        positive_count = len(df_sentiment[df_sentiment['sentiment'] > 0.1])
        negative_count = len(df_sentiment[df_sentiment['sentiment'] < -0.1])
        neutral_count = len(df_sentiment) - positive_count - negative_count
        
        positive_ratio = positive_count / len(df_sentiment)
        negative_ratio = negative_count / len(df_sentiment) 
        neutral_ratio = neutral_count / len(df_sentiment)
        
        # 6. 결과 출력
        print(f"\n📊 {stock_code} 감정분석 결과")
        print("=" * 60)
        print(f"📈 종합 감정지수: {overall_sentiment:.3f}")
        
        # 감정 등급 판정
        if overall_sentiment >= 0.2:
            grade = "매우 긍정적 🚀"
            color = "🟢"
        elif overall_sentiment >= 0.05:
            grade = "긍정적 😊"
            color = "🟢"
        elif overall_sentiment >= -0.05:
            grade = "중립적 😐"
            color = "🟡"
        elif overall_sentiment >= -0.2:
            grade = "부정적 😔"
            color = "🔴"
        else:
            grade = "매우 부정적 😰"
            color = "🔴"
        
        print(f"{color} 감정 등급: {grade}")
        print(f"📊 분포: 긍정 {positive_ratio:.1%} | 중립 {neutral_ratio:.1%} | 부정 {negative_ratio:.1%}")
        
        # 7. 상위/하위 뉴스
        print(f"\n📈 가장 긍정적인 뉴스 TOP 3:")
        top_positive = df_sentiment.nlargest(3, 'sentiment')
        for i, (_, row) in enumerate(top_positive.iterrows(), 1):
            print(f"  {i}. {row['title']} (점수: {row['sentiment']})")
        
        print(f"\n📉 가장 부정적인 뉴스 TOP 3:")
        top_negative = df_sentiment.nsmallest(3, 'sentiment')
        for i, (_, row) in enumerate(top_negative.iterrows(), 1):
            print(f"  {i}. {row['title']} (점수: {row['sentiment']})")
        
        # 8. 시간별 감정 추이 (간단한 버전)
        print(f"\n📅 일별 감정 추이:")
        df_sentiment['date'] = pd.to_datetime(df_sentiment['date'], errors='coerce')
        
        # 시간대 정보 제거
        if df_sentiment['date'].dt.tz is not None:
            df_sentiment['date'] = df_sentiment['date'].dt.tz_localize(None)
            
        df_sentiment['date_only'] = df_sentiment['date'].dt.date
        
        daily_sentiment = df_sentiment.groupby('date_only').agg({
            'sentiment': 'mean',
            'title': 'count'
        }).round(3)
        daily_sentiment.columns = ['평균감정', '뉴스수']
        
        for date, row in daily_sentiment.tail(7).iterrows():
            emoji = "📈" if row['평균감정'] > 0.05 else "📉" if row['평균감정'] < -0.05 else "📊"
            print(f"  {emoji} {date}: {row['평균감정']} ({row['뉴스수']}건)")
        
        conn.close()
        
        print(f"\n✅ 감정분석 완료!")
        print(f"⏰ 분석 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return {
            'stock_code': stock_code,
            'company_name': company_name,
            'news_count': len(df_sentiment),
            'overall_sentiment': round(overall_sentiment, 3),
            'sentiment_grade': grade,
            'positive_ratio': round(positive_ratio, 3),
            'negative_ratio': round(negative_ratio, 3),
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
    except Exception as e:
        print(f"❌ 감정분석 실행 중 오류 발생:")
        print(f"   {type(e).__name__}: {str(e)}")
        import traceback
        print(f"\n🔍 상세 오류 정보:")
        traceback.print_exc()

def analyze_multiple_stocks():
    """주요 종목들 일괄 감정분석"""
    stocks = {
        '005930': '삼성전자',
        '000660': 'SK하이닉스',
        '005380': '현대차',
        '035420': 'NAVER',
        '005490': 'POSCO'
    }
    
    print("🎯 주요 종목 일괄 감정분석")
    print("=" * 60)
    
    results = []
    for stock_code, company_name in stocks.items():
        print(f"\n🔍 {company_name}({stock_code}) 분석 중...")
        result = quick_sentiment_analysis(stock_code, days=7)
        if result:
            results.append(result)
    
    # 결과 요약
    if results:
        print(f"\n📊 종목별 감정지수 순위:")
        results.sort(key=lambda x: x['overall_sentiment'], reverse=True)
        
        for i, result in enumerate(results, 1):
            emoji = "🚀" if result['overall_sentiment'] > 0.1 else "📉" if result['overall_sentiment'] < -0.1 else "📊"
            print(f"  {i}위. {emoji} {result['company_name']}({result['stock_code']}): {result['overall_sentiment']}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'multi':
            analyze_multiple_stocks()
        else:
            stock_code = sys.argv[1]
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
            quick_sentiment_analysis(stock_code, days)
    else:
        # 기본 실행: 삼성전자 감정분석
        quick_sentiment_analysis('005930', 7)
