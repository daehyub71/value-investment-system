#!/usr/bin/env python3
"""
감정분석 실행 스크립트

실행 방법:
python scripts/analysis/run_sentiment_analysis.py --stock_code=005930 --days=30
python scripts/analysis/run_sentiment_analysis.py --market --days=7
python scripts/analysis/run_sentiment_analysis.py --all_stocks --top=20
"""

import sys
import os
import argparse
import json
import pandas as pd
from pathlib import Path
import logging

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.database_config import DatabaseConfig
from config.logging_config import setup_logging
from src.analysis.sentiment.sentiment_analyzer import SentimentAnalyzer

def analyze_single_stock(stock_code: str, days: int = 30) -> dict:
    """단일 종목 감정분석"""
    analyzer = SentimentAnalyzer()
    
    print(f"\n💭 감정분석: {stock_code}")
    print("=" * 60)
    
    # 분석 실행
    result = analyzer.analyze_stock_sentiment(stock_code, days)
    
    if 'error' in result:
        print(f"❌ 분석 실패: {result['error']}")
        return result
    
    # 기본 정보
    print(f"📊 종목코드: {stock_code}")
    print(f"📅 분석일: {result['analysis_date']}")
    print(f"📋 분석기간: {days}일")
    print()
    
    # 종합 감정지수
    overall_sentiment = result.get('overall_sentiment', 0.0)
    sentiment_grade = result.get('sentiment_grade', 'N/A')
    
    # 감정 이모티콘
    if overall_sentiment >= 0.3:
        emotion = "😄"
    elif overall_sentiment >= 0.1:
        emotion = "🙂"
    elif overall_sentiment >= -0.1:
        emotion = "😐"
    elif overall_sentiment >= -0.3:
        emotion = "🙁"
    else:
        emotion = "😞"
    
    print(f"🎯 종합 감정지수")
    print(f"감정점수: {overall_sentiment:+.3f} {emotion}")
    print(f"감정등급: {sentiment_grade}")
    print()
    
    # 뉴스 통계
    total_news = result.get('total_news_count', 0)
    positive_news = result.get('positive_news_count', 0)
    negative_news = result.get('negative_news_count', 0)
    neutral_news = result.get('neutral_news_count', 0)
    
    print(f"📰 뉴스 분석 현황")
    print(f"총 뉴스 수: {total_news}건")
    print(f"긍정 뉴스: {positive_news}건 ({positive_news/max(total_news,1)*100:.1f}%)")
    print(f"부정 뉴스: {negative_news}건 ({negative_news/max(total_news,1)*100:.1f}%)")
    print(f"중립 뉴스: {neutral_news}건 ({neutral_news/max(total_news,1)*100:.1f}%)")
    print()
    
    # 추세 분석
    weekly_sentiment = result.get('weekly_sentiment', 0.0)
    monthly_sentiment = result.get('monthly_sentiment', 0.0)
    sentiment_trend = result.get('sentiment_trend', 0.0)
    
    print(f"📈 감정 추세")
    print(f"주간 감정지수: {weekly_sentiment:+.3f}")
    print(f"월간 감정지수: {monthly_sentiment:+.3f}")
    
    if sentiment_trend > 0.1:
        trend_desc = "📈 개선 추세"
    elif sentiment_trend < -0.1:
        trend_desc = "📉 악화 추세"
    else:
        trend_desc = "➡️ 안정적"
    print(f"감정 변화: {sentiment_trend:+.3f} ({trend_desc})")
    print()
    
    # 품질 지표
    avg_confidence = result.get('avg_confidence', 0.0)
    avg_importance = result.get('avg_importance', 0.0)
    volatility = result.get('sentiment_volatility', 0.0)
    
    print(f"📊 분석 품질")
    print(f"평균 신뢰도: {avg_confidence:.1%}")
    print(f"평균 중요도: {avg_importance:.1%}")
    print(f"감정 변동성: {volatility:.3f}")
    print()
    
    # 주요 긍정 뉴스
    top_positive = result.get('top_positive_news', [])
    if top_positive:
        print(f"📈 주요 긍정 뉴스 (상위 {len(top_positive)}개)")
        for i, news in enumerate(top_positive, 1):
            title = news.get('title', '')[:50] + '...' if len(news.get('title', '')) > 50 else news.get('title', '')
            score = news.get('sentiment_score', 0)
            importance = news.get('importance', 0)
            print(f"{i}. {title}")
            print(f"   감정점수: {score:+.2f}, 중요도: {importance:.1%}")
        print()
    
    # 주요 부정 뉴스
    top_negative = result.get('top_negative_news', [])
    if top_negative:
        print(f"📉 주요 부정 뉴스 (상위 {len(top_negative)}개)")
        for i, news in enumerate(top_negative, 1):
            title = news.get('title', '')[:50] + '...' if len(news.get('title', '')) > 50 else news.get('title', '')
            score = news.get('sentiment_score', 0)
            importance = news.get('importance', 0)
            print(f"{i}. {title}")
            print(f"   감정점수: {score:+.2f}, 중요도: {importance:.1%}")
        print()
    
    # 최근 일별 감정 변화
    daily_sentiments = result.get('daily_sentiments', [])
    if daily_sentiments:
        print(f"📅 최근 일별 감정변화 (최근 {len(daily_sentiments)}일)")
        for daily in daily_sentiments:
            date = daily.get('date', '')
            sentiment = daily.get('daily_sentiment', 0)
            news_count = daily.get('news_count', 0)
            
            emotion_daily = "😄" if sentiment >= 0.3 else "🙂" if sentiment >= 0.1 else "😐" if sentiment >= -0.1 else "🙁" if sentiment >= -0.3 else "😞"
            print(f"  {date}: {sentiment:+.3f} {emotion_daily} ({news_count}건)")
        print()
    
    # 투자 관점 해석
    print(f"💡 투자 관점 해석")
    if overall_sentiment >= 0.3:
        print("🟢 매우 긍정적 - 시장의 기대감이 높습니다. 상승 모멘텀이 있을 수 있습니다.")
    elif overall_sentiment >= 0.1:
        print("🟢 긍정적 - 전반적으로 좋은 뉴스가 우세합니다.")
    elif overall_sentiment >= -0.1:
        print("🟡 중립적 - 특별한 호재나 악재가 없는 상태입니다.")
    elif overall_sentiment >= -0.3:
        print("🟠 부정적 - 우려스러운 뉴스들이 있습니다. 주의가 필요합니다.")
    else:
        print("🔴 매우 부정적 - 심각한 악재가 있을 수 있습니다. 신중한 판단이 필요합니다.")
    
    return result

def analyze_market_sentiment(days: int = 7) -> dict:
    """전체 시장 감정분석"""
    analyzer = SentimentAnalyzer()
    
    print(f"\n💭 시장 전체 감정분석")
    print("=" * 60)
    
    # 분석 실행
    result = analyzer.analyze_market_sentiment(days)
    
    if 'error' in result:
        print(f"❌ 분석 실패: {result['error']}")
        return result
    
    # 기본 정보
    print(f"📅 분석일: {result['analysis_date']}")
    print(f"📋 분석기간: {days}일")
    print()
    
    # 시장 전체 감정지수
    market_sentiment = result.get('market_sentiment', 0.0)
    market_grade = result.get('market_sentiment_grade', 'N/A')
    
    # 시장 감정 이모티콘
    if market_sentiment >= 0.3:
        market_emotion = "🚀"
    elif market_sentiment >= 0.1:
        market_emotion = "📈"
    elif market_sentiment >= -0.1:
        market_emotion = "➡️"
    elif market_sentiment >= -0.3:
        market_emotion = "📉"
    else:
        market_emotion = "💥"
    
    print(f"🎯 시장 감정지수")
    print(f"시장 감정: {market_sentiment:+.3f} {market_emotion}")
    print(f"감정 등급: {market_grade}")
    print()
    
    # 시장 통계
    total_news = result.get('total_news_count', 0)
    analyzed_stocks = result.get('analyzed_stocks_count', 0)
    avg_confidence = result.get('avg_confidence', 0.0)
    
    print(f"📊 분석 현황")
    print(f"총 뉴스 수: {total_news}건")
    print(f"분석 종목 수: {analyzed_stocks}개")
    print(f"평균 신뢰도: {avg_confidence:.1%}")
    print()
    
    # 감정 분포
    sentiment_dist = result.get('sentiment_distribution', {})
    if sentiment_dist:
        print(f"📈 감정 분포")
        print(f"매우 긍정: {sentiment_dist.get('very_positive', 0)}건")
        print(f"긍정: {sentiment_dist.get('positive', 0)}건")
        print(f"중립: {sentiment_dist.get('neutral', 0)}건")
        print(f"부정: {sentiment_dist.get('negative', 0)}건")
        print(f"매우 부정: {sentiment_dist.get('very_negative', 0)}건")
        print()
    
    # 상위 긍정 종목
    top_positive_stocks = result.get('top_positive_stocks', [])
    if top_positive_stocks:
        print(f"🟢 긍정 감정 상위 종목 (TOP 5)")
        for i, stock in enumerate(top_positive_stocks[:5], 1):
            company_name = stock.get('company_name', stock.get('stock_code', ''))
            sentiment = stock.get('sentiment_score', 0)
            news_count = stock.get('news_count', 0)
            print(f"{i}. {company_name:<15} {sentiment:+.3f} ({news_count}건)")
        print()
    
    # 상위 부정 종목
    top_negative_stocks = result.get('top_negative_stocks', [])
    if top_negative_stocks:
        print(f"🔴 부정 감정 상위 종목 (TOP 5)")
        for i, stock in enumerate(top_negative_stocks[:5], 1):
            company_name = stock.get('company_name', stock.get('stock_code', ''))
            sentiment = stock.get('sentiment_score', 0)
            news_count = stock.get('news_count', 0)
            print(f"{i}. {company_name:<15} {sentiment:+.3f} ({news_count}건)")
        print()
    
    # 시장 전망
    print(f"💡 시장 전망")
    if market_sentiment >= 0.3:
        print("🟢 시장이 매우 낙관적입니다. 전반적인 상승 기대감이 높습니다.")
    elif market_sentiment >= 0.1:
        print("🟢 시장이 긍정적입니다. 안정적인 상승세가 예상됩니다.")
    elif market_sentiment >= -0.1:
        print("🟡 시장이 관망세입니다. 특별한 방향성이 없는 상태입니다.")
    elif market_sentiment >= -0.3:
        print("🟠 시장에 우려가 감돌고 있습니다. 변동성이 클 수 있습니다.")
    else:
        print("🔴 시장이 매우 부정적입니다. 하락 위험에 주의해야 합니다.")
    
    return result

def analyze_multiple_stocks(limit: int = 20, days: int = 30) -> list:
    """다중 종목 감정분석"""
    db_config = DatabaseConfig()
    
    try:
        # 뉴스가 있는 종목들 조회
        with db_config.get_connection('news') as conn:
            query = """
            SELECT na.stock_code, ci.company_name, COUNT(*) as news_count
            FROM news_articles na
            LEFT JOIN (
                SELECT stock_code, company_name 
                FROM stock_data.company_info
            ) ci ON na.stock_code = ci.stock_code
            WHERE date(na.created_at) >= date('now', '-{} days')
            AND na.stock_code IS NOT NULL
            GROUP BY na.stock_code
            ORDER BY news_count DESC
            LIMIT ?
            """.format(days)
            
            stocks_df = pd.read_sql(query, conn, params=(limit,))
        
        if stocks_df.empty:
            print("❌ 분석할 종목을 찾을 수 없습니다.")
            return []
        
        print(f"\n💭 다중 종목 감정분석 (상위 {len(stocks_df)}개 종목)")
        print("=" * 80)
        
        analyzer = SentimentAnalyzer()
        results = []
        
        for idx, row in stocks_df.iterrows():
            stock_code = row['stock_code']
            company_name = row['company_name'] or stock_code
            news_count = row['news_count']
            
            print(f"\n진행률: {idx+1}/{len(stocks_df)} - {company_name}({stock_code}) [{news_count}건]")
            
            try:
                result = analyzer.analyze_stock_sentiment(stock_code, days)
                
                if 'error' not in result:
                    # 간단한 결과 출력
                    sentiment = result.get('overall_sentiment', 0.0)
                    grade = result.get('sentiment_grade', 'N/A')
                    print(f"  감정: {sentiment:+.3f} ({grade})")
                    
                    results.append(result)
                else:
                    print(f"  ❌ 분석 실패: {result['error']}")
                
            except Exception as e:
                print(f"  ❌ 오류 발생: {e}")
                continue
        
        # 결과 요약
        if results:
            print(f"\n📊 감정분석 결과 요약")
            print("=" * 50)
            
            # 감정 점수순 정렬
            sorted_results = sorted(results, key=lambda x: x.get('overall_sentiment', 0), reverse=True)
            
            print("🟢 긍정 감정 상위 10개 종목:")
            for i, result in enumerate(sorted_results[:10], 1):
                stock_code = result.get('stock_code', '')
                sentiment = result.get('overall_sentiment', 0)
                grade = result.get('sentiment_grade', '')
                news_count = result.get('total_news_count', 0)
                
                # 회사명 조회
                try:
                    company_name = stocks_df[stocks_df['stock_code'] == stock_code]['company_name'].iloc[0] or stock_code
                except:
                    company_name = stock_code
                
                print(f"{i:2d}. {company_name:<15} {sentiment:+.3f} {grade:<15} ({news_count}건)")
            
            print(f"\n🔴 부정 감정 상위 5개 종목:")
            negative_results = [r for r in sorted_results if r.get('overall_sentiment', 0) < -0.1]
            for i, result in enumerate(reversed(negative_results[-5:]), 1):
                stock_code = result.get('stock_code', '')
                sentiment = result.get('overall_sentiment', 0)
                grade = result.get('sentiment_grade', '')
                news_count = result.get('total_news_count', 0)
                
                try:
                    company_name = stocks_df[stocks_df['stock_code'] == stock_code]['company_name'].iloc[0] or stock_code
                except:
                    company_name = stock_code
                
                print(f"{i:2d}. {company_name:<15} {sentiment:+.3f} {grade:<15} ({news_count}건)")
            
            # 전체 통계
            sentiments = [r.get('overall_sentiment', 0) for r in results]
            avg_sentiment = sum(sentiments) / len(sentiments)
            
            print(f"\n📈 전체 통계:")
            print(f"• 분석 완료: {len(results)}개 종목")
            print(f"• 평균 감정: {avg_sentiment:+.3f}")
            print(f"• 최고 감정: {max(sentiments):+.3f}")
            print(f"• 최저 감정: {min(sentiments):+.3f}")
            
            # 감정 등급별 분포
            grades = {}
            for result in results:
                grade = result.get('sentiment_grade', 'N/A')
                grades[grade] = grades.get(grade, 0) + 1
            
            print(f"\n📊 감정등급별 분포:")
            for grade, count in sorted(grades.items()):
                print(f"• {grade}: {count}개 종목")
        
        return results
        
    except Exception as e:
        print(f"❌ 다중 종목 감정분석 실패: {e}")
        return []

def generate_sentiment_report(results: list, output_file: str = None):
    """감정분석 결과 리포트 생성"""
    if not results:
        print("생성할 리포트 데이터가 없습니다.")
        return
    
    # 감정 통계
    sentiments = [r.get('overall_sentiment', 0) for r in results if 'overall_sentiment' in r]
    
    if sentiments:
        avg_sentiment = sum(sentiments) / len(sentiments)
        max_sentiment = max(sentiments)
        min_sentiment = min(sentiments)
    else:
        avg_sentiment = max_sentiment = min_sentiment = 0
    
    # 등급별 분포
    grade_dist = {}
    for result in results:
        grade = result.get('sentiment_grade', 'N/A')
        grade_dist[grade] = grade_dist.get(grade, 0) + 1
    
    # 리포트 생성
    report = {
        'analysis_date': results[0].get('analysis_date', ''),
        'total_analyzed': len(results),
        'sentiment_statistics': {
            'average': round(avg_sentiment, 3),
            'maximum': round(max_sentiment, 3),
            'minimum': round(min_sentiment, 3)
        },
        'grade_distribution': grade_dist,
        'top_positive': sorted(
            [r for r in results if r.get('overall_sentiment', 0) > 0.1],
            key=lambda x: x.get('overall_sentiment', 0),
            reverse=True
        )[:10],
        'top_negative': sorted(
            [r for r in results if r.get('overall_sentiment', 0) < -0.1],
            key=lambda x: x.get('overall_sentiment', 0)
        )[:10],
        'detailed_results': results
    }
    
    # 파일 저장
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"📄 감정분석 리포트가 저장되었습니다: {output_file}")
    else:
        # 콘솔 출력
        print("\n📋 감정분석 요약:")
        print(f"분석 종목 수: {report['total_analyzed']}")
        print(f"평균 감정: {report['sentiment_statistics']['average']:+.3f}")
        print("등급 분포:", json.dumps(report['grade_distribution'], ensure_ascii=False, indent=2))

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='감정분석 실행')
    parser.add_argument('--stock_code', type=str, help='분석할 종목코드 (예: 005930)')
    parser.add_argument('--market', action='store_true', help='전체 시장 감정분석')
    parser.add_argument('--all_stocks', action='store_true', help='전체 종목 감정분석')
    parser.add_argument('--top', type=int, default=20, help='분석할 상위 종목 수 (기본값: 20)')
    parser.add_argument('--days', type=int, default=30, help='분석 기간 (일수, 기본값: 30)')
    parser.add_argument('--output', type=str, help='결과를 JSON 파일로 저장')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 로깅 설정
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        if args.stock_code:
            # 단일 종목 분석
            result = analyze_single_stock(args.stock_code, args.days)
            
            if args.output:
                generate_sentiment_report([result], args.output)
            
        elif args.market:
            # 시장 전체 분석
            result = analyze_market_sentiment(args.days)
            
            if args.output:
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                print(f"📄 시장 감정분석 결과가 저장되었습니다: {args.output}")
            
        elif args.all_stocks:
            # 다중 종목 분석
            results = analyze_multiple_stocks(args.top, args.days)
            
            if args.output:
                generate_sentiment_report(results, args.output)
            
        else:
            parser.print_help()
            print(f"\n💡 사용 예시:")
            print(f"  {sys.argv[0]} --stock_code=005930 --days=30")
            print(f"  {sys.argv[0]} --market --days=7")
            print(f"  {sys.argv[0]} --all_stocks --top=20 --days=14")
            print(f"  {sys.argv[0]} --market --output=market_sentiment.json")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()