#!/usr/bin/env python3
"""
간단한 뉴스 감정분석 엔진
네이버 뉴스 API로 수집된 77,729건 뉴스 데이터를 감정분석
"""

import sqlite3
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple

class SentimentAnalyzer:
    """뉴스 감정분석 클래스"""
    
    def __init__(self):
        self.logger = logging.getLogger('SentimentAnalysis')
        self.db_path = Path('data/databases/news_data.db')
        
        # 간단한 한국어 금융 감정사전
        self.positive_words = {
            '성장', '상승', '증가', '개선', '호실적', '성공', '확장', '투자',
            '수익', '이익', '매출', '순이익', '배당', '실적', '호조', '신고가',
            '긍정', '전망', '기대', '목표가', '상향', '추천', '매수', '상한가',
            '돌파', '급등', '강세', '회복', '반등', '최고', '우수', '선도'
        }
        
        self.negative_words = {
            '하락', '감소', '악화', '적자', '손실', '부진', '침체', '위험',
            '우려', '불안', '하향', '매도', '하한가', '급락', '약세', '폭락',
            '최저', '최악', '위기', '파산', '부도', '문제', '논란', '비관',
            '실망', '충격', '타격', '피해', '손해', '악재', '부정', '취소'
        }
        
        self.neutral_words = {
            '발표', '공시', '보고', '계획', '예정', '진행', '검토', '논의',
            '회의', '미팅', '컨퍼런스', '설명회', '일반', '보통', '유지'
        }
    
    def analyze_stock_sentiment(self, stock_code: str, days: int = 7) -> Dict:
        """종목별 감정분석 실행"""
        try:
            # 뉴스 데이터 조회
            news_data = self._get_news_data(stock_code, days)
            
            if news_data.empty:
                return {
                    'error': f'종목 {stock_code}의 뉴스 데이터가 없습니다.',
                    'stock_code': stock_code,
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # 감정분석 실행
            sentiment_results = []
            
            for _, news in news_data.iterrows():
                sentiment_score = self._calculate_sentiment_score(
                    news.get('title', ''), 
                    news.get('description', '')
                )
                
                sentiment_results.append({
                    'date': news.get('pubDate', ''),
                    'title': news.get('title', ''),
                    'sentiment_score': sentiment_score,
                    'source': news.get('source', '')
                })
            
            # 종합 분석 결과
            df_sentiment = pd.DataFrame(sentiment_results)
            
            # 전체 감정 점수 계산
            overall_sentiment = df_sentiment['sentiment_score'].mean()
            positive_ratio = len(df_sentiment[df_sentiment['sentiment_score'] > 0.1]) / len(df_sentiment)
            negative_ratio = len(df_sentiment[df_sentiment['sentiment_score'] < -0.1]) / len(df_sentiment)
            
            # 감정 등급 판정
            sentiment_grade = self._get_sentiment_grade(overall_sentiment)
            
            return {
                'stock_code': stock_code,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'analysis_period': days,
                'news_count': len(news_data),
                'overall_sentiment': round(overall_sentiment, 3),
                'sentiment_grade': sentiment_grade,
                'positive_ratio': round(positive_ratio, 3),
                'negative_ratio': round(negative_ratio, 3),
                'daily_sentiment': self._get_daily_sentiment(df_sentiment),
                'top_positive_news': self._get_top_news(df_sentiment, positive=True),
                'top_negative_news': self._get_top_news(df_sentiment, positive=False)
            }
            
        except Exception as e:
            self.logger.error(f"감정분석 실행 중 오류: {e}")
            return {
                'error': f'감정분석 실행 중 오류가 발생했습니다: {str(e)}',
                'stock_code': stock_code,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    def _get_news_data(self, stock_code: str, days: int) -> pd.DataFrame:
        """뉴스 데이터 조회"""
        try:
            # 날짜 계산
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # 데이터베이스 연결
            conn = sqlite3.connect(self.db_path)
            
            # 종목코드로 회사명 찾기 (간단한 매핑)
            company_names = {
                '005930': '삼성전자',
                '000660': 'SK하이닉스',
                '005380': '현대차',
                '035420': 'NAVER',
                '005490': 'POSCO',
                '051910': 'LG화학',
                '006400': '삼성SDI',
                '035720': '카카오'
            }
            
            company_name = company_names.get(stock_code, '')
            
            if not company_name:
                # 회사명을 찾을 수 없으면 종목코드로 검색
                query = """
                    SELECT * FROM news_articles 
                    WHERE stock_code = ? OR title LIKE ? OR description LIKE ?
                    ORDER BY pubDate DESC
                    LIMIT 1000
                """
                df = pd.read_sql_query(query, conn, params=[stock_code, f'%{stock_code}%', f'%{stock_code}%'])
            else:
                # 회사명으로 검색
                query = """
                    SELECT * FROM news_articles 
                    WHERE company_name = ? OR title LIKE ? OR description LIKE ?
                    ORDER BY pubDate DESC
                    LIMIT 1000
                """
                df = pd.read_sql_query(query, conn, params=[company_name, f'%{company_name}%', f'%{company_name}%'])
            
            conn.close()
            
            self.logger.info(f"종목 {stock_code} 뉴스 {len(df)}건 조회")
            return df
            
        except Exception as e:
            self.logger.error(f"뉴스 데이터 조회 실패: {e}")
            return pd.DataFrame()
    
    def _calculate_sentiment_score(self, title: str, description: str) -> float:
        """개별 뉴스의 감정점수 계산"""
        text = f"{title} {description}".lower()
        
        # 단어 토큰화 (간단한 공백 기준)
        words = re.findall(r'[가-힣]+', text)
        
        positive_count = sum(1 for word in words if word in self.positive_words)
        negative_count = sum(1 for word in words if word in self.negative_words)
        total_count = len(words)
        
        if total_count == 0:
            return 0.0
        
        # 감정점수 계산 (-1 ~ +1)
        sentiment_score = (positive_count - negative_count) / max(total_count, 1)
        
        # 점수 정규화
        sentiment_score = max(-1.0, min(1.0, sentiment_score * 5))
        
        return sentiment_score
    
    def _get_sentiment_grade(self, score: float) -> str:
        """감정점수를 등급으로 변환"""
        if score >= 0.3:
            return 'Very Positive'
        elif score >= 0.1:
            return 'Positive'
        elif score >= -0.1:
            return 'Neutral'
        elif score >= -0.3:
            return 'Negative'
        else:
            return 'Very Negative'
    
    def _get_daily_sentiment(self, df_sentiment: pd.DataFrame) -> List[Dict]:
        """일별 감정점수 계산"""
        try:
            # pubDate를 날짜로 변환
            df_sentiment['date'] = pd.to_datetime(df_sentiment['date'], errors='coerce')
            df_sentiment['date_only'] = df_sentiment['date'].dt.date
            
            # 일별 평균 감정점수
            daily_sentiment = df_sentiment.groupby('date_only')['sentiment_score'].agg([
                'mean', 'count'
            ]).reset_index()
            
            daily_sentiment.columns = ['date', 'avg_sentiment', 'news_count']
            
            return daily_sentiment.to_dict('records')
            
        except Exception as e:
            self.logger.error(f"일별 감정점수 계산 실패: {e}")
            return []
    
    def _get_top_news(self, df_sentiment: pd.DataFrame, positive: bool = True, top_n: int = 3) -> List[Dict]:
        """상위/하위 감정점수 뉴스 반환"""
        try:
            if positive:
                top_news = df_sentiment.nlargest(top_n, 'sentiment_score')
            else:
                top_news = df_sentiment.nsmallest(top_n, 'sentiment_score')
            
            return top_news[['title', 'sentiment_score', 'date']].to_dict('records')
            
        except Exception as e:
            self.logger.error(f"상위 뉴스 조회 실패: {e}")
            return []
    
    def analyze_market_sentiment(self, days: int = 7) -> Dict:
        """전체 시장 감정분석"""
        try:
            # 전체 뉴스 데이터 조회
            conn = sqlite3.connect(self.db_path)
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            query = """
                SELECT title, description, pubDate, source 
                FROM news_articles 
                ORDER BY pubDate DESC 
                LIMIT 10000
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if df.empty:
                return {'error': '시장 뉴스 데이터가 없습니다.'}
            
            # 전체 뉴스 감정분석
            sentiment_scores = []
            for _, news in df.iterrows():
                score = self._calculate_sentiment_score(
                    news.get('title', ''), 
                    news.get('description', '')
                )
                sentiment_scores.append(score)
            
            # 시장 감정지수 계산
            market_sentiment = np.mean(sentiment_scores)
            positive_ratio = len([s for s in sentiment_scores if s > 0.1]) / len(sentiment_scores)
            negative_ratio = len([s for s in sentiment_scores if s < -0.1]) / len(sentiment_scores)
            
            return {
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'analysis_period': days,
                'total_news': len(df),
                'market_sentiment': round(market_sentiment, 3),
                'sentiment_grade': self._get_sentiment_grade(market_sentiment),
                'positive_ratio': round(positive_ratio, 3),
                'negative_ratio': round(negative_ratio, 3),
                'neutral_ratio': round(1 - positive_ratio - negative_ratio, 3)
            }
            
        except Exception as e:
            self.logger.error(f"시장 감정분석 실패: {e}")
            return {'error': f'시장 감정분석 실패: {str(e)}'}

# 사용 예시
if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    
    # 삼성전자 감정분석
    result = analyzer.analyze_stock_sentiment('005930', days=7)
    print("📊 삼성전자 감정분석 결과:")
    print(f"종합 감정점수: {result.get('overall_sentiment', 0)}")
    print(f"감정 등급: {result.get('sentiment_grade', 'N/A')}")
    print(f"뉴스 건수: {result.get('news_count', 0)}")
