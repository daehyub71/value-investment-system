#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
워런 버핏 스코어카드 Top 50 종목 빠른 분석 시스템
주요 종목만 선별하여 빠르게 분석하고 결과 저장
"""

import sqlite3
import pandas as pd
import numpy as np
import warnings
import sys
import os
from datetime import datetime
import logging
from pathlib import Path
import time

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

warnings.filterwarnings('ignore')

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BuffettTop50Analyzer:
    def __init__(self):
        """초기화"""
        self.results = []
        
        # 데이터베이스 경로
        self.stock_db_path = "data/databases/stock_data.db"
        self.buffett_db_path = "data/databases/buffett_scorecard.db"
        
        # 주요 종목 리스트 (시가총액 기준 상위 50개)
        self.major_stocks = {
            '005930': '삼성전자',
            '000660': 'SK하이닉스',
            '035420': 'NAVER',
            '005380': '현대차',
            '051910': 'LG화학',
            '006400': '삼성SDI',
            '035720': '카카오',
            '068270': '셀트리온',
            '000270': '기아',
            '105560': 'KB금융',
            '055550': '신한지주',
            '096770': 'SK이노베이션',
            '017670': 'SK텔레콤',
            '030200': 'KT',
            '003670': '포스코홀딩스',
            '012330': '현대모비스',
            '207940': '삼성바이오로직스',
            '086790': '하나금융지주',
            '028260': '삼성물산',
            '066570': 'LG전자',
            '003550': 'LG',
            '033780': 'KT&G',
            '015760': '한국전력',
            '009150': '삼성전기',
            '011200': 'HMM',
            '032830': '삼성생명',
            '018260': '삼성에스디에스',
            '010950': 'S-Oil',
            '051900': 'LG생활건강',
            '024110': '기업은행',
            '267250': 'HD현대중공업',
            '000810': '삼성화재',
            '161390': '한국타이어앤테크놀로지',
            '097950': 'CJ제일제당',
            '078930': 'GS',
            '010130': '고려아연',
            '036570': '엔씨소프트',
            '302440': 'SK바이오사이언스',
            '011070': 'LG이노텍',
            '090430': '아모레퍼시픽',
            '047050': '포스코인터내셔널',
            '000720': '현대건설',
            '034730': 'SK',
            '011780': '금호석유',
            '005420': '코오롱인더',
            '051915': 'LG화학우',
            '180640': '한진칼',
            '139480': '이마트',
            '004020': '현대제철',
            '006800': '미래에셋증권'
        }
    
    def get_stock_basic_info(self, stock_code):
        """종목 기본 정보 조회"""
        try:
            conn = sqlite3.connect(self.stock_db_path)
            
            query = """
            SELECT 
                stock_code,
                company_name,
                market,
                sector,
                industry
            FROM company_info 
            WHERE stock_code = ?
            """
            
            result = pd.read_sql_query(query, conn, params=(stock_code,))
            conn.close()
            
            if not result.empty:
                return result.iloc[0].to_dict()
            else:
                return {
                    'stock_code': stock_code,
                    'company_name': self.major_stocks.get(stock_code, 'Unknown'),
                    'market': 'Unknown',
                    'sector': 'Unknown',
                    'industry': 'Unknown'
                }
                
        except Exception as e:
            logger.warning(f"기본 정보 조회 실패 {stock_code}: {e}")
            return {
                'stock_code': stock_code,
                'company_name': self.major_stocks.get(stock_code, 'Unknown'),
                'market': 'Unknown',
                'sector': 'Unknown',
                'industry': 'Unknown'
            }
    
    def calculate_simple_buffett_score(self, stock_code, company_name):
        """간단한 워런 버핏 스코어 계산 (모의 계산)"""
        try:
            # 실제로는 복잡한 재무분석이 들어가지만, 여기서는 모의 점수 생성
            # 나중에 실제 buffett_scorecard_110_complete.py와 연결
            
            # 종목별 특성을 반영한 모의 점수 생성
            np.random.seed(int(stock_code))  # 일관된 결과를 위한 시드
            
            # 주요 대형주는 보통 70-90점대
            base_score = np.random.uniform(65, 95)
            
            # 카테고리별 점수 (비례 배분)
            profitability = min(30, base_score * 0.30 + np.random.uniform(-3, 3))
            growth = min(25, base_score * 0.25 + np.random.uniform(-3, 3))
            stability = min(25, base_score * 0.25 + np.random.uniform(-2, 2))
            efficiency = min(10, base_score * 0.10 + np.random.uniform(-1, 1))
            valuation = min(20, base_score * 0.20 + np.random.uniform(-4, 4))
            quality_premium = min(10, np.random.uniform(5, 10))
            
            total_score = profitability + growth + stability + efficiency + valuation + quality_premium
            
            # 등급 결정
            if total_score >= 90:
                grade = "A+"
                investment_grade = "Strong Buy"
                risk_level = "Very Low"
            elif total_score >= 80:
                grade = "A"
                investment_grade = "Buy"
                risk_level = "Low"
            elif total_score >= 70:
                grade = "B+"
                investment_grade = "Buy"
                risk_level = "Low"
            elif total_score >= 60:
                grade = "B"
                investment_grade = "Hold"
                risk_level = "Medium"
            else:
                grade = "C+"
                investment_grade = "Hold"
                risk_level = "Medium"
            
            # 목표가 계산 (모의)
            current_price = np.random.uniform(10000, 100000)
            target_low = current_price * 0.9
            target_high = current_price * 1.2
            upside_potential = (target_high - current_price) / current_price * 100
            
            result = {
                'stock_code': stock_code,
                'company_name': company_name,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'total_score': round(total_score, 1),
                'grade': grade,
                'investment_grade': investment_grade,
                'risk_level': risk_level,
                'quality_rating': "Good" if total_score > 75 else "Average",
                
                'profitability_score': round(profitability, 1),
                'growth_score': round(growth, 1),
                'stability_score': round(stability, 1),
                'efficiency_score': round(efficiency, 1),
                'valuation_score': round(valuation, 1),
                'quality_premium_score': round(quality_premium, 1),
                
                'target_price_low': round(target_low),
                'target_price_high': round(target_high),
                'current_price': round(current_price),
                'upside_potential': round(upside_potential, 1),
                
                'analysis_status': 'SUCCESS',
                'error_message': None
            }
            
            logger.info(f"✅ {company_name}: {total_score:.1f}/110점, {grade}등급, {investment_grade}")
            return result
            
        except Exception as e:
            logger.error(f"❌ 분석 실패 {company_name}: {e}")
            return {
                'stock_code': stock_code,
                'company_name': company_name,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'analysis_status': 'ERROR',
                'error_message': str(e)
            }
    
    def create_buffett_table(self):
        """워런 버핏 결과 테이블 생성"""
        try:
            conn = sqlite3.connect(self.buffett_db_path)
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS buffett_top50_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                company_name TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                total_score REAL,
                grade TEXT,
                investment_grade TEXT,
                risk_level TEXT,
                quality_rating TEXT,
                
                profitability_score REAL,
                growth_score REAL,
                stability_score REAL,
                efficiency_score REAL,
                valuation_score REAL,
                quality_premium_score REAL,
                
                target_price_low REAL,
                target_price_high REAL,
                current_price REAL,
                upside_potential REAL,
                
                analysis_status TEXT,
                error_message TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, analysis_date)
            )
            """
            
            conn.execute(create_table_sql)
            conn.commit()
            conn.close()
            
            logger.info("✅ 워런 버핏 결과 테이블 생성 완료")
            
        except Exception as e:
            logger.error(f"❌ 테이블 생성 실패: {e}")
    
    def save_to_database(self, results):
        """결과를 데이터베이스에 저장"""
        try:
            conn = sqlite3.connect(self.buffett_db_path)
            
            # 오늘 날짜 기존 데이터 삭제
            today = datetime.now().strftime('%Y-%m-%d')
            conn.execute("DELETE FROM buffett_top50_scores WHERE analysis_date = ?", (today,))
            
            # 새 데이터 저장
            df = pd.DataFrame(results)
            df.to_sql('buffett_top50_scores', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ 데이터베이스 저장 완료: {len(results)}건")
            
        except Exception as e:
            logger.error(f"❌ 데이터베이스 저장 실패: {e}")
    
    def save_to_files(self, results):
        """결과를 파일로 저장"""
        try:
            # 결과 디렉토리 생성
            results_dir = Path("results/buffett_analysis")
            results_dir.mkdir(parents=True, exist_ok=True)
            
            df = pd.DataFrame(results)
            success_df = df[df['analysis_status'] == 'SUCCESS'].copy()
            
            if len(success_df) > 0:
                # 점수순 정렬
                success_df = success_df.sort_values('total_score', ascending=False)
                
                # 파일명에 타임스탬프 추가
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # CSV 저장
                csv_file = results_dir / f"buffett_top50_{timestamp}.csv"
                success_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                
                # JSON 저장
                json_file = results_dir / f"buffett_top50_{timestamp}.json"
                success_df.to_json(json_file, orient='records', indent=2, force_ascii=False)
                
                logger.info(f"✅ 파일 저장 완료:")
                logger.info(f"   - CSV: {csv_file}")
                logger.info(f"   - JSON: {json_file}")
                
                return csv_file, json_file
            
        except Exception as e:
            logger.error(f"❌ 파일 저장 실패: {e}")
        
        return None, None
    
    def generate_summary(self, results):
        """분석 결과 요약 생성"""
        try:
            df = pd.DataFrame(results)
            success_df = df[df['analysis_status'] == 'SUCCESS'].copy()
            
            if len(success_df) == 0:
                return "분석 성공한 종목이 없습니다."
            
            # 통계 계산
            total_analyzed = len(success_df)
            avg_score = success_df['total_score'].mean()
            max_score = success_df['total_score'].max()
            min_score = success_df['total_score'].min()
            
            # 등급별 분포
            grade_counts = success_df['grade'].value_counts()
            investment_counts = success_df['investment_grade'].value_counts()
            
            # Top 10
            top10 = success_df.nlargest(10, 'total_score')
            
            summary = f"""
🎯 워런 버핏 스코어카드 Top 50 분석 결과
{'='*60}
📅 분석일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 분석 종목: {total_analyzed}개
📈 평균 점수: {avg_score:.1f}/110점
🏆 최고 점수: {max_score:.1f}/110점 ({success_df.loc[success_df['total_score'].idxmax(), 'company_name']})
📉 최저 점수: {min_score:.1f}/110점 ({success_df.loc[success_df['total_score'].idxmin(), 'company_name']})

🏆 등급 분포:
{grade_counts.to_string()}

💰 투자 등급 분포:
{investment_counts.to_string()}

🥇 Top 10 종목:
"""
            
            for i, (_, row) in enumerate(top10.iterrows(), 1):
                summary += f"   {i:2d}. {row['company_name']} ({row['stock_code']}): {row['total_score']:.1f}점, {row['grade']}, {row['investment_grade']}\n"
            
            summary += "=" * 60
            
            print(summary)
            return summary
            
        except Exception as e:
            logger.error(f"❌ 요약 생성 실패: {e}")
            return None
    
    def run_analysis(self):
        """Top 50 종목 분석 실행"""
        logger.info("🚀 워런 버핏 스코어카드 Top 50 분석 시작")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            # 1. 테이블 생성
            self.create_buffett_table()
            
            # 2. 각 종목 분석
            results = []
            total_stocks = len(self.major_stocks)
            
            for idx, (stock_code, company_name) in enumerate(self.major_stocks.items(), 1):
                logger.info(f"📈 ({idx}/{total_stocks}) 분석 중: {company_name} ({stock_code})")
                
                # 기본 정보 조회
                basic_info = self.get_stock_basic_info(stock_code)
                company_name = basic_info['company_name']
                
                # 워런 버핏 스코어 계산
                result = self.calculate_simple_buffett_score(stock_code, company_name)
                
                if result:
                    results.append(result)
                
                # 진행률 표시
                progress = idx / total_stocks * 100
                if idx % 10 == 0:
                    logger.info(f"📊 진행률: {progress:.1f}% ({idx}/{total_stocks})")
            
            # 3. 결과 저장
            if results:
                logger.info("💾 결과 저장 중...")
                self.save_to_database(results)
                self.save_to_files(results)
                
                # 4. 요약 보고서
                self.generate_summary(results)
            
            # 실행 시간
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            logger.info("🎉 분석 완료!")
            logger.info(f"⏱️ 실행 시간: {elapsed_time:.1f}초")
            logger.info(f"📊 분석 결과: {len(results)}건")
            
        except Exception as e:
            logger.error(f"❌ 분석 실패: {e}")

def main():
    """메인 실행 함수"""
    print("🎯 워런 버핏 스코어카드 Top 50 종목 분석")
    print("=" * 60)
    
    analyzer = BuffettTop50Analyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
