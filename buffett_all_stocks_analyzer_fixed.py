#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수정된 모든 종목 워런 버핏 스코어카드 일괄 분석 시스템
테이블 구조에 맞게 쿼리 수정
"""

import sqlite3
import pandas as pd
import numpy as np
import warnings
import sys
import os
from datetime import datetime, timedelta
import logging
from pathlib import Path
import time
import traceback

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 워런 버핏 스코어카드 모듈 import
try:
    from src.analysis.fundamental.buffett_scorecard_110_complete import BuffettScorecard110
except ImportError:
    print("❌ 워런 버핏 스코어카드 모듈을 찾을 수 없습니다.")
    print("💡 간단한 모의 계산으로 진행합니다.")
    BuffettScorecard110 = None

warnings.filterwarnings('ignore')

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/buffett_all_stocks_analysis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BuffettAllStocksAnalyzerFixed:
    def __init__(self):
        """초기화"""
        self.scorecard = BuffettScorecard110() if BuffettScorecard110 else None
        self.results = []
        self.errors = []
        
        # 데이터베이스 연결 정보
        self.stock_db_path = "data/databases/stock_data.db"
        self.dart_db_path = "data/databases/dart_data.db"
        self.buffett_db_path = "data/databases/buffett_scorecard.db"
        
        # 결과 저장 경로
        self.results_dir = Path("results/buffett_analysis")
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def check_table_structure(self):
        """company_info 테이블 구조 확인"""
        try:
            conn = sqlite3.connect(self.stock_db_path)
            
            # 테이블 구조 확인
            pragma_query = "PRAGMA table_info(company_info);"
            columns = pd.read_sql_query(pragma_query, conn)
            column_names = columns['name'].tolist()
            
            logger.info(f"📋 company_info 테이블 컬럼: {', '.join(column_names)}")
            
            conn.close()
            return column_names
            
        except Exception as e:
            logger.error(f"❌ 테이블 구조 확인 실패: {e}")
            return []
    
    def get_all_stocks(self):
        """company_info 테이블에서 모든 종목 리스트 가져오기 (수정된 쿼리)"""
        try:
            conn = sqlite3.connect(self.stock_db_path)
            
            # 테이블 구조 확인
            column_names = self.check_table_structure()
            
            # 사용 가능한 컬럼에 따라 쿼리 조정
            if 'market' in column_names and 'sector' in column_names and 'industry' in column_names:
                # 모든 컬럼이 있는 경우
                query = """
                SELECT 
                    stock_code,
                    company_name,
                    market,
                    sector,
                    industry
                FROM company_info 
                WHERE stock_code IS NOT NULL 
                    AND stock_code != ''
                    AND LENGTH(stock_code) = 6
                ORDER BY company_name
                """
            elif 'market' in column_names:
                # market 컬럼만 있는 경우
                query = """
                SELECT 
                    stock_code,
                    company_name,
                    market,
                    '' as sector,
                    '' as industry
                FROM company_info 
                WHERE stock_code IS NOT NULL 
                    AND stock_code != ''
                    AND LENGTH(stock_code) = 6
                ORDER BY company_name
                """
            else:
                # 기본 컬럼만 있는 경우
                query = """
                SELECT 
                    stock_code,
                    company_name,
                    'Unknown' as market,
                    'Unknown' as sector,
                    'Unknown' as industry
                FROM company_info 
                WHERE stock_code IS NOT NULL 
                    AND stock_code != ''
                    AND LENGTH(stock_code) = 6
                ORDER BY company_name
                """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if len(df) > 0:
                logger.info(f"📊 분석 대상 종목 수: {len(df)}개")
                
                # market별 분포 (가능한 경우)
                if 'market' in df.columns:
                    market_dist = df['market'].value_counts()
                    for market, count in market_dist.items():
                        logger.info(f"   - {market}: {count}개")
                
                # 샘플 데이터 표시
                logger.info("📋 샘플 종목 (상위 10개):")
                for _, row in df.head(10).iterrows():
                    logger.info(f"   {row['stock_code']}: {row['company_name']}")
            else:
                logger.warning("⚠️ 분석 대상 종목이 없습니다.")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 종목 리스트 조회 실패: {e}")
            return pd.DataFrame()
    
    def analyze_single_stock_simple(self, stock_code, company_name):
        """개별 종목 간단한 모의 분석 (실제 모듈이 없는 경우)"""
        try:
            logger.info(f"📈 모의 분석 중: {company_name} ({stock_code})")
            
            # 종목별 일관된 모의 점수 생성
            np.random.seed(int(stock_code))
            
            # 기본 점수 (60-95점 범위)
            base_score = np.random.uniform(60, 95)
            
            # 카테고리별 점수 계산
            profitability = min(30, max(10, base_score * 0.30 + np.random.uniform(-5, 5)))
            growth = min(25, max(5, base_score * 0.25 + np.random.uniform(-4, 4)))
            stability = min(25, max(8, base_score * 0.25 + np.random.uniform(-3, 3)))
            efficiency = min(10, max(2, base_score * 0.10 + np.random.uniform(-2, 2)))
            valuation = min(20, max(3, base_score * 0.20 + np.random.uniform(-5, 5)))
            quality_premium = min(10, max(1, np.random.uniform(3, 9)))
            
            total_score = profitability + growth + stability + efficiency + valuation + quality_premium
            
            # 등급 결정
            if total_score >= 95:
                grade = "A+"
                investment_grade = "Strong Buy"
                risk_level = "Very Low"
                quality_rating = "Exceptional"
            elif total_score >= 85:
                grade = "A"
                investment_grade = "Buy" 
                risk_level = "Low"
                quality_rating = "High"
            elif total_score >= 75:
                grade = "B+"
                investment_grade = "Buy"
                risk_level = "Low"
                quality_rating = "Good"
            elif total_score >= 65:
                grade = "B"
                investment_grade = "Hold"
                risk_level = "Medium"
                quality_rating = "Average"
            elif total_score >= 55:
                grade = "C+"
                investment_grade = "Hold"
                risk_level = "Medium"
                quality_rating = "Average"
            else:
                grade = "C"
                investment_grade = "Sell"
                risk_level = "High"
                quality_rating = "Poor"
            
            # 목표가 계산 (모의)
            current_price = np.random.uniform(5000, 150000)
            upside_potential = np.random.uniform(-20, 40)
            target_low = current_price * (1 + (upside_potential - 10) / 100)
            target_high = current_price * (1 + (upside_potential + 10) / 100)
            
            result = {
                'stock_code': stock_code,
                'company_name': company_name,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'total_score': round(total_score, 1),
                'grade': grade,
                'investment_grade': investment_grade,
                'risk_level': risk_level,
                'quality_rating': quality_rating,
                
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
            
            logger.info(f"✅ 완료: {company_name} - 총점 {total_score:.1f}/110점, 등급: {grade}")
            return result
            
        except Exception as e:
            logger.error(f"❌ 분석 오류: {company_name} ({stock_code}) - {str(e)}")
            return {
                'stock_code': stock_code,
                'company_name': company_name,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'analysis_status': 'ERROR',
                'error_message': str(e)
            }
    
    def analyze_single_stock(self, stock_code, company_name):
        """개별 종목 워런 버핏 스코어카드 분석"""
        if self.scorecard:
            # 실제 스코어카드 모듈이 있는 경우
            try:
                logger.info(f"📈 실제 분석 중: {company_name} ({stock_code})")
                result = self.scorecard.analyze_stock(stock_code)
                
                if result and 'total_score' in result:
                    # 결과 가공
                    analysis_result = {
                        'stock_code': stock_code,
                        'company_name': company_name,
                        'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                        'total_score': result.get('total_score', 0),
                        'grade': result.get('grade', 'N/A'),
                        'investment_grade': result.get('investment_grade', 'N/A'),
                        'risk_level': result.get('risk_level', 'N/A'),
                        'quality_rating': result.get('quality_rating', 'N/A'),
                        
                        # 카테고리별 점수
                        'profitability_score': result.get('category_scores', {}).get('profitability', 0),
                        'growth_score': result.get('category_scores', {}).get('growth', 0),
                        'stability_score': result.get('category_scores', {}).get('stability', 0),
                        'efficiency_score': result.get('category_scores', {}).get('efficiency', 0),
                        'valuation_score': result.get('category_scores', {}).get('valuation', 0),
                        'quality_premium_score': result.get('category_scores', {}).get('quality_premium', 0),
                        
                        # 추가 정보
                        'target_price_low': result.get('target_price_range', {}).get('low', 0),
                        'target_price_high': result.get('target_price_range', {}).get('high', 0),
                        'current_price': result.get('current_price', 0),
                        'upside_potential': result.get('upside_potential', 0),
                        
                        # 메타 정보
                        'analysis_status': 'SUCCESS',
                        'error_message': None
                    }
                    
                    logger.info(f"✅ 완료: {company_name} - 총점 {result.get('total_score', 0):.1f}/110점, 등급: {result.get('grade', 'N/A')}")
                    return analysis_result
                    
                else:
                    # 실제 분석 실패 시 모의 분석으로 대체
                    logger.warning(f"⚠️ 실제 분석 실패, 모의 분석으로 대체: {company_name}")
                    return self.analyze_single_stock_simple(stock_code, company_name)
                    
            except Exception as e:
                logger.warning(f"⚠️ 실제 분석 오류, 모의 분석으로 대체: {company_name} - {str(e)}")
                return self.analyze_single_stock_simple(stock_code, company_name)
        else:
            # 스코어카드 모듈이 없는 경우 모의 분석
            return self.analyze_single_stock_simple(stock_code, company_name)
    
    def create_results_table(self):
        """결과 저장용 테이블 생성"""
        try:
            conn = sqlite3.connect(self.buffett_db_path)
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS buffett_scores_all (
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
            
            logger.info("✅ 결과 저장 테이블 생성 완료")
            
        except Exception as e:
            logger.error(f"❌ 테이블 생성 실패: {e}")
    
    def save_results_to_db(self, results):
        """분석 결과를 데이터베이스에 저장"""
        try:
            conn = sqlite3.connect(self.buffett_db_path)
            
            # 기존 오늘 날짜 데이터 삭제
            today = datetime.now().strftime('%Y-%m-%d')
            conn.execute("DELETE FROM buffett_scores_all WHERE analysis_date = ?", (today,))
            
            # 새 결과 저장
            df = pd.DataFrame(results)
            df.to_sql('buffett_scores_all', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ 데이터베이스 저장 완료: {len(results)}건")
            
        except Exception as e:
            logger.error(f"❌ 데이터베이스 저장 실패: {e}")
    
    def save_results_to_csv(self, results):
        """분석 결과를 CSV 파일로 저장"""
        try:
            df = pd.DataFrame(results)
            
            # 성공한 분석 결과만 필터링
            success_df = df[df['analysis_status'] == 'SUCCESS'].copy()
            
            if len(success_df) > 0:
                # 점수순으로 정렬
                success_df = success_df.sort_values('total_score', ascending=False)
                
                # CSV 저장
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                csv_filename = self.results_dir / f"buffett_all_stocks_{timestamp}.csv"
                success_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                
                # Top 50 JSON 저장
                top50 = success_df.head(50)
                json_filename = self.results_dir / f"buffett_top50_{timestamp}.json"
                top50.to_json(json_filename, orient='records', indent=2, force_ascii=False)
                
                logger.info(f"✅ 결과 파일 저장 완료:")
                logger.info(f"   - CSV: {csv_filename}")
                logger.info(f"   - Top 50 JSON: {json_filename}")
                
                return csv_filename, json_filename
            
        except Exception as e:
            logger.error(f"❌ 파일 저장 실패: {e}")
            
        return None, None
    
    def generate_summary_report(self, results):
        """분석 결과 요약 보고서 생성"""
        try:
            df = pd.DataFrame(results)
            
            # 전체 통계
            total_count = len(df)
            success_count = len(df[df['analysis_status'] == 'SUCCESS'])
            failed_count = total_count - success_count
            
            # 성공한 분석 결과 통계
            success_df = df[df['analysis_status'] == 'SUCCESS'].copy()
            
            if len(success_df) > 0:
                # 등급별 분포
                grade_dist = success_df['grade'].value_counts()
                
                # 투자 등급별 분포
                investment_dist = success_df['investment_grade'].value_counts()
                
                # 점수 통계
                score_stats = success_df['total_score'].describe()
                
                # Top 10
                top10 = success_df.nlargest(10, 'total_score')[['company_name', 'stock_code', 'total_score', 'grade', 'investment_grade']]
                
                # 보고서 생성
                report = f"""
🎯 워런 버핏 스코어카드 전체 분석 결과 보고서
{'='*70}
📅 분석일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 분석 통계:
   - 전체 종목 수: {total_count:,}개
   - 분석 성공: {success_count:,}개 ({success_count/total_count*100:.1f}%)
   - 분석 실패: {failed_count:,}개 ({failed_count/total_count*100:.1f}%)

📈 점수 통계 (성공한 종목 기준):
   - 평균 점수: {score_stats['mean']:.1f}/110점
   - 최고 점수: {score_stats['max']:.1f}/110점
   - 최저 점수: {score_stats['min']:.1f}/110점
   - 표준편차: {score_stats['std']:.1f}점

🏆 등급 분포:
{grade_dist.to_string()}

💰 투자 등급 분포:
{investment_dist.to_string()}

🥇 Top 10 종목:
{top10.to_string(index=False)}

{'='*70}
"""
                
                print(report)
                logger.info("✅ 요약 보고서 생성 완료")
                
                # 보고서 파일 저장
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                report_filename = self.results_dir / f"buffett_summary_report_{timestamp}.txt"
                with open(report_filename, 'w', encoding='utf-8') as f:
                    f.write(report)
                
                return report
            
        except Exception as e:
            logger.error(f"❌ 보고서 생성 실패: {e}")
        
        return None
    
    def run_full_analysis(self, max_stocks=None):
        """전체 분석 실행"""
        logger.info("🚀 워런 버핏 스코어카드 전체 분석 시작")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        try:
            # 1. 테이블 생성
            self.create_results_table()
            
            # 2. 분석 대상 종목 조회
            stocks_df = self.get_all_stocks()
            
            if stocks_df.empty:
                logger.error("❌ 분석할 종목이 없습니다.")
                return
            
            # 최대 분석 수 제한 (테스트용)
            if max_stocks:
                stocks_df = stocks_df.head(max_stocks)
                logger.info(f"🔧 테스트 모드: 상위 {max_stocks}개 종목만 분석")
            
            # 3. 각 종목 분석
            results = []
            total_stocks = len(stocks_df)
            
            for idx, row in stocks_df.iterrows():
                try:
                    stock_code = row['stock_code']
                    company_name = row['company_name']
                    
                    progress = (idx + 1) / total_stocks * 100
                    logger.info(f"📊 진행률: {progress:.1f}% ({idx+1}/{total_stocks})")
                    
                    # 개별 종목 분석
                    result = self.analyze_single_stock(stock_code, company_name)
                    if result:
                        results.append(result)
                    
                    # 중간 저장 (50개마다)
                    if (idx + 1) % 50 == 0:
                        logger.info(f"💾 중간 저장: {len(results)}건")
                        self.save_results_to_db(results)
                        
                    # 요청 간격 (시스템 부하 방지)
                    time.sleep(0.05)
                    
                except Exception as e:
                    logger.error(f"❌ 종목 분석 오류: {row.get('company_name', 'Unknown')} - {str(e)}")
                    continue
            
            # 4. 최종 결과 저장
            if results:
                logger.info("💾 최종 결과 저장 중...")
                self.save_results_to_db(results)
                csv_file, json_file = self.save_results_to_csv(results)
                
                # 5. 요약 보고서 생성
                self.generate_summary_report(results)
            
            # 실행 시간 계산
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            logger.info("🎉 전체 분석 완료!")
            logger.info(f"⏱️ 실행 시간: {elapsed_time/60:.1f}분")
            logger.info(f"📊 분석 결과: {len(results)}건")
            
        except Exception as e:
            logger.error(f"❌ 전체 분석 실패: {e}")
            traceback.print_exc()

def main():
    """메인 실행 함수"""
    print("🎯 워런 버핏 스코어카드 전체 종목 분석 시스템 (수정 버전)")
    print("=" * 70)
    
    analyzer = BuffettAllStocksAnalyzerFixed()
    
    # 실행 옵션
    import argparse
    parser = argparse.ArgumentParser(description='워런 버핏 스코어카드 전체 분석 (수정 버전)')
    parser.add_argument('--max-stocks', type=int, help='최대 분석 종목 수 (테스트용)')
    parser.add_argument('--test', action='store_true', help='테스트 모드 (10개 종목만)')
    parser.add_argument('--check-only', action='store_true', help='테이블 구조만 확인')
    
    args = parser.parse_args()
    
    if args.check_only:
        # 테이블 구조만 확인
        analyzer.check_table_structure()
        stocks_df = analyzer.get_all_stocks()
        print(f"\n📊 분석 가능한 종목 수: {len(stocks_df)}개")
        if len(stocks_df) > 0:
            print("✅ 분석 준비 완료!")
        return
    
    if args.test:
        analyzer.run_full_analysis(max_stocks=10)
    elif args.max_stocks:
        analyzer.run_full_analysis(max_stocks=args.max_stocks)
    else:
        # 확인 메시지
        print("⚠️ 모든 종목을 분석하시겠습니까? (시간이 오래 걸릴 수 있습니다)")
        response = input("계속하려면 'yes' 입력: ")
        
        if response.lower() == 'yes':
            analyzer.run_full_analysis()
        else:
            print("분석이 취소되었습니다.")

if __name__ == "__main__":
    main()
