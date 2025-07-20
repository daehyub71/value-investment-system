#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
모든 종목 워런 버핏 스코어카드 일괄 분석 시스템 (투자 가능 여부 포함)
company_info 테이블의 모든 종목을 대상으로 워런 버핏 110점 체계 분석하고 DB에 저장
투자 가능 여부 필드 자동 업데이트 포함
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
    sys.exit(1)

# 투자 가능 여부 업데이터 import
try:
    from investment_status_updater import InvestmentStatusUpdater
except ImportError:
    print("⚠️ 투자 가능 여부 업데이터를 찾을 수 없습니다. 기본 분석만 실행됩니다.")
    InvestmentStatusUpdater = None

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

class BuffettAllStocksAnalyzer:
    def __init__(self, update_investment_status=True):
        """초기화"""
        self.scorecard = BuffettScorecard110()
        self.results = []
        self.errors = []
        self.update_investment_status = update_investment_status
        
        # 투자 가능 여부 업데이터 초기화
        if update_investment_status and InvestmentStatusUpdater:
            self.investment_updater = InvestmentStatusUpdater()
        else:
            self.investment_updater = None
        
        # 데이터베이스 연결 정보
        self.stock_db_path = "data/databases/stock_data.db"
        self.dart_db_path = "data/databases/dart_data.db"
        self.buffett_db_path = "data/databases/buffett_scorecard.db"
        
        # 결과 저장 경로
        self.results_dir = Path("results/buffett_analysis")
        self.results_dir.mkdir(parents=True, exist_ok=True)
    
    def get_all_stocks(self):
        """company_info 테이블에서 모든 종목 리스트 가져오기"""
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
            WHERE stock_code IS NOT NULL 
                AND stock_code != ''
                AND LENGTH(stock_code) = 6
            ORDER BY market, company_name
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            logger.info(f"📊 분석 대상 종목 수: {len(df)}개")
            logger.info(f"   - KOSPI: {len(df[df['market'] == 'KOSPI'])}개")
            logger.info(f"   - KOSDAQ: {len(df[df['market'] == 'KOSDAQ'])}개")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 종목 리스트 조회 실패: {e}")
            return pd.DataFrame()
    
    def analyze_single_stock(self, stock_code, company_name):
        """개별 종목 워런 버핏 스코어카드 분석"""
        try:
            logger.info(f"📈 분석 중: {company_name} ({stock_code})")
            
            # 워런 버핏 스코어카드 분석
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
                    
                    # 투자 가능 여부 관련 (기본값 설정)
                    'is_investable': True,
                    'investment_warning': 'NONE',
                    'listing_status': 'LISTED',
                    'last_status_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    
                    # 메타 정보
                    'analysis_status': 'SUCCESS',
                    'error_message': None
                }
                
                logger.info(f"✅ 완료: {company_name} - 총점 {result.get('total_score', 0):.1f}/110점, 등급: {result.get('grade', 'N/A')}")
                return analysis_result
                
            else:
                logger.warning(f"⚠️ 분석 실패: {company_name} ({stock_code}) - 데이터 부족")
                return {
                    'stock_code': stock_code,
                    'company_name': company_name,
                    'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                    'analysis_status': 'FAILED_NO_DATA',
                    'error_message': '분석에 필요한 데이터 부족',
                    # 투자 가능 여부 기본값
                    'is_investable': False,
                    'investment_warning': 'ALERT',
                    'listing_status': 'LISTED'
                }
                
        except Exception as e:
            logger.error(f"❌ 분석 오류: {company_name} ({stock_code}) - {str(e)}")
            return {
                'stock_code': stock_code,
                'company_name': company_name,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'analysis_status': 'ERROR',
                'error_message': str(e),
                # 투자 가능 여부 기본값
                'is_investable': False,
                'investment_warning': 'ALERT',
                'listing_status': 'LISTED'
            }
    
    def update_investment_status_for_results(self, results):
        """분석 결과를 바탕으로 투자 가능 여부 업데이트"""
        if not self.investment_updater:
            logger.warning("⚠️ 투자 가능 여부 업데이터가 없습니다. 기본값을 사용합니다.")
            return results
        
        try:
            logger.info("🔄 투자 가능 여부 업데이트 중...")
            
            updated_results = []
            for result in results:
                if result.get('analysis_status') == 'SUCCESS':
                    stock_code = result['stock_code']
                    total_score = result.get('total_score', 0)
                    profitability_score = result.get('profitability_score', 0)
                    stability_score = result.get('stability_score', 0)
                    
                    # 투자 경고 수준 결정
                    investment_warning = 'NONE'
                    is_investable = True
                    
                    if total_score < 20:
                        investment_warning = 'DESIGNATED'  # 관리종목 수준
                        is_investable = False
                    elif total_score < 30 or stability_score < 5 or profitability_score < 5:
                        investment_warning = 'ALERT'
                        is_investable = True  # 경고하지만 투자는 가능
                    elif total_score < 50:
                        investment_warning = 'CAUTION'
                        is_investable = True
                    
                    # 결과 업데이트
                    result['is_investable'] = is_investable
                    result['investment_warning'] = investment_warning
                    result['listing_status'] = 'LISTED'
                    result['last_status_check'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                updated_results.append(result)
            
            logger.info(f"✅ 투자 가능 여부 업데이트 완료: {len(updated_results)}건")
            return updated_results
            
        except Exception as e:
            logger.error(f"❌ 투자 가능 여부 업데이트 실패: {e}")
            return results
    
    def create_results_table(self):
        """결과 저장용 테이블 생성 (투자 가능 여부 필드 포함)"""
        try:
            conn = sqlite3.connect(self.buffett_db_path)
            
            # 기존 buffett_all_stocks_final 테이블이 있는지 확인
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='buffett_all_stocks_final'
            """)
            
            if cursor.fetchone():
                # 기존 테이블에 새 필드 추가 (없을 경우에만)
                try:
                    cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN is_investable BOOLEAN DEFAULT 1")
                except sqlite3.OperationalError:
                    pass  # 이미 존재
                
                try:
                    cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN investment_warning TEXT DEFAULT 'NONE'")
                except sqlite3.OperationalError:
                    pass  # 이미 존재
                
                try:
                    cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN listing_status TEXT DEFAULT 'LISTED'")
                except sqlite3.OperationalError:
                    pass  # 이미 존재
                
                try:
                    cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN last_status_check TEXT")
                except sqlite3.OperationalError:
                    pass  # 이미 존재
            else:
                # 새 테이블 생성
                create_table_sql = """
                CREATE TABLE buffett_all_stocks_final (
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
                    
                    -- 투자 가능 여부 필드
                    is_investable BOOLEAN DEFAULT 1,
                    investment_warning TEXT DEFAULT 'NONE',
                    listing_status TEXT DEFAULT 'LISTED',
                    last_status_check TEXT,
                    
                    analysis_status TEXT,
                    error_message TEXT,
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(stock_code, analysis_date)
                )
                """
                cursor.execute(create_table_sql)
            
            # 인덱스 생성
            try:
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buffett_final_investable ON buffett_all_stocks_final(is_investable)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buffett_final_warning ON buffett_all_stocks_final(investment_warning)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buffett_final_score ON buffett_all_stocks_final(total_score)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buffett_final_grade ON buffett_all_stocks_final(investment_grade)")
            except Exception as e:
                logger.warning(f"인덱스 생성 중 일부 실패: {e}")
            
            conn.commit()
            conn.close()
            
            logger.info("✅ 결과 저장 테이블 생성/업데이트 완료")
            
        except Exception as e:
            logger.error(f"❌ 테이블 생성 실패: {e}")
    
    def save_results_to_db(self, results):
        """분석 결과를 데이터베이스에 저장"""
        try:
            conn = sqlite3.connect(self.buffett_db_path)
            
            # 기존 오늘 날짜 데이터 삭제
            today = datetime.now().strftime('%Y-%m-%d')
            conn.execute("DELETE FROM buffett_all_stocks_final WHERE analysis_date = ?", (today,))
            
            # 새 결과 저장
            df = pd.DataFrame(results)
            df.to_sql('buffett_all_stocks_final', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ 데이터베이스 저장 완료: {len(results)}건")
            
        except Exception as e:
            logger.error(f"❌ 데이터베이스 저장 실패: {e}")
    
    def save_results_to_csv(self, results):
        """분석 결과를 CSV 파일로 저장 (투자 가능 여부 포함)"""
        try:
            df = pd.DataFrame(results)
            
            # 성공한 분석 결과만 필터링
            success_df = df[df['analysis_status'] == 'SUCCESS'].copy()
            
            if len(success_df) > 0:
                # 점수순으로 정렬
                success_df = success_df.sort_values('total_score', ascending=False)
                
                # 전체 결과 CSV 저장
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                csv_filename = self.results_dir / f"buffett_all_stocks_with_status_{timestamp}.csv"
                success_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                
                # 투자 가능한 종목만 별도 저장
                investable_df = success_df[success_df['is_investable'] == True].copy()
                if len(investable_df) > 0:
                    investable_filename = self.results_dir / f"buffett_investable_stocks_{timestamp}.csv"
                    investable_df.to_csv(investable_filename, index=False, encoding='utf-8-sig')
                    
                    # 투자 추천 종목 (Strong Buy, Buy)
                    recommendations = investable_df[investable_df['investment_grade'].isin(['Strong Buy', 'Buy'])].copy()
                    if len(recommendations) > 0:
                        rec_filename = self.results_dir / f"buffett_investable_recommendations_{timestamp}.csv"
                        recommendations.to_csv(rec_filename, index=False, encoding='utf-8-sig')
                        logger.info(f"💎 투자 추천 가능 종목: {len(recommendations)}개 → {rec_filename}")
                
                # 투자 불가 종목 별도 저장
                non_investable_df = success_df[success_df['is_investable'] == False].copy()
                if len(non_investable_df) > 0:
                    non_inv_filename = self.results_dir / f"buffett_non_investable_{timestamp}.csv"
                    non_investable_df.to_csv(non_inv_filename, index=False, encoding='utf-8-sig')
                    logger.info(f"❌ 투자 불가 종목: {len(non_investable_df)}개 → {non_inv_filename}")
                
                # Top 50 JSON 저장 (투자 가능한 종목 기준)
                top50 = investable_df.head(50) if len(investable_df) >= 50 else success_df.head(50)
                json_filename = self.results_dir / f"buffett_top50_{timestamp}.json"
                top50.to_json(json_filename, orient='records', indent=2, force_ascii=False)
                
                logger.info(f"✅ 결과 파일 저장 완료:")
                logger.info(f"   - 전체 결과: {csv_filename}")
                if len(investable_df) > 0:
                    logger.info(f"   - 투자 가능: {investable_filename}")
                logger.info(f"   - Top 50 JSON: {json_filename}")
                
                return csv_filename, json_filename
            
        except Exception as e:
            logger.error(f"❌ 파일 저장 실패: {e}")
            
        return None, None
    
    def generate_summary_report(self, results):
        """분석 결과 요약 보고서 생성 (투자 가능 여부 포함)"""
        try:
            df = pd.DataFrame(results)
            
            # 전체 통계
            total_count = len(df)
            success_count = len(df[df['analysis_status'] == 'SUCCESS'])
            failed_count = total_count - success_count
            
            # 성공한 분석 결과 통계
            success_df = df[df['analysis_status'] == 'SUCCESS'].copy()
            
            if len(success_df) > 0:
                # 투자 가능 여부 통계
                investable_count = len(success_df[success_df['is_investable'] == True])
                non_investable_count = len(success_df[success_df['is_investable'] == False])
                
                # 투자 경고 수준 분포
                warning_dist = success_df['investment_warning'].value_counts()
                
                # 등급별 분포
                grade_dist = success_df['grade'].value_counts()
                
                # 투자 등급별 분포 (투자 가능한 종목만)
                investable_df = success_df[success_df['is_investable'] == True]
                if len(investable_df) > 0:
                    investment_dist = investable_df['investment_grade'].value_counts()
                    # 점수 통계 (투자 가능한 종목 기준)
                    score_stats = investable_df['total_score'].describe()
                    # Top 10 (투자 가능한 종목 기준)
                    top10 = investable_df.nlargest(10, 'total_score')[['company_name', 'stock_code', 'total_score', 'grade', 'investment_grade', 'investment_warning']]
                else:
                    investment_dist = pd.Series(dtype='int64')
                    score_stats = pd.Series(dtype='float64')
                    top10 = pd.DataFrame()
                
                # 보고서 생성
                report = f"""
🎯 워런 버핏 스코어카드 전체 분석 결과 보고서 (투자 가능 여부 포함)
{'='*80}
📅 분석일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📊 분석 통계:
   - 전체 종목 수: {total_count:,}개
   - 분석 성공: {success_count:,}개 ({success_count/total_count*100:.1f}%)
   - 분석 실패: {failed_count:,}개 ({failed_count/total_count*100:.1f}%)

🚦 투자 가능 여부:
   - 투자 가능: {investable_count:,}개 ({investable_count/success_count*100:.1f}%)
   - 투자 불가: {non_investable_count:,}개 ({non_investable_count/success_count*100:.1f}%)

⚠️ 투자 경고 수준 분포:
{warning_dist.to_string()}

📈 점수 통계 (투자 가능한 종목 기준):
   - 평균 점수: {score_stats.get('mean', 0):.1f}/110점
   - 최고 점수: {score_stats.get('max', 0):.1f}/110점
   - 최저 점수: {score_stats.get('min', 0):.1f}/110점
   - 표준편차: {score_stats.get('std', 0):.1f}점

🏆 등급 분포 (전체):
{grade_dist.to_string()}

💰 투자 등급 분포 (투자 가능한 종목만):
{investment_dist.to_string() if len(investment_dist) > 0 else '데이터 없음'}

🥇 Top 10 투자 가능 종목:
{top10.to_string(index=False) if len(top10) > 0 else '데이터 없음'}

{'='*80}
"""
                
                print(report)
                logger.info("✅ 요약 보고서 생성 완료")
                
                # 보고서 파일 저장
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                report_filename = self.results_dir / f"buffett_summary_report_with_status_{timestamp}.txt"
                with open(report_filename, 'w', encoding='utf-8') as f:
                    f.write(report)
                
                return report
            
        except Exception as e:
            logger.error(f"❌ 보고서 생성 실패: {e}")
        
        return None
    
    def run_full_analysis(self, max_stocks=None):
        """전체 분석 실행"""
        logger.info("🚀 워런 버핏 스코어카드 전체 분석 시작 (투자 가능 여부 포함)")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        try:
            # 1. 테이블 생성/업데이트
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
                    
                    # 중간 저장 (100개마다)
                    if (idx + 1) % 100 == 0:
                        logger.info(f"💾 중간 저장: {len(results)}건")
                        # 투자 가능 여부 업데이트 후 저장
                        updated_results = self.update_investment_status_for_results(results)
                        self.save_results_to_db(updated_results)
                        
                    # 요청 간격 (API 부하 방지)
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"❌ 종목 분석 오류: {row.get('company_name', 'Unknown')} - {str(e)}")
                    continue
            
            # 4. 투자 가능 여부 최종 업데이트
            if results:
                logger.info("🔄 투자 가능 여부 최종 업데이트 중...")
                updated_results = self.update_investment_status_for_results(results)
                
                # 5. 최종 결과 저장
                logger.info("💾 최종 결과 저장 중...")
                self.save_results_to_db(updated_results)
                csv_file, json_file = self.save_results_to_csv(updated_results)
                
                # 6. 요약 보고서 생성
                self.generate_summary_report(updated_results)
            
            # 실행 시간 계산
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            logger.info("🎉 전체 분석 완료!")
            logger.info(f"⏱️ 실행 시간: {elapsed_time/60:.1f}분")
            logger.info(f"📊 분석 결과: {len(results)}건")
            
            if self.update_investment_status:
                investable_count = len([r for r in updated_results if r.get('is_investable')])
                logger.info(f"💎 투자 가능 종목: {investable_count}개")
            
        except Exception as e:
            logger.error(f"❌ 전체 분석 실패: {e}")
            traceback.print_exc()

def main():
    """메인 실행 함수"""
    print("🎯 워런 버핏 스코어카드 전체 종목 분석 시스템 (투자 가능 여부 포함)")
    print("=" * 80)
    
    # 실행 옵션
    import argparse
    parser = argparse.ArgumentParser(description='워런 버핏 스코어카드 전체 분석')
    parser.add_argument('--max-stocks', type=int, help='최대 분석 종목 수 (테스트용)')
    parser.add_argument('--test', action='store_true', help='테스트 모드 (10개 종목만)')
    parser.add_argument('--no-investment-status', action='store_true', help='투자 가능 여부 업데이트 생략')
    
    args = parser.parse_args()
    
    # 투자 가능 여부 업데이트 옵션
    update_investment_status = not args.no_investment_status
    
    analyzer = BuffettAllStocksAnalyzer(update_investment_status=update_investment_status)
    
    if args.test:
        analyzer.run_full_analysis(max_stocks=10)
    elif args.max_stocks:
        analyzer.run_full_analysis(max_stocks=args.max_stocks)
    else:
        # 확인 메시지
        print("⚠️ 모든 종목을 분석하시겠습니까? (시간이 오래 걸릴 수 있습니다)")
        if update_investment_status:
            print("📊 투자 가능 여부도 함께 업데이트됩니다.")
        response = input("계속하려면 'yes' 입력: ")
        
        if response.lower() == 'yes':
            analyzer.run_full_analysis()
        else:
            print("분석이 취소되었습니다.")

if __name__ == "__main__":
    main()
