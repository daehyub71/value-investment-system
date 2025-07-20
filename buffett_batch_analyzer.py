#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전 종목 워런 버핏 스코어카드 대량 분석 시스템
성공 확인된 buffett_final_analyzer 기반으로 전체 종목 분석
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
import argparse

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from buffett_final_analyzer import BuffettFinalAnalyzer

warnings.filterwarnings('ignore')

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/buffett_all_stocks_batch.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BuffettBatchAnalyzer:
    def __init__(self):
        """초기화"""
        self.analyzer = BuffettFinalAnalyzer()
        self.results = []
        self.failed_stocks = []
        
        # 결과 저장 경로
        self.results_dir = Path("results/buffett_analysis")
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # 통계
        self.stats = {
            'total_analyzed': 0,
            'successful': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None
        }
    
    def create_results_table(self):
        """결과 저장 테이블 생성 (확장된 버전)"""
        try:
            conn = sqlite3.connect("data/databases/buffett_scorecard.db")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS buffett_all_stocks_final (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                company_name TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                
                -- 기본 점수 정보
                total_score REAL,
                grade TEXT,
                investment_grade TEXT,
                risk_level TEXT,
                quality_rating TEXT,
                
                -- 카테고리별 점수 (110점 만점)
                profitability_score REAL,
                growth_score REAL,
                stability_score REAL,
                efficiency_score REAL,
                valuation_score REAL,
                quality_premium_score REAL,
                
                -- 재무 비율
                roe REAL,
                roa REAL,
                debt_ratio REAL,
                current_ratio REAL,
                pe_ratio REAL,
                pb_ratio REAL,
                net_margin REAL,
                operating_margin REAL,
                
                -- 목표가 정보
                target_price_low REAL,
                target_price_high REAL,
                current_price REAL,
                upside_potential REAL,
                
                -- 메타 정보
                analysis_status TEXT,
                error_message TEXT,
                
                -- 타임스탬프
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- 유니크 제약
                UNIQUE(stock_code, analysis_date)
            )
            """
            
            conn.execute(create_table_sql)
            conn.commit()
            conn.close()
            
            logger.info("✅ 전체 종목 결과 테이블 생성 완료")
            
        except Exception as e:
            logger.error(f"❌ 테이블 생성 실패: {e}")
    
    def save_results_to_database(self):
        """결과를 데이터베이스에 저장"""
        if not self.results:
            logger.warning("⚠️ 저장할 결과가 없습니다.")
            return
        
        try:
            conn = sqlite3.connect("data/databases/buffett_scorecard.db")
            
            # 오늘 날짜 기존 데이터 삭제
            today = datetime.now().strftime('%Y-%m-%d')
            delete_count = conn.execute(
                "DELETE FROM buffett_all_stocks_final WHERE analysis_date = ?", 
                (today,)
            ).rowcount
            
            if delete_count > 0:
                logger.info(f"🗑️ 기존 데이터 삭제: {delete_count}건")
            
            # 새 결과 저장
            df = pd.DataFrame(self.results)
            df.to_sql('buffett_all_stocks_final', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ 데이터베이스 저장 완료: {len(self.results)}건")
            
        except Exception as e:
            logger.error(f"❌ 데이터베이스 저장 실패: {e}")
    
    def save_results_to_files(self):
        """결과를 파일로 저장"""
        if not self.results:
            logger.warning("⚠️ 저장할 결과가 없습니다.")
            return
        
        try:
            df = pd.DataFrame(self.results)
            df_sorted = df.sort_values('total_score', ascending=False)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # 1. 전체 결과 CSV
            csv_file = self.results_dir / f"buffett_all_stocks_{timestamp}.csv"
            df_sorted.to_csv(csv_file, index=False, encoding='utf-8-sig')
            
            # 2. Top 100 JSON
            top100 = df_sorted.head(100)
            json_file = self.results_dir / f"buffett_top100_{timestamp}.json"
            top100.to_json(json_file, orient='records', indent=2, force_ascii=False)
            
            # 3. 투자 추천 종목 (Buy 이상)
            buy_stocks = df_sorted[df_sorted['investment_grade'].isin(['Strong Buy', 'Buy'])]
            if len(buy_stocks) > 0:
                buy_file = self.results_dir / f"buffett_buy_recommendations_{timestamp}.csv"
                buy_stocks.to_csv(buy_file, index=False, encoding='utf-8-sig')
                logger.info(f"💰 투자 추천 종목: {len(buy_stocks)}개 → {buy_file}")
            
            # 4. 등급별 분류
            for grade in ['A+', 'A', 'A-', 'B+', 'B']:
                grade_stocks = df_sorted[df_sorted['grade'] == grade]
                if len(grade_stocks) > 0:
                    grade_file = self.results_dir / f"buffett_grade_{grade.replace('+', 'plus').replace('-', 'minus')}_{timestamp}.csv"
                    grade_stocks.to_csv(grade_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"✅ 파일 저장 완료:")
            logger.info(f"   📊 전체 결과: {csv_file}")
            logger.info(f"   🏆 Top 100: {json_file}")
            
            return csv_file, json_file
            
        except Exception as e:
            logger.error(f"❌ 파일 저장 실패: {e}")
            return None, None
    
    def generate_comprehensive_report(self):
        """종합 분석 보고서 생성"""
        if not self.results:
            return
        
        try:
            df = pd.DataFrame(self.results)
            df_sorted = df.sort_values('total_score', ascending=False)
            
            # 기본 통계
            total_count = len(df)
            avg_score = df['total_score'].mean()
            std_score = df['total_score'].std()
            max_score = df['total_score'].max()
            min_score = df['total_score'].min()
            
            # 등급별 분포
            grade_dist = df['grade'].value_counts().sort_index()
            
            # 투자등급별 분포
            investment_dist = df['investment_grade'].value_counts()
            
            # 리스크별 분포
            risk_dist = df['risk_level'].value_counts()
            
            # Top 20
            top20 = df_sorted.head(20)[['company_name', 'stock_code', 'total_score', 'grade', 'investment_grade']]
            
            # 실행 시간 계산
            if self.stats['start_time'] and self.stats['end_time']:
                elapsed_time = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
                elapsed_minutes = elapsed_time / 60
            else:
                elapsed_minutes = 0
            
            # 보고서 생성
            report = f"""
🎯 워런 버핏 스코어카드 전체 종목 분석 결과 보고서
{'='*80}
📅 분석일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏱️ 분석 소요시간: {elapsed_minutes:.1f}분
📊 분석 현황:
   - 전체 분석 시도: {self.stats['total_analyzed']:,}개 종목
   - 성공: {self.stats['successful']:,}개 ({self.stats['successful']/max(1, self.stats['total_analyzed'])*100:.1f}%)
   - 실패: {self.stats['failed']:,}개 ({self.stats['failed']/max(1, self.stats['total_analyzed'])*100:.1f}%)

📈 점수 통계:
   - 평균 점수: {avg_score:.1f}/110점
   - 표준편차: {std_score:.1f}점
   - 최고 점수: {max_score:.1f}/110점 ({df_sorted.iloc[0]['company_name']})
   - 최저 점수: {min_score:.1f}/110점 ({df_sorted.iloc[-1]['company_name']})

🏆 등급별 분포:
{grade_dist.to_string()}

💰 투자등급별 분포:
{investment_dist.to_string()}

⚠️ 리스크별 분포:
{risk_dist.to_string()}

🥇 Top 20 종목:
{top20.to_string(index=False)}

📊 주요 통계:
   - A등급 이상 (85점+): {len(df[df['total_score'] >= 85])}개 ({len(df[df['total_score'] >= 85])/total_count*100:.1f}%)
   - B+등급 이상 (75점+): {len(df[df['total_score'] >= 75])}개 ({len(df[df['total_score'] >= 75])/total_count*100:.1f}%)
   - 투자 추천 (Buy 이상): {len(df[df['investment_grade'].isin(['Strong Buy', 'Buy'])])}개
   - 저위험 종목 (Low Risk 이하): {len(df[df['risk_level'].isin(['Very Low', 'Low'])])}개

💡 투자 인사이트:
   - 최고 수익성 종목: {df.loc[df['profitability_score'].idxmax(), 'company_name']} ({df['profitability_score'].max():.1f}/30점)
   - 최고 성장성 종목: {df.loc[df['growth_score'].idxmax(), 'company_name']} ({df['growth_score'].max():.1f}/25점)
   - 최고 안정성 종목: {df.loc[df['stability_score'].idxmax(), 'company_name']} ({df['stability_score'].max():.1f}/25점)
   - 최고 가치평가 종목: {df.loc[df['valuation_score'].idxmax(), 'company_name']} ({df['valuation_score'].max():.1f}/20점)

{'='*80}
"""
            
            print(report)
            
            # 보고서 파일 저장
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = self.results_dir / f"buffett_comprehensive_report_{timestamp}.txt"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            
            logger.info(f"📄 종합 보고서 저장: {report_file}")
            
        except Exception as e:
            logger.error(f"❌ 보고서 생성 실패: {e}")
    
    def run_batch_analysis(self, max_stocks=None, save_interval=100):
        """전체 종목 배치 분석 실행"""
        logger.info("🚀 워런 버핏 스코어카드 전체 종목 배치 분석 시작")
        logger.info("=" * 80)
        
        self.stats['start_time'] = datetime.now()
        
        try:
            # 1. 테이블 생성
            self.create_results_table()
            
            # 2. 분석 대상 종목 조회
            stocks_df = self.analyzer.get_all_stocks_safe()
            
            if stocks_df.empty:
                logger.error("❌ 분석할 종목이 없습니다.")
                return
            
            # 최대 분석 수 제한
            if max_stocks:
                stocks_df = stocks_df.head(max_stocks)
                logger.info(f"🔧 제한 모드: 상위 {max_stocks}개 종목만 분석")
            
            total_stocks = len(stocks_df)
            self.stats['total_analyzed'] = total_stocks
            
            logger.info(f"📊 분석 대상: {total_stocks:,}개 종목")
            logger.info(f"💾 중간 저장 주기: {save_interval}개마다")
            
            # 3. 각 종목 분석
            for idx, row in stocks_df.iterrows():
                try:
                    stock_code = row['stock_code']
                    company_name = row['company_name']
                    
                    progress = (idx + 1) / total_stocks * 100
                    
                    # 진행률 출력 (10개마다)
                    if (idx + 1) % 10 == 0 or idx == 0:
                        logger.info(f"📊 진행률: {progress:.1f}% ({idx+1:,}/{total_stocks:,}) - {company_name}")
                    
                    # 개별 종목 분석
                    result = self.analyzer.analyze_single_stock(stock_code, company_name)
                    
                    if result and result.get('analysis_status', '').startswith('SUCCESS'):
                        self.results.append(result)
                        self.stats['successful'] += 1
                        
                        # 간단한 결과 로그 (50개마다)
                        if (idx + 1) % 50 == 0:
                            logger.info(f"✅ {company_name}: {result['total_score']:.1f}점, {result['grade']}")
                    else:
                        self.failed_stocks.append({
                            'stock_code': stock_code,
                            'company_name': company_name,
                            'error': result.get('error_message', 'Unknown error')
                        })
                        self.stats['failed'] += 1
                        
                        if (idx + 1) % 50 == 0:
                            logger.warning(f"❌ {company_name}: 분석 실패")
                    
                    # 중간 저장
                    if (idx + 1) % save_interval == 0 and self.results:
                        logger.info(f"💾 중간 저장: {len(self.results)}건")
                        self.save_results_to_database()
                    
                    # 시스템 부하 방지를 위한 짧은 대기
                    time.sleep(0.01)
                    
                except KeyboardInterrupt:
                    logger.warning("⚠️ 사용자 중단 요청")
                    break
                except Exception as e:
                    logger.error(f"❌ 종목 분석 오류: {row.get('company_name', 'Unknown')} - {str(e)}")
                    self.stats['failed'] += 1
                    continue
            
            self.stats['end_time'] = datetime.now()
            
            # 4. 최종 결과 저장
            if self.results:
                logger.info("💾 최종 결과 저장 중...")
                self.save_results_to_database()
                csv_file, json_file = self.save_results_to_files()
                
                # 5. 종합 보고서 생성
                self.generate_comprehensive_report()
                
                # 6. 최종 통계
                success_rate = self.stats['successful'] / max(1, self.stats['total_analyzed']) * 100
                elapsed_time = (self.stats['end_time'] - self.stats['start_time']).total_seconds() / 60
                
                logger.info("🎉 전체 배치 분석 완료!")
                logger.info(f"📊 최종 통계:")
                logger.info(f"   - 총 분석 시도: {self.stats['total_analyzed']:,}개")
                logger.info(f"   - 성공: {self.stats['successful']:,}개 ({success_rate:.1f}%)")
                logger.info(f"   - 실패: {self.stats['failed']:,}개")
                logger.info(f"   - 소요 시간: {elapsed_time:.1f}분")
                logger.info(f"   - 분석 속도: {self.stats['total_analyzed']/elapsed_time:.1f}개/분")
                
                if csv_file:
                    logger.info(f"📁 결과 파일: {csv_file}")
                
            else:
                logger.error("❌ 분석 성공한 종목이 없습니다.")
                
        except Exception as e:
            logger.error(f"❌ 배치 분석 실패: {e}")
            import traceback
            traceback.print_exc()

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='워런 버핏 스코어카드 전체 종목 배치 분석')
    parser.add_argument('--max-stocks', type=int, help='최대 분석 종목 수')
    parser.add_argument('--save-interval', type=int, default=100, help='중간 저장 주기 (기본: 100개)')
    parser.add_argument('--test', action='store_true', help='테스트 모드 (50개 종목)')
    parser.add_argument('--medium', action='store_true', help='중간 규모 (500개 종목)')
    
    args = parser.parse_args()
    
    print("🎯 워런 버핏 스코어카드 전체 종목 배치 분석 시스템")
    print("=" * 80)
    
    analyzer = BuffettBatchAnalyzer()
    
    if args.test:
        print("🔧 테스트 모드: 50개 종목 분석")
        analyzer.run_batch_analysis(max_stocks=50, save_interval=25)
    elif args.medium:
        print("📊 중간 규모: 500개 종목 분석")
        analyzer.run_batch_analysis(max_stocks=500, save_interval=100)
    elif args.max_stocks:
        print(f"🔢 제한 모드: {args.max_stocks}개 종목 분석")
        analyzer.run_batch_analysis(max_stocks=args.max_stocks, save_interval=args.save_interval)
    else:
        print("⚠️ 전체 종목을 분석하시겠습니까? (시간이 매우 오래 걸릴 수 있습니다)")
        print("💡 추천: --test (50개), --medium (500개), --max-stocks N (N개)")
        response = input("전체 분석을 원하면 'yes' 입력: ")
        
        if response.lower() == 'yes':
            print("🚀 전체 종목 분석 시작...")
            analyzer.run_batch_analysis(save_interval=args.save_interval)
        else:
            print("❌ 분석이 취소되었습니다.")
            print("💡 테스트 모드 실행: python buffett_batch_analyzer.py --test")

if __name__ == "__main__":
    main()
