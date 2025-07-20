"""
워런 버핏 스코어카드 110점 체계 - 배치 실행기
실제 데이터베이스 연동 및 대량 처리

주요 기능:
1. DART 데이터베이스에서 재무 데이터 자동 추출
2. 주가 데이터베이스에서 시장 데이터 연동
3. 워런 버핏 110점 스코어 일괄 계산
4. 결과를 데이터베이스에 저장
5. 스크리닝 결과 JSON 파일 생성
"""

import sqlite3
import logging
import json
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from pathlib import Path
import sys
import os

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

try:
    from src.analysis.fundamental.buffett_scorecard_110_complete import BuffettScorecard110, BuffettAnalysis
except ImportError:
    from buffett_scorecard_110_complete import BuffettScorecard110, BuffettAnalysis

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('buffett_scorecard_batch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BuffettBatchProcessor:
    """워런 버핏 스코어카드 배치 처리기"""
    
    def __init__(self, data_dir: str = "data"):
        """초기화"""
        self.data_dir = Path(data_dir)
        self.scorecard = BuffettScorecard110()
        
        # 데이터베이스 경로 설정
        self.dart_db_path = self.data_dir / "dart_data.db"
        self.stock_db_path = self.data_dir / "stock_data.db"
        self.scorecard_db_path = self.data_dir / "buffett_scorecard.db"
        
        # 스코어카드 데이터베이스 초기화
        self._init_scorecard_database()
        
        logger.info("워런 버핏 배치 처리기 초기화 완료")
    
    def _init_scorecard_database(self):
        """스코어카드 데이터베이스 초기화"""
        try:
            with sqlite3.connect(self.scorecard_db_path) as conn:
                cursor = conn.cursor()
                
                # 분석 결과 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS buffett_analysis_110 (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        company_name TEXT NOT NULL,
                        analysis_date DATE NOT NULL,
                        total_score REAL NOT NULL,
                        score_percentage REAL NOT NULL,
                        overall_grade TEXT NOT NULL,
                        investment_grade TEXT NOT NULL,
                        risk_level TEXT NOT NULL,
                        quality_rating TEXT NOT NULL,
                        
                        -- 카테고리별 점수
                        profitability_score REAL NOT NULL,
                        profitability_percentage REAL NOT NULL,
                        growth_score REAL NOT NULL,
                        growth_percentage REAL NOT NULL,
                        stability_score REAL NOT NULL,
                        stability_percentage REAL NOT NULL,
                        efficiency_score REAL NOT NULL,
                        efficiency_percentage REAL NOT NULL,
                        valuation_score REAL NOT NULL,
                        valuation_percentage REAL NOT NULL,
                        quality_score REAL NOT NULL,
                        quality_percentage REAL NOT NULL,
                        
                        -- 부가 정보
                        key_strengths TEXT,
                        key_weaknesses TEXT,
                        investment_thesis TEXT,
                        target_price_low REAL,
                        target_price_high REAL,
                        
                        -- 메타데이터
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        
                        UNIQUE(stock_code, analysis_date)
                    )
                """)
                
                # 세부 점수 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS buffett_details_110 (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        analysis_id INTEGER NOT NULL,
                        category TEXT NOT NULL,
                        indicator_name TEXT NOT NULL,
                        indicator_value REAL,
                        score REAL NOT NULL,
                        max_score REAL NOT NULL,
                        score_percentage REAL NOT NULL,
                        description TEXT,
                        
                        FOREIGN KEY (analysis_id) REFERENCES buffett_analysis_110 (id)
                    )
                """)
                
                # 인덱스 생성
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_code ON buffett_analysis_110(stock_code)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_date ON buffett_analysis_110(analysis_date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_total_score ON buffett_analysis_110(total_score DESC)")
                
                conn.commit()
                logger.info("스코어카드 데이터베이스 초기화 완료")
                
        except Exception as e:
            logger.error(f"데이터베이스 초기화 오류: {e}")
            raise
    
    def get_stock_list(self) -> List[Dict[str, str]]:
        """분석 대상 종목 목록 조회"""
        try:
            with sqlite3.connect(self.dart_db_path) as conn:
                query = """
                    SELECT DISTINCT stock_code, corp_name
                    FROM corp_codes
                    WHERE stock_code IS NOT NULL 
                    AND stock_code != ''
                    AND LENGTH(stock_code) = 6
                    ORDER BY corp_name
                """
                df = pd.read_sql_query(query, conn)
                
                stock_list = []
                for _, row in df.iterrows():
                    stock_list.append({
                        'stock_code': row['stock_code'],
                        'company_name': row['corp_name']
                    })
                
                logger.info(f"분석 대상 종목 {len(stock_list)}개 조회 완료")
                return stock_list
                
        except Exception as e:
            logger.error(f"종목 목록 조회 오류: {e}")
            return []
    
    def get_financial_data(self, stock_code: str) -> Optional[Dict]:
        """특정 종목의 재무 데이터 조회"""
        try:
            with sqlite3.connect(self.dart_db_path) as conn:
                # 최신 재무제표 데이터
                query = """
                    SELECT *
                    FROM financial_statements
                    WHERE stock_code = ?
                    ORDER BY bsns_year DESC, reprt_code DESC
                    LIMIT 10
                """
                df = pd.read_sql_query(query, conn, params=[stock_code])
                
                if df.empty:
                    return None
                
                # 최신 데이터
                latest = df.iloc[0]
                
                # 시계열 데이터 구성 (최근 4년)
                revenue_history = []
                income_history = []
                equity_history = []
                
                for _, row in df.head(4).iterrows():
                    if pd.notna(row.get('thstrm_amount')):
                        revenue_history.append(float(row['thstrm_amount']) * 1000000)  # 백만원 -> 원
                    if pd.notna(row.get('frmtrm_amount')):
                        income_history.append(float(row['frmtrm_amount']) * 1000000)
                
                # 재무 데이터 정리
                financial_data = {
                    'stock_code': stock_code,
                    'company_name': latest.get('corp_name', ''),
                    
                    # 기본 재무 데이터 (단위: 원)
                    'net_income': float(latest.get('thstrm_amount', 0)) * 1000000,
                    'revenue': float(latest.get('revenue', 0)) * 1000000,
                    'total_assets': float(latest.get('total_assets', 0)) * 1000000,
                    'shareholders_equity': float(latest.get('total_equity', 0)) * 1000000,
                    'current_assets': float(latest.get('current_assets', 0)) * 1000000,
                    'current_liabilities': float(latest.get('current_liabilities', 0)) * 1000000,
                    'total_debt': float(latest.get('total_debt', 0)) * 1000000,
                    
                    # 시계열 데이터
                    'revenue_history': revenue_history[::-1] if revenue_history else [],
                    'net_income_history': income_history[::-1] if income_history else [],
                    
                    # 기타 필요한 데이터들을 실제 DB 스키마에 맞게 추가
                }
                
                return financial_data
                
        except Exception as e:
            logger.error(f"재무 데이터 조회 오류 ({stock_code}): {e}")
            return None
    
    def get_market_data(self, stock_code: str) -> Optional[Dict]:
        """특정 종목의 시장 데이터 조회"""
        try:
            with sqlite3.connect(self.stock_db_path) as conn:
                query = """
                    SELECT close, shares_outstanding
                    FROM stock_data
                    WHERE stock_code = ?
                    ORDER BY date DESC
                    LIMIT 1
                """
                df = pd.read_sql_query(query, conn, params=[stock_code])
                
                if df.empty:
                    return {'stock_price': 0}
                
                latest = df.iloc[0]
                
                market_data = {
                    'stock_price': float(latest.get('close', 0)),
                    'shares_outstanding': float(latest.get('shares_outstanding', 0))
                }
                
                return market_data
                
        except Exception as e:
            logger.error(f"시장 데이터 조회 오류 ({stock_code}): {e}")
            return {'stock_price': 0}
    
    def save_analysis_result(self, analysis: BuffettAnalysis) -> int:
        """분석 결과를 데이터베이스에 저장"""
        try:
            with sqlite3.connect(self.scorecard_db_path) as conn:
                cursor = conn.cursor()
                
                # 기존 데이터 삭제 (같은 날짜)
                cursor.execute("""
                    DELETE FROM buffett_analysis_110 
                    WHERE stock_code = ? AND analysis_date = ?
                """, [analysis.stock_code, analysis.analysis_date])
                
                # 분석 결과 저장
                cursor.execute("""
                    INSERT INTO buffett_analysis_110 (
                        stock_code, company_name, analysis_date,
                        total_score, score_percentage, overall_grade,
                        investment_grade, risk_level, quality_rating,
                        profitability_score, profitability_percentage,
                        growth_score, growth_percentage,
                        stability_score, stability_percentage,
                        efficiency_score, efficiency_percentage,
                        valuation_score, valuation_percentage,
                        quality_score, quality_percentage,
                        key_strengths, key_weaknesses, investment_thesis,
                        target_price_low, target_price_high
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    analysis.stock_code, analysis.company_name, analysis.analysis_date,
                    analysis.total_score, analysis.score_percentage, analysis.overall_grade,
                    analysis.investment_grade.value, analysis.risk_level.value, 
                    analysis.quality_rating.value,
                    analysis.profitability.actual_score, analysis.profitability.percentage,
                    analysis.growth.actual_score, analysis.growth.percentage,
                    analysis.stability.actual_score, analysis.stability.percentage,
                    analysis.efficiency.actual_score, analysis.efficiency.percentage,
                    analysis.valuation.actual_score, analysis.valuation.percentage,
                    analysis.quality.actual_score, analysis.quality.percentage,
                    json.dumps(analysis.key_strengths, ensure_ascii=False),
                    json.dumps(analysis.key_weaknesses, ensure_ascii=False),
                    analysis.investment_thesis,
                    analysis.target_price_range[0], analysis.target_price_range[1]
                ])
                
                analysis_id = cursor.lastrowid
                
                # 세부 점수 저장
                all_categories = [
                    analysis.profitability, analysis.growth, analysis.stability,
                    analysis.efficiency, analysis.valuation, analysis.quality
                ]
                
                for category in all_categories:
                    for detail in category.details:
                        cursor.execute("""
                            INSERT INTO buffett_details_110 (
                                analysis_id, category, indicator_name,
                                indicator_value, score, max_score,
                                score_percentage, description
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            analysis_id, category.category, detail.name,
                            detail.value, detail.score, detail.max_score,
                            detail.percentage, detail.description
                        ])
                
                conn.commit()
                logger.info(f"분석 결과 저장 완료: {analysis.company_name} ({analysis.stock_code})")
                return analysis_id
                
        except Exception as e:
            logger.error(f"분석 결과 저장 오류: {e}")
            return -1
    
    def process_single_stock(self, stock_code: str, company_name: str) -> Optional[BuffettAnalysis]:
        """단일 종목 워런 버핏 분석 처리"""
        try:
            logger.info(f"분석 시작: {company_name} ({stock_code})")
            
            # 재무 데이터 조회
            financial_data = self.get_financial_data(stock_code)
            if not financial_data:
                logger.warning(f"재무 데이터 없음: {stock_code}")
                return None
            
            # 시장 데이터 조회
            market_data = self.get_market_data(stock_code)
            
            # 워런 버핏 분석 실행
            analysis = self.scorecard.calculate_comprehensive_score(financial_data, market_data)
            
            # 결과 저장
            analysis_id = self.save_analysis_result(analysis)
            if analysis_id > 0:
                logger.info(f"분석 완료: {company_name} - 점수 {analysis.total_score:.1f}/110")
                return analysis
            else:
                logger.error(f"결과 저장 실패: {stock_code}")
                return None
                
        except Exception as e:
            logger.error(f"종목 분석 오류 ({stock_code}): {e}")
            return None
    
    def process_all_stocks(self, limit: Optional[int] = None) -> List[BuffettAnalysis]:
        """전체 종목 배치 처리"""
        logger.info("워런 버핏 스코어카드 배치 처리 시작")
        
        # 종목 목록 조회
        stock_list = self.get_stock_list()
        if not stock_list:
            logger.error("분석 대상 종목이 없습니다")
            return []
        
        if limit:
            stock_list = stock_list[:limit]
            logger.info(f"처리 제한: {limit}개 종목")
        
        results = []
        success_count = 0
        
        for i, stock_info in enumerate(stock_list, 1):
            stock_code = stock_info['stock_code']
            company_name = stock_info['company_name']
            
            logger.info(f"진행률: {i}/{len(stock_list)} ({i/len(stock_list)*100:.1f}%)")
            
            try:
                analysis = self.process_single_stock(stock_code, company_name)
                if analysis:
                    results.append(analysis)
                    success_count += 1
                    
                    # 중간 결과 출력
                    if success_count % 10 == 0:
                        logger.info(f"중간 집계: {success_count}개 종목 완료")
                        
            except Exception as e:
                logger.error(f"종목 처리 중 오류 ({stock_code}): {e}")
                continue
        
        logger.info(f"배치 처리 완료: 전체 {len(stock_list)}개 중 {success_count}개 성공")
        return results
    
    def generate_screening_report(self) -> Dict[str, Any]:
        """스크리닝 리포트 생성"""
        try:
            with sqlite3.connect(self.scorecard_db_path) as conn:
                # 전체 분석 결과 조회
                query = """
                    SELECT *
                    FROM buffett_analysis_110
                    WHERE analysis_date = (
                        SELECT MAX(analysis_date) FROM buffett_analysis_110
                    )
                    ORDER BY total_score DESC
                """
                df = pd.read_sql_query(query, conn)
                
                if df.empty:
                    return {"error": "분석 결과가 없습니다"}
                
                # 등급별 분류
                strong_buy = df[df['investment_grade'] == 'Strong Buy']
                buy = df[df['investment_grade'] == 'Buy']
                hold = df[df['investment_grade'] == 'Hold']
                
                # 상위 종목들
                top_10 = df.head(10)
                
                # 스크리닝 기준 (워런 버핏 스타일)
                buffett_criteria = df[
                    (df['total_score'] >= 75) &
                    (df['stability_percentage'] >= 70) &
                    (df['profitability_percentage'] >= 70) &
                    (df['valuation_percentage'] >= 60)
                ]
                
                report = {
                    "analysis_date": df.iloc[0]['analysis_date'],
                    "total_stocks": len(df),
                    "summary": {
                        "average_score": df['total_score'].mean(),
                        "median_score": df['total_score'].median(),
                        "max_score": df['total_score'].max(),
                        "min_score": df['total_score'].min()
                    },
                    "grade_distribution": {
                        "Strong Buy": len(strong_buy),
                        "Buy": len(buy),
                        "Hold": len(hold),
                        "Others": len(df) - len(strong_buy) - len(buy) - len(hold)
                    },
                    "top_10_stocks": [
                        {
                            "rank": i + 1,
                            "stock_code": row['stock_code'],
                            "company_name": row['company_name'],
                            "total_score": row['total_score'],
                            "grade": row['overall_grade'],
                            "investment_grade": row['investment_grade']
                        }
                        for i, (_, row) in enumerate(top_10.iterrows())
                    ],
                    "buffett_recommendations": [
                        {
                            "stock_code": row['stock_code'],
                            "company_name": row['company_name'],
                            "total_score": row['total_score'],
                            "profitability": row['profitability_percentage'],
                            "stability": row['stability_percentage'],
                            "valuation": row['valuation_percentage'],
                            "investment_thesis": row['investment_thesis']
                        }
                        for _, row in buffett_criteria.head(20).iterrows()
                    ]
                }
                
                return report
                
        except Exception as e:
            logger.error(f"리포트 생성 오류: {e}")
            return {"error": str(e)}
    
    def save_screening_results(self, output_path: str = "buffett_screening_results_110.json"):
        """스크리닝 결과를 JSON 파일로 저장"""
        try:
            report = self.generate_screening_report()
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"스크리닝 결과 저장: {output_path}")
            
            # 요약 출력
            if "error" not in report:
                print("\n🎯 워런 버핏 스코어카드 110점 스크리닝 결과")
                print("=" * 60)
                print(f"📊 분석 종목 수: {report['total_stocks']:,}개")
                print(f"📈 평균 점수: {report['summary']['average_score']:.1f}점")
                print(f"🏆 최고 점수: {report['summary']['max_score']:.1f}점")
                print()
                
                print("📊 투자 등급 분포:")
                for grade, count in report['grade_distribution'].items():
                    print(f"  {grade}: {count}개")
                print()
                
                print("🥇 상위 10개 종목:")
                for stock in report['top_10_stocks'][:5]:
                    print(f"  {stock['rank']}. {stock['company_name']} ({stock['stock_code']}) "
                          f"- {stock['total_score']:.1f}점 ({stock['grade']})")
                print()
                
                buffett_count = len(report['buffett_recommendations'])
                print(f"✨ 워런 버핏 추천 종목: {buffett_count}개")
                
        except Exception as e:
            logger.error(f"결과 저장 오류: {e}")

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='워런 버핏 스코어카드 110점 체계 배치 처리')
    parser.add_argument('--limit', type=int, help='처리할 종목 수 제한')
    parser.add_argument('--stock-code', type=str, help='특정 종목만 처리')
    parser.add_argument('--data-dir', type=str, default='data', help='데이터 디렉토리 경로')
    parser.add_argument('--output', type=str, default='buffett_screening_results_110.json', 
                       help='결과 파일 출력 경로')
    
    args = parser.parse_args()
    
    # 배치 처리기 초기화
    processor = BuffettBatchProcessor(data_dir=args.data_dir)
    
    try:
        if args.stock_code:
            # 특정 종목 처리
            logger.info(f"특정 종목 분석: {args.stock_code}")
            
            # 종목명 조회
            stock_list = processor.get_stock_list()
            stock_info = next((s for s in stock_list if s['stock_code'] == args.stock_code), None)
            
            if stock_info:
                result = processor.process_single_stock(args.stock_code, stock_info['company_name'])
                if result:
                    print(f"\n✅ 분석 완료: {result.company_name}")
                    print(f"총점: {result.total_score:.1f}/110점")
                    print(f"등급: {result.overall_grade}")
                    print(f"추천: {result.investment_grade.value}")
                else:
                    print(f"❌ 분석 실패: {args.stock_code}")
            else:
                print(f"❌ 종목을 찾을 수 없습니다: {args.stock_code}")
                
        else:
            # 전체 배치 처리
            results = processor.process_all_stocks(limit=args.limit)
            
            if results:
                # 스크리닝 결과 저장
                processor.save_screening_results(args.output)
                
                print(f"\n🎉 배치 처리 완료!")
                print(f"처리된 종목: {len(results)}개")
                print(f"결과 파일: {args.output}")
                
                # 간단한 통계
                scores = [r.total_score for r in results]
                print(f"평균 점수: {sum(scores)/len(scores):.1f}점")
                print(f"최고 점수: {max(scores):.1f}점")
                
                # 상위 5개 종목
                top_5 = sorted(results, key=lambda x: x.total_score, reverse=True)[:5]
                print("\n🏆 상위 5개 종목:")
                for i, result in enumerate(top_5, 1):
                    print(f"  {i}. {result.company_name} ({result.stock_code}) - {result.total_score:.1f}점")
            else:
                print("❌ 처리된 종목이 없습니다.")
                
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        print("\n⏹️ 처리가 중단되었습니다.")
    except Exception as e:
        logger.error(f"실행 중 오류: {e}")
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()
