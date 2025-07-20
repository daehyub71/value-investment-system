#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 종목 배치 워런 버핏 스코어카드 분석기
수집된 모든 기업에 대해 워런 버핏 스코어 계산 및 랭킹

주요 기능:
1. 전체 수집된 기업 자동 발견
2. 배치로 워런 버핏 스코어 계산
3. 상위/하위 종목 랭킹
4. 투자 추천 종목 필터링
5. 결과를 CSV/JSON으로 저장
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import json
import warnings
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import time

warnings.filterwarnings('ignore')

class BatchBuffettAnalyzer:
    """전체 종목 배치 워런 버핏 분석기"""
    
    def __init__(self):
        """초기화"""
        import logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('BatchBuffettAnalyzer')
        
        # 데이터베이스 경로
        self.dart_db = Path("data/databases/dart_data.db")
        self.stock_db = Path("data/databases/stock_data.db")
        
        # 점수 가중치
        self.PROFITABILITY_WEIGHT = 30
        self.GROWTH_WEIGHT = 25
        self.STABILITY_WEIGHT = 25  
        self.EFFICIENCY_WEIGHT = 10
        self.VALUATION_WEIGHT = 20
        self.MAX_SCORE = 110
        
        # 결과 저장 경로
        self.output_dir = Path("results/buffett_analysis")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("BatchBuffettAnalyzer 초기화 완료")
    
    def discover_available_companies(self) -> pd.DataFrame:
        """분석 가능한 기업들 자동 발견"""
        try:
            self.logger.info("🔍 분석 가능한 기업들 탐색 중...")
            
            if not self.dart_db.exists():
                self.logger.error("DART 데이터베이스가 없습니다.")
                return pd.DataFrame()
            
            with sqlite3.connect(self.dart_db) as conn:
                # 재무데이터가 있는 모든 기업 조회
                discovery_query = """
                SELECT 
                    corp_code,
                    stock_code,
                    company_name,
                    COUNT(*) as financial_records,
                    MAX(bsns_year) as latest_year,
                    COUNT(DISTINCT account_nm) as unique_accounts
                FROM financial_statements
                WHERE stock_code IS NOT NULL 
                    AND company_name IS NOT NULL
                    AND thstrm_amount IS NOT NULL
                GROUP BY corp_code, stock_code, company_name
                HAVING financial_records >= 5  -- 최소 5개 재무항목 필요
                ORDER BY financial_records DESC
                """
                
                companies_df = pd.read_sql_query(discovery_query, conn)
                
                if not companies_df.empty:
                    self.logger.info(f"✅ 분석 가능한 기업: {len(companies_df)}개 발견")
                    self.logger.info(f"📊 평균 재무항목 수: {companies_df['financial_records'].mean():.1f}개")
                    
                    # 상위 10개 기업 미리보기
                    self.logger.info("\n📈 재무데이터 상위 10개 기업:")
                    for _, row in companies_df.head(10).iterrows():
                        self.logger.info(f"   • {row['company_name']} ({row['stock_code']}): {row['financial_records']}개 항목")
                    
                    return companies_df
                else:
                    self.logger.warning("분석 가능한 기업을 찾을 수 없습니다.")
                    return pd.DataFrame()
                    
        except Exception as e:
            self.logger.error(f"기업 탐색 실패: {e}")
            return pd.DataFrame()
    
    def get_company_financial_data(self, corp_code: str, stock_code: str) -> pd.DataFrame:
        """개별 기업 재무데이터 조회"""
        try:
            with sqlite3.connect(self.dart_db) as conn:
                query = """
                SELECT * FROM financial_statements
                WHERE corp_code = ? AND stock_code = ?
                ORDER BY bsns_year DESC, reprt_code DESC
                """
                
                df = pd.read_sql_query(query, conn, params=[corp_code, stock_code])
                return df
                
        except Exception as e:
            self.logger.debug(f"재무데이터 조회 실패 {stock_code}: {e}")
            return pd.DataFrame()
    
    def parse_financial_data(self, df: pd.DataFrame) -> Dict[str, float]:
        """재무데이터 파싱 (안전한 버전)"""
        try:
            if df.empty:
                return {}
            
            financial_metrics = {}
            
            # 주요 계정과목 매핑
            account_mapping = {
                '매출액': ['매출액', '수익(매출액)', '영업수익', '매출', '총매출액', '영업수익', '수익'],
                '영업이익': ['영업이익', '영업손익', '영업이익(손실)'],
                '당기순이익': ['당기순이익', '순이익', '당기순손익', '당기순이익(손실)', '당기순손익(세후)', '법인세비용차감전순이익'],
                '총자산': ['자산총계', '총자산', '자산합계'],
                '자기자본': ['자본총계', '자기자본총계', '주주지분', '자본합계', '지배기업소유주지분'],
                '부채총계': ['부채총계', '총부채', '부채합계'],
                '유동자산': ['유동자산'],
                '유동부채': ['유동부채']
            }
            
            # 계정과목별 데이터 추출
            for metric, possible_names in account_mapping.items():
                found = False
                for name in possible_names:
                    if found:
                        break
                    
                    # 계정과목 매칭
                    matching_rows = df[
                        df['account_nm'].astype(str).str.contains(name, na=False, case=False)
                    ]
                    
                    if not matching_rows.empty:
                        # 최신 데이터 선택
                        latest_row = matching_rows.iloc[0]
                        amount_str = str(latest_row.get('thstrm_amount', '0'))
                        
                        # 안전한 숫자 변환
                        try:
                            clean_amount = amount_str.replace(',', '').replace(' ', '').replace('-', '0')
                            if clean_amount and clean_amount.replace('.', '').replace('-', '').isdigit():
                                amount = float(clean_amount)
                                if amount > 0:  # 양수만 저장
                                    financial_metrics[metric] = amount
                                    found = True
                                    break
                        except (ValueError, TypeError):
                            continue
            
            return financial_metrics
            
        except Exception as e:
            return {}
    
    def calculate_buffett_score(self, financial_data: Dict[str, float], stock_code: str = None) -> Dict:
        """워런 버핏 스코어 계산"""
        try:
            scores = {}
            details = {}
            
            # 1. 수익성 지표 (30점)
            profitability_score = 0
            prof_details = {}
            
            # ROE 계산
            if '당기순이익' in financial_data and '자기자본' in financial_data and financial_data['자기자본'] > 0:
                roe = (financial_data['당기순이익'] / financial_data['자기자본']) * 100
                if roe >= 20:
                    roe_score = 12
                elif roe >= 15:
                    roe_score = 10
                elif roe >= 10:
                    roe_score = 6
                elif roe >= 5:
                    roe_score = 3
                else:
                    roe_score = 0
                profitability_score += roe_score
                prof_details['ROE'] = f"{roe:.1f}%"
            
            # 영업이익률
            if '영업이익' in financial_data and '매출액' in financial_data and financial_data['매출액'] > 0:
                op_margin = (financial_data['영업이익'] / financial_data['매출액']) * 100
                if op_margin >= 20:
                    op_score = 10
                elif op_margin >= 15:
                    op_score = 8
                elif op_margin >= 10:
                    op_score = 5
                elif op_margin >= 5:
                    op_score = 2
                else:
                    op_score = 0
                profitability_score += op_score
                prof_details['영업이익률'] = f"{op_margin:.1f}%"
            
            # 순이익률
            if '당기순이익' in financial_data and '매출액' in financial_data and financial_data['매출액'] > 0:
                net_margin = (financial_data['당기순이익'] / financial_data['매출액']) * 100
                if net_margin >= 15:
                    net_score = 8
                elif net_margin >= 10:
                    net_score = 6
                elif net_margin >= 5:
                    net_score = 3
                elif net_margin >= 0:
                    net_score = 1
                else:
                    net_score = 0
                profitability_score += net_score
                prof_details['순이익률'] = f"{net_margin:.1f}%"
            
            scores['profitability'] = min(profitability_score, self.PROFITABILITY_WEIGHT)
            details['profitability'] = prof_details
            
            # 2. 안정성 지표 (25점)
            stability_score = 0
            stab_details = {}
            
            # 부채비율
            if '부채총계' in financial_data and '자기자본' in financial_data and financial_data['자기자본'] > 0:
                debt_ratio = (financial_data['부채총계'] / financial_data['자기자본']) * 100
                if debt_ratio <= 30:
                    debt_score = 15
                elif debt_ratio <= 50:
                    debt_score = 12
                elif debt_ratio <= 100:
                    debt_score = 8
                elif debt_ratio <= 200:
                    debt_score = 4
                else:
                    debt_score = 0
                stability_score += debt_score
                stab_details['부채비율'] = f"{debt_ratio:.1f}%"
            
            # 유동비율
            if '유동자산' in financial_data and '유동부채' in financial_data and financial_data['유동부채'] > 0:
                current_ratio = (financial_data['유동자산'] / financial_data['유동부채']) * 100
                if current_ratio >= 200:
                    current_score = 10
                elif current_ratio >= 150:
                    current_score = 8
                elif current_ratio >= 100:
                    current_score = 5
                elif current_ratio >= 80:
                    current_score = 2
                else:
                    current_score = 0
                stability_score += current_score
                stab_details['유동비율'] = f"{current_ratio:.1f}%"
            
            scores['stability'] = min(stability_score, self.STABILITY_WEIGHT)
            details['stability'] = stab_details
            
            # 3. 효율성 지표 (10점)
            efficiency_score = 0
            eff_details = {}
            
            # 총자산회전율
            if '매출액' in financial_data and '총자산' in financial_data and financial_data['총자산'] > 0:
                asset_turnover = financial_data['매출액'] / financial_data['총자산']
                if asset_turnover >= 1.0:
                    turnover_score = 10
                elif asset_turnover >= 0.7:
                    turnover_score = 7
                elif asset_turnover >= 0.5:
                    turnover_score = 4
                elif asset_turnover >= 0.3:
                    turnover_score = 2
                else:
                    turnover_score = 0
                efficiency_score += turnover_score
                eff_details['총자산회전율'] = f"{asset_turnover:.2f}회"
            
            scores['efficiency'] = min(efficiency_score, self.EFFICIENCY_WEIGHT)
            details['efficiency'] = eff_details
            
            # 4. 성장성 & 가치평가 (기본값)
            scores['growth'] = 15  # 기본값 (추후 다년도 데이터로 개선 가능)
            scores['valuation'] = 12  # 기본값 (주가 데이터 연동 시 개선 가능)
            
            details['growth'] = {"성장성": "추정값"}
            details['valuation'] = {"가치평가": "추정값"}
            
            # 총점 계산
            total_score = sum(scores.values())
            
            return {
                'total_score': total_score,
                'scores': scores,
                'details': details,
                'financial_data': financial_data,
                'analysis_date': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'total_score': 0,
                'scores': {},
                'details': {},
                'error': str(e),
                'analysis_date': datetime.now().isoformat()
            }
    
    def analyze_all_companies(self, max_companies: int = None) -> pd.DataFrame:
        """모든 기업 배치 분석"""
        try:
            self.logger.info("🚀 전체 기업 워런 버핏 스코어카드 배치 분석 시작")
            
            # 1. 분석 가능한 기업들 발견
            companies_df = self.discover_available_companies()
            
            if companies_df.empty:
                self.logger.error("분석할 기업이 없습니다.")
                return pd.DataFrame()
            
            # 2. 분석할 기업 수 제한 (옵션)
            if max_companies:
                companies_df = companies_df.head(max_companies)
                self.logger.info(f"📊 분석 대상을 상위 {max_companies}개 기업으로 제한")
            
            # 3. 배치 분석 실행
            results = []
            total_companies = len(companies_df)
            
            self.logger.info(f"📈 총 {total_companies}개 기업 분석 시작...")
            
            for idx, (_, company) in enumerate(companies_df.iterrows(), 1):
                try:
                    # 진행률 표시
                    if idx % 50 == 0 or idx == total_companies:
                        progress = (idx / total_companies) * 100
                        self.logger.info(f"진행률: {idx}/{total_companies} ({progress:.1f}%)")
                    
                    # 개별 기업 분석
                    corp_code = company['corp_code']
                    stock_code = company['stock_code']
                    company_name = company['company_name']
                    
                    # 재무데이터 조회 및 파싱
                    financial_df = self.get_company_financial_data(corp_code, stock_code)
                    financial_data = self.parse_financial_data(financial_df)
                    
                    if not financial_data:
                        continue  # 재무데이터가 없으면 스킵
                    
                    # 워런 버핏 스코어 계산
                    score_result = self.calculate_buffett_score(financial_data, stock_code)
                    
                    # 결과 수집
                    result = {
                        'stock_code': stock_code,
                        'company_name': company_name,
                        'corp_code': corp_code,
                        'total_score': score_result['total_score'],
                        'profitability_score': score_result['scores'].get('profitability', 0),
                        'stability_score': score_result['scores'].get('stability', 0),
                        'efficiency_score': score_result['scores'].get('efficiency', 0),
                        'growth_score': score_result['scores'].get('growth', 0),
                        'valuation_score': score_result['scores'].get('valuation', 0),
                        'financial_records': company['financial_records'],
                        'latest_year': company['latest_year'],
                        'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    # 재무 지표 추가
                    for metric, value in financial_data.items():
                        result[f'{metric}_amount'] = value
                    
                    # 비율 지표 추가
                    details = score_result['details']
                    for category, metrics in details.items():
                        for metric_name, metric_value in metrics.items():
                            result[f'{metric_name}'] = metric_value
                    
                    results.append(result)
                    
                except Exception as e:
                    self.logger.debug(f"기업 {company_name} 분석 실패: {e}")
                    continue
            
            # 4. 결과 DataFrame 생성
            if results:
                results_df = pd.DataFrame(results)
                results_df = results_df.sort_values('total_score', ascending=False).reset_index(drop=True)
                
                self.logger.info(f"✅ 분석 완료: {len(results_df)}개 기업")
                return results_df
            else:
                self.logger.warning("분석된 기업이 없습니다.")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"배치 분석 실패: {e}")
            return pd.DataFrame()
    
    def save_results(self, results_df: pd.DataFrame):
        """결과 저장"""
        try:
            if results_df.empty:
                return
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # CSV 저장
            csv_path = self.output_dir / f"buffett_analysis_{timestamp}.csv"
            results_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"📄 CSV 결과 저장: {csv_path}")
            
            # JSON 저장 (상위 50개만)
            top_50 = results_df.head(50)
            json_data = {
                'analysis_date': datetime.now().isoformat(),
                'total_companies': len(results_df),
                'top_companies': top_50.to_dict('records')
            }
            
            json_path = self.output_dir / f"buffett_top50_{timestamp}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"📄 JSON 결과 저장: {json_path}")
            
        except Exception as e:
            self.logger.error(f"결과 저장 실패: {e}")
    
    def print_summary_report(self, results_df: pd.DataFrame):
        """요약 보고서 출력"""
        if results_df.empty:
            print("📊 분석된 데이터가 없습니다.")
            return
        
        print("\n" + "="*80)
        print("🏆 워런 버핏 스코어카드 전체 기업 분석 결과")
        print("="*80)
        
        print(f"📊 분석 기업 수: {len(results_df)}개")
        print(f"📅 분석 시점: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📈 평균 점수: {results_df['total_score'].mean():.1f}점")
        print(f"🎯 최고 점수: {results_df['total_score'].max():.1f}점")
        print(f"📉 최저 점수: {results_df['total_score'].min():.1f}점")
        
        # 등급별 분포
        print(f"\n📊 등급별 분포:")
        s_grade = len(results_df[results_df['total_score'] >= 88])  # 80% of 110
        a_grade = len(results_df[(results_df['total_score'] >= 77) & (results_df['total_score'] < 88)])
        b_grade = len(results_df[(results_df['total_score'] >= 66) & (results_df['total_score'] < 77)])
        c_grade = len(results_df[(results_df['total_score'] >= 44) & (results_df['total_score'] < 66)])
        d_grade = len(results_df[results_df['total_score'] < 44])
        
        print(f"   🥇 S급 (Strong Buy): {s_grade}개 ({s_grade/len(results_df)*100:.1f}%)")
        print(f"   🥈 A급 (Buy): {a_grade}개 ({a_grade/len(results_df)*100:.1f}%)")
        print(f"   🥉 B급 (Hold): {b_grade}개 ({b_grade/len(results_df)*100:.1f}%)")
        print(f"   📊 C급 (Sell): {c_grade}개 ({c_grade/len(results_df)*100:.1f}%)")
        print(f"   ⚠️ D급 (Strong Sell): {d_grade}개 ({d_grade/len(results_df)*100:.1f}%)")
        
        # 상위 20개 기업
        print(f"\n🏆 상위 20개 투자 추천 기업:")
        print("-" * 80)
        print(f"{'순위':<4} {'종목코드':<8} {'기업명':<20} {'총점':<6} {'수익성':<6} {'안정성':<6} {'등급'}")
        print("-" * 80)
        
        for idx, (_, row) in enumerate(results_df.head(20).iterrows(), 1):
            total_score = row['total_score']
            if total_score >= 88:
                grade = "S급"
            elif total_score >= 77:
                grade = "A급"
            elif total_score >= 66:
                grade = "B급"
            else:
                grade = "C급"
            
            print(f"{idx:<4} {row['stock_code']:<8} {row['company_name']:<20} "
                  f"{total_score:<6.1f} {row['profitability_score']:<6.1f} "
                  f"{row['stability_score']:<6.1f} {grade}")
        
        # 하위 10개 기업 (주의 필요)
        print(f"\n⚠️ 하위 10개 주의 기업:")
        print("-" * 60)
        bottom_10 = results_df.tail(10)
        for idx, (_, row) in enumerate(bottom_10.iterrows(), 1):
            print(f"{len(results_df)-10+idx:<4} {row['stock_code']:<8} {row['company_name']:<20} {row['total_score']:<6.1f}점")

def main():
    """메인 실행 함수"""
    print("🚀 전체 종목 워런 버핏 스코어카드 배치 분석기")
    print("="*70)
    
    try:
        analyzer = BatchBuffettAnalyzer()
        
        # 사용자 입력
        print("\n📊 분석 옵션:")
        print("1. 전체 기업 분석 (시간 소요)")
        print("2. 상위 100개 기업만 분석 (추천)")
        print("3. 상위 50개 기업만 분석 (빠름)")
        
        choice = input("\n선택하세요 (1/2/3): ").strip()
        
        if choice == "1":
            max_companies = None
            print("전체 기업 분석을 시작합니다...")
        elif choice == "2":
            max_companies = 100
            print("상위 100개 기업 분석을 시작합니다...")
        elif choice == "3":
            max_companies = 50
            print("상위 50개 기업 분석을 시작합니다...")
        else:
            max_companies = 50
            print("기본값: 상위 50개 기업 분석을 시작합니다...")
        
        # 배치 분석 실행
        results_df = analyzer.analyze_all_companies(max_companies=max_companies)
        
        if not results_df.empty:
            # 요약 보고서 출력
            analyzer.print_summary_report(results_df)
            
            # 결과 저장
            analyzer.save_results(results_df)
            
            print(f"\n🎉 분석 완료!")
            print(f"📊 총 {len(results_df)}개 기업 분석")
            print(f"📁 결과 파일 저장: results/buffett_analysis/")
            
        else:
            print("❌ 분석할 수 있는 기업이 없습니다.")
            
    except KeyboardInterrupt:
        print("\n⏹️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()