#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
개선된 워런 버핏 스코어카드 계산기
데이터베이스 조회 개선 버전

주요 개선사항:
1. 더 유연한 데이터 검색 조건
2. 테이블 스키마 자동 확인
3. 실제 데이터 존재 여부 검증
4. 대안 검색 방법 추가
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import sys
from pathlib import Path

warnings.filterwarnings('ignore')

# 프로젝트 루트 경로 추가
sys.path.insert(0, str(Path(__file__).parent))

# 수정된 ConfigManager 임포트
try:
    from config import ConfigManager, get_database_path, get_logger
    CONFIG_AVAILABLE = True
    print("✅ ConfigManager 임포트 성공")
except ImportError as e:
    print(f"⚠️ ConfigManager 임포트 실패: {e}")
    print("기본 설정으로 진행합니다.")
    CONFIG_AVAILABLE = False

class ImprovedBuffettScorecard:
    """개선된 워런 버핏 스코어카드 계산기"""
    
    def __init__(self):
        """초기화 - 안전한 설정 로드"""
        if CONFIG_AVAILABLE:
            try:
                self.config_manager = ConfigManager()
                self.logger = self.config_manager.get_logger('BuffettScorecard')
                
                # 데이터베이스 경로 설정
                self.dart_db = self.config_manager.get_database_path('dart')
                self.stock_db = self.config_manager.get_database_path('stock')
                
                # 분석 설정
                analysis_config = self.config_manager.get_analysis_config()
                scorecard_config = analysis_config.get('buffett_scorecard', {})
                
                self.PROFITABILITY_WEIGHT = scorecard_config.get('profitability', 30)
                self.GROWTH_WEIGHT = scorecard_config.get('growth', 25)
                self.STABILITY_WEIGHT = scorecard_config.get('stability', 25)
                self.EFFICIENCY_WEIGHT = scorecard_config.get('efficiency', 10)
                self.VALUATION_WEIGHT = scorecard_config.get('valuation', 20)
                self.MAX_SCORE = scorecard_config.get('max_score', 110)
                
                self.logger.info("ImprovedBuffettScorecard 초기화 완료")
                
            except Exception as e:
                print(f"⚠️ ConfigManager 사용 중 오류: {e}")
                self._use_fallback_config()
        else:
            self._use_fallback_config()
    
    def _use_fallback_config(self):
        """Fallback 설정 사용"""
        print("📝 Fallback 설정을 사용합니다.")
        
        import logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('BuffettScorecard')
        
        # 기본 경로 설정
        self.dart_db = Path("data/databases/dart_data.db")
        self.stock_db = Path("data/databases/stock_data.db")
        
        # 기본 점수 가중치
        self.PROFITABILITY_WEIGHT = 30
        self.GROWTH_WEIGHT = 25
        self.STABILITY_WEIGHT = 25  
        self.EFFICIENCY_WEIGHT = 10
        self.VALUATION_WEIGHT = 20
        self.MAX_SCORE = 110
    
    def inspect_database_schema(self, db_path: Path) -> dict:
        """데이터베이스 스키마 상세 분석"""
        try:
            if not db_path.exists():
                return {"error": f"데이터베이스 파일이 없습니다: {db_path}"}
            
            schema_info = {}
            
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                # 모든 테이블 목록
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                schema_info['tables'] = tables
                
                # 각 테이블의 스키마와 데이터 샘플
                schema_info['table_details'] = {}
                
                for table in tables:
                    try:
                        # 테이블 스키마
                        cursor.execute(f"PRAGMA table_info([{table}])")
                        columns = cursor.fetchall()
                        
                        # 레코드 수
                        cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                        count = cursor.fetchone()[0]
                        
                        # 데이터 샘플 (첫 3개)
                        cursor.execute(f"SELECT * FROM [{table}] LIMIT 3")
                        samples = cursor.fetchall()
                        
                        schema_info['table_details'][table] = {
                            'columns': columns,
                            'count': count,
                            'samples': samples
                        }
                        
                    except Exception as e:
                        schema_info['table_details'][table] = {'error': str(e)}
                
                return schema_info
                
        except Exception as e:
            return {"error": f"스키마 분석 실패: {e}"}
    
    def search_samsung_data_flexible(self, db_path: Path) -> pd.DataFrame:
        """삼성전자 데이터 유연한 검색"""
        try:
            if not db_path.exists():
                self.logger.warning(f"데이터베이스 파일이 없습니다: {db_path}")
                return pd.DataFrame()
            
            with sqlite3.connect(db_path) as conn:
                # 1. 테이블 목록 확인
                tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
                tables = pd.read_sql_query(tables_query, conn)
                self.logger.info(f"사용 가능한 테이블: {list(tables['name'])}")
                
                # 2. 재무 관련 테이블들 시도
                financial_tables = [
                    'samsung_financial_statements',
                    'financial_statements', 
                    'dart_financial_data',
                    'corp_financial_data',
                    'multi_stock_financial_statements'
                ]
                
                for table_name in financial_tables:
                    if table_name not in list(tables['name']):
                        continue
                    
                    try:
                        # 먼저 테이블 구조 확인
                        structure_query = f"PRAGMA table_info([{table_name}])"
                        columns_df = pd.read_sql_query(structure_query, conn)
                        column_names = list(columns_df['name'])
                        self.logger.info(f"{table_name} 컬럼: {column_names[:10]}...")  # 처음 10개만
                        
                        # 레코드 수 확인
                        count_query = f"SELECT COUNT(*) as total FROM [{table_name}]"
                        count_df = pd.read_sql_query(count_query, conn)
                        total_records = count_df.iloc[0]['total']
                        self.logger.info(f"{table_name} 총 레코드: {total_records:,}개")
                        
                        if total_records == 0:
                            self.logger.warning(f"{table_name}에 데이터가 없습니다.")
                            continue
                        
                        # 다양한 검색 조건 시도
                        search_conditions = []
                        
                        # stock_code 컬럼이 있는 경우
                        if 'stock_code' in column_names:
                            search_conditions.extend([
                                "stock_code = '005930'",
                                "stock_code = '005930.KS'",
                                "stock_code LIKE '%005930%'"
                            ])
                        
                        # corp_code 컬럼이 있는 경우
                        if 'corp_code' in column_names:
                            search_conditions.append("corp_code = '00126380'")
                        
                        # 회사명 관련 컬럼들
                        name_columns = [col for col in column_names if any(keyword in col.lower() for keyword in ['name', 'corp', 'company'])]
                        for col in name_columns:
                            search_conditions.extend([
                                f"{col} LIKE '%삼성전자%'",
                                f"{col} LIKE '%SAMSUNG%'",
                                f"{col} LIKE '%Samsung%'"
                            ])
                        
                        # 각 조건으로 검색 시도
                        for condition in search_conditions:
                            try:
                                search_query = f"""
                                SELECT * FROM [{table_name}]
                                WHERE {condition}
                                LIMIT 20
                                """
                                
                                df = pd.read_sql_query(search_query, conn)
                                
                                if not df.empty:
                                    self.logger.info(f"✅ {table_name}에서 조건 '{condition}'으로 {len(df)}건 발견!")
                                    return df
                                    
                            except Exception as search_error:
                                self.logger.debug(f"검색 조건 '{condition}' 실패: {search_error}")
                                continue
                        
                        # 검색 실패 시 샘플 데이터 확인
                        self.logger.info(f"{table_name}의 샘플 데이터 확인 중...")
                        sample_query = f"SELECT * FROM [{table_name}] LIMIT 5"
                        sample_df = pd.read_sql_query(sample_query, conn)
                        
                        if not sample_df.empty:
                            self.logger.info(f"샘플 데이터 컬럼: {list(sample_df.columns)}")
                            # 첫 번째 행의 주요 컬럼들만 출력
                            first_row = sample_df.iloc[0]
                            key_info = {}
                            for col in ['stock_code', 'corp_code', 'corp_name', 'company_name', 'name']:
                                if col in first_row.index:
                                    key_info[col] = first_row[col]
                            self.logger.info(f"샘플 데이터 주요 정보: {key_info}")
                        
                    except Exception as table_error:
                        self.logger.error(f"{table_name} 처리 중 오류: {table_error}")
                        continue
                
                # 모든 테이블에서 찾지 못한 경우
                self.logger.warning("모든 테이블에서 삼성전자 데이터를 찾지 못했습니다.")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"삼성전자 데이터 검색 실패: {e}")
            return pd.DataFrame()
    
    def get_real_samsung_data_improved(self):
        """개선된 삼성전자 데이터 조회"""
        try:
            self.logger.info("🔍 개선된 삼성전자 데이터 조회 시작...")
            
            # 1. DART 데이터베이스 스키마 분석
            dart_schema = self.inspect_database_schema(self.dart_db)
            if 'error' not in dart_schema:
                self.logger.info(f"DART DB 테이블 수: {len(dart_schema['tables'])}")
                
                # 각 테이블의 레코드 수 출력
                for table, details in dart_schema['table_details'].items():
                    if 'count' in details:
                        self.logger.info(f"  - {table}: {details['count']:,}개")
            
            # 2. 유연한 삼성전자 데이터 검색
            financial_df = self.search_samsung_data_flexible(self.dart_db)
            
            # 3. 주식 데이터 조회 (기존 방식 유지)
            company_info = self._get_stock_company_info('005930')
            price_data = self._get_stock_price_data('005930')
            
            # 4. 결과 요약
            self.logger.info("📊 데이터 조회 결과:")
            self.logger.info(f"  - 재무데이터: {len(financial_df)}건")
            self.logger.info(f"  - 기업정보: {len(company_info)}건")
            self.logger.info(f"  - 주가데이터: {len(price_data)}건")
            
            return financial_df, company_info, price_data
            
        except Exception as e:
            self.logger.error(f"개선된 데이터 조회 오류: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    def _get_stock_company_info(self, stock_code: str) -> pd.DataFrame:
        """주식 기업정보 조회 (기존과 동일)"""
        try:
            if not self.stock_db.exists():
                self.logger.warning(f"주식 데이터베이스가 없습니다: {self.stock_db}")
                return pd.DataFrame()
            
            with sqlite3.connect(self.stock_db) as conn:
                query = f"SELECT * FROM company_info WHERE stock_code = '{stock_code}'"
                df = pd.read_sql_query(query, conn)
                
                if not df.empty:
                    self.logger.info(f"✅ 기업정보 조회 성공: {len(df)}건")
                else:
                    self.logger.warning("기업정보를 찾을 수 없습니다.")
                
                return df
                
        except Exception as e:
            self.logger.error(f"기업정보 조회 실패: {e}")
            return pd.DataFrame()
    
    def _get_stock_price_data(self, stock_code: str) -> pd.DataFrame:
        """주가 데이터 조회 (기존과 동일)"""
        try:
            if not self.stock_db.exists():
                return pd.DataFrame()
            
            with sqlite3.connect(self.stock_db) as conn:
                query = f"""
                SELECT * FROM stock_prices 
                WHERE stock_code = '{stock_code}'
                ORDER BY date DESC 
                LIMIT 252
                """
                df = pd.read_sql_query(query, conn)
                
                if not df.empty:
                    self.logger.info(f"✅ 주가데이터 조회 성공: {len(df)}건")
                else:
                    self.logger.warning("주가데이터를 찾을 수 없습니다.")
                
                return df
                
        except Exception as e:
            self.logger.error(f"주가데이터 조회 실패: {e}")
            return pd.DataFrame()
    
    def parse_financial_data_improved(self, financial_df: pd.DataFrame) -> dict:
        """개선된 재무데이터 파싱"""
        try:
            if financial_df.empty:
                self.logger.warning("재무데이터가 비어있습니다.")
                return {}
            
            self.logger.info("📊 재무데이터 파싱 시작...")
            self.logger.info(f"데이터 행 수: {len(financial_df)}")
            self.logger.info(f"데이터 컬럼: {list(financial_df.columns)}")
            
            # 컬럼명 확인 후 적절한 파싱 로직 적용
            if 'account_nm' in financial_df.columns:
                return self._parse_dart_format(financial_df)
            elif 'item' in financial_df.columns:
                return self._parse_alternative_format(financial_df)
            else:
                self.logger.warning("알 수 없는 재무데이터 형식입니다.")
                return self._extract_any_numeric_data(financial_df)
                
        except Exception as e:
            self.logger.error(f"재무데이터 파싱 실패: {e}")
            return {}
    
    def _parse_dart_format(self, financial_df: pd.DataFrame) -> dict:
        """DART 형식 재무데이터 파싱"""
        try:
            financial_metrics = {}
            
            # 최신 연결재무제표 데이터 필터링
            if 'reprt_code' in financial_df.columns:
                latest_data = financial_df[
                    (financial_df['reprt_code'] == '11000') |  # 연결재무제표
                    (financial_df['reprt_code'] == '11')       # 1분기보고서
                ].copy()
            else:
                latest_data = financial_df.copy()
            
            if latest_data.empty:
                self.logger.warning("적절한 보고서 코드를 찾을 수 없습니다.")
                latest_data = financial_df.copy()
            
            # 주요 계정과목 매핑 (더 포괄적)
            account_mapping = {
                '매출액': ['매출액', '수익(매출액)', '영업수익', '매출', '총매출액'],
                '영업이익': ['영업이익', '영업손익', '영업이익(손실)'],
                '당기순이익': ['당기순이익', '순이익', '당기순손익', '당기순이익(손실)'],
                '총자산': ['자산총계', '총자산', '자산합계'],
                '자기자본': ['자본총계', '자기자본총계', '주주지분', '자본합계'],
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
                    
                    # 대소문자 구분없이 부분 매칭
                    matching_rows = latest_data[
                        latest_data['account_nm'].str.contains(name, na=False, case=False)
                    ]
                    
                    if not matching_rows.empty:
                        # 가장 최근 데이터 선택
                        if 'bsns_year' in matching_rows.columns:
                            matching_rows = matching_rows.sort_values('bsns_year', ascending=False)
                        
                        # 금액 컬럼 찾기
                        amount_columns = ['thstrm_amount', 'amount', 'value', 'curr_amount']
                        for amt_col in amount_columns:
                            if amt_col in matching_rows.columns:
                                amount = matching_rows.iloc[0][amt_col]
                                if pd.notna(amount) and str(amount).replace(',', '').replace('-', '').replace('.', '').isdigit():
                                    financial_metrics[metric] = float(str(amount).replace(',', ''))
                                    self.logger.info(f"✅ {metric}: {amount}")
                                    found = True
                                    break
                        
                        if found:
                            break
            
            return financial_metrics
            
        except Exception as e:
            self.logger.error(f"DART 형식 파싱 실패: {e}")
            return {}
    
    def _parse_alternative_format(self, financial_df: pd.DataFrame) -> dict:
        """대안 형식 재무데이터 파싱"""
        # 다른 형식의 재무데이터 파싱 로직
        return {}
    
    def _extract_any_numeric_data(self, financial_df: pd.DataFrame) -> dict:
        """숫자 데이터 추출 시도"""
        try:
            self.logger.info("숫자 데이터 추출을 시도합니다...")
            
            # 숫자 컬럼들 찾기
            numeric_columns = financial_df.select_dtypes(include=[np.number]).columns
            self.logger.info(f"숫자 컬럼들: {list(numeric_columns)}")
            
            # 첫 번째 행의 데이터 샘플 출력
            if not financial_df.empty:
                first_row = financial_df.iloc[0]
                self.logger.info(f"첫 번째 행 샘플: {dict(first_row)}")
            
            return {}
            
        except Exception as e:
            self.logger.error(f"숫자 데이터 추출 실패: {e}")
            return {}
    
    def calculate_scorecard_improved(self):
        """개선된 스코어카드 계산"""
        try:
            self.logger.info("🚀 개선된 워런 버핏 스코어카드 계산 시작")
            
            # 1. 개선된 데이터 조회
            financial_df, company_info, price_data = self.get_real_samsung_data_improved()
            
            # 2. 개선된 재무데이터 파싱
            financial_data = self.parse_financial_data_improved(financial_df)
            
            # 3. 데이터 상태 확인
            if not financial_data:
                self.logger.warning("❌ 재무데이터 파싱 실패 - 대안 점수 계산")
                return self._calculate_fallback_score(company_info, price_data)
            
            # 4. 각 카테고리별 점수 계산 (기존 로직 사용)
            prof_score = self._calculate_simple_profitability(financial_data)
            growth_score = 15  # 성장성 기본값
            stab_score = self._calculate_simple_stability(financial_data)
            eff_score = self._calculate_simple_efficiency(financial_data)
            val_score = 12  # 가치평가 기본값
            
            total_score = prof_score + growth_score + stab_score + eff_score + val_score
            
            # 5. 결과 출력
            self._print_improved_results(
                total_score, prof_score, growth_score, stab_score, eff_score, val_score,
                financial_data, len(financial_df)
            )
            
            return {
                'total_score': total_score,
                'data_quality': 'real_data' if financial_data else 'estimated',
                'financial_data': financial_data
            }
            
        except Exception as e:
            self.logger.error(f"❌ 개선된 스코어카드 계산 실패: {e}")
            return None
    
    def _calculate_simple_profitability(self, financial_data: dict) -> float:
        """간단한 수익성 계산"""
        if not financial_data:
            return 15  # 기본값
        
        score = 0
        
        # ROE 계산
        if '당기순이익' in financial_data and '자기자본' in financial_data:
            roe = (financial_data['당기순이익'] / financial_data['자기자본']) * 100
            score += min(roe / 4, 10)  # 최대 10점
        
        # 영업이익률 계산
        if '영업이익' in financial_data and '매출액' in financial_data:
            margin = (financial_data['영업이익'] / financial_data['매출액']) * 100
            score += min(margin / 2, 10)  # 최대 10점
        
        return min(score, self.PROFITABILITY_WEIGHT)
    
    def _calculate_simple_stability(self, financial_data: dict) -> float:
        """간단한 안정성 계산"""
        if not financial_data:
            return 15  # 기본값
        
        score = 0
        
        # 부채비율
        if '부채총계' in financial_data and '자기자본' in financial_data:
            debt_ratio = (financial_data['부채총계'] / financial_data['자기자본']) * 100
            if debt_ratio <= 50:
                score += 15
            elif debt_ratio <= 100:
                score += 10
            else:
                score += 5
        
        return min(score, self.STABILITY_WEIGHT)
    
    def _calculate_simple_efficiency(self, financial_data: dict) -> float:
        """간단한 효율성 계산"""
        if not financial_data:
            return 8  # 기본값
        
        score = 0
        
        # 총자산회전율
        if '매출액' in financial_data and '총자산' in financial_data:
            turnover = financial_data['매출액'] / financial_data['총자산']
            score += min(turnover * 10, 8)
        
        return min(score, self.EFFICIENCY_WEIGHT)
    
    def _calculate_fallback_score(self, company_info: pd.DataFrame, price_data: pd.DataFrame) -> dict:
        """대안 점수 계산 (데이터 부족 시)"""
        self.logger.info("📊 대안 점수 계산을 사용합니다.")
        
        # 기본 점수 할당
        fallback_scores = {
            'profitability': self.PROFITABILITY_WEIGHT * 0.7,  # 70% 수준
            'growth': self.GROWTH_WEIGHT * 0.6,
            'stability': self.STABILITY_WEIGHT * 0.8,
            'efficiency': self.EFFICIENCY_WEIGHT * 0.7,
            'valuation': self.VALUATION_WEIGHT * 0.6
        }
        
        total_score = sum(fallback_scores.values())
        
        return {
            'total_score': total_score,
            'data_quality': 'estimated',
            'scores': fallback_scores,
            'note': '실제 재무데이터 부족으로 추정값 사용'
        }
    
    def _print_improved_results(self, total_score, prof_score, growth_score, stab_score, eff_score, val_score, financial_data, data_count):
        """개선된 결과 출력"""
        print("\n🏆 개선된 워런 버핏 스코어카드 결과")
        print("=" * 70)
        
        print(f"📊 분석 대상: 삼성전자 (005930)")
        print(f"📅 분석 시점: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📈 사용된 재무데이터: {data_count}건")
        
        if financial_data:
            print(f"\n💰 추출된 재무 지표 ({len(financial_data)}개):")
            for key, value in financial_data.items():
                if isinstance(value, (int, float)):
                    print(f"   • {key}: {value:,.0f}")
        
        print(f"\n📊 카테고리별 점수:")
        print(f"   • 수익성: {prof_score:.1f}/{self.PROFITABILITY_WEIGHT}점")
        print(f"   • 성장성: {growth_score:.1f}/{self.GROWTH_WEIGHT}점")
        print(f"   • 안정성: {stab_score:.1f}/{self.STABILITY_WEIGHT}점")
        print(f"   • 효율성: {eff_score:.1f}/{self.EFFICIENCY_WEIGHT}점")
        print(f"   • 가치평가: {val_score:.1f}/{self.VALUATION_WEIGHT}점")
        
        percentage = (total_score / self.MAX_SCORE) * 100
        grade = self._get_grade(percentage)
        
        print(f"\n🎯 최종 결과:")
        print(f"   • 총점: {total_score:.1f}/{self.MAX_SCORE}점 ({percentage:.1f}%)")
        print(f"   • 등급: {grade}")
        
        print(f"\n🔧 이번 개선사항:")
        print("   ✅ 유연한 데이터베이스 검색 로직 추가")
        print("   ✅ 테이블 스키마 자동 분석")
        print("   ✅ 다양한 검색 조건 적용")
        print("   ✅ 데이터 존재 여부 상세 확인")
    
    def _get_grade(self, percentage):
        """등급 결정"""
        if percentage >= 80:
            return "S (매우우수)"
        elif percentage >= 70:
            return "A (우수)"
        elif percentage >= 60:
            return "B (양호)"
        elif percentage >= 40:
            return "C (보통)"
        else:
            return "D (주의)"

def main():
    """메인 실행 함수"""
    print("🚀 개선된 삼성전자 워런 버핏 스코어카드 분석")
    print("=" * 70)
    
    try:
        # 개선된 스코어카드 계산기 생성
        scorecard = ImprovedBuffettScorecard()
        
        # 개선된 분석 실행
        result = scorecard.calculate_scorecard_improved()
        
        if result:
            print(f"\n🎉 분석 완료!")
            print(f"📈 데이터 품질: {result['data_quality']}")
            print(f"📊 최종 점수: {result['total_score']:.1f}점")
            
        else:
            print("❌ 분석 실패")
            
    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")
        print("🔧 해결 방법:")
        print("1. 데이터베이스 파일 경로 확인")
        print("2. 재무데이터 수집 스크립트 실행")

if __name__ == "__main__":
    main()