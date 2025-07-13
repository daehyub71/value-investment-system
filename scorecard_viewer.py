#!/usr/bin/env python3
"""
워런 버핏 스코어카드 결과 조회 및 분석
Streamlit 웹앱 기초 자료 활용 예시
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# 투자등급 한글 매핑 (유연한 매핑)
INVESTMENT_GRADE_KOR = {
    'Strong Buy': '적극매수',
    'Buy': '매수', 
    'Hold': '보유',
    'Weak Hold': '약보유',
    'Avoid': '투자회피',
    # 추가 가능한 값들
    'STRONG_BUY': '적극매수',
    'BUY': '매수',
    'HOLD': '보유',
    'WEAK_HOLD': '약보유',
    'AVOID': '투자회피',
    'strong_buy': '적극매수',
    'buy': '매수',
    'hold': '보유',
    'weak_hold': '약보유',
    'avoid': '투자회피'
}

# 투자등급 색상 매핑
INVESTMENT_GRADE_COLORS = {
    '적극매수': '#00CC96',
    '매수': '#19D3F3', 
    '보유': '#FFA15A',
    '약보유': '#FFFF00',
    '투자회피': '#EF553B'
}

# 섹터 코드를 한글명으로 매핑
SECTOR_CODE_MAPPING = {
    # GICS 섹터 코드 매핑
    '10': '에너지',
    '15': '소재',
    '20': '산업재',
    '25': '필수소비재',
    '30': '임의소비재',
    '35': '건강관리',
    '40': '금융',
    '45': '정보기술',
    '50': '통신서비스',
    '55': '유틸리티',
    '60': '부동산',
    
    # 한국 산업분류 코드 (일부)
    '28112': 'IT 하드웨어',
    '581': '통신업',
    '28520': '화학',
    '7011': '건설업',
    '2511': '철강',
    '31201': '전자부품',
    '35300': '자동차',
    '12000': '음식료품',
    '969': '금융업',
    '55103': '전기·가스업',
    '202': '섬유',
    '64992': '서비스업',
    '303': '화학',
    '212': '종이·목재',
    '66121': '유통업',
    '108': '비철금속',
    '20423': '기계',
    '204': '석유화학',
    '467': '운송업',
    '201': '음식료',
    '649': '기타서비스',
    '592': '방송통신',
    '715': '소프트웨어',
    '2419': '기타제조업',
    
    # 기본값
    'default': '기타'
}

def safe_format_number(x, decimal_places=2):
    """안전한 숫자 포맷팅 함수"""
    if pd.isna(x) or x is None:
        return "없음"
    try:
        if isinstance(x, (int, float)) and not pd.isna(x):
            if decimal_places == 1:
                return f"{float(x):.1f}"
            else:
                return f"{float(x):.2f}"
        else:
            return "없음"
    except (ValueError, TypeError):
        return "없음"

def load_scorecard_data():
    """스코어카드 데이터 로드 (company_info에서 한글 회사명 조회)"""
    buffett_db_path = Path('data/databases/buffett_scorecard.db')
    stock_db_path = Path('data/databases/stock_data.db')
    
    if not buffett_db_path.exists():
        st.error("❌ 스코어카드 데이터베이스가 없습니다. 먼저 배치 처리를 실행하세요.")
        st.code("python batch_buffett_scorecard.py --test")
        return None
    
    try:
        # buffett_scorecard 데이터 로드
        with sqlite3.connect(buffett_db_path) as conn:
            query = '''
                SELECT 
                    stock_code, company_name, sector,
                    total_score, investment_grade,
                    valuation_score, profitability_score, growth_score, financial_health_score,
                    forward_pe, pbr, roe, current_price, target_price, upside_potential,
                    calculation_date
                FROM buffett_scorecard 
                WHERE total_score > 0
                ORDER BY total_score DESC
            '''
            df = pd.read_sql_query(query, conn)
        
        # 디버깅: 원본 investment_grade 값들 확인 (간결하게)
        unique_grades = df['investment_grade'].unique()
        st.info(f"📊 로드된 데이터: {len(df)}개 종목, 투자등급: {len(unique_grades)}가지")
        
        # company_info에서 한글 회사명 조회 (있는 경우)
        if stock_db_path.exists():
            try:
                with sqlite3.connect(stock_db_path) as stock_conn:
                    company_query = '''
                        SELECT stock_code, company_name as korean_name, sector as korean_sector
                        FROM company_info
                    '''
                    company_df = pd.read_sql_query(company_query, stock_conn)
                    
                    # 종목코드로 JOIN하여 한글명 업데이트
                    df = df.merge(company_df, on='stock_code', how='left')
                    
                    # 한글 회사명이 있으면 교체, 없으면 기존 이름 유지
                    df['company_name'] = df['korean_name'].fillna(df['company_name'])
                    df['sector'] = df['korean_sector'].fillna(df['sector'])
                    
                    # 임시 컬럼 제거
                    df = df.drop(['korean_name', 'korean_sector'], axis=1)
                    
                    st.success("✅ company_info에서 한글 회사명을 성공적으로 로드했습니다.")
            except Exception as company_error:
                st.warning(f"⚠️ company_info 조회 실패, 기본 회사명 사용: {company_error}")
        else:
            st.warning("⚠️ stock_data.db가 없어 기본 회사명을 사용합니다.")
        
        # 투자등급을 한글로 변환 (안전한 방식)
        df['투자등급'] = df['investment_grade'].map(INVESTMENT_GRADE_KOR)
        
        # 섹터 코드를 한글명으로 변환
        df['섹터명'] = df['sector'].astype(str).map(SECTOR_CODE_MAPPING).fillna(df['sector'])
        
        # 매핑되지 않은 섹터는 원본값 유지하되 '기타'로 표시
        unmapped_sectors = df[~df['sector'].astype(str).isin(SECTOR_CODE_MAPPING.keys())]['sector'].unique()
        if len(unmapped_sectors) > 0:
            st.info(f"📊 새로운 섹터 발견: {unmapped_sectors[:5]}..." if len(unmapped_sectors) > 5 else f"📊 새로운 섹터: {unmapped_sectors}")
            # 매핑되지 않은 섹터는 '기타 (코드)' 형식으로 표시
            df.loc[~df['sector'].astype(str).isin(SECTOR_CODE_MAPPING.keys()), '섹터명'] = df.loc[~df['sector'].astype(str).isin(SECTOR_CODE_MAPPING.keys()), 'sector'].astype(str).apply(lambda x: f'기타({x})')
        
        # 매핑되지 않은 값들 확인
        unmapped_grades = df[df['투자등급'].isna()]['investment_grade'].unique()
        if len(unmapped_grades) > 0:
            st.warning(f"⚠️ 매핑되지 않은 투자등급: {unmapped_grades}")
            # 매핑되지 않은 값들은 원본 값 사용
            df['투자등급'] = df['투자등급'].fillna(df['investment_grade'])
        
        # 모든 값이 매핑되지 않았다면 원본 컬럼 사용
        if df['투자등급'].isna().all():
            st.warning("⚠️ 모든 투자등급 매핑 실패. 원본 값을 사용합니다.")
            df['투자등급'] = df['investment_grade']
        
        # 디버깅: 변환 후 투자등급 값들 확인 (간결하게)
        converted_grades = df['투자등급'].unique()
        st.success(f"✅ 투자등급 한글화 완료: {converted_grades}")
        

        
        return df
        
    except Exception as e:
        st.error(f"데이터 로드 실패: {e}")
        return None

def main():
    st.set_page_config(
        page_title="워런 버핏 스코어카드",
        page_icon="🏆",
        layout="wide"
    )
    
    st.title("🏆 워런 버핏 스타일 가치투자 스코어카드")
    st.markdown("**KOSPI/KOSDAQ 전 종목 실시간 분석 결과**")
    
    # 데이터 로드
    df = load_scorecard_data()
    
    if df is None:
        return
    
    if len(df) == 0:
        st.warning("⚠️ 스코어카드 데이터가 없습니다. 배치 처리를 실행하세요.")
        return
    
    # 사이드바 필터
    st.sidebar.header("🔍 필터링")
    
    # 필터 초기화 버튼
    if st.sidebar.button("🔄 모든 필터 초기화"):
        st.rerun()
    
    st.sidebar.markdown("---")
    
    # 투자등급 필터 (한글) - 안전한 기본값 설정
    available_grades = [grade for grade in df['투자등급'].unique() if pd.notna(grade)]
    if len(available_grades) == 0:
        st.error("❌ 투자등급 데이터가 없습니다.")
        return
    
    grades = st.sidebar.multiselect(
        "투자등급 선택",
        options=available_grades,
        default=available_grades,  # 모든 등급을 기본으로 선택
        help="투자 추천 등급을 선택하세요"
    )
    
    # 섹터 필터 - 한글 섹터명 사용
    available_sectors = [sector for sector in df['섹터명'].unique() if pd.notna(sector)]
    
    sectors = st.sidebar.multiselect(
        "섹터 선택", 
        options=sorted(available_sectors),
        default=sorted(available_sectors),  # 모든 섹터를 기본으로 선택
        help="분석하고 싶은 산업 섹터를 선택하세요"
    )
    
    # 점수 범위 필터
    min_score = st.sidebar.slider(
        "최소 점수", 
        min_value=0, 
        max_value=100, 
        value=0,  # 기본값을 0으로 변경
        help="워런 버핏 스코어 최소 기준을 설정하세요"
    )
    
    # 데이터 필터링 (안전한 방식)
    filter_conditions = []
    
    # 투자등급 필터 (선택된 것이 있을 때만)
    if grades:
        filter_conditions.append(df['투자등급'].isin(grades))
    
    # 섹터 필터 (선택된 것이 있을 때만)
    if sectors:
        filter_conditions.append(df['섹터명'].isin(sectors))
    
    # 점수 필터 (항상 적용)
    filter_conditions.append(df['total_score'] >= min_score)
    
    # 모든 조건을 AND로 결합
    if filter_conditions:
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = combined_filter & condition
        filtered_df = df[combined_filter]
    else:
        filtered_df = df.copy()
    
    # 디버깅: 필터링 결과 확인
    st.sidebar.write("🔍 디버깅 정보:")
    st.sidebar.write(f"- 전체 데이터: {len(df)}개")
    st.sidebar.write(f"- 선택된 투자등급: {grades}")
    if grades:
        grade_filter_count = len(df[df['투자등급'].isin(grades)])
        st.sidebar.write(f"- 투자등급 필터 통과: {grade_filter_count}개")
    if sectors:
        sector_filter_count = len(df[df['섹터명'].isin(sectors)])
        st.sidebar.write(f"- 섹터 필터 통과: {sector_filter_count}개")
    score_filter_count = len(df[df['total_score'] >= min_score])
    st.sidebar.write(f"- 점수 필터 통과 (>={min_score}점): {score_filter_count}개")
    st.sidebar.write(f"- **최종 필터링 결과: {len(filtered_df)}개**")
    
    # 데이터 상태 확인 도구
    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 데이터 상태")
    
    if st.sidebar.button("🔍 PBR 데이터 상세 확인"):
        st.sidebar.write("**PBR 상태:**")
        pbr_valid = df[df['pbr'].notna() & (df['pbr'] > 0)]
        st.sidebar.write(f"- 유효한 PBR: {len(pbr_valid)}개")
        if len(pbr_valid) > 0:
            st.sidebar.write(f"- PBR 범위: {pbr_valid['pbr'].min():.2f} ~ {pbr_valid['pbr'].max():.2f}")
            st.sidebar.write(f"- 평균 PBR: {pbr_valid['pbr'].mean():.2f}")
    
    if st.sidebar.button("🔍 모든 컬럼 상태 확인"):
        st.sidebar.write("**데이터 완성도:**")
        for col in ['forward_pe', 'pbr', 'roe', 'upside_potential']:
            valid_count = len(df[df[col].notna() & (df[col] != 0)])
            percentage = (valid_count / len(df)) * 100
            st.sidebar.write(f"- {col}: {valid_count}/{len(df)} ({percentage:.1f}%)")
    
    # 대시보드 메트릭
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "📊 분석 종목 수", 
            f"{len(df):,}개",
            f"필터링 후: {len(filtered_df):,}개"
        )
    
    with col2:
        if len(filtered_df) > 0:
            avg_score = filtered_df['total_score'].mean()
            st.metric(
                "📈 평균 점수", 
                f"{avg_score:.1f}점",
                f"전체 평균: {df['total_score'].mean():.1f}점"
            )
        else:
            st.metric(
                "📈 평균 점수", 
                "데이터 없음",
                f"전체 평균: {df['total_score'].mean():.1f}점"
            )
    
    with col3:
        if len(filtered_df) > 0:
            strong_buy_count = len(filtered_df[filtered_df['투자등급'] == '적극매수'])
            percentage = (strong_buy_count / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
            st.metric(
                "🌟 적극매수", 
                f"{strong_buy_count}개",
                f"전체 비율: {percentage:.1f}%"
            )
        else:
            st.metric(
                "🌟 적극매수", 
                "0개",
                "전체 비율: 0.0%"
            )
    
    with col4:
        if len(filtered_df) > 0:
            avg_upside = filtered_df['upside_potential'].mean()
            st.metric(
                "📈 평균 상승여력", 
                f"{avg_upside:.1f}%",
                "애널리스트 목표가 기준"
            )
        else:
            st.metric(
                "📈 평균 상승여력", 
                "데이터 없음",
                "애널리스트 목표가 기준"
            )
    
    # 탭 구성
    tab1, tab2, tab3, tab4 = st.tabs(["🏆 상위 종목", "📊 섹터 분석", "📈 상관관계", "🔍 개별 검색"])
    
    # 필터링된 데이터가 없는 경우 처리
    if len(filtered_df) == 0:
        with tab1:
            st.warning("⚠️ 선택한 필터 조건에 맞는 데이터가 없습니다.")
            st.info("💡 필터 조건을 완화해 보세요:")
            st.info("   • 사이드바에서 '모든 필터 초기화' 버튼 클릭")
            st.info("   • 더 많은 투자등급/섹터 선택")
            st.info("   • 최소 점수를 낮게 설정")
        
        with tab2:
            st.warning("⚠️ 선택한 필터 조건에 맞는 데이터가 없습니다.")
        
        with tab3:
            st.warning("⚠️ 선택한 필터 조건에 맞는 데이터가 없습니다.")
        
        with tab4:
            st.warning("⚠️ 선택한 필터 조건에 맞는 데이터가 없습니다.")
        
        return  # 함수 종료
    
    with tab1:
        st.subheader("🏆 워런 버핏 스코어카드 상위 20개 종목")
        
        top_20 = filtered_df.head(20)
        
        # 시각화 (한글 투자등급으로 색상 매핑)
        fig = px.bar(
            top_20, 
            x='company_name', 
            y='total_score',
            color='투자등급',
            title="상위 20개 종목 점수",
            color_discrete_map=INVESTMENT_GRADE_COLORS
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        
        # 상세 테이블
        display_cols = [
            'stock_code', 'company_name', 'total_score', '투자등급',
            'forward_pe', 'pbr', 'roe', 'upside_potential'
        ]
        
        styled_df = top_20[display_cols].copy()
        styled_df.columns = ['종목코드', '회사명', '총점', '투자등급', '예상주가수익비율', '주가순자산배수', '자기자본이익률(%)', '상승여력(%)']
        

        
        # 숫자 형식 포맷팅 (개선된 버전)
        if '예상주가수익비율' in styled_df.columns:
            styled_df['예상주가수익비율'] = styled_df['예상주가수익비율'].apply(lambda x: safe_format_number(x, 2))
        if '주가순자산배수' in styled_df.columns:
            styled_df['주가순자산배수'] = styled_df['주가순자산배수'].apply(lambda x: safe_format_number(x, 2))
        if '자기자본이익률(%)' in styled_df.columns:
            styled_df['자기자본이익률(%)'] = styled_df['자기자본이익률(%)'].apply(lambda x: safe_format_number(x, 1))
        if '상승여력(%)' in styled_df.columns:
            styled_df['상승여력(%)'] = styled_df['상승여력(%)'].apply(lambda x: safe_format_number(x, 1))
        
        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True
        )
    
    with tab2:
        st.subheader("📊 섹터별 분석")
        
        # 섹터별 평균 점수 (한글 섹터명 사용)
        sector_analysis = filtered_df.groupby('섹터명').agg({
            'total_score': 'mean',
            'stock_code': 'count',
            'upside_potential': 'mean'
        }).round(1)
        
        sector_analysis.columns = ['평균점수', '종목수', '평균상승여력']
        sector_analysis = sector_analysis.sort_values('평균점수', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 섹터별 평균 점수 차트
            fig = px.bar(
                x=sector_analysis.index,
                y=sector_analysis['평균점수'],
                title="섹터별 평균 워런 버핏 점수"
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # 섹터별 종목 수 파이차트
            fig = px.pie(
                values=sector_analysis['종목수'],
                names=sector_analysis.index,
                title="섹터별 종목 분포"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(sector_analysis, use_container_width=True)
    
    with tab3:
        st.subheader("📈 지표 간 상관관계")
        
        # 숫자형 컬럼만 선택
        numeric_cols = ['total_score', 'forward_pe', 'pbr', 'roe', 'upside_potential']
        corr_data = filtered_df[numeric_cols].corr()
        
        # 컬럼명을 한글로 변경
        corr_data.columns = ['총점', '예상PER', '주가순자산배수', '자기자본이익률', '상승여력']
        corr_data.index = ['총점', '예상PER', '주가순자산배수', '자기자본이익률', '상승여력']
        
        # 히트맵
        fig = px.imshow(
            corr_data,
            title="주요 지표 간 상관관계",
            color_continuous_scale='RdBu_r',
            aspect="auto"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # 산점도
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.scatter(
                filtered_df,
                x='forward_pe',
                y='total_score',
                color='투자등급',
                title="예상주가수익비율 vs 총점",
                labels={'forward_pe': '예상주가수익비율', 'total_score': '총점'},
                color_discrete_map=INVESTMENT_GRADE_COLORS
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(
                filtered_df,
                x='roe',
                y='total_score', 
                color='투자등급',
                title="자기자본이익률 vs 총점",
                labels={'roe': '자기자본이익률(%)', 'total_score': '총점'},
                color_discrete_map=INVESTMENT_GRADE_COLORS
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("🔍 개별 종목 검색")
        
        # 검색
        search_term = st.text_input("종목명 또는 종목코드 검색")
        
        if search_term:
            search_results = filtered_df[
                (filtered_df['company_name'].str.contains(search_term, case=False, na=False)) |
                (filtered_df['stock_code'].str.contains(search_term, case=False, na=False))
            ]
            
            if len(search_results) > 0:
                for _, stock in search_results.iterrows():
                    with st.expander(f"📊 {stock['company_name']} ({stock['stock_code']}) - {stock['total_score']}점"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.metric("총점", f"{stock['total_score']}점")
                            st.metric("투자등급", stock['투자등급'])
                            
                            # 안전한 메트릭 표시
                            pe_value = safe_format_number(stock['forward_pe'], 2) if 'forward_pe' in stock and pd.notna(stock['forward_pe']) else "없음"
                            pbr_value = safe_format_number(stock['pbr'], 2) if 'pbr' in stock and pd.notna(stock['pbr']) else "없음"
                            
                            st.metric("예상주가수익비율", pe_value)
                            st.metric("주가순자산배수", pbr_value)
                        
                        with col2:
                            roe_value = safe_format_number(stock['roe'], 1) if 'roe' in stock and pd.notna(stock['roe']) else "없음"
                            upside_value = safe_format_number(stock['upside_potential'], 1) if 'upside_potential' in stock and pd.notna(stock['upside_potential']) else "없음"
                            
                            st.metric("자기자본이익률", f"{roe_value}%" if roe_value != "없음" else "없음")
                            st.metric("상승여력", f"{upside_value}%" if upside_value != "없음" else "없음")
                            
                            # 가격 정보
                            current_price = f"{stock['current_price']:,.0f}원" if pd.notna(stock['current_price']) and stock['current_price'] > 0 else "없음"
                            target_price = f"{stock['target_price']:,.0f}원" if pd.notna(stock['target_price']) and stock['target_price'] > 0 else "없음"
                            
                            st.metric("현재가", current_price)
                            st.metric("목표가", target_price)
                        
                        # 카테고리별 점수 시각화
                        categories = ['가치평가', '수익성', '성장성', '재무건전성']
                        scores = [
                            stock['valuation_score'],
                            stock['profitability_score'], 
                            stock['growth_score'],
                            stock['financial_health_score']
                        ]
                        max_scores = [40, 30, 20, 10]
                        
                        fig = go.Figure()
                        fig.add_trace(go.Scatterpolar(
                            r=scores,
                            theta=categories,
                            fill='toself',
                            name='실제 점수'
                        ))
                        fig.add_trace(go.Scatterpolar(
                            r=max_scores,
                            theta=categories,
                            fill='toself',
                            name='만점',
                            opacity=0.3
                        ))
                        
                        fig.update_layout(
                            polar=dict(
                                radialaxis=dict(
                                    visible=True,
                                    range=[0, max(max_scores)]
                                )
                            ),
                            title=f"{stock['company_name']} 카테고리별 점수"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("검색 결과가 없습니다.")
    
    # 푸터 (개선된 데이터 소스 정보)
    st.markdown("---")
    
    # 현재 날짜 및 분기 계산
    from datetime import datetime
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    
    # 분기 계산
    if current_month <= 3:
        quarter = "1분기"
        estimate_period = f"{current_year}년 2분기"
    elif current_month <= 6:
        quarter = "2분기" 
        estimate_period = f"{current_year}년 3분기"
    elif current_month <= 9:
        quarter = "3분기"
        estimate_period = f"{current_year}년 4분기"
    else:
        quarter = "4분기"
        estimate_period = f"{current_year + 1}년 1분기"
    
    # 데이터 업데이트 정보 표시
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **📊 워런 버핏 스코어카드**  
        - 100점 만점 시스템 (PBR 포함)
        - 가치평가 20점 + 수익성 25점 + 성장성 20점 + 안정성 25점 + 효율성 10점
        """)
    
    with col2:
        st.markdown(f"""
        **📈 Yahoo Finance 데이터**  
        - 추정 EPS: {estimate_period} 기준
        - 실시간 주가: {current_date.strftime('%Y-%m-%d %H:%M')} 기준
        - PER/PBR: 최신 재무제표 반영
        """)
    
    with col3:
        st.markdown("""
        **🔄 데이터 업데이트**  
        - 매일 자동 (배치 처리)
        - DART: 분기별 재무제표
        - 뉴스: 실시간 수집
        """)
    
    st.markdown("""
    **⚠️ 투자 유의사항**: 본 분석은 투자 참고용이며, 최종 투자 책임은 투자자 본인에게 있습니다.  
    **📊 점수 해석**: 70점 이상(매수), 50-69점(보유), 50점 미만(신중검토)
    """)

if __name__ == "__main__":
    main()
