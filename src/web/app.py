"""
Streamlit 메인 애플리케이션
Finance Data Vibe - 워런 버핏 스타일 가치투자 시스템
"""

import streamlit as st
import sys
import os

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def main():
    st.set_page_config(
        page_title="Finance Data Vibe",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("📊 Finance Data Vibe")
    st.subheader("워런 버핏 스타일 가치투자 시스템")
    
    # 사이드바 메뉴
    with st.sidebar:
        st.header("🎯 메뉴")
        page = st.selectbox(
            "페이지 선택",
            ["메인 대시보드", "종목 분석", "스크리닝 결과", "포트폴리오 관리", "시장 개요"]
        )
    
    # 페이지 라우팅
    if page == "메인 대시보드":
        show_main_dashboard()
    elif page == "종목 분석":
        show_stock_analysis()
    elif page == "스크리닝 결과":
        show_screening_results()
    elif page == "포트폴리오 관리":
        show_portfolio_management()
    elif page == "시장 개요":
        show_market_overview()

def show_main_dashboard():
    """메인 대시보드 표시"""
    st.header("📈 메인 대시보드")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("기본분석 비중", "45%", "📊")
    
    with col2:
        st.metric("기술분석 비중", "30%", "📈")
    
    with col3:
        st.metric("뉴스분석 비중", "25%", "📰")
    
    st.info("워런 버핏 스타일 가치투자 시스템이 준비되었습니다!")

def show_stock_analysis():
    """종목 분석 페이지"""
    st.header("🔍 종목 분석")
    
    stock_code = st.text_input("종목 코드 입력", placeholder="예: 005930")
    
    if stock_code:
        st.success(f"'{stock_code}' 종목 분석을 시작합니다.")
        # 여기에 실제 분석 로직 추가

def show_screening_results():
    """스크리닝 결과 페이지"""
    st.header("🎯 스크리닝 결과")
    st.info("저평가 우량주 스크리닝 결과가 표시됩니다.")

def show_portfolio_management():
    """포트폴리오 관리 페이지"""
    st.header("💼 포트폴리오 관리")
    st.info("포트폴리오 관리 기능이 준비 중입니다.")

def show_market_overview():
    """시장 개요 페이지"""
    st.header("🌐 시장 개요")
    st.info("시장 전반적인 개요가 표시됩니다.")

if __name__ == "__main__":
    main()
