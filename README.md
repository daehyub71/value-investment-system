# Finance Data Vibe
## 워런 버핏 스타일 가치투자 시스템

### 🎯 프로젝트 개요
50-60대 은퇴 준비 직장인을 위한 30분 투자 의사결정 지원 시스템

### 📊 분석 비중
- **기본분석**: 45% (워런 버핏 스코어카드)
- **기술분석**: 30% (장기 추세 중심)
- **뉴스분석**: 25% (감정분석 기반)

### 🚀 주요 기능
1. 워런 버핏 100점 스코어카드 시스템
2. 5가지 모델 통합 내재가치 계산
3. 3단계 저평가 우량주 스크리닝
4. 실시간 기술적 분석
5. 뉴스 감정분석 기반 시장 심리 파악

### 🛠️ 기술 스택
- **Backend**: Python 3.9+, Pandas, NumPy, TA-Lib
- **Frontend**: Streamlit, Plotly
- **Database**: SQLite
- **API**: DART API, FinanceDataReader, 네이버 뉴스 API

### 📋 설치 및 실행
```bash
# 의존성 설치
pip install -r requirements.txt

# 환경변수 설정
cp .env.example .env
# .env 파일에 API 키 설정

# 애플리케이션 실행
streamlit run src/web/app.py
```

### 📈 개발 단계
- **Phase 1**: 기본분석 시스템 (1-4주)
- **Phase 2**: 기술분석 + 감정분석 (5-8주)
- **Phase 3**: 웹 인터페이스 (9-10주)

### 📞 연락처
프로젝트 관련 문의: [이메일 주소]
