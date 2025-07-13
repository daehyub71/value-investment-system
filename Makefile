# Makefile for Finance Data Vibe

.PHONY: install test run clean lint format

# 개발 환경 설정
install:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

# 테스트 실행
test:
	pytest tests/ -v

# 애플리케이션 실행
run:
	streamlit run src/web/app.py

# 코드 정리
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/

# 코드 린팅
lint:
	flake8 src/
	pylint src/

# 코드 포맷팅
format:
	black src/
	isort src/

# 데이터 수집
collect-data:
	python scripts/data_collection/collect_dart_data.py
	python scripts/data_collection/collect_stock_data.py
	python scripts/data_collection/collect_news_data.py

# 실시간 데이터 수집 (KIS API)
collect-realtime-kis:
	python scripts/data_collection/collect_kis_data.py --market_indicators
	python scripts/data_collection/collect_kis_data.py --all_stocks --limit=30

# 실시간 데이터 수집 (FinanceDataReader 대안)
collect-realtime:
	python scripts/data_collection/collect_alternative_realtime.py --market_indicators
	python scripts/data_collection/collect_alternative_realtime.py --all_stocks --limit=30

# 시장 지표만 수집
collect-market:
	python scripts/data_collection/collect_alternative_realtime.py --market_indicators

# 상위 종목 실시간 데이터 수집
collect-top-stocks:
	python scripts/data_collection/collect_alternative_realtime.py --all_stocks --limit=50

# 분석 실행
analyze:
	python scripts/analysis/run_buffett_analysis.py
	python scripts/analysis/run_technical_analysis.py
	python scripts/analysis/run_sentiment_analysis.py
