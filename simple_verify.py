import sqlite3
import pandas as pd

print("🔍 카카오뱅크(323410) 실제 데이터 검증")
print("=" * 60)

stock_code = '323410'

# DART 데이터베이스 확인
try:
    print("📊 DART 재무데이터 확인")
    dart_conn = sqlite3.connect("data/databases/dart_data.db")
    
    financial_query = "SELECT COUNT(*) as count FROM financial_statements WHERE stock_code = ?"
    financial_count = pd.read_sql_query(financial_query, dart_conn, params=(stock_code,))
    
    print(f"재무제표 데이터: {financial_count.iloc[0]['count']}건")
    
    corp_query = "SELECT COUNT(*) as count FROM corp_codes WHERE stock_code = ?"
    corp_count = pd.read_sql_query(corp_query, dart_conn, params=(stock_code,))
    
    print(f"기업정보: {corp_count.iloc[0]['count']}건")
    
    dart_conn.close()
    
except Exception as e:
    print(f"DART 데이터 오류: {e}")

# 주식 데이터베이스 확인
try:
    print("\n📈 주식 데이터 확인")
    stock_conn = sqlite3.connect("data/databases/stock_data.db")
    
    company_query = "SELECT COUNT(*) as count FROM company_info WHERE stock_code = ?"
    company_count = pd.read_sql_query(company_query, stock_conn, params=(stock_code,))
    
    print(f"회사정보: {company_count.iloc[0]['count']}건")
    
    price_query = "SELECT COUNT(*) as count FROM stock_prices WHERE stock_code = ?"
    price_count = pd.read_sql_query(price_query, stock_conn, params=(stock_code,))
    
    print(f"주가데이터: {price_count.iloc[0]['count']}건")
    
    # 최신 주가 확인
    if price_count.iloc[0]['count'] > 0:
        latest_query = "SELECT date, close_price FROM stock_prices WHERE stock_code = ? ORDER BY date DESC LIMIT 1"
        latest_price = pd.read_sql_query(latest_query, stock_conn, params=(stock_code,))
        print(f"최신 주가: {latest_price.iloc[0]['date']} - {latest_price.iloc[0]['close_price']:,}원")
    
    stock_conn.close()
    
except Exception as e:
    print(f"주식 데이터 오류: {e}")

print("\n🎯 검증 결과")
print("=" * 60)
print("❌ 현재 워런 버핏 스코어는 실제 데이터가 아닙니다!")
print("📊 제시된 ROE 15.2%, 부채비율 46.1% 등은 추정치입니다")
print("⚠️ 실제 투자 의사결정에 사용 금지!")
print("\n🚀 해결책:")
print("1. DART API 실제 연동 필요")
print("2. 재무비율 계산 엔진 구현 필요")
print("3. 실제 재무제표 데이터 파싱 필요")
