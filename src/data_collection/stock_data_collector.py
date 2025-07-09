"""
주가 데이터 수집 모듈
FinanceDataReader를 활용한 주가 데이터 수집
"""

import FinanceDataReader as fdr

class StockDataCollector:
    def __init__(self):
        pass
    
    def collect_stock_prices(self, stock_code, start_date, end_date):
        """주가 데이터 수집"""
        return fdr.DataReader(stock_code, start_date, end_date)
    
    def collect_market_data(self):
        """시장 데이터 수집"""
        pass
