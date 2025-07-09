"""
DART API 데이터 수집 모듈
공시정보 및 재무제표 데이터 수집
"""

class DartCollector:
    def __init__(self, api_key):
        self.api_key = api_key
    
    def collect_financial_data(self, corp_code):
        """재무제표 데이터 수집"""
        pass
    
    def collect_disclosure_data(self, corp_code):
        """공시정보 데이터 수집"""
        pass
