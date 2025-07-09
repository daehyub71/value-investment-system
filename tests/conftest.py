import pytest
import os
import sys

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

@pytest.fixture
def sample_financial_data():
    """테스트용 재무 데이터"""
    return {
        'revenue': 1000000,
        'net_income': 100000,
        'total_assets': 2000000,
        'shareholders_equity': 1500000,
        'total_debt': 500000,
        'current_assets': 800000,
        'current_liabilities': 400000
    }

@pytest.fixture
def sample_stock_data():
    """테스트용 주가 데이터"""
    import pandas as pd
    import numpy as np
    
    dates = pd.date_range('2023-01-01', periods=100)
    prices = np.random.uniform(50000, 60000, 100)
    
    return pd.DataFrame({
        'Close': prices,
        'High': prices * 1.02,
        'Low': prices * 0.98,
        'Volume': np.random.randint(1000000, 5000000, 100)
    }, index=dates)
