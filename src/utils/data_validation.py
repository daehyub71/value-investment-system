"""
데이터 검증 유틸리티
입력 데이터의 유효성을 검증하는 함수들
"""

import re
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

class DataValidationError(Exception):
    """데이터 검증 오류"""
    pass

class StockCodeValidator:
    """종목코드 검증 클래스"""
    
    @staticmethod
    def validate_krx_code(stock_code: str) -> bool:
        """한국거래소 종목코드 검증"""
        if not isinstance(stock_code, str):
            return False
        
        # 6자리 숫자 형태 확인
        if not re.match(r'^\d{6}$', stock_code):
            return False
        
        # 일반적인 종목코드 범위 확인
        code_num = int(stock_code)
        
        # KOSPI: 000000-999999 (실제로는 더 세부적인 범위가 있음)
        # KOSDAQ: 000000-999999
        if code_num < 0 or code_num > 999999:
            return False
        
        return True
    
    @staticmethod
    def validate_dart_corp_code(corp_code: str) -> bool:
        """DART 기업코드 검증"""
        if not isinstance(corp_code, str):
            return False
        
        # 8자리 숫자 형태 확인
        return re.match(r'^\d{8}$', corp_code) is not None
    
    @staticmethod
    def normalize_stock_code(stock_code: str) -> str:
        """종목코드 정규화"""
        if not isinstance(stock_code, str):
            raise DataValidationError(f"종목코드는 문자열이어야 합니다: {type(stock_code)}")
        
        # 공백 제거 및 6자리로 패딩
        normalized = stock_code.strip().zfill(6)
        
        if not StockCodeValidator.validate_krx_code(normalized):
            raise DataValidationError(f"유효하지 않은 종목코드: {stock_code}")
        
        return normalized

class FinancialDataValidator:
    """재무 데이터 검증 클래스"""
    
    @staticmethod
    def validate_price_data(price_data: Union[pd.DataFrame, Dict]) -> bool:
        """주가 데이터 검증"""
        if isinstance(price_data, pd.DataFrame):
            return FinancialDataValidator._validate_price_dataframe(price_data)
        elif isinstance(price_data, dict):
            return FinancialDataValidator._validate_price_dict(price_data)
        else:
            return False
    
    @staticmethod
    def _validate_price_dataframe(df: pd.DataFrame) -> bool:
        """주가 데이터프레임 검증"""
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        
        # 필수 컬럼 확인 (대소문자 무시)
        df_columns_lower = [col.lower() for col in df.columns]
        
        for col in required_columns:
            if col not in df_columns_lower:
                logger.warning(f"필수 컬럼 누락: {col}")
                return False
        
        # 데이터 타입 확인
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df_columns_lower:
                actual_col = df.columns[df_columns_lower.index(col)]
                if not pd.api.types.is_numeric_dtype(df[actual_col]):
                    logger.warning(f"숫자 타입이 아닌 컬럼: {col}")
                    return False
        
        # 가격 데이터 논리 검증
        for idx, row in df.iterrows():
            try:
                high_col = df.columns[df_columns_lower.index('high')]
                low_col = df.columns[df_columns_lower.index('low')]
                open_col = df.columns[df_columns_lower.index('open')]
                close_col = df.columns[df_columns_lower.index('close')]
                volume_col = df.columns[df_columns_lower.index('volume')]
                
                # 고가 >= 저가
                if row[high_col] < row[low_col]:
                    logger.warning(f"고가 < 저가: {idx}")
                    return False
                
                # 시가, 종가가 고가-저가 범위 내에 있는지 확인
                if not (row[low_col] <= row[open_col] <= row[high_col]):
                    logger.warning(f"시가가 범위를 벗어남: {idx}")
                    return False
                
                if not (row[low_col] <= row[close_col] <= row[high_col]):
                    logger.warning(f"종가가 범위를 벗어남: {idx}")
                    return False
                
                # 거래량이 음수가 아닌지 확인
                if row[volume_col] < 0:
                    logger.warning(f"거래량이 음수: {idx}")
                    return False
                
            except Exception as e:
                logger.error(f"가격 데이터 검증 중 오류: {e}")
                return False
        
        return True
    
    @staticmethod
    def _validate_price_dict(price_dict: Dict) -> bool:
        """주가 딕셔너리 검증"""
        required_keys = ['open', 'high', 'low', 'close', 'volume']
        
        for key in required_keys:
            if key not in price_dict:
                logger.warning(f"필수 키 누락: {key}")
                return False
            
            if not isinstance(price_dict[key], (int, float)):
                logger.warning(f"숫자 타입이 아닌 값: {key}")
                return False
        
        # 논리 검증
        if price_dict['high'] < price_dict['low']:
            logger.warning("고가 < 저가")
            return False
        
        if not (price_dict['low'] <= price_dict['open'] <= price_dict['high']):
            logger.warning("시가가 범위를 벗어남")
            return False
        
        if not (price_dict['low'] <= price_dict['close'] <= price_dict['high']):
            logger.warning("종가가 범위를 벗어남")
            return False
        
        if price_dict['volume'] < 0:
            logger.warning("거래량이 음수")
            return False
        
        return True
    
    @staticmethod
    def validate_financial_statement(statement: Dict) -> bool:
        """재무제표 데이터 검증"""
        required_keys = ['revenue', 'net_income', 'total_assets', 'total_equity']
        
        for key in required_keys:
            if key not in statement:
                logger.warning(f"필수 재무 항목 누락: {key}")
                return False
            
            if not isinstance(statement[key], (int, float)):
                logger.warning(f"숫자 타입이 아닌 재무 항목: {key}")
                return False
        
        # 논리 검증
        if statement['total_assets'] <= 0:
            logger.warning("총자산이 0 이하")
            return False
        
        if statement['total_equity'] <= 0:
            logger.warning("총자본이 0 이하")
            return False
        
        if statement['revenue'] <= 0:
            logger.warning("매출이 0 이하")
            return False
        
        return True
    
    @staticmethod
    def validate_financial_ratios(ratios: Dict) -> bool:
        """재무비율 검증"""
        ratio_ranges = {
            'roe': (-100, 100),      # ROE: -100% ~ 100%
            'roa': (-100, 100),      # ROA: -100% ~ 100%
            'debt_ratio': (0, 1000), # 부채비율: 0% ~ 1000%
            'current_ratio': (0, 100), # 유동비율: 0% ~ 1000%
            'per': (0, 1000),        # PER: 0 ~ 1000
            'pbr': (0, 100),         # PBR: 0 ~ 100
        }
        
        for ratio_name, (min_val, max_val) in ratio_ranges.items():
            if ratio_name in ratios:
                value = ratios[ratio_name]
                
                if not isinstance(value, (int, float)):
                    logger.warning(f"숫자 타입이 아닌 재무비율: {ratio_name}")
                    return False
                
                if not (min_val <= value <= max_val):
                    logger.warning(f"재무비율 범위 초과: {ratio_name} = {value}")
                    return False
        
        return True

class DateValidator:
    """날짜 검증 클래스"""
    
    @staticmethod
    def validate_date_string(date_str: str, format: str = '%Y%m%d') -> bool:
        """날짜 문자열 검증"""
        try:
            datetime.strptime(date_str, format)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def validate_date_range(start_date: str, end_date: str, format: str = '%Y%m%d') -> bool:
        """날짜 범위 검증"""
        try:
            start = datetime.strptime(start_date, format)
            end = datetime.strptime(end_date, format)
            
            # 시작일이 종료일보다 이전이어야 함
            if start > end:
                return False
            
            # 미래 날짜는 허용하지 않음
            if end > datetime.now():
                return False
            
            return True
        except ValueError:
            return False
    
    @staticmethod
    def normalize_date(date_input: Union[str, datetime, date]) -> str:
        """날짜 정규화"""
        if isinstance(date_input, str):
            # 다양한 형태의 날짜 문자열 처리
            date_formats = ['%Y%m%d', '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d']
            
            for fmt in date_formats:
                try:
                    dt = datetime.strptime(date_input, fmt)
                    return dt.strftime('%Y%m%d')
                except ValueError:
                    continue
            
            raise DataValidationError(f"지원하지 않는 날짜 형식: {date_input}")
        
        elif isinstance(date_input, datetime):
            return date_input.strftime('%Y%m%d')
        
        elif isinstance(date_input, date):
            return date_input.strftime('%Y%m%d')
        
        else:
            raise DataValidationError(f"지원하지 않는 날짜 타입: {type(date_input)}")

class APIResponseValidator:
    """API 응답 검증 클래스"""
    
    @staticmethod
    def validate_dart_response(response: Dict) -> bool:
        """DART API 응답 검증"""
        if not isinstance(response, dict):
            return False
        
        # 상태 코드 확인
        if 'status' in response:
            if response['status'] != '000':
                logger.warning(f"DART API 오류: {response.get('message', 'Unknown error')}")
                return False
        
        # 데이터 존재 확인
        if 'list' not in response:
            logger.warning("DART API 응답에 데이터가 없음")
            return False
        
        return True
    
    @staticmethod
    def validate_kis_response(response: Dict) -> bool:
        """KIS API 응답 검증"""
        if not isinstance(response, dict):
            return False
        
        # 응답 코드 확인
        if 'rt_cd' in response:
            if response['rt_cd'] != '0':
                logger.warning(f"KIS API 오류: {response.get('msg1', 'Unknown error')}")
                return False
        
        return True
    
    @staticmethod
    def validate_naver_news_response(response: Dict) -> bool:
        """네이버 뉴스 API 응답 검증"""
        if not isinstance(response, dict):
            return False
        
        # 필수 필드 확인
        required_fields = ['lastBuildDate', 'total', 'start', 'display', 'items']
        
        for field in required_fields:
            if field not in response:
                logger.warning(f"네이버 뉴스 API 응답에 필수 필드 누락: {field}")
                return False
        
        # 뉴스 항목 검증
        if not isinstance(response['items'], list):
            logger.warning("네이버 뉴스 items가 리스트가 아님")
            return False
        
        return True

class DataRangeValidator:
    """데이터 범위 검증 클래스"""
    
    @staticmethod
    def validate_percentage(value: float, min_val: float = -100, max_val: float = 100) -> bool:
        """퍼센트 값 검증"""
        if not isinstance(value, (int, float)):
            return False
        
        return min_val <= value <= max_val
    
    @staticmethod
    def validate_positive_number(value: Union[int, float]) -> bool:
        """양수 검증"""
        if not isinstance(value, (int, float)):
            return False
        
        return value > 0
    
    @staticmethod
    def validate_non_negative_number(value: Union[int, float]) -> bool:
        """음이 아닌 수 검증"""
        if not isinstance(value, (int, float)):
            return False
        
        return value >= 0
    
    @staticmethod
    def validate_ratio(value: float, min_val: float = 0, max_val: float = 10) -> bool:
        """비율 검증"""
        if not isinstance(value, (int, float)):
            return False
        
        return min_val <= value <= max_val

class DataSanitizer:
    """데이터 정제 클래스"""
    
    @staticmethod
    def sanitize_numeric_value(value: Any) -> Optional[float]:
        """숫자 값 정제"""
        if pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            if np.isnan(value) or np.isinf(value):
                return None
            return float(value)
        
        if isinstance(value, str):
            # 문자열에서 숫자만 추출
            cleaned = re.sub(r'[^\d.-]', '', value)
            try:
                return float(cleaned)
            except ValueError:
                return None
        
        return None
    
    @staticmethod
    def sanitize_string_value(value: Any) -> Optional[str]:
        """문자열 값 정제"""
        if pd.isna(value):
            return None
        
        if isinstance(value, str):
            # 앞뒤 공백 제거, 연속된 공백을 하나로 변환
            cleaned = re.sub(r'\s+', ' ', str(value).strip())
            return cleaned if cleaned else None
        
        return str(value)
    
    @staticmethod
    def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """데이터프레임 정제"""
        cleaned_df = df.copy()
        
        # 숫자 컬럼 정제
        numeric_columns = cleaned_df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            cleaned_df[col] = cleaned_df[col].apply(DataSanitizer.sanitize_numeric_value)
        
        # 문자열 컬럼 정제
        string_columns = cleaned_df.select_dtypes(include=['object']).columns
        for col in string_columns:
            cleaned_df[col] = cleaned_df[col].apply(DataSanitizer.sanitize_string_value)
        
        return cleaned_df

# 편의 함수들
def validate_stock_code(stock_code: str) -> bool:
    """종목코드 검증"""
    return StockCodeValidator.validate_krx_code(stock_code)

def validate_price_data(price_data: Union[pd.DataFrame, Dict]) -> bool:
    """주가 데이터 검증"""
    return FinancialDataValidator.validate_price_data(price_data)

def validate_date_string(date_str: str, format: str = '%Y%m%d') -> bool:
    """날짜 문자열 검증"""
    return DateValidator.validate_date_string(date_str, format)

def normalize_stock_code(stock_code: str) -> str:
    """종목코드 정규화"""
    return StockCodeValidator.normalize_stock_code(stock_code)

def normalize_date(date_input: Union[str, datetime, date]) -> str:
    """날짜 정규화"""
    return DateValidator.normalize_date(date_input)

def sanitize_numeric(value: Any) -> Optional[float]:
    """숫자 값 정제"""
    return DataSanitizer.sanitize_numeric_value(value)

def sanitize_string(value: Any) -> Optional[str]:
    """문자열 값 정제"""
    return DataSanitizer.sanitize_string_value(value)

# 사용 예시
if __name__ == "__main__":
    # 종목코드 검증 테스트
    print("🔍 종목코드 검증 테스트")
    test_codes = ['005930', '000660', '123456', 'INVALID']
    
    for code in test_codes:
        is_valid = validate_stock_code(code)
        print(f"  {code}: {'✅ 유효' if is_valid else '❌ 무효'}")
    
    # 날짜 검증 테스트
    print("\n📅 날짜 검증 테스트")
    test_dates = ['20240101', '2024-01-01', '2024/01/01', 'invalid']
    
    for date_str in test_dates:
        try:
            normalized = normalize_date(date_str)
            print(f"  {date_str} -> {normalized}")
        except DataValidationError as e:
            print(f"  {date_str}: ❌ {e}")
    
    # 주가 데이터 검증 테스트
    print("\n📊 주가 데이터 검증 테스트")
    test_price_data = {
        'open': 70000,
        'high': 72000,
        'low': 69000,
        'close': 71000,
        'volume': 1000000
    }
    
    is_valid = validate_price_data(test_price_data)
    print(f"  주가 데이터: {'✅ 유효' if is_valid else '❌ 무효'}")
    
    # 데이터 정제 테스트
    print("\n🧹 데이터 정제 테스트")
    test_values = [123.45, '123.45', '123,456.78', 'invalid', np.nan, np.inf]
    
    for value in test_values:
        sanitized = sanitize_numeric(value)
        print(f"  {value} -> {sanitized}")