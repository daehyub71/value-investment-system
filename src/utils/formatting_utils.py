"""
포맷팅 유틸리티
데이터 표시 및 포맷팅 관련 유틸리티 함수들
"""

import re
import locale
from typing import Union, Optional, Dict, List, Any
from datetime import datetime, date
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class FormattingError(Exception):
    """포맷팅 오류"""
    pass

class NumberFormatter:
    """숫자 포맷팅 클래스"""
    
    @staticmethod
    def format_currency(amount: Union[int, float], currency: str = 'KRW', 
                       show_symbol: bool = True, decimal_places: int = 0) -> str:
        """통화 포맷팅"""
        if pd.isna(amount) or amount is None:
            return "N/A"
        
        try:
            # 통화 기호 설정
            symbols = {
                'KRW': '₩',
                'USD': '$',
                'EUR': '€',
                'JPY': '¥',
                'CNY': '¥'
            }
            
            symbol = symbols.get(currency.upper(), '₩')
            
            # 숫자 포맷팅
            if abs(amount) >= 1e12:  # 조 단위
                formatted = f"{amount/1e12:.1f}조"
            elif abs(amount) >= 1e8:  # 억 단위
                formatted = f"{amount/1e8:.1f}억"
            elif abs(amount) >= 1e4:  # 만 단위
                formatted = f"{amount/1e4:.1f}만"
            else:
                formatted = f"{amount:,.{decimal_places}f}"
            
            if show_symbol:
                return f"{symbol}{formatted}"
            else:
                return formatted
                
        except Exception as e:
            logger.error(f"통화 포맷팅 오류: {e}")
            return str(amount)
    
    @staticmethod
    def format_percentage(value: Union[int, float], decimal_places: int = 2,
                         show_sign: bool = True) -> str:
        """백분율 포맷팅"""
        if pd.isna(value) or value is None:
            return "N/A"
        
        try:
            if show_sign:
                sign = "+" if value > 0 else ""
                return f"{sign}{value:.{decimal_places}f}%"
            else:
                return f"{value:.{decimal_places}f}%"
        except Exception as e:
            logger.error(f"백분율 포맷팅 오류: {e}")
            return str(value)
    
    @staticmethod
    def format_number(value: Union[int, float], decimal_places: int = 2,
                     use_separator: bool = True, unit: str = "") -> str:
        """일반 숫자 포맷팅"""
        if pd.isna(value) or value is None:
            return "N/A"
        
        try:
            if use_separator:
                formatted = f"{value:,.{decimal_places}f}"
            else:
                formatted = f"{value:.{decimal_places}f}"
            
            # 소수점 이하가 0이면 제거
            if decimal_places > 0 and formatted.endswith('0' * decimal_places):
                formatted = formatted[:-decimal_places-1]
            
            return f"{formatted}{unit}"
        except Exception as e:
            logger.error(f"숫자 포맷팅 오류: {e}")
            return str(value)
    
    @staticmethod
    def format_large_number(value: Union[int, float], decimal_places: int = 1) -> str:
        """큰 숫자 포맷팅 (K, M, B 단위)"""
        if pd.isna(value) or value is None:
            return "N/A"
        
        try:
            abs_value = abs(value)
            sign = "-" if value < 0 else ""
            
            if abs_value >= 1e12:  # 조 (Trillion)
                return f"{sign}{abs_value/1e12:.{decimal_places}f}T"
            elif abs_value >= 1e9:  # 십억 (Billion)
                return f"{sign}{abs_value/1e9:.{decimal_places}f}B"
            elif abs_value >= 1e6:  # 백만 (Million)
                return f"{sign}{abs_value/1e6:.{decimal_places}f}M"
            elif abs_value >= 1e3:  # 천 (Thousand)
                return f"{sign}{abs_value/1e3:.{decimal_places}f}K"
            else:
                return f"{sign}{abs_value:.{decimal_places}f}"
        except Exception as e:
            logger.error(f"큰 숫자 포맷팅 오류: {e}")
            return str(value)
    
    @staticmethod
    def format_korean_number(value: Union[int, float], decimal_places: int = 1) -> str:
        """한국식 숫자 포맷팅 (만, 억, 조)"""
        if pd.isna(value) or value is None:
            return "N/A"
        
        try:
            abs_value = abs(value)
            sign = "-" if value < 0 else ""
            
            if abs_value >= 1e12:  # 조
                return f"{sign}{abs_value/1e12:.{decimal_places}f}조"
            elif abs_value >= 1e8:  # 억
                return f"{sign}{abs_value/1e8:.{decimal_places}f}억"
            elif abs_value >= 1e4:  # 만
                return f"{sign}{abs_value/1e4:.{decimal_places}f}만"
            else:
                return f"{sign}{abs_value:,.{decimal_places}f}"
        except Exception as e:
            logger.error(f"한국식 숫자 포맷팅 오류: {e}")
            return str(value)
    
    @staticmethod
    def format_ratio(value: Union[int, float], decimal_places: int = 2) -> str:
        """비율 포맷팅 (배수)"""
        if pd.isna(value) or value is None:
            return "N/A"
        
        try:
            if value == float('inf'):
                return "∞"
            elif value == float('-inf'):
                return "-∞"
            else:
                return f"{value:.{decimal_places}f}배"
        except Exception as e:
            logger.error(f"비율 포맷팅 오류: {e}")
            return str(value)
    
    @staticmethod
    def format_change(current: Union[int, float], previous: Union[int, float],
                     format_type: str = 'percentage') -> str:
        """변화량 포맷팅"""
        if pd.isna(current) or pd.isna(previous) or current is None or previous is None:
            return "N/A"
        
        if previous == 0:
            return "N/A"
        
        try:
            change = current - previous
            change_rate = (change / previous) * 100
            
            if format_type == 'percentage':
                sign = "+" if change > 0 else ""
                return f"{sign}{change_rate:.2f}%"
            elif format_type == 'absolute':
                return NumberFormatter.format_number(change, show_sign=True)
            elif format_type == 'both':
                change_str = NumberFormatter.format_number(change, show_sign=True)
                rate_str = NumberFormatter.format_percentage(change_rate, show_sign=True)
                return f"{change_str} ({rate_str})"
            else:
                return f"{change_rate:.2f}%"
        except Exception as e:
            logger.error(f"변화량 포맷팅 오류: {e}")
            return "N/A"

class DateFormatter:
    """날짜 포맷팅 클래스"""
    
    @staticmethod
    def format_date(date_input: Union[str, datetime, date], 
                   format_type: str = 'korean') -> str:
        """날짜 포맷팅"""
        if pd.isna(date_input) or date_input is None:
            return "N/A"
        
        try:
            if isinstance(date_input, str):
                if len(date_input) == 8 and date_input.isdigit():
                    dt = datetime.strptime(date_input, '%Y%m%d')
                else:
                    dt = datetime.fromisoformat(date_input.replace('Z', '+00:00'))
            elif isinstance(date_input, datetime):
                dt = date_input
            elif isinstance(date_input, date):
                dt = datetime.combine(date_input, datetime.min.time())
            else:
                return str(date_input)
            
            formats = {
                'korean': '%Y년 %m월 %d일',
                'korean_short': '%Y.%m.%d',
                'iso': '%Y-%m-%d',
                'us': '%m/%d/%Y',
                'full': '%Y년 %m월 %d일 %H시 %M분',
                'datetime': '%Y-%m-%d %H:%M:%S',
                'time': '%H:%M:%S'
            }
            
            return dt.strftime(formats.get(format_type, '%Y-%m-%d'))
        except Exception as e:
            logger.error(f"날짜 포맷팅 오류: {e}")
            return str(date_input)
    
    @staticmethod
    def format_relative_date(date_input: Union[str, datetime, date]) -> str:
        """상대적 날짜 포맷팅 (예: 3일 전, 1주일 전)"""
        if pd.isna(date_input) or date_input is None:
            return "N/A"
        
        try:
            if isinstance(date_input, str):
                if len(date_input) == 8 and date_input.isdigit():
                    dt = datetime.strptime(date_input, '%Y%m%d')
                else:
                    dt = datetime.fromisoformat(date_input.replace('Z', '+00:00'))
            elif isinstance(date_input, datetime):
                dt = date_input
            elif isinstance(date_input, date):
                dt = datetime.combine(date_input, datetime.min.time())
            else:
                return str(date_input)
            
            now = datetime.now()
            diff = now - dt
            
            if diff.days == 0:
                if diff.seconds < 3600:
                    return f"{diff.seconds // 60}분 전"
                else:
                    return f"{diff.seconds // 3600}시간 전"
            elif diff.days == 1:
                return "어제"
            elif diff.days < 7:
                return f"{diff.days}일 전"
            elif diff.days < 30:
                return f"{diff.days // 7}주 전"
            elif diff.days < 365:
                return f"{diff.days // 30}개월 전"
            else:
                return f"{diff.days // 365}년 전"
        except Exception as e:
            logger.error(f"상대적 날짜 포맷팅 오류: {e}")
            return str(date_input)

class TextFormatter:
    """텍스트 포맷팅 클래스"""
    
    @staticmethod
    def format_stock_code(stock_code: str) -> str:
        """종목코드 포맷팅"""
        if not stock_code:
            return "N/A"
        
        # 6자리 숫자로 정규화
        normalized = str(stock_code).zfill(6)
        return normalized
    
    @staticmethod
    def format_company_name(company_name: str, max_length: int = 20) -> str:
        """회사명 포맷팅"""
        if not company_name:
            return "N/A"
        
        # 불필요한 문자 제거
        cleaned = re.sub(r'[^\w\s가-힣]', '', company_name)
        
        if len(cleaned) > max_length:
            return cleaned[:max_length-2] + ".."
        
        return cleaned
    
    @staticmethod
    def format_news_title(title: str, max_length: int = 50) -> str:
        """뉴스 제목 포맷팅"""
        if not title:
            return "N/A"
        
        # HTML 태그 제거
        cleaned = re.sub(r'<[^>]+>', '', title)
        
        # 특수 문자 정리
        cleaned = re.sub(r'&[^;]+;', '', cleaned)
        
        if len(cleaned) > max_length:
            return cleaned[:max_length-2] + ".."
        
        return cleaned.strip()
    
    @staticmethod
    def format_grade(value: Union[int, float], max_score: int = 100) -> str:
        """점수 등급 포맷팅"""
        if pd.isna(value) or value is None:
            return "N/A"
        
        try:
            score = (value / max_score) * 100
            
            if score >= 90:
                return "A+ (매우 우수)"
            elif score >= 80:
                return "A (우수)"
            elif score >= 70:
                return "B+ (양호)"
            elif score >= 60:
                return "B (보통)"
            elif score >= 50:
                return "C+ (미흡)"
            elif score >= 40:
                return "C (불량)"
            else:
                return "D (매우 불량)"
        except Exception as e:
            logger.error(f"등급 포맷팅 오류: {e}")
            return "N/A"
    
    @staticmethod
    def format_investment_grade(per: float, pbr: float, roe: float) -> str:
        """투자 등급 포맷팅"""
        try:
            score = 0
            
            # PER 평가
            if per < 10:
                score += 3
            elif per < 15:
                score += 2
            elif per < 20:
                score += 1
            
            # PBR 평가
            if pbr < 1:
                score += 3
            elif pbr < 1.5:
                score += 2
            elif pbr < 2:
                score += 1
            
            # ROE 평가
            if roe > 20:
                score += 3
            elif roe > 15:
                score += 2
            elif roe > 10:
                score += 1
            
            if score >= 8:
                return "★★★★★ (매우 우수)"
            elif score >= 6:
                return "★★★★☆ (우수)"
            elif score >= 4:
                return "★★★☆☆ (보통)"
            elif score >= 2:
                return "★★☆☆☆ (미흡)"
            else:
                return "★☆☆☆☆ (불량)"
        except Exception as e:
            logger.error(f"투자 등급 포맷팅 오류: {e}")
            return "N/A"

class TableFormatter:
    """테이블 포맷팅 클래스"""
    
    @staticmethod
    def format_dataframe(df: pd.DataFrame, 
                        numeric_columns: List[str] = None,
                        percentage_columns: List[str] = None,
                        currency_columns: List[str] = None,
                        date_columns: List[str] = None) -> pd.DataFrame:
        """데이터프레임 포맷팅"""
        formatted_df = df.copy()
        
        try:
            # 숫자 컬럼 포맷팅
            if numeric_columns:
                for col in numeric_columns:
                    if col in formatted_df.columns:
                        formatted_df[col] = formatted_df[col].apply(
                            lambda x: NumberFormatter.format_number(x, 2)
                        )
            
            # 백분율 컬럼 포맷팅
            if percentage_columns:
                for col in percentage_columns:
                    if col in formatted_df.columns:
                        formatted_df[col] = formatted_df[col].apply(
                            lambda x: NumberFormatter.format_percentage(x, 2)
                        )
            
            # 통화 컬럼 포맷팅
            if currency_columns:
                for col in currency_columns:
                    if col in formatted_df.columns:
                        formatted_df[col] = formatted_df[col].apply(
                            lambda x: NumberFormatter.format_currency(x)
                        )
            
            # 날짜 컬럼 포맷팅
            if date_columns:
                for col in date_columns:
                    if col in formatted_df.columns:
                        formatted_df[col] = formatted_df[col].apply(
                            lambda x: DateFormatter.format_date(x, 'korean_short')
                        )
            
            return formatted_df
        except Exception as e:
            logger.error(f"데이터프레임 포맷팅 오류: {e}")
            return df
    
    @staticmethod
    def create_summary_table(data: Dict[str, Any], title: str = "요약 정보") -> str:
        """요약 테이블 생성"""
        try:
            lines = [f"📊 {title}", "=" * 40]
            
            for key, value in data.items():
                if isinstance(value, (int, float)):
                    if 'percent' in key.lower() or 'rate' in key.lower():
                        formatted_value = NumberFormatter.format_percentage(value)
                    elif 'price' in key.lower() or 'amount' in key.lower():
                        formatted_value = NumberFormatter.format_currency(value)
                    else:
                        formatted_value = NumberFormatter.format_number(value)
                else:
                    formatted_value = str(value)
                
                lines.append(f"{key}: {formatted_value}")
            
            return "\n".join(lines)
        except Exception as e:
            logger.error(f"요약 테이블 생성 오류: {e}")
            return f"오류: {e}"

class ColorFormatter:
    """색상 포맷팅 클래스 (Streamlit용)"""
    
    @staticmethod
    def get_color_by_value(value: Union[int, float], 
                          positive_color: str = "green",
                          negative_color: str = "red",
                          neutral_color: str = "gray") -> str:
        """값에 따른 색상 반환"""
        if pd.isna(value) or value is None:
            return neutral_color
        
        if value > 0:
            return positive_color
        elif value < 0:
            return negative_color
        else:
            return neutral_color
    
    @staticmethod
    def format_colored_text(text: str, color: str) -> str:
        """색상이 적용된 텍스트 포맷팅 (Streamlit용)"""
        return f":{color}[{text}]"
    
    @staticmethod
    def format_colored_metric(value: Union[int, float], 
                            format_type: str = 'number') -> str:
        """색상이 적용된 지표 포맷팅"""
        if pd.isna(value) or value is None:
            return "N/A"
        
        color = ColorFormatter.get_color_by_value(value)
        
        if format_type == 'percentage':
            formatted = NumberFormatter.format_percentage(value)
        elif format_type == 'currency':
            formatted = NumberFormatter.format_currency(value)
        else:
            formatted = NumberFormatter.format_number(value)
        
        return ColorFormatter.format_colored_text(formatted, color)

# 편의 함수들
def format_currency(amount: Union[int, float], currency: str = 'KRW') -> str:
    """통화 포맷팅"""
    return NumberFormatter.format_currency(amount, currency)

def format_percentage(value: Union[int, float], decimal_places: int = 2) -> str:
    """백분율 포맷팅"""
    return NumberFormatter.format_percentage(value, decimal_places)

def format_number(value: Union[int, float], decimal_places: int = 2) -> str:
    """숫자 포맷팅"""
    return NumberFormatter.format_number(value, decimal_places)

def format_large_number(value: Union[int, float]) -> str:
    """큰 숫자 포맷팅"""
    return NumberFormatter.format_large_number(value)

def format_korean_number(value: Union[int, float]) -> str:
    """한국식 숫자 포맷팅"""
    return NumberFormatter.format_korean_number(value)

def format_date(date_input: Union[str, datetime, date], format_type: str = 'korean') -> str:
    """날짜 포맷팅"""
    return DateFormatter.format_date(date_input, format_type)

def format_stock_code(stock_code: str) -> str:
    """종목코드 포맷팅"""
    return TextFormatter.format_stock_code(stock_code)

def format_company_name(company_name: str, max_length: int = 20) -> str:
    """회사명 포맷팅"""
    return TextFormatter.format_company_name(company_name, max_length)

def format_change(current: Union[int, float], previous: Union[int, float]) -> str:
    """변화량 포맷팅"""
    return NumberFormatter.format_change(current, previous)

def format_grade(value: Union[int, float], max_score: int = 100) -> str:
    """점수 등급 포맷팅"""
    return TextFormatter.format_grade(value, max_score)

# 사용 예시
if __name__ == "__main__":
    print("🎨 포맷팅 유틸리티 테스트")
    print("=" * 50)
    
    # 통화 포맷팅 테스트
    print("💰 통화 포맷팅 테스트:")
    amounts = [1234, 123456789, 1234567890123]
    for amount in amounts:
        formatted = format_currency(amount)
        print(f"  {amount:,} → {formatted}")
    
    # 백분율 포맷팅 테스트
    print("\n📊 백분율 포맷팅 테스트:")
    percentages = [12.34, -5.67, 0.12]
    for pct in percentages:
        formatted = format_percentage(pct)
        print(f"  {pct} → {formatted}")
    
    # 큰 숫자 포맷팅 테스트
    print("\n🔢 큰 숫자 포맷팅 테스트:")
    large_numbers = [1234, 1234567, 1234567890, 1234567890123]
    for num in large_numbers:
        formatted = format_large_number(num)
        korean_formatted = format_korean_number(num)
        print(f"  {num:,} → {formatted} / {korean_formatted}")
    
    # 날짜 포맷팅 테스트
    print("\n📅 날짜 포맷팅 테스트:")
    test_date = datetime(2024, 1, 15, 14, 30, 0)
    formats = ['korean', 'korean_short', 'iso', 'us', 'full']
    for fmt in formats:
        formatted = format_date(test_date, fmt)
        print(f"  {fmt}: {formatted}")
    
    # 종목코드 포맷팅 테스트
    print("\n📈 종목코드 포맷팅 테스트:")
    stock_codes = ['5930', '660', '35420']
    for code in stock_codes:
        formatted = format_stock_code(code)
        print(f"  {code} → {formatted}")
    
    # 회사명 포맷팅 테스트
    print("\n🏢 회사명 포맷팅 테스트:")
    company_names = ['삼성전자(주)', 'SK하이닉스', '매우긴회사명을가진회사입니다']
    for name in company_names:
        formatted = format_company_name(name, 15)
        print(f"  {name} → {formatted}")
    
    # 변화량 포맷팅 테스트
    print("\n📈 변화량 포맷팅 테스트:")
    changes = [(60000, 50000), (45000, 50000), (50000, 50000)]
    for current, previous in changes:
        formatted = format_change(current, previous)
        print(f"  {previous:,} → {current:,}: {formatted}")
    
    # 등급 포맷팅 테스트
    print("\n⭐ 등급 포맷팅 테스트:")
    scores = [95, 85, 75, 65, 55, 45, 35]
    for score in scores:
        formatted = format_grade(score)
        print(f"  {score}점 → {formatted}")
    
    # 투자 등급 테스트
    print("\n💎 투자 등급 테스트:")
    investment_data = [(10, 0.8, 25), (15, 1.2, 18), (25, 2.0, 12), (30, 3.0, 8)]
    for per, pbr, roe in investment_data:
        grade = TextFormatter.format_investment_grade(per, pbr, roe)
        print(f"  PER:{per}, PBR:{pbr}, ROE:{roe}% → {grade}")
    
    print("\n✅ 모든 포맷팅 테스트 완료!")