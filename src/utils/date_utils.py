"""
날짜 유틸리티
날짜 처리 및 변환 관련 유틸리티 함수들
"""

import re
import pytz
from datetime import datetime, date, timedelta
from typing import Optional, Union, List, Tuple
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import logging

logger = logging.getLogger(__name__)

class DateFormatError(Exception):
    """날짜 형식 오류"""
    pass

class DateRangeError(Exception):
    """날짜 범위 오류"""
    pass

class DateUtils:
    """날짜 관련 유틸리티 클래스"""
    
    # 한국 시간대
    KST = pytz.timezone('Asia/Seoul')
    
    # 일반적인 날짜 형식들
    DATE_FORMATS = [
        '%Y%m%d',       # 20240101
        '%Y-%m-%d',     # 2024-01-01
        '%Y/%m/%d',     # 2024/01/01
        '%Y.%m.%d',     # 2024.01.01
        '%Y-%m-%d %H:%M:%S',  # 2024-01-01 12:00:00
        '%Y%m%d%H%M%S', # 20240101120000
        '%Y-%m-%dT%H:%M:%S',  # 2024-01-01T12:00:00
        '%Y-%m-%dT%H:%M:%S.%f',  # 2024-01-01T12:00:00.123456
    ]
    
    # 한국 거래소 휴장일 (고정 휴일)
    FIXED_HOLIDAYS = {
        '0101': '신정',
        '0301': '삼일절',
        '0505': '어린이날',
        '0606': '현충일',
        '0815': '광복절',
        '1003': '개천절',
        '1009': '한글날',
        '1225': '성탄절'
    }
    
    @classmethod
    def now(cls, timezone: Optional[str] = 'Asia/Seoul') -> datetime:
        """현재 시간 반환"""
        if timezone:
            tz = pytz.timezone(timezone)
            return datetime.now(tz)
        return datetime.now()
    
    @classmethod
    def today(cls, timezone: Optional[str] = 'Asia/Seoul') -> date:
        """오늘 날짜 반환"""
        return cls.now(timezone).date()
    
    @classmethod
    def parse_date(cls, date_input: Union[str, datetime, date, pd.Timestamp]) -> datetime:
        """다양한 형태의 날짜 입력을 datetime으로 변환"""
        if isinstance(date_input, datetime):
            return date_input
        
        elif isinstance(date_input, date):
            return datetime.combine(date_input, datetime.min.time())
        
        elif isinstance(date_input, pd.Timestamp):
            return date_input.to_pydatetime()
        
        elif isinstance(date_input, str):
            # 숫자만 있는 경우 (20240101 등)
            if date_input.isdigit():
                if len(date_input) == 8:
                    return datetime.strptime(date_input, '%Y%m%d')
                elif len(date_input) == 14:
                    return datetime.strptime(date_input, '%Y%m%d%H%M%S')
            
            # 다양한 형식 시도
            for fmt in cls.DATE_FORMATS:
                try:
                    return datetime.strptime(date_input, fmt)
                except ValueError:
                    continue
            
            raise DateFormatError(f"지원하지 않는 날짜 형식: {date_input}")
        
        else:
            raise DateFormatError(f"지원하지 않는 날짜 타입: {type(date_input)}")
    
    @classmethod
    def format_date(cls, date_input: Union[str, datetime, date], 
                   format: str = '%Y%m%d') -> str:
        """날짜를 지정된 형식으로 포맷팅"""
        dt = cls.parse_date(date_input)
        return dt.strftime(format)
    
    @classmethod
    def get_date_range(cls, start_date: Union[str, datetime, date], 
                      end_date: Union[str, datetime, date], 
                      freq: str = 'D') -> List[datetime]:
        """날짜 범위 생성"""
        start_dt = cls.parse_date(start_date)
        end_dt = cls.parse_date(end_date)
        
        if start_dt > end_dt:
            raise DateRangeError("시작 날짜가 종료 날짜보다 늦습니다.")
        
        date_range = pd.date_range(start=start_dt, end=end_dt, freq=freq)
        return date_range.to_pydatetime().tolist()
    
    @classmethod
    def add_business_days(cls, date_input: Union[str, datetime, date], 
                         days: int) -> datetime:
        """영업일 기준으로 날짜 더하기"""
        dt = cls.parse_date(date_input)
        
        if days == 0:
            return dt
        
        # pandas의 business day 기능 사용
        if days > 0:
            business_dates = pd.bdate_range(start=dt, periods=days + 1, freq='B')
            return business_dates[-1].to_pydatetime()
        else:
            business_dates = pd.bdate_range(end=dt, periods=abs(days) + 1, freq='B')
            return business_dates[0].to_pydatetime()
    
    @classmethod
    def get_business_days_between(cls, start_date: Union[str, datetime, date], 
                                 end_date: Union[str, datetime, date]) -> int:
        """두 날짜 사이의 영업일 수 계산"""
        start_dt = cls.parse_date(start_date)
        end_dt = cls.parse_date(end_date)
        
        if start_dt > end_dt:
            return 0
        
        business_dates = pd.bdate_range(start=start_dt, end=end_dt, freq='B')
        return len(business_dates)
    
    @classmethod
    def is_business_day(cls, date_input: Union[str, datetime, date]) -> bool:
        """영업일 여부 확인"""
        dt = cls.parse_date(date_input)
        
        # 주말 확인 (월요일: 0, 일요일: 6)
        if dt.weekday() >= 5:  # 토요일(5), 일요일(6)
            return False
        
        # 한국 공휴일 확인
        if cls.is_korean_holiday(dt):
            return False
        
        return True
    
    @classmethod
    def is_korean_holiday(cls, date_input: Union[str, datetime, date]) -> bool:
        """한국 공휴일 여부 확인"""
        dt = cls.parse_date(date_input)
        
        # 고정 휴일 확인
        date_str = dt.strftime('%m%d')
        if date_str in cls.FIXED_HOLIDAYS:
            return True
        
        # 음력 공휴일 (간략화된 버전)
        year = dt.year
        
        # 설날 (음력 1월 1일) - 대략적인 계산
        lunar_new_year = cls._get_lunar_new_year(year)
        if lunar_new_year:
            # 설날 연휴 (전날, 당일, 다음날)
            for i in range(-1, 2):
                holiday_date = lunar_new_year + timedelta(days=i)
                if dt.date() == holiday_date:
                    return True
        
        # 부처님 오신 날 (음력 4월 8일) - 대략적인 계산
        buddha_birthday = cls._get_buddha_birthday(year)
        if buddha_birthday and dt.date() == buddha_birthday:
            return True
        
        # 추석 (음력 8월 15일) - 대략적인 계산
        chuseok = cls._get_chuseok(year)
        if chuseok:
            # 추석 연휴 (전날, 당일, 다음날)
            for i in range(-1, 2):
                holiday_date = chuseok + timedelta(days=i)
                if dt.date() == holiday_date:
                    return True
        
        return False
    
    @classmethod
    def _get_lunar_new_year(cls, year: int) -> Optional[date]:
        """설날 날짜 계산 (근사치)"""
        # 실제로는 음력 계산이 복잡하므로 사전 정의된 값 사용
        lunar_new_years = {
            2023: date(2023, 1, 22),
            2024: date(2024, 2, 10),
            2025: date(2025, 1, 29),
            2026: date(2026, 2, 17),
            2027: date(2027, 2, 6),
            2028: date(2028, 1, 26),
            2029: date(2029, 2, 13),
            2030: date(2030, 2, 3),
        }
        return lunar_new_years.get(year)
    
    @classmethod
    def _get_buddha_birthday(cls, year: int) -> Optional[date]:
        """부처님 오신 날 계산 (근사치)"""
        buddha_birthdays = {
            2023: date(2023, 5, 27),
            2024: date(2024, 5, 15),
            2025: date(2025, 5, 5),
            2026: date(2026, 5, 24),
            2027: date(2027, 5, 13),
            2028: date(2028, 5, 2),
            2029: date(2029, 5, 20),
            2030: date(2030, 5, 9),
        }
        return buddha_birthdays.get(year)
    
    @classmethod
    def _get_chuseok(cls, year: int) -> Optional[date]:
        """추석 날짜 계산 (근사치)"""
        chuseoks = {
            2023: date(2023, 9, 29),
            2024: date(2024, 9, 17),
            2025: date(2025, 10, 6),
            2026: date(2026, 9, 25),
            2027: date(2027, 9, 15),
            2028: date(2028, 10, 3),
            2029: date(2029, 9, 22),
            2030: date(2030, 9, 12),
        }
        return chuseoks.get(year)
    
    @classmethod
    def get_quarter(cls, date_input: Union[str, datetime, date]) -> int:
        """분기 계산"""
        dt = cls.parse_date(date_input)
        return (dt.month - 1) // 3 + 1
    
    @classmethod
    def get_quarter_dates(cls, year: int, quarter: int) -> Tuple[date, date]:
        """분기의 시작일과 종료일 반환"""
        if quarter not in [1, 2, 3, 4]:
            raise ValueError("분기는 1, 2, 3, 4 중 하나여야 합니다.")
        
        start_month = (quarter - 1) * 3 + 1
        start_date = date(year, start_month, 1)
        
        if quarter == 4:
            end_date = date(year, 12, 31)
        else:
            end_month = start_month + 2
            # 해당 월의 마지막 날 계산
            next_month = date(year, end_month + 1, 1) if end_month < 12 else date(year + 1, 1, 1)
            end_date = next_month - timedelta(days=1)
        
        return start_date, end_date
    
    @classmethod
    def get_year_dates(cls, year: int) -> Tuple[date, date]:
        """연도의 시작일과 종료일 반환"""
        return date(year, 1, 1), date(year, 12, 31)
    
    @classmethod
    def get_month_dates(cls, year: int, month: int) -> Tuple[date, date]:
        """월의 시작일과 종료일 반환"""
        start_date = date(year, month, 1)
        
        if month == 12:
            end_date = date(year, 12, 31)
        else:
            next_month = date(year, month + 1, 1)
            end_date = next_month - timedelta(days=1)
        
        return start_date, end_date
    
    @classmethod
    def get_week_dates(cls, date_input: Union[str, datetime, date]) -> Tuple[date, date]:
        """해당 주의 시작일(월요일)과 종료일(일요일) 반환"""
        dt = cls.parse_date(date_input)
        
        # 해당 주의 월요일 계산
        monday = dt - timedelta(days=dt.weekday())
        sunday = monday + timedelta(days=6)
        
        return monday.date(), sunday.date()
    
    @classmethod
    def get_relative_date(cls, date_input: Union[str, datetime, date], 
                         years: int = 0, months: int = 0, days: int = 0) -> datetime:
        """상대적 날짜 계산"""
        dt = cls.parse_date(date_input)
        
        # relativedelta 사용하여 월/년 계산
        result = dt + relativedelta(years=years, months=months, days=days)
        
        return result
    
    @classmethod
    def get_age_in_days(cls, start_date: Union[str, datetime, date], 
                       end_date: Optional[Union[str, datetime, date]] = None) -> int:
        """두 날짜 사이의 일수 계산"""
        start_dt = cls.parse_date(start_date)
        end_dt = cls.parse_date(end_date) if end_date else cls.now()
        
        return (end_dt - start_dt).days
    
    @classmethod
    def get_trading_calendar(cls, start_date: Union[str, datetime, date], 
                           end_date: Union[str, datetime, date]) -> List[date]:
        """거래일 캘린더 생성"""
        start_dt = cls.parse_date(start_date)
        end_dt = cls.parse_date(end_date)
        
        trading_days = []
        current_date = start_dt
        
        while current_date <= end_dt:
            if cls.is_business_day(current_date):
                trading_days.append(current_date.date())
            current_date += timedelta(days=1)
        
        return trading_days
    
    @classmethod
    def get_recent_business_day(cls, date_input: Optional[Union[str, datetime, date]] = None) -> date:
        """가장 최근 영업일 반환"""
        if date_input is None:
            dt = cls.now()
        else:
            dt = cls.parse_date(date_input)
        
        # 해당 날짜가 영업일이면 그대로 반환
        if cls.is_business_day(dt):
            return dt.date()
        
        # 아니면 이전 영업일 찾기
        current_date = dt - timedelta(days=1)
        while not cls.is_business_day(current_date):
            current_date -= timedelta(days=1)
        
        return current_date.date()
    
    @classmethod
    def get_next_business_day(cls, date_input: Optional[Union[str, datetime, date]] = None) -> date:
        """다음 영업일 반환"""
        if date_input is None:
            dt = cls.now()
        else:
            dt = cls.parse_date(date_input)
        
        current_date = dt + timedelta(days=1)
        while not cls.is_business_day(current_date):
            current_date += timedelta(days=1)
        
        return current_date.date()
    
    @classmethod
    def convert_timezone(cls, date_input: Union[str, datetime], 
                        from_tz: str = 'UTC', to_tz: str = 'Asia/Seoul') -> datetime:
        """시간대 변환"""
        dt = cls.parse_date(date_input)
        
        from_timezone = pytz.timezone(from_tz)
        to_timezone = pytz.timezone(to_tz)
        
        # 시간대 정보가 없으면 from_tz로 설정
        if dt.tzinfo is None:
            dt = from_timezone.localize(dt)
        
        return dt.astimezone(to_timezone)
    
    @classmethod
    def get_market_open_time(cls, date_input: Union[str, datetime, date]) -> datetime:
        """해당 날짜의 시장 개장 시간 반환 (09:00 KST)"""
        dt = cls.parse_date(date_input)
        market_open = dt.replace(hour=9, minute=0, second=0, microsecond=0)
        
        # 한국 시간대로 변환
        if market_open.tzinfo is None:
            market_open = cls.KST.localize(market_open)
        
        return market_open
    
    @classmethod
    def get_market_close_time(cls, date_input: Union[str, datetime, date]) -> datetime:
        """해당 날짜의 시장 마감 시간 반환 (15:30 KST)"""
        dt = cls.parse_date(date_input)
        market_close = dt.replace(hour=15, minute=30, second=0, microsecond=0)
        
        # 한국 시간대로 변환
        if market_close.tzinfo is None:
            market_close = cls.KST.localize(market_close)
        
        return market_close
    
    @classmethod
    def is_market_open(cls, date_input: Optional[Union[str, datetime]] = None) -> bool:
        """현재 시장이 열려있는지 확인"""
        if date_input is None:
            dt = cls.now('Asia/Seoul')
        else:
            dt = cls.parse_date(date_input)
            if dt.tzinfo is None:
                dt = cls.KST.localize(dt)
        
        # 영업일인지 확인
        if not cls.is_business_day(dt):
            return False
        
        # 시장 시간 확인
        market_open = cls.get_market_open_time(dt)
        market_close = cls.get_market_close_time(dt)
        
        return market_open <= dt <= market_close

# 편의 함수들
def now(timezone: str = 'Asia/Seoul') -> datetime:
    """현재 시간 반환"""
    return DateUtils.now(timezone)

def today(timezone: str = 'Asia/Seoul') -> date:
    """오늘 날짜 반환"""
    return DateUtils.today(timezone)

def parse_date(date_input: Union[str, datetime, date]) -> datetime:
    """날짜 파싱"""
    return DateUtils.parse_date(date_input)

def format_date(date_input: Union[str, datetime, date], format: str = '%Y%m%d') -> str:
    """날짜 포맷팅"""
    return DateUtils.format_date(date_input, format)

def is_business_day(date_input: Union[str, datetime, date]) -> bool:
    """영업일 여부 확인"""
    return DateUtils.is_business_day(date_input)

def get_business_days_between(start_date: Union[str, datetime, date], 
                             end_date: Union[str, datetime, date]) -> int:
    """영업일 수 계산"""
    return DateUtils.get_business_days_between(start_date, end_date)

def add_business_days(date_input: Union[str, datetime, date], days: int) -> datetime:
    """영업일 더하기"""
    return DateUtils.add_business_days(date_input, days)

def get_recent_business_day(date_input: Optional[Union[str, datetime, date]] = None) -> date:
    """최근 영업일 반환"""
    return DateUtils.get_recent_business_day(date_input)

def get_quarter(date_input: Union[str, datetime, date]) -> int:
    """분기 계산"""
    return DateUtils.get_quarter(date_input)

def get_quarter_dates(year: int, quarter: int) -> Tuple[date, date]:
    """분기 시작일/종료일"""
    return DateUtils.get_quarter_dates(year, quarter)

def get_trading_calendar(start_date: Union[str, datetime, date], 
                        end_date: Union[str, datetime, date]) -> List[date]:
    """거래일 캘린더"""
    return DateUtils.get_trading_calendar(start_date, end_date)

def is_market_open(date_input: Optional[Union[str, datetime]] = None) -> bool:
    """시장 개장 여부"""
    return DateUtils.is_market_open(date_input)

# 사용 예시
if __name__ == "__main__":
    print("📅 날짜 유틸리티 테스트")
    print("=" * 50)
    
    # 현재 시간
    print(f"현재 시간: {now()}")
    print(f"오늘 날짜: {today()}")
    
    # 날짜 파싱 테스트
    print("\n🔍 날짜 파싱 테스트:")
    test_dates = ['20240101', '2024-01-01', '2024/01/01', '2024.01.01']
    for date_str in test_dates:
        parsed = parse_date(date_str)
        formatted = format_date(parsed)
        print(f"  {date_str} -> {formatted}")
    
    # 영업일 테스트
    print("\n💼 영업일 테스트:")
    test_dates = ['20240101', '20240102', '20240103', '20240104', '20240105']
    for date_str in test_dates:
        is_business = is_business_day(date_str)
        day_name = parse_date(date_str).strftime('%A')
        print(f"  {date_str} ({day_name}): {'영업일' if is_business else '휴일'}")
    
    # 분기 테스트
    print("\n📊 분기 테스트:")
    for month in [1, 4, 7, 10]:
        date_str = f"2024{month:02d}01"
        quarter = get_quarter(date_str)
        print(f"  {date_str}: {quarter}분기")
    
    # 거래일 캘린더 테스트
    print("\n📈 거래일 캘린더 테스트:")
    trading_days = get_trading_calendar('20240101', '20240110')
    print(f"  2024년 1월 1-10일 거래일: {len(trading_days)}일")
    for trading_day in trading_days:
        print(f"    {trading_day}")
    
    # 시장 개장 여부 테스트
    print(f"\n🕒 현재 시장 개장 여부: {'개장' if is_market_open() else '휴장'}")
    
    # 최근 영업일 테스트
    recent_bday = get_recent_business_day()
    print(f"📅 최근 영업일: {recent_bday}")
    
    # 영업일 간 거리 테스트
    days_between = get_business_days_between('20240101', '20240110')
    print(f"📊 2024-01-01 ~ 2024-01-10 영업일 수: {days_between}일")