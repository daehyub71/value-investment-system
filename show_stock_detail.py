#!/usr/bin/env python3
"""
특정 종목의 Financial Ratios 상세 정보 표시 프로그램
모든 재무비율과 시장 데이터를 상세하게 분석

실행 방법:
python show_stock_detail.py --stock_code 000660
python show_stock_detail.py --stock_code 005930
"""

import sqlite3
import pandas as pd
from pathlib import Path
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional

def find_financial_tables():
    """사용 가능한 financial_ratios 테이블 찾기"""
    db_path = Path('data/databases/stock_data.db')
    
    if not db_path.exists():
        return []
    
    try:
        with sqlite3.connect(db_path) as conn:
            tables = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name LIKE '%financial_ratio%'
                ORDER BY name
            """).fetchall()
            return [table[0] for table in tables]
    except:
        return []

def get_table_columns(table_name: str) -> List[str]:
    """테이블의 모든 컬럼 이름 조회"""
    db_path = Path('data/databases/stock_data.db')
    
    try:
        with sqlite3.connect(db_path) as conn:
            columns_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            return [col[1] for col in columns_info]
    except:
        return []

def get_stock_financial_data(stock_code: str, table_name: str) -> Optional[Dict[str, Any]]:
    """특정 종목의 financial_ratios 데이터 조회"""
    db_path = Path('data/databases/stock_data.db')
    
    try:
        with sqlite3.connect(db_path) as conn:
            # 해당 종목의 모든 데이터 조회
            query = f"SELECT * FROM {table_name} WHERE stock_code = ? ORDER BY year DESC, quarter DESC"
            cursor = conn.execute(query, (stock_code,))
            
            # 컬럼 이름 가져오기
            columns = [description[0] for description in cursor.description]
            
            # 데이터 가져오기
            rows = cursor.fetchall()
            
            if not rows:
                return None
            
            # 첫 번째 레코드를 딕셔너리로 변환
            stock_data = dict(zip(columns, rows[0]))
            
            # 모든 레코드도 포함
            all_records = [dict(zip(columns, row)) for row in rows]
            
            return {
                'latest_data': stock_data,
                'all_records': all_records,
                'columns': columns,
                'table_name': table_name
            }
    except Exception as e:
        print(f"❌ 데이터 조회 실패: {e}")
        return None

def get_stock_price_data(stock_code: str) -> Optional[Dict[str, Any]]:
    """주가 데이터 조회"""
    db_path = Path('data/databases/stock_data.db')
    
    try:
        with sqlite3.connect(db_path) as conn:
            # 최신 주가 데이터
            latest_price = conn.execute("""
                SELECT date, open_price, high_price, low_price, close_price, volume, amount
                FROM stock_prices 
                WHERE stock_code = ?
                ORDER BY date DESC
                LIMIT 1
            """, (stock_code,)).fetchone()
            
            if not latest_price:
                return None
            
            # 52주 고저점
            week52_data = conn.execute("""
                SELECT 
                    MAX(high_price) as week52_high,
                    MIN(low_price) as week52_low,
                    AVG(volume) as avg_volume
                FROM stock_prices 
                WHERE stock_code = ? AND date >= date('now', '-365 days')
            """, (stock_code,)).fetchone()
            
            # 주가 변동률 계산
            price_changes = conn.execute("""
                SELECT 
                    (SELECT close_price FROM stock_prices WHERE stock_code = ? ORDER BY date DESC LIMIT 1 OFFSET 1) as prev_1d,
                    (SELECT close_price FROM stock_prices WHERE stock_code = ? ORDER BY date DESC LIMIT 1 OFFSET 5) as prev_1w,
                    (SELECT close_price FROM stock_prices WHERE stock_code = ? ORDER BY date DESC LIMIT 1 OFFSET 20) as prev_1m
            """, (stock_code, stock_code, stock_code)).fetchone()
            
            return {
                'latest_price': {
                    'date': latest_price[0],
                    'open': latest_price[1],
                    'high': latest_price[2],
                    'low': latest_price[3],
                    'close': latest_price[4],
                    'volume': latest_price[5],
                    'amount': latest_price[6]
                },
                'week52': {
                    'high': week52_data[0] if week52_data else None,
                    'low': week52_data[1] if week52_data else None,
                    'avg_volume': week52_data[2] if week52_data else None
                },
                'price_changes': {
                    'prev_1d': price_changes[0] if price_changes else None,
                    'prev_1w': price_changes[1] if price_changes else None,
                    'prev_1m': price_changes[2] if price_changes else None
                }
            }
    except Exception as e:
        print(f"❌ 주가 데이터 조회 실패: {e}")
        return None

def calculate_additional_ratios(financial_data: Dict[str, Any], price_data: Dict[str, Any]) -> Dict[str, Any]:
    """추가 재무비율 계산"""
    try:
        current_price = price_data['latest_price']['close']
        
        additional_ratios = {}
        
        # 기본 데이터에서 추가 계산
        market_cap = financial_data.get('market_cap', 0)
        per = financial_data.get('per', 0)
        pbr = financial_data.get('pbr', 0)
        eps = financial_data.get('eps', 0)
        bps = financial_data.get('bps', 0)
        
        # 52주 고저점 대비 비율
        if price_data['week52']['high']:
            additional_ratios['week52_high_ratio'] = (current_price / price_data['week52']['high']) * 100
        
        if price_data['week52']['low']:
            additional_ratios['week52_low_ratio'] = (current_price / price_data['week52']['low']) * 100
        
        # 주가 변동률
        if price_data['price_changes']['prev_1d']:
            additional_ratios['change_1d'] = ((current_price - price_data['price_changes']['prev_1d']) / price_data['price_changes']['prev_1d']) * 100
        
        if price_data['price_changes']['prev_1w']:
            additional_ratios['change_1w'] = ((current_price - price_data['price_changes']['prev_1w']) / price_data['price_changes']['prev_1w']) * 100
        
        if price_data['price_changes']['prev_1m']:
            additional_ratios['change_1m'] = ((current_price - price_data['price_changes']['prev_1m']) / price_data['price_changes']['prev_1m']) * 100
        
        # 시가총액 계산 (시총이 없는 경우)
        if not market_cap and eps and per:
            shares_outstanding = financial_data.get('shares_outstanding', 0)
            if shares_outstanding:
                calculated_market_cap = current_price * shares_outstanding
                additional_ratios['calculated_market_cap'] = calculated_market_cap
        
        return additional_ratios
        
    except Exception as e:
        print(f"❌ 추가 비율 계산 실패: {e}")
        return {}

def format_value(value, value_type='number'):
    """값 포맷팅"""
    if value is None or value == '':
        return 'N/A'
    
    try:
        if value_type == 'price':
            return f"{float(value):,.0f}원"
        elif value_type == 'percentage':
            return f"{float(value):.2f}%"
        elif value_type == 'ratio':
            return f"{float(value):.2f}"
        elif value_type == 'volume':
            return f"{int(value):,}"
        elif value_type == 'market_cap':
            return f"{float(value)/1000000000000:.1f}조원"
        else:
            return f"{float(value):,.2f}"
    except:
        return str(value)

def display_stock_detail(stock_code: str):
    """종목 상세 정보 표시"""
    print("=" * 100)
    print(f"📊 종목 상세 분석: {stock_code}")
    print("=" * 100)
    
    # 1. Financial Ratios 테이블 찾기
    financial_tables = find_financial_tables()
    
    if not financial_tables:
        print("❌ Financial Ratios 테이블을 찾을 수 없습니다.")
        return
    
    print(f"🗃️ 사용 가능한 테이블: {', '.join(financial_tables)}")
    
    # 2. 각 테이블에서 데이터 찾기
    financial_data = None
    used_table = None
    
    for table in financial_tables:
        data = get_stock_financial_data(stock_code, table)
        if data:
            financial_data = data
            used_table = table
            break
    
    if not financial_data:
        print(f"❌ 종목 {stock_code}의 Financial Ratios 데이터를 찾을 수 없습니다.")
        print(f"💡 데이터 수집을 위해 다음을 실행하세요:")
        print(f"   python market_data_calculator_real.py --mode single --stock_code {stock_code}")
        return
    
    print(f"✅ 데이터 출처: {used_table} 테이블")
    
    # 3. 주가 데이터 조회
    price_data = get_stock_price_data(stock_code)
    
    # 4. 기본 정보 표시
    latest = financial_data['latest_data']
    
    print(f"\n📋 기본 정보")
    print("-" * 60)
    print(f"종목코드: {latest.get('stock_code', 'N/A')}")
    print(f"회사명: {latest.get('company_name', 'N/A')}")
    print(f"업종: {latest.get('sector', 'N/A')}")
    print(f"시장: {latest.get('market', 'N/A')}")
    print(f"기준연도: {latest.get('year', 'N/A')}년 {latest.get('quarter', 'N/A')}분기")
    print(f"마지막 업데이트: {latest.get('updated_at', 'N/A')}")
    
    # 5. 주가 정보
    print(f"\n📈 주가 정보")
    print("-" * 60)
    
    if price_data:
        price_info = price_data['latest_price']
        print(f"기준일: {price_info['date']}")
        print(f"현재가: {format_value(price_info['close'], 'price')}")
        print(f"시가: {format_value(price_info['open'], 'price')}")
        print(f"고가: {format_value(price_info['high'], 'price')}")
        print(f"저가: {format_value(price_info['low'], 'price')}")
        print(f"거래량: {format_value(price_info['volume'], 'volume')}")
        print(f"거래대금: {format_value(price_info['amount'], 'price')}")
        
        # 52주 고저점
        if price_data['week52']['high']:
            print(f"52주 최고가: {format_value(price_data['week52']['high'], 'price')}")
        if price_data['week52']['low']:
            print(f"52주 최저가: {format_value(price_data['week52']['low'], 'price')}")
    else:
        print(f"현재가: {format_value(latest.get('current_price'), 'price')}")
        print("⚠️ 상세 주가 정보를 조회할 수 없습니다.")
    
    # 6. 재무비율 정보
    print(f"\n💰 재무비율")
    print("-" * 60)
    
    # 핵심 비율들
    core_ratios = [
        ('PER (주가수익비율)', 'per', 'ratio'),
        ('PBR (주가순자산비율)', 'pbr', 'ratio'),
        ('EPS (주당순이익)', 'eps', 'price'),
        ('BPS (주당순자산)', 'bps', 'price'),
        ('배당수익률', 'dividend_yield', 'percentage'),
        ('시가총액', 'market_cap', 'market_cap'),
        ('발행주식수', 'shares_outstanding', 'volume')
    ]
    
    for name, key, format_type in core_ratios:
        value = latest.get(key)
        print(f"{name}: {format_value(value, format_type)}")
    
    # 7. 주가 변동률
    print(f"\n📊 주가 변동률")
    print("-" * 60)
    
    if price_data:
        additional_ratios = calculate_additional_ratios(latest, price_data)
        
        change_items = [
            ('1일 변동률', 'change_1d', 'percentage'),
            ('1주 변동률', 'change_1w', 'percentage'),
            ('1개월 변동률', 'change_1m', 'percentage'),
            ('52주 고점 대비', 'week52_high_ratio', 'percentage'),
            ('52주 저점 대비', 'week52_low_ratio', 'percentage')
        ]
        
        for name, key, format_type in change_items:
            value = additional_ratios.get(key)
            if value is not None:
                print(f"{name}: {format_value(value, format_type)}")
    else:
        # financial_ratios 테이블에서 변동률 정보 확인
        change_columns = ['price_change_1d', 'price_change_1w', 'price_change_1m', 'week52_high_ratio', 'week52_low_ratio']
        for col in change_columns:
            value = latest.get(col)
            if value is not None:
                print(f"{col.replace('_', ' ').title()}: {format_value(value * 100, 'percentage')}")
    
    # 8. 기타 재무정보 (테이블에 있는 모든 데이터)
    print(f"\n📋 상세 재무정보")
    print("-" * 60)
    
    # 표시하지 않을 컬럼들
    skip_columns = ['id', 'stock_code', 'company_name', 'year', 'quarter', 'updated_at', 'calculated_at', 'data_source']
    skip_columns.extend(['per', 'pbr', 'eps', 'bps', 'dividend_yield', 'market_cap', 'shares_outstanding', 'current_price'])
    
    other_data = {}
    for key, value in latest.items():
        if key not in skip_columns and value is not None and value != '':
            other_data[key] = value
    
    if other_data:
        for key, value in sorted(other_data.items()):
            display_name = key.replace('_', ' ').title()
            
            # 값 타입에 따른 포맷팅
            if 'ratio' in key.lower() or key.endswith('_ratio'):
                formatted_value = format_value(value, 'ratio')
            elif 'price' in key.lower() or 'amount' in key.lower():
                formatted_value = format_value(value, 'price')
            elif 'volume' in key.lower():
                formatted_value = format_value(value, 'volume')
            elif 'yield' in key.lower() or 'margin' in key.lower() or 'change' in key.lower():
                formatted_value = format_value(value * 100, 'percentage')
            else:
                formatted_value = format_value(value)
            
            print(f"{display_name}: {formatted_value}")
    
    # 9. 데이터 출처 정보
    if 'per_source' in latest or 'pbr_source' in latest:
        print(f"\n📊 데이터 출처")
        print("-" * 60)
        
        if 'per_source' in latest:
            print(f"PER 출처: {latest['per_source']}")
        if 'pbr_source' in latest:
            print(f"PBR 출처: {latest['pbr_source']}")
    
    # 10. 여러 레코드가 있는 경우 히스토리 표시
    if len(financial_data['all_records']) > 1:
        print(f"\n📅 과거 데이터 히스토리 ({len(financial_data['all_records'])}개 레코드)")
        print("-" * 60)
        
        for i, record in enumerate(financial_data['all_records'][:5]):  # 최대 5개만
            print(f"{i+1}. {record.get('year', 'N/A')}년 {record.get('quarter', 'N/A')}분기:")
            print(f"   PER: {format_value(record.get('per'), 'ratio')}, "
                  f"PBR: {format_value(record.get('pbr'), 'ratio')}, "
                  f"가격: {format_value(record.get('current_price'), 'price')}")
        
        if len(financial_data['all_records']) > 5:
            print(f"   ... 외 {len(financial_data['all_records']) - 5}개 더")
    
    print(f"\n" + "=" * 100)
    print(f"✅ {stock_code} 상세 분석 완료")
    print(f"💡 추가 분석을 원하면 다른 종목코드로 실행하세요.")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='특정 종목의 Financial Ratios 상세 정보 표시')
    parser.add_argument('--stock_code', type=str, default='000660', 
                       help='분석할 종목코드 (기본값: 000660 SK하이닉스)')
    
    args = parser.parse_args()
    
    try:
        display_stock_detail(args.stock_code)
    except Exception as e:
        print(f"❌ 프로그램 실행 실패: {e}")

if __name__ == "__main__":
    main()
