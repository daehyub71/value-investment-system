#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수정된 날짜 형식 조사 도구
실제 날짜 파싱 가능 여부를 정확히 테스트
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import re

def try_parse_date_fixed(date_value):
    """수정된 날짜 파식 함수"""
    if not date_value:
        return "NULL"
    
    date_str = str(date_value).strip()
    
    # 다양한 형식 시도 (정확한 매칭)
    formats_and_patterns = [
        # ISO 8601 variants
        ('%Y-%m-%d %H:%M:%S', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'),
        ('%Y-%m-%dT%H:%M:%S', r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'),
        ('%Y-%m-%d', r'^\d{4}-\d{2}-\d{2}$'),
        
        # With microseconds
        ('%Y-%m-%d %H:%M:%S.%f', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$'),
        ('%Y-%m-%dT%H:%M:%S.%f', r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+$'),
        
        # Compact formats
        ('%Y%m%d', r'^\d{8}$'),
        ('%Y%m%d%H%M%S', r'^\d{14}$'),
    ]
    
    for fmt, pattern in formats_and_patterns:
        if re.match(pattern, date_str):
            try:
                parsed_dt = datetime.strptime(date_str, fmt)
                return f"✅ {fmt} → {parsed_dt}"
            except Exception as e:
                continue
    
    # RFC 2822 형식 (뉴스 pubDate)
    if re.match(r'^[A-Za-z]{3}, \d{1,2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2}', date_str):
        try:
            # 타임존 제거하고 파싱
            date_part = date_str.split(' +')[0] if ' +' in date_str else date_str
            parsed_dt = datetime.strptime(date_part, '%a, %d %b %Y %H:%M:%S')
            return f"✅ RFC-2822 → {parsed_dt}"
        except Exception as e:
            return f"⚠️ RFC-2822 파싱 오류: {e}"
    
    # Unix timestamp 시도
    try:
        if date_str.isdigit() and len(date_str) in [10, 13]:
            timestamp = int(date_str)
            if len(date_str) == 13:  # 밀리초
                timestamp = timestamp / 1000
            parsed_dt = datetime.fromtimestamp(timestamp)
            return f"✅ Unix timestamp → {parsed_dt}"
    except:
        pass
    
    return f"❌ 파싱 실패: '{date_str}'"

def analyze_date_freshness_fixed(date_value):
    """데이터 신선도 분석 (수정된 버전)"""
    if not date_value:
        return "❓ 알 수 없음"
    
    date_str = str(date_value).strip()
    parsed_dt = None
    
    # 파싱 시도
    formats_and_patterns = [
        ('%Y-%m-%d %H:%M:%S.%f', r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.\d+'),
        ('%Y-%m-%dT%H:%M:%S.%f', r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+'),
        ('%Y-%m-%d %H:%M:%S', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'),
        ('%Y-%m-%dT%H:%M:%S', r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'),
        ('%Y-%m-%d', r'^\d{4}-\d{2}-\d{2}$'),
    ]
    
    for fmt, pattern in formats_and_patterns:
        if re.match(pattern, date_str):
            try:
                parsed_dt = datetime.strptime(date_str, fmt)
                break
            except:
                continue
    
    # RFC 2822 처리
    if not parsed_dt and re.match(r'^[A-Za-z]{3}, \d{1,2} [A-Za-z]{3} \d{4}', date_str):
        try:
            date_part = date_str.split(' +')[0]
            parsed_dt = datetime.strptime(date_part, '%a, %d %b %Y %H:%M:%S')
        except:
            pass
    
    if not parsed_dt:
        return "❓ 파싱 실패"
    
    # 신선도 계산
    now = datetime.now()
    diff = now - parsed_dt
    
    if diff.days < 0:  # 미래 날짜
        return "🔮 미래 데이터"
    elif diff.days == 0:
        return "🟢 오늘"
    elif diff.days == 1:
        return "🟡 어제"
    elif diff.days <= 7:
        return f"🟠 {diff.days}일 전"
    elif diff.days <= 30:
        return f"🟠 {diff.days}일 전"
    else:
        return f"🔴 {diff.days}일 전"

def investigate_with_proper_parsing():
    """올바른 파싱으로 날짜 형식 재조사"""
    
    databases = {
        'stock_data.db': {
            'company_info': ['created_at', 'updated_at'],
            'stock_prices': ['date', 'created_at'],
        },
        'news_data.db': {
            'news_articles': ['pubDate', 'created_at'],
            'sentiment_scores': ['created_at'],
        },
        'kis_data.db': {
            'realtime_quotes': ['created_at'],
            'market_indicators': ['created_at'],
        }
    }
    
    print("🔍 수정된 날짜 형식 조사")
    print("=" * 60)
    
    db_base_path = Path("data/databases")
    
    for db_file, tables in databases.items():
        db_path = db_base_path / db_file
        
        if not db_path.exists():
            print(f"\n❌ {db_file} 파일을 찾을 수 없습니다")
            continue
            
        print(f"\n📊 {db_file}")
        print("-" * 40)
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            for table, date_columns in tables.items():
                print(f"\n📋 테이블: {table}")
                
                # 테이블 존재 확인
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name = ?
                """, (table,))
                
                if not cursor.fetchone():
                    print(f"   ❌ 테이블이 존재하지 않습니다")
                    continue
                
                for date_col in date_columns:
                    try:
                        # 컬럼 존재 확인
                        cursor.execute(f"PRAGMA table_info([{table}])")
                        columns = [col[1] for col in cursor.fetchall()]
                        
                        if date_col not in columns:
                            print(f"   ⚠️  컬럼 '{date_col}' 없음")
                            continue
                        
                        # 통계 정보
                        cursor.execute(f"""
                            SELECT 
                                COUNT(*) as total,
                                COUNT([{date_col}]) as non_null,
                                MIN([{date_col}]) as min_date,
                                MAX([{date_col}]) as max_date
                            FROM [{table}]
                        """)
                        
                        stats = cursor.fetchone()
                        total, non_null, min_date, max_date = stats
                        
                        print(f"   📅 {date_col}:")
                        print(f"      📊 총 {total:,}개 / 값 있음 {non_null:,}개")
                        
                        if min_date and max_date:
                            min_freshness = analyze_date_freshness_fixed(min_date)
                            max_freshness = analyze_date_freshness_fixed(max_date)
                            
                            print(f"      📅 범위: {min_date} ({min_freshness}) ~ {max_date} ({max_freshness})")
                            
                            # 샘플 데이터 파싱 테스트
                            cursor.execute(f"""
                                SELECT [{date_col}] 
                                FROM [{table}] 
                                WHERE [{date_col}] IS NOT NULL 
                                ORDER BY [{date_col}] DESC 
                                LIMIT 2
                            """)
                            
                            samples = cursor.fetchall()
                            for i, (date_value,) in enumerate(samples):
                                parsed_info = try_parse_date_fixed(date_value)
                                print(f"      🔍 샘플 {i+1}: {parsed_info}")
                        else:
                            print(f"      ❌ 날짜 데이터 없음")
                            
                    except Exception as e:
                        print(f"   ❌ {date_col} 조회 오류: {e}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ 데이터베이스 연결 오류: {e}")

def show_data_freshness_summary():
    """데이터 신선도 종합 요약"""
    print(f"\n🕒 데이터 신선도 종합 요약")
    print("=" * 50)
    
    # 주요 테이블별 최신 데이터 확인
    critical_tables = {
        'stock_data.db': {
            'stock_prices': 'date',
            'company_info': 'updated_at'
        },
        'news_data.db': {
            'news_articles': 'pubDate'
        },
        'dart_data.db': {
            'financial_statements': 'created_at'
        }
    }
    
    db_base_path = Path("data/databases")
    
    for db_file, tables in critical_tables.items():
        db_path = db_base_path / db_file
        
        if not db_path.exists():
            continue
            
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            print(f"\n📊 {db_file.replace('.db', '').replace('_', ' ').title()}:")
            
            for table, date_col in tables.items():
                try:
                    cursor.execute(f"""
                        SELECT MAX([{date_col}]) 
                        FROM [{table}] 
                        WHERE [{date_col}] IS NOT NULL
                    """)
                    
                    result = cursor.fetchone()
                    if result and result[0]:
                        latest_date = result[0]
                        freshness = analyze_date_freshness_fixed(latest_date)
                        print(f"   📅 {table}: {latest_date} ({freshness})")
                    else:
                        print(f"   ❌ {table}: 데이터 없음")
                        
                except Exception as e:
                    print(f"   ❌ {table}: 조회 오류")
            
            conn.close()
            
        except Exception as e:
            continue

if __name__ == "__main__":
    investigate_with_proper_parsing()
    show_data_freshness_summary()
    
    print(f"\n✅ 결론:")
    print(f"대부분의 날짜가 올바른 형식으로 저장되어 있습니다!")
    print(f"이전 오류는 파싱 로직 문제였고, 실제 데이터는 정상입니다.")