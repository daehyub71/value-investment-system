#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 데이터베이스 상태 점검 도구 (의존성 없는 버전)
4개 데이터베이스의 모든 테이블 데이터 수집 현황을 종합 분석
"""

import sqlite3
import os
from datetime import datetime, timedelta
from pathlib import Path

def format_size(size_bytes):
    """파일 크기를 읽기 쉬운 형태로 변환"""
    if size_bytes == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB']
    unit_index = 0
    size = float(size_bytes)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.1f} {units[unit_index]}"

class DatabaseStatusChecker:
    def __init__(self, db_base_path="data/databases"):
        """초기화"""
        self.db_base_path = Path(db_base_path)
        
        # 데이터베이스 설정
        self.databases = {
            'stock_data.db': {
                'name': '주식 데이터',
                'expected_tables': ['stock_prices', 'company_info', 'financial_ratios', 'technical_indicators'],
                'description': '주가, 기업정보, 재무비율, 기술지표'
            },
            'dart_data.db': {
                'name': 'DART 공시 데이터', 
                'expected_tables': ['corp_codes', 'financial_statements', 'disclosures', 'company_outlines'],
                'description': '기업코드, 재무제표, 공시정보, 기업개요'
            },
            'news_data.db': {
                'name': '뉴스 감정분석',
                'expected_tables': ['news_articles', 'sentiment_scores', 'market_sentiment'],
                'description': '뉴스기사, 감정점수, 시장감정'
            },
            'kis_data.db': {
                'name': 'KIS API 데이터',
                'expected_tables': ['realtime_quotes', 'account_balance', 'order_history', 'market_indicators'],
                'description': '실시간 시세, 계좌잔고, 주문내역, 시장지표'
            }
        }
        
        self.results = {}
    
    def get_file_info(self, db_file):
        """데이터베이스 파일 정보 조회"""
        db_path = self.db_base_path / db_file
        
        if not db_path.exists():
            return {
                'exists': False,
                'size': 0,
                'size_human': 'N/A',
                'modified': 'N/A'
            }
        
        stat = db_path.stat()
        return {
            'exists': True,
            'size': stat.st_size,
            'size_human': format_size(stat.st_size),
            'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M')
        }
    
    def get_table_info(self, db_file):
        """데이터베이스 내 테이블 정보 조회"""
        db_path = self.db_base_path / db_file
        
        if not db_path.exists():
            return {}
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 모든 테이블 목록 조회
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            table_info = {}
            
            for table in tables:
                try:
                    # 레코드 수 조회
                    cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                    count = cursor.fetchone()[0]
                    
                    # 테이블 스키마 정보
                    cursor.execute(f"PRAGMA table_info([{table}])")
                    columns = cursor.fetchall()
                    
                    # 최신 데이터 조회 (created_at 또는 updated_at 컬럼이 있는 경우)
                    latest_data = None
                    date_columns = ['created_at', 'updated_at', 'date', 'pubDate', 'rcept_dt']
                    
                    for date_col in date_columns:
                        try:
                            cursor.execute(f"SELECT MAX([{date_col}]) FROM [{table}] WHERE [{date_col}] IS NOT NULL")
                            result = cursor.fetchone()
                            if result and result[0]:
                                latest_data = result[0]
                                break
                        except:
                            continue
                    
                    # 데이터 샘플 (처음 3개 레코드)
                    cursor.execute(f"SELECT * FROM [{table}] LIMIT 3")
                    sample_data = cursor.fetchall()
                    
                    table_info[table] = {
                        'count': count,
                        'columns': len(columns),
                        'column_names': [col[1] for col in columns],
                        'latest_data': latest_data,
                        'sample_data': sample_data
                    }
                    
                except Exception as e:
                    table_info[table] = {
                        'count': 0,
                        'error': str(e),
                        'columns': 0,
                        'column_names': [],
                        'latest_data': None,
                        'sample_data': []
                    }
            
            conn.close()
            return table_info
            
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_data_freshness(self, latest_data):
        """데이터 신선도 분석"""
        if not latest_data:
            return "❓ 알 수 없음", "N/A"
        
        try:
            # 다양한 날짜 형식 처리
            if isinstance(latest_data, str):
                # ISO 형식 또는 기타 형식 파싱
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y%m%d', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        # 문자열 길이에 맞게 형식 조정
                        date_str = latest_data[:19] if len(latest_data) > 19 else latest_data
                        if 'T' in date_str:
                            latest_dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
                        elif len(date_str) == 8:  # YYYYMMDD
                            latest_dt = datetime.strptime(date_str, '%Y%m%d')
                        elif len(date_str) == 10:  # YYYY-MM-DD
                            latest_dt = datetime.strptime(date_str, '%Y-%m-%d')
                        elif len(date_str) >= 19:  # YYYY-MM-DD HH:MM:SS
                            latest_dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                        else:
                            continue
                        break
                    except:
                        continue
                else:
                    return "❓ 형식 오류", latest_data
            else:
                return "❓ 타입 오류", str(latest_data)
            
            now = datetime.now()
            diff = now - latest_dt
            
            if diff.days == 0:
                return "🟢 최신", latest_data
            elif diff.days <= 1:
                return "🟡 1일 전", latest_data
            elif diff.days <= 7:
                return f"🟠 {diff.days}일 전", latest_data
            else:
                return f"🔴 {diff.days}일 전", latest_data
                
        except Exception as e:
            return "❓ 파싱 오류", latest_data
    
    def get_database_health_score(self, db_info):
        """데이터베이스 건강도 점수 계산"""
        if not db_info.get('file_info', {}).get('exists'):
            return 0, "파일 없음", ["