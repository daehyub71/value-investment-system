"""
데이터베이스 설정 파일
SQLite 및 향후 확장을 위한 데이터베이스 설정 관리
"""

import os
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
from datetime import datetime

# 환경변수 로드
load_dotenv()

class DatabaseConfig:
    """데이터베이스 설정 관리 클래스"""
    
    def __init__(self):
        self.base_path = Path(os.getenv('DB_PATH', 'data/databases/'))
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # 데이터베이스 파일 설정
        self.databases = {
            'stock': {
                'name': os.getenv('STOCK_DB_NAME', 'stock_data.db'),
                'path': self.base_path / os.getenv('STOCK_DB_NAME', 'stock_data.db'),
                'description': '주식 데이터 저장소',
                'tables': ['stock_prices', 'company_info', 'financial_ratios', 'technical_indicators']
            },
            'dart': {
                'name': os.getenv('DART_DB_NAME', 'dart_data.db'),
                'path': self.base_path / os.getenv('DART_DB_NAME', 'dart_data.db'),
                'description': 'DART 공시 데이터 저장소',
                'tables': ['corp_codes', 'financial_statements', 'disclosures', 'company_outlines']
            },
            'news': {
                'name': os.getenv('NEWS_DB_NAME', 'news_data.db'),
                'path': self.base_path / os.getenv('NEWS_DB_NAME', 'news_data.db'),
                'description': '뉴스 및 감정분석 데이터 저장소',
                'tables': ['news_articles', 'sentiment_scores', 'market_sentiment']
            },
            'kis': {
                'name': os.getenv('KIS_DB_NAME', 'kis_data.db'),
                'path': self.base_path / os.getenv('KIS_DB_NAME', 'kis_data.db'),
                'description': 'KIS API 데이터 저장소',
                'tables': ['realtime_quotes', 'account_balance', 'order_history', 'market_indicators']
            }
        }
        
        # 공통 설정
        self.common_config = {
            'connection_timeout': 30,
            'pragma_settings': {
                'journal_mode': 'WAL',
                'synchronous': 'NORMAL',
                'cache_size': -64000,  # 64MB 캐시
                'temp_store': 'memory',
                'mmap_size': 268435456,  # 256MB 메모리 맵
                'foreign_keys': 'ON'
            },
            'backup_enabled': True,
            'backup_interval': 86400,  # 24시간
            'backup_retention': 7,     # 7일간 보관
            'auto_vacuum': 'incremental',
            'page_size': 4096
        }
        
        # 테이블 스키마 정의
        self.table_schemas = self._define_table_schemas()
    
    def _define_table_schemas(self) -> Dict[str, Dict[str, str]]:
        """테이블 스키마 정의"""
        return {
            # 주식 데이터 테이블들
            'stock_prices': '''
                CREATE TABLE IF NOT EXISTS stock_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    close_price REAL,
                    volume INTEGER,
                    amount INTEGER,
                    adjusted_close REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(stock_code, date)
                )
            ''',
            
            'company_info': '''
                CREATE TABLE IF NOT EXISTS company_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT UNIQUE NOT NULL,
                    company_name TEXT NOT NULL,
                    market_type TEXT,
                    sector TEXT,
                    industry TEXT,
                    listing_date TEXT,
                    market_cap INTEGER,
                    shares_outstanding INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            
            'financial_ratios': '''
                CREATE TABLE IF NOT EXISTS financial_ratios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    quarter INTEGER,
                    revenue REAL,
                    net_income REAL,
                    total_assets REAL,
                    total_equity REAL,
                    total_debt REAL,
                    roe REAL,
                    roa REAL,
                    debt_ratio REAL,
                    current_ratio REAL,
                    quick_ratio REAL,
                    per REAL,
                    pbr REAL,
                    eps REAL,
                    bps REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(stock_code, year, quarter)
                )
            ''',
            
            'technical_indicators': '''
                CREATE TABLE IF NOT EXISTS technical_indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    date TEXT NOT NULL,
                    sma_20 REAL,
                    sma_60 REAL,
                    sma_120 REAL,
                    sma_200 REAL,
                    ema_12 REAL,
                    ema_26 REAL,
                    rsi REAL,
                    macd REAL,
                    macd_signal REAL,
                    macd_histogram REAL,
                    bollinger_upper REAL,
                    bollinger_lower REAL,
                    volume_sma_20 REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(stock_code, date)
                )
            ''',
            
            # DART 데이터 테이블들
            'corp_codes': '''
                CREATE TABLE IF NOT EXISTS corp_codes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    corp_code TEXT UNIQUE NOT NULL,
                    corp_name TEXT NOT NULL,
                    stock_code TEXT,
                    modify_date TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            
            'financial_statements': '''
                CREATE TABLE IF NOT EXISTS financial_statements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    corp_code TEXT NOT NULL,
                    bsns_year INTEGER NOT NULL,
                    reprt_code TEXT NOT NULL,
                    fs_div TEXT,
                    fs_nm TEXT,
                    account_nm TEXT,
                    thstrm_amount REAL,
                    frmtrm_amount REAL,
                    bfefrmtrm_amount REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(corp_code, bsns_year, reprt_code, fs_div, account_nm)
                )
            ''',
            
            'disclosures': '''
                CREATE TABLE IF NOT EXISTS disclosures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    corp_code TEXT NOT NULL,
                    corp_name TEXT,
                    stock_code TEXT,
                    report_nm TEXT,
                    rcept_no TEXT UNIQUE,
                    flr_nm TEXT,
                    rcept_dt TEXT,
                    rm TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            
            # 뉴스 데이터 테이블들
            'news_articles': '''
                CREATE TABLE IF NOT EXISTS news_articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    originallink TEXT,
                    link TEXT UNIQUE,
                    description TEXT,
                    pubDate TEXT,
                    stock_code TEXT,
                    company_name TEXT,
                    category TEXT,
                    source TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            
            'sentiment_scores': '''
                CREATE TABLE IF NOT EXISTS sentiment_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_id INTEGER,
                    stock_code TEXT,
                    sentiment_score REAL,
                    positive_score REAL,
                    negative_score REAL,
                    neutral_score REAL,
                    confidence REAL,
                    keywords TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (news_id) REFERENCES news_articles (id)
                )
            ''',
            
            'market_sentiment': '''
                CREATE TABLE IF NOT EXISTS market_sentiment (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    stock_code TEXT,
                    daily_sentiment REAL,
                    weekly_sentiment REAL,
                    monthly_sentiment REAL,
                    news_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, stock_code)
                )
            ''',
            
            # KIS 데이터 테이블들
            'realtime_quotes': '''
                CREATE TABLE IF NOT EXISTS realtime_quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    current_price REAL,
                    change_price REAL,
                    change_rate REAL,
                    volume INTEGER,
                    amount INTEGER,
                    bid_price REAL,
                    ask_price REAL,
                    market_cap INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            
            'account_balance': '''
                CREATE TABLE IF NOT EXISTS account_balance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT,
                    stock_name TEXT,
                    quantity INTEGER,
                    avg_price REAL,
                    current_price REAL,
                    eval_amount INTEGER,
                    profit_loss INTEGER,
                    profit_loss_rate REAL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            
            'order_history': '''
                CREATE TABLE IF NOT EXISTS order_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_no TEXT UNIQUE,
                    stock_code TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    order_condition TEXT NOT NULL,
                    quantity INTEGER,
                    price REAL,
                    order_status TEXT,
                    order_time TIMESTAMP,
                    execution_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            
            'market_indicators': '''
                CREATE TABLE IF NOT EXISTS market_indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    index_name TEXT NOT NULL,
                    index_value REAL,
                    change_value REAL,
                    change_rate REAL,
                    volume INTEGER,
                    date TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(index_name, date)
                )
            '''
        }
    
    def get_connection(self, db_name: str) -> sqlite3.Connection:
        """데이터베이스 연결 반환"""
        if db_name not in self.databases:
            raise ValueError(f"지원하지 않는 데이터베이스: {db_name}")
        
        db_path = self.databases[db_name]['path']
        conn = sqlite3.connect(
            str(db_path),
            timeout=self.common_config['connection_timeout']
        )
        
        # PRAGMA 설정 적용
        for pragma, value in self.common_config['pragma_settings'].items():
            conn.execute(f"PRAGMA {pragma} = {value}")
        
        # Row factory 설정 (딕셔너리 형태로 결과 반환)
        conn.row_factory = sqlite3.Row
        
        return conn
    
    def create_database(self, db_name: str) -> bool:
        """데이터베이스 및 테이블 생성"""
        if db_name not in self.databases:
            raise ValueError(f"지원하지 않는 데이터베이스: {db_name}")
        
        try:
            with self.get_connection(db_name) as conn:
                # 데이터베이스 테이블들 생성
                for table_name in self.databases[db_name]['tables']:
                    if table_name in self.table_schemas:
                        conn.execute(self.table_schemas[table_name])
                
                # 인덱스 생성
                self._create_indexes(conn, db_name)
                
                conn.commit()
                return True
        except Exception as e:
            print(f"데이터베이스 생성 실패 ({db_name}): {e}")
            return False
    
    def _create_indexes(self, conn: sqlite3.Connection, db_name: str):
        """인덱스 생성"""
        index_queries = {
            'stock': [
                'CREATE INDEX IF NOT EXISTS idx_stock_prices_code_date ON stock_prices(stock_code, date)',
                'CREATE INDEX IF NOT EXISTS idx_company_info_code ON company_info(stock_code)',
                'CREATE INDEX IF NOT EXISTS idx_financial_ratios_code_year ON financial_ratios(stock_code, year)',
                'CREATE INDEX IF NOT EXISTS idx_technical_indicators_code_date ON technical_indicators(stock_code, date)'
            ],
            'dart': [
                'CREATE INDEX IF NOT EXISTS idx_corp_codes_code ON corp_codes(corp_code)',
                'CREATE INDEX IF NOT EXISTS idx_corp_codes_stock ON corp_codes(stock_code)',
                'CREATE INDEX IF NOT EXISTS idx_financial_statements_corp_year ON financial_statements(corp_code, bsns_year)',
                'CREATE INDEX IF NOT EXISTS idx_disclosures_corp_date ON disclosures(corp_code, rcept_dt)'
            ],
            'news': [
                'CREATE INDEX IF NOT EXISTS idx_news_articles_stock_date ON news_articles(stock_code, pubDate)',
                'CREATE INDEX IF NOT EXISTS idx_news_articles_source ON news_articles(source)',
                'CREATE INDEX IF NOT EXISTS idx_sentiment_scores_stock ON sentiment_scores(stock_code)',
                'CREATE INDEX IF NOT EXISTS idx_market_sentiment_date_stock ON market_sentiment(date, stock_code)'
            ],
            'kis': [
                'CREATE INDEX IF NOT EXISTS idx_realtime_quotes_stock_time ON realtime_quotes(stock_code, timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_account_balance_stock ON account_balance(stock_code)',
                'CREATE INDEX IF NOT EXISTS idx_order_history_stock ON order_history(stock_code)',
                'CREATE INDEX IF NOT EXISTS idx_market_indicators_name_date ON market_indicators(index_name, date)'
            ]
        }
        
        if db_name in index_queries:
            for query in index_queries[db_name]:
                conn.execute(query)
    
    def create_all_databases(self) -> Dict[str, bool]:
        """모든 데이터베이스 생성"""
        results = {}
        for db_name in self.databases.keys():
            results[db_name] = self.create_database(db_name)
        return results
    
    def backup_database(self, db_name: str, backup_path: Optional[Path] = None) -> bool:
        """데이터베이스 백업"""
        if db_name not in self.databases:
            raise ValueError(f"지원하지 않는 데이터베이스: {db_name}")
        
        source_path = self.databases[db_name]['path']
        
        if backup_path is None:
            backup_dir = self.base_path / 'backups'
            backup_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = backup_dir / f"{db_name}_{timestamp}.db"
        
        try:
            # SQLite 백업 사용
            with sqlite3.connect(str(source_path)) as source:
                with sqlite3.connect(str(backup_path)) as backup:
                    source.backup(backup)
            return True
        except Exception as e:
            print(f"백업 실패 ({db_name}): {e}")
            return False
    
    def get_database_info(self, db_name: str) -> Dict[str, Any]:
        """데이터베이스 정보 반환"""
        if db_name not in self.databases:
            raise ValueError(f"지원하지 않는 데이터베이스: {db_name}")
        
        db_path = self.databases[db_name]['path']
        
        info = {
            'name': db_name,
            'path': str(db_path),
            'exists': db_path.exists(),
            'size': db_path.stat().st_size if db_path.exists() else 0,
            'tables': [],
            'total_records': 0
        }
        
        if db_path.exists():
            try:
                with self.get_connection(db_name) as conn:
                    # 테이블 목록 조회
                    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    # 각 테이블의 레코드 수 조회
                    table_info = []
                    total_records = 0
                    
                    for table in tables:
                        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        table_info.append({
                            'name': table,
                            'records': count
                        })
                        total_records += count
                    
                    info['tables'] = table_info
                    info['total_records'] = total_records
                    
            except Exception as e:
                info['error'] = str(e)
        
        return info
    
    def get_all_database_info(self) -> Dict[str, Any]:
        """모든 데이터베이스 정보 반환"""
        return {db_name: self.get_database_info(db_name) for db_name in self.databases.keys()}
    
    def optimize_database(self, db_name: str) -> bool:
        """데이터베이스 최적화"""
        if db_name not in self.databases:
            raise ValueError(f"지원하지 않는 데이터베이스: {db_name}")
        
        try:
            with self.get_connection(db_name) as conn:
                # VACUUM 실행
                conn.execute("VACUUM")
                # ANALYZE 실행
                conn.execute("ANALYZE")
                conn.commit()
                return True
        except Exception as e:
            print(f"데이터베이스 최적화 실패 ({db_name}): {e}")
            return False
    
    def cleanup_old_data(self, db_name: str, table_name: str, 
                        date_column: str, days_to_keep: int = 30) -> int:
        """오래된 데이터 정리"""
        if db_name not in self.databases:
            raise ValueError(f"지원하지 않는 데이터베이스: {db_name}")
        
        try:
            with self.get_connection(db_name) as conn:
                # days_to_keep 이전의 데이터 삭제
                cursor = conn.execute(f'''
                    DELETE FROM {table_name} 
                    WHERE {date_column} < date('now', '-{days_to_keep} days')
                ''')
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                return deleted_count
        except Exception as e:
            print(f"데이터 정리 실패 ({db_name}.{table_name}): {e}")
            return 0

# 글로벌 데이터베이스 설정 인스턴스
database_config = DatabaseConfig()

# 편의 함수들
def get_db_connection(db_name: str) -> sqlite3.Connection:
    """데이터베이스 연결 반환"""
    return database_config.get_connection(db_name)

def get_database_path(db_name: str) -> Path:
    """데이터베이스 경로 반환"""
    return database_config.databases[db_name]['path']

def create_all_databases() -> Dict[str, bool]:
    """모든 데이터베이스 생성"""
    return database_config.create_all_databases()

def get_database_info(db_name: str = None) -> Dict[str, Any]:
    """데이터베이스 정보 반환"""
    if db_name:
        return database_config.get_database_info(db_name)
    else:
        return database_config.get_all_database_info()

# 사용 예시
if __name__ == "__main__":
    print("💾 데이터베이스 설정 및 초기화")
    print("=" * 50)
    
    # 모든 데이터베이스 생성
    results = create_all_databases()
    
    for db_name, success in results.items():
        status = "✅ 성공" if success else "❌ 실패"
        print(f"{db_name}: {status}")
    
    print("\n📊 데이터베이스 정보:")
    all_info = get_database_info()
    
    for db_name, info in all_info.items():
        print(f"\n{db_name}:")
        print(f"  - 경로: {info['path']}")
        print(f"  - 존재: {'✅' if info['exists'] else '❌'}")
        print(f"  - 크기: {info['size']:,} bytes")
        print(f"  - 총 레코드: {info['total_records']:,}")
        
        if info['tables']:
            print("  - 테이블:")
            for table in info['tables']:
                print(f"    * {table['name']}: {table['records']:,} 레코드")