#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
데이터베이스 스키마 마이그레이션 스크립트
투자 가능 여부 관련 필드 및 테이블 추가
"""

import sqlite3
import logging
from datetime import datetime
import os

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseMigrator:
    """데이터베이스 마이그레이션 클래스"""
    
    def __init__(self, db_path: str = "data/databases/buffett_scorecard.db"):
        self.db_path = db_path
        self.migration_version = "v1.1.0_investment_status"
        
    def backup_database(self):
        """데이터베이스 백업"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = "data/backups"
            os.makedirs(backup_dir, exist_ok=True)
            
            backup_path = f"{backup_dir}/buffett_scorecard_backup_{timestamp}.db"
            
            # 원본 데이터베이스를 백업
            import shutil
            shutil.copy2(self.db_path, backup_path)
            
            logger.info(f"데이터베이스 백업 완료: {backup_path}")
            return backup_path
            
        except Exception as e:
            logger.error(f"데이터베이스 백업 실패: {e}")
            return None
    
    def get_current_schema_version(self):
        """현재 스키마 버전 확인"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # schema_migrations 테이블이 있는지 확인
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='schema_migrations'
            """)
            
            if cursor.fetchone() is None:
                # 테이블이 없으면 초기 상태
                conn.close()
                return "v1.0.0_initial"
            
            # 최신 마이그레이션 버전 조회
            cursor.execute("""
                SELECT version FROM schema_migrations 
                ORDER BY migrated_at DESC LIMIT 1
            """)
            
            result = cursor.fetchone()
            conn.close()
            
            return result[0] if result else "v1.0.0_initial"
            
        except Exception as e:
            logger.error(f"스키마 버전 확인 실패: {e}")
            return "unknown"
    
    def create_schema_migrations_table(self):
        """스키마 마이그레이션 추적 테이블 생성"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version TEXT UNIQUE NOT NULL,
                    description TEXT,
                    migrated_at TEXT NOT NULL,
                    success BOOLEAN DEFAULT 1
                )
            """)
            
            conn.commit()
            conn.close()
            
            logger.info("schema_migrations 테이블 생성 완료")
            
        except Exception as e:
            logger.error(f"schema_migrations 테이블 생성 실패: {e}")
    
    def check_column_exists(self, table_name: str, column_name: str) -> bool:
        """테이블에 컬럼이 존재하는지 확인"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            
            conn.close()
            return column_name in columns
            
        except Exception as e:
            logger.error(f"컬럼 존재 확인 실패: {e}")
            return False
    
    def check_table_exists(self, table_name: str) -> bool:
        """테이블이 존재하는지 확인"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            result = cursor.fetchone()
            conn.close()
            
            return result is not None
            
        except Exception as e:
            logger.error(f"테이블 존재 확인 실패: {e}")
            return False
    
    def migrate_to_v1_1_0(self):
        """v1.1.0 마이그레이션: 투자 가능 여부 필드 추가"""
        logger.info("v1.1.0 마이그레이션 시작: 투자 가능 여부 필드 추가")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 트랜잭션 시작
            cursor.execute("BEGIN TRANSACTION")
            
            # 1. investment_status 테이블 생성
            if not self.check_table_exists('investment_status'):
                cursor.execute("""
                    CREATE TABLE investment_status (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT UNIQUE NOT NULL,
                        company_name TEXT,
                        market_type TEXT,  -- KOSPI, KOSDAQ, KONEX
                        listing_status TEXT DEFAULT 'LISTED',  -- LISTED, DELISTED, SUSPENDED
                        trading_status TEXT DEFAULT 'NORMAL',  -- NORMAL, HALTED, RESTRICTED
                        investment_warning TEXT DEFAULT 'NONE',  -- NONE, CAUTION, ALERT, DESIGNATED
                        is_investable BOOLEAN DEFAULT 1,  -- 투자 가능 여부
                        delisting_date TEXT,  -- 상장폐지일
                        suspension_date TEXT,  -- 거래정지일
                        warning_date TEXT,  -- 투자주의환기일
                        last_updated TEXT,
                        notes TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(stock_code)
                    )
                """)
                logger.info("investment_status 테이블 생성 완료")
            
            # 2. buffett_all_stocks_final 테이블에 필드 추가
            if not self.check_column_exists('buffett_all_stocks_final', 'is_investable'):
                cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN is_investable BOOLEAN DEFAULT 1")
                logger.info("buffett_all_stocks_final.is_investable 필드 추가")
            
            if not self.check_column_exists('buffett_all_stocks_final', 'investment_warning'):
                cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN investment_warning TEXT DEFAULT 'NONE'")
                logger.info("buffett_all_stocks_final.investment_warning 필드 추가")
            
            if not self.check_column_exists('buffett_all_stocks_final', 'listing_status'):
                cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN listing_status TEXT DEFAULT 'LISTED'")
                logger.info("buffett_all_stocks_final.listing_status 필드 추가")
            
            if not self.check_column_exists('buffett_all_stocks_final', 'last_status_check'):
                cursor.execute("ALTER TABLE buffett_all_stocks_final ADD COLUMN last_status_check TEXT")
                logger.info("buffett_all_stocks_final.last_status_check 필드 추가")
            
            # 3. 인덱스 생성
            try:
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_investment_status_stock_code ON investment_status(stock_code)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_investment_status_investable ON investment_status(is_investable)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buffett_investable ON buffett_all_stocks_final(is_investable)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_buffett_warning ON buffett_all_stocks_final(investment_warning)")
                logger.info("인덱스 생성 완료")
            except Exception as e:
                logger.warning(f"인덱스 생성 중 일부 실패: {e}")
            
            # 4. 기존 데이터의 기본값 설정
            cursor.execute("""
                UPDATE buffett_all_stocks_final 
                SET is_investable = 1, 
                    investment_warning = 'NONE', 
                    listing_status = 'LISTED'
                WHERE is_investable IS NULL 
                   OR investment_warning IS NULL 
                   OR listing_status IS NULL
            """)
            
            updated_rows = cursor.rowcount
            if updated_rows > 0:
                logger.info(f"기존 데이터 기본값 설정 완료: {updated_rows}건")
            
            # 5. investment_status 테이블에 기본 데이터 삽입
            cursor.execute("""
                INSERT OR IGNORE INTO investment_status 
                (stock_code, company_name, market_type, is_investable, last_updated)
                SELECT 
                    stock_code, 
                    company_name,
                    CASE 
                        WHEN stock_code LIKE '0%' OR stock_code LIKE '1%' OR stock_code LIKE '2%' OR stock_code LIKE '3%' THEN 'KOSPI'
                        ELSE 'KOSDAQ'
                    END as market_type,
                    1 as is_investable,
                    datetime('now') as last_updated
                FROM buffett_all_stocks_final
                WHERE stock_code IS NOT NULL
            """)
            
            inserted_rows = cursor.rowcount
            if inserted_rows > 0:
                logger.info(f"investment_status 기본 데이터 삽입 완료: {inserted_rows}건")
            
            # 트랜잭션 커밋
            cursor.execute("COMMIT")
            
            # 6. 마이그레이션 기록
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("""
                INSERT OR REPLACE INTO schema_migrations 
                (version, description, migrated_at, success)
                VALUES (?, ?, ?, ?)
            """, (
                self.migration_version,
                "투자 가능 여부 필드 및 테이블 추가",
                current_time,
                1
            ))
            
            conn.commit()
            conn.close()
            
            logger.info("v1.1.0 마이그레이션 완료")
            return True
            
        except Exception as e:
            logger.error(f"v1.1.0 마이그레이션 실패: {e}")
            try:
                cursor.execute("ROLLBACK")
                conn.close()
            except:
                pass
            return False
    
    def validate_migration(self):
        """마이그레이션 결과 검증"""
        logger.info("마이그레이션 결과 검증 중...")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 1. investment_status 테이블 존재 확인
            if not self.check_table_exists('investment_status'):
                logger.error("investment_status 테이블이 없습니다")
                return False
            
            # 2. 필수 필드 존재 확인
            required_fields = [
                ('buffett_all_stocks_final', 'is_investable'),
                ('buffett_all_stocks_final', 'investment_warning'),
                ('buffett_all_stocks_final', 'listing_status'),
                ('investment_status', 'stock_code'),
                ('investment_status', 'is_investable'),
                ('investment_status', 'listing_status')
            ]
            
            for table, field in required_fields:
                if not self.check_column_exists(table, field):
                    logger.error(f"{table}.{field} 필드가 없습니다")
                    return False
            
            # 3. 데이터 검증
            cursor.execute("SELECT COUNT(*) FROM investment_status")
            investment_status_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM buffett_all_stocks_final")
            buffett_count = cursor.fetchone()[0]
            
            logger.info(f"investment_status 테이블 레코드 수: {investment_status_count}")
            logger.info(f"buffett_all_stocks_final 테이블 레코드 수: {buffett_count}")
            
            # 4. 기본값 확인
            cursor.execute("""
                SELECT COUNT(*) FROM buffett_all_stocks_final 
                WHERE is_investable IS NULL 
                   OR investment_warning IS NULL 
                   OR listing_status IS NULL
            """)
            null_count = cursor.fetchone()[0]
            
            if null_count > 0:
                logger.warning(f"기본값이 설정되지 않은 레코드: {null_count}건")
            
            conn.close()
            
            logger.info("마이그레이션 검증 완료")
            return True
            
        except Exception as e:
            logger.error(f"마이그레이션 검증 실패: {e}")
            return False
    
    def run_migration(self):
        """전체 마이그레이션 실행"""
        logger.info("데이터베이스 마이그레이션 시작")
        print("🔄 데이터베이스 스키마 업데이트")
        print("=" * 50)
        
        # 1. 현재 버전 확인
        current_version = self.get_current_schema_version()
        print(f"📊 현재 스키마 버전: {current_version}")
        print(f"🎯 목표 스키마 버전: {self.migration_version}")
        
        if current_version == self.migration_version:
            print("✅ 이미 최신 버전입니다.")
            return True
        
        # 2. 데이터베이스 백업
        print("\n💾 데이터베이스 백업 중...")
        backup_path = self.backup_database()
        if backup_path:
            print(f"✅ 백업 완료: {backup_path}")
        else:
            print("❌ 백업 실패 - 마이그레이션을 중단합니다.")
            return False
        
        # 3. 스키마 마이그레이션 테이블 생성
        print("\n📋 마이그레이션 추적 테이블 준비...")
        self.create_schema_migrations_table()
        
        # 4. 마이그레이션 실행
        print(f"\n🚀 {self.migration_version} 마이그레이션 실행 중...")
        
        if current_version in ["v1.0.0_initial", "unknown"]:
            success = self.migrate_to_v1_1_0()
            
            if success:
                print("✅ 마이그레이션 성공")
                
                # 5. 검증
                print("\n🔍 마이그레이션 결과 검증 중...")
                if self.validate_migration():
                    print("✅ 검증 완료 - 모든 변경사항이 정상적으로 적용되었습니다.")
                    
                    # 6. 요약 정보 출력
                    self.print_migration_summary()
                    return True
                else:
                    print("❌ 검증 실패 - 백업에서 복원을 고려하세요.")
                    return False
            else:
                print("❌ 마이그레이션 실패")
                return False
        else:
            print(f"⚠️ 알 수 없는 스키마 버전: {current_version}")
            return False
    
    def print_migration_summary(self):
        """마이그레이션 요약 정보 출력"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 테이블 정보
            print("\n📊 업데이트된 데이터베이스 구조:")
            print("-" * 40)
            
            # investment_status 테이블 정보
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM investment_status")
            investment_count = cursor.fetchone()[0]
            print(f"📋 investment_status 테이블: {investment_count}개 레코드")
            
            # buffett_all_stocks_final 테이블 정보
            cursor.execute("SELECT COUNT(*) FROM buffett_all_stocks_final")
            buffett_count = cursor.fetchone()[0]
            print(f"📈 buffett_all_stocks_final 테이블: {buffett_count}개 레코드")
            
            # 새로 추가된 필드 확인
            print("\n🆕 추가된 필드:")
            new_fields = [
                "buffett_all_stocks_final.is_investable (투자 가능 여부)",
                "buffett_all_stocks_final.investment_warning (투자 경고 수준)",
                "buffett_all_stocks_final.listing_status (상장 상태)",
                "buffett_all_stocks_final.last_status_check (마지막 상태 확인일)"
            ]
            
            for field in new_fields:
                print(f"   ✅ {field}")
            
            print(f"\n📋 새로 생성된 테이블:")
            print(f"   ✅ investment_status (투자 상태 관리)")
            
            # 인덱스 정보
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='index' AND name LIKE 'idx_%'
                ORDER BY name
            """)
            indexes = cursor.fetchall()
            
            if indexes:
                print(f"\n🔍 생성된 인덱스 ({len(indexes)}개):")
                for idx in indexes:
                    print(f"   ✅ {idx[0]}")
            
            conn.close()
            
            print("\n" + "=" * 50)
            print("🎉 마이그레이션이 성공적으로 완료되었습니다!")
            print("🔧 이제 다음 기능을 사용할 수 있습니다:")
            print("   - 투자 가능 여부 자동 확인")
            print("   - 상장폐지/관리종목 필터링")
            print("   - 투자 경고 수준 관리")
            print("   - 실제 투자 가능한 종목만 추천")
            
        except Exception as e:
            logger.error(f"요약 정보 출력 실패: {e}")

def main():
    """메인 실행 함수"""
    print("🗃️ Value Investment System 데이터베이스 마이그레이션")
    print("=" * 60)
    print("투자 가능 여부 관련 필드 및 테이블 추가")
    print()
    
    # 데이터베이스 경로 확인
    db_path = "data/databases/buffett_scorecard.db"
    if not os.path.exists(db_path):
        print(f"❌ 데이터베이스 파일을 찾을 수 없습니다: {db_path}")
        print("먼저 워런 버핏 분석을 실행하여 데이터베이스를 생성하세요.")
        return False
    
    # 마이그레이션 실행
    migrator = DatabaseMigrator(db_path)
    success = migrator.run_migration()
    
    if success:
        print("\n🚀 다음 단계:")
        print("1. python investment_status_updater.py  # 투자 가능 여부 업데이트")
        print("2. python get_reliable_stocks_updated.py  # 업데이트된 추천 종목 확인")
        return True
    else:
        print("\n❌ 마이그레이션 실패")
        print("백업 파일에서 복원하거나 기술 지원을 요청하세요.")
        return False

if __name__ == "__main__":
    main()
