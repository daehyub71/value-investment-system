#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
간단한 데이터베이스 상태 체커 (의존성 없음)
4개 데이터베이스의 기본 정보와 테이블 현황을 빠르게 확인
"""

import sqlite3
import os
from datetime import datetime
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

def check_database(db_path):
    """개별 데이터베이스 상태 확인"""
    if not os.path.exists(db_path):
        return {
            'exists': False,
            'error': '파일 없음'
        }
    
    try:
        # 파일 정보
        stat = os.stat(db_path)
        file_size = stat.st_size
        modified_time = datetime.fromtimestamp(stat.st_mtime)
        
        # 데이터베이스 연결
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 테이블 목록 조회
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        # 각 테이블의 레코드 수 조회
        table_counts = {}
        total_records = 0
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                count = cursor.fetchone()[0]
                table_counts[table] = count
                total_records += count
            except Exception as e:
                table_counts[table] = f"오류: {str(e)}"
        
        conn.close()
        
        return {
            'exists': True,
            'size': file_size,
            'size_formatted': format_size(file_size),
            'modified': modified_time.strftime('%Y-%m-%d %H:%M:%S'),
            'tables': tables,
            'table_counts': table_counts,
            'total_records': total_records
        }
        
    except Exception as e:
        return {
            'exists': True,
            'error': f"접근 오류: {str(e)}"
        }

def main():
    """메인 함수"""
    print("🔍 데이터베이스 상태 간단 점검")
    print("=" * 70)
    
    # 데이터베이스 파일 목록
    db_base_path = Path("data/databases")
    
    # 현재 디렉터리에서도 찾기
    if not db_base_path.exists():
        db_base_path = Path(".")
    
    databases = {
        'stock_data.db': '📈 주식 데이터',
        'dart_data.db': '📋 DART 공시 데이터', 
        'news_data.db': '📰 뉴스 감정분석',
        'kis_data.db': '💹 KIS API 데이터'
    }
    
    total_size = 0
    total_records = 0
    active_dbs = 0
    
    # 각 데이터베이스 점검
    for db_file, description in databases.items():
        db_path = db_base_path / db_file
        
        print(f"\n{description} ({db_file})")
        print("-" * 50)
        
        result = check_database(db_path)
        
        if not result['exists']:
            print("❌ 파일이 존재하지 않습니다")
            continue
        
        if 'error' in result:
            print(f"❌ {result['error']}")
            continue
        
        # 기본 정보
        print(f"📁 파일 크기: {result['size_formatted']}")
        print(f"🕒 수정 시간: {result['modified']}")
        print(f"📊 총 레코드: {result['total_records']:,}개")
        
        total_size += result['size']
        total_records += result['total_records']
        active_dbs += 1
        
        # 테이블별 상세 정보
        if result['tables']:
            print(f"📋 테이블 현황 ({len(result['tables'])}개):")
            for table in result['tables']:
                count = result['table_counts'][table]
                if isinstance(count, int):
                    if count > 0:
                        print(f"   ✅ {table}: {count:,}개")
                    else:
                        print(f"   ⚠️  {table}: 데이터 없음")
                else:
                    print(f"   ❌ {table}: {count}")
        else:
            print("❌ 테이블이 없습니다")
        
        # 상태 평가
        if result['total_records'] == 0:
            status = "🔴 데이터 없음"
        elif result['total_records'] < 1000:
            status = "🟡 데이터 부족"
        else:
            status = "🟢 정상"
        
        print(f"📈 상태: {status}")
    
    # 전체 요약
    print(f"\n{'='*70}")
    print("📊 전체 요약")
    print(f"{'='*70}")
    print(f"💾 총 데이터베이스 크기: {format_size(total_size)}")
    print(f"📊 총 레코드 수: {total_records:,}개")
    print(f"✅ 활성 데이터베이스: {active_dbs}/{len(databases)}개")
    
    # 권장 사항
    print(f"\n💡 권장 사항:")
    
    if active_dbs < len(databases):
        missing_count = len(databases) - active_dbs
        print(f"🚨 {missing_count}개 데이터베이스가 누락되었습니다 - 데이터 수집 필요")
    
    if total_records < 10000:
        print(f"📈 전체 데이터가 부족합니다 - 데이터 수집 스크립트 실행 권장")
    
    if total_records > 0:
        print(f"✅ 기본 데이터는 수집되어 있습니다")
    
    print(f"\n🔧 다음 단계:")
    print(f"1. 상세 분석: python database_status_checker.py")
    print(f"2. 개별 테이블 확인: python company_info_checker.py")
    print(f"3. 데이터 수집: python scripts/data_collection/collect_all_data.py")

if __name__ == "__main__":
    main()