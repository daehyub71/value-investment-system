import sqlite3
import os
from datetime import datetime

def quick_inspect_databases():
    """빠른 데이터베이스 검사"""
    db_path = r"C:\data_analysis\value-investment-system\value-investment-system\data\databases"
    target_stock = "090430"
    stock_name = "아모레퍼시픽"
    
    print(f"🔍 {stock_name}({target_stock}) 데이터 현황 빠른 검사")
    print(f"검사 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 1. 파일 현황 확인
    print("\n📁 데이터베이스 파일 현황:")
    db_files = {
        'stock_data.db': '주가 데이터',
        'dart_data.db': '재무 데이터', 
        'news_data.db': '뉴스 데이터',
        'kis_data.db': 'KIS API 데이터',
        'yahoo_finance_data.db': 'Yahoo Finance 데이터'
    }
    
    for db_file, description in db_files.items():
        file_path = os.path.join(db_path, db_file)
        if os.path.exists(file_path):
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            print(f"✅ {db_file:<20} ({description}) - {size_mb:.1f}MB, 수정: {mod_time.strftime('%m/%d %H:%M')}")
        else:
            print(f"❌ {db_file:<20} ({description}) - 파일 없음")
    
    # 2. 주요 데이터베이스 내용 확인
    critical_checks = []
    
    # 주가 데이터 확인
    try:
        conn = sqlite3.connect(os.path.join(db_path, 'stock_data.db'))
        cursor = conn.cursor()
        
        # 테이블 존재 확인
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"\n📊 stock_data.db 테이블: {tables}")
        
        stock_data_status = "❌ 데이터 없음"
        
        # 아모레퍼시픽 데이터 찾기 - 여러 패턴으로 검색
        search_patterns = [f"%{target_stock}%", "%090430%", "%아모레퍼시픽%", "090430"]
        
        for table in tables:
            print(f"\n  테이블 '{table}' 검사 중...")
            try:
                # 테이블 구조 확인
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                column_names = [col[1] for col in columns]
                print(f"    컬럼: {column_names}")
                
                # 데이터 개수 확인
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total_count = cursor.fetchone()[0]
                print(f"    총 행 수: {total_count:,}")
                
                # 아모레퍼시픽 데이터 검색
                if 'symbol' in column_names or 'code' in column_names or 'stock_code' in column_names:
                    symbol_col = 'symbol' if 'symbol' in column_names else ('code' if 'code' in column_names else 'stock_code')
                    
                    for pattern in search_patterns:
                        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {symbol_col} LIKE ?", (pattern,))
                        count = cursor.fetchone()[0]
                        if count > 0:
                            print(f"    ✅ {pattern} 패턴으로 {count:,}개 발견!")
                            
                            # 날짜 범위 확인
                            if 'date' in column_names:
                                cursor.execute(f"SELECT MIN(date), MAX(date) FROM {table} WHERE {symbol_col} LIKE ?", (pattern,))
                                date_range = cursor.fetchone()
                                print(f"    📅 데이터 기간: {date_range[0]} ~ {date_range[1]}")
                            
                            # 최근 데이터 샘플
                            cursor.execute(f"SELECT * FROM {table} WHERE {symbol_col} LIKE ? LIMIT 3", (pattern,))
                            samples = cursor.fetchall()
                            print(f"    📋 샘플 데이터: {samples[0] if samples else '없음'}")
                            
                            stock_data_status = f"✅ {count:,}개 ({date_range[0] if date_range[0] else '?'} ~ {date_range[1] if date_range[1] else '?'})"
                            break
                    
                    if stock_data_status != "❌ 데이터 없음":
                        break
                        
            except Exception as e:
                print(f"    ❌ 테이블 검사 오류: {e}")
                        
        critical_checks.append(("주가 데이터", stock_data_status))
        conn.close()
        
    except Exception as e:
        critical_checks.append(("주가 데이터", f"❌ 오류: {str(e)[:50]}..."))
        print(f"주가 데이터 검사 오류: {e}")
    
    # 재무 데이터 확인  
    try:
        conn = sqlite3.connect(os.path.join(db_path, 'dart_data.db'))
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"\n📊 dart_data.db 테이블: {tables}")
        
        financial_status = "❌ 데이터 없음"
        
        # 회사 정보 테이블에서 아모레퍼시픽 찾기
        for table in tables:
            print(f"\n  테이블 '{table}' 검사 중...")
            try:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                column_names = [col[1] for col in columns]
                print(f"    컬럼: {column_names}")
                
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total_count = cursor.fetchone()[0]
                print(f"    총 행 수: {total_count:,}")
                
                # 아모레퍼시픽 검색
                search_columns = [col for col in column_names if any(keyword in col.lower() for keyword in ['stock', 'code', 'name', 'corp'])]
                
                for col in search_columns:
                    try:
                        for pattern in [target_stock, stock_name, "090430"]:
                            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} LIKE ?", (f"%{pattern}%",))
                            count = cursor.fetchone()[0]
                            if count > 0:
                                print(f"    ✅ {col} 컬럼에서 '{pattern}' 패턴으로 {count}개 발견!")
                                
                                # 샘플 데이터
                                cursor.execute(f"SELECT * FROM {table} WHERE {col} LIKE ? LIMIT 2", (f"%{pattern}%",))
                                samples = cursor.fetchall()
                                for sample in samples:
                                    print(f"    📋 샘플: {sample}")
                                
                                financial_status = f"✅ {table} 테이블에 {count}개 항목"
                                break
                    except Exception as e:
                        continue
                        
                if financial_status != "❌ 데이터 없음":
                    break
                    
            except Exception as e:
                print(f"    ❌ 테이블 검사 오류: {e}")
                    
        critical_checks.append(("재무 데이터", financial_status))
        conn.close()
        
    except Exception as e:
        critical_checks.append(("재무 데이터", f"❌ 오류: {str(e)[:50]}..."))
        print(f"재무 데이터 검사 오류: {e}")
    
    # 뉴스 데이터 확인
    try:
        conn = sqlite3.connect(os.path.join(db_path, 'news_data.db'))
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"\n📊 news_data.db 테이블: {tables}")
        
        news_status = "❌ 데이터 없음"
        
        for table in tables:
            print(f"\n  테이블 '{table}' 검사 중...")
            try:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                column_names = [col[1] for col in columns]
                print(f"    컬럼: {column_names}")
                
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total_count = cursor.fetchone()[0]
                print(f"    총 행 수: {total_count:,}")
                
                # 텍스트 검색 가능한 컬럼 찾기
                text_columns = [col for col in column_names if any(keyword in col.lower() for keyword in ['title', 'content', 'text', 'body', 'company'])]
                
                for col in text_columns:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} LIKE ?", (f"%{stock_name}%",))
                        count = cursor.fetchone()[0]
                        if count > 0:
                            print(f"    ✅ {col} 컬럼에서 '{stock_name}' 관련 {count}개 발견!")
                            
                            # 날짜 범위 확인
                            date_columns = [col for col in column_names if 'date' in col.lower()]
                            if date_columns:
                                date_col = date_columns[0]
                                cursor.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table} WHERE {col} LIKE ?", (f"%{stock_name}%",))
                                date_range = cursor.fetchone()
                                print(f"    📅 뉴스 기간: {date_range[0]} ~ {date_range[1]}")
                            
                            # 최근 뉴스 샘플
                            cursor.execute(f"SELECT {col} FROM {table} WHERE {col} LIKE ? LIMIT 3", (f"%{stock_name}%",))
                            samples = cursor.fetchall()
                            for i, sample in enumerate(samples):
                                print(f"    📰 뉴스{i+1}: {str(sample[0])[:100]}...")
                            
                            news_status = f"✅ {count:,}개 ({date_range[0] if date_range and date_range[0] else '?'} ~ {date_range[1] if date_range and date_range[1] else '?'})"
                            break
                    except Exception as e:
                        continue
                        
                if news_status != "❌ 데이터 없음":
                    break
                    
            except Exception as e:
                print(f"    ❌ 테이블 검사 오류: {e}")
                
        critical_checks.append(("뉴스 데이터", news_status))
        conn.close()
        
    except Exception as e:
        critical_checks.append(("뉴스 데이터", f"❌ 오류: {str(e)[:50]}..."))
        print(f"뉴스 데이터 검사 오류: {e}")
    
    # 3. 결과 요약
    print(f"\n📊 {stock_name}({target_stock}) 핵심 데이터 현황:")
    print("-" * 60)
    for check_name, status in critical_checks:
        print(f"{check_name:<15}: {status}")
    
    return critical_checks

if __name__ == "__main__":
    results = quick_inspect_databases()
    print(f"\n✅ 아모레퍼시픽 데이터 검사 완료!")
