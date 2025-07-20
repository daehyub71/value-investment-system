#!/usr/bin/env python3
"""
데이터 현황 대시보드 생성기
77,729건 뉴스, 250MB 주가 데이터 등 전체 현황을 HTML 대시보드로 생성
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import json
import os

def create_data_dashboard():
    """데이터 현황 대시보드 생성"""
    
    print("📊 데이터 현황 대시보드 생성 시작...")
    print("=" * 60)
    
    # 데이터베이스 경로들
    db_dir = Path('data/databases')
    databases = {
        'news_data.db': '뉴스 데이터',
        'stock_data.db': '주가 데이터', 
        'dart_data.db': 'DART 재무데이터',
        'buffett_scorecard.db': '버핏 스코어카드',
        'kis_data.db': 'KIS API',
        'forecast_data.db': '예측 데이터',
        'yahoo_finance_data.db': '야후 파이낸스'
    }
    
    dashboard_data = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_size_mb': 0,
        'databases': {}
    }
    
    # 1. 각 데이터베이스 분석
    for db_file, db_name in databases.items():
        db_path = db_dir / db_file
        
        if not db_path.exists():
            print(f"⚠️ {db_name} 데이터베이스가 없습니다: {db_file}")
            dashboard_data['databases'][db_file] = {
                'name': db_name,
                'exists': False,
                'size_mb': 0,
                'tables': {},
                'status': '❌ 미존재'
            }
            continue
        
        try:
            # 파일 크기
            size_bytes = db_path.stat().st_size
            size_mb = size_bytes / (1024 * 1024)
            dashboard_data['total_size_mb'] += size_mb
            
            print(f"🔍 {db_name} 분석 중... ({size_mb:.1f}MB)")
            
            # 데이터베이스 연결
            conn = sqlite3.connect(db_path)
            
            # 테이블 목록
            tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
            tables_df = pd.read_sql_query(tables_query, conn)
            
            tables_info = {}
            total_records = 0
            
            # 각 테이블 분석
            for table_name in tables_df['name']:
                try:
                    count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                    count_result = pd.read_sql_query(count_query, conn)
                    record_count = count_result['count'][0]
                    total_records += record_count
                    
                    # 최신 데이터 확인 (날짜 필드가 있는 경우)
                    latest_date = None
                    date_columns = ['created_at', 'pubDate', 'date', 'updated_at']
                    
                    for date_col in date_columns:
                        try:
                            date_query = f"SELECT {date_col} FROM {table_name} ORDER BY {date_col} DESC LIMIT 1"
                            date_result = pd.read_sql_query(date_query, conn)
                            if not date_result.empty:
                                latest_date = date_result.iloc[0, 0]
                                break
                        except:
                            continue
                    
                    tables_info[table_name] = {
                        'records': record_count,
                        'latest_date': latest_date
                    }
                    
                except Exception as e:
                    tables_info[table_name] = {
                        'records': 0,
                        'error': str(e)
                    }
            
            conn.close()
            
            # 상태 판정
            if total_records > 1000:
                status = "✅ 양호"
            elif total_records > 100:
                status = "🟡 보통"
            elif total_records > 0:
                status = "🟠 부족"
            else:
                status = "🔴 데이터 없음"
            
            dashboard_data['databases'][db_file] = {
                'name': db_name,
                'exists': True,
                'size_mb': round(size_mb, 2),
                'total_records': total_records,
                'tables': tables_info,
                'status': status,
                'modified': datetime.fromtimestamp(db_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M')
            }
            
        except Exception as e:
            print(f"❌ {db_name} 분석 실패: {e}")
            dashboard_data['databases'][db_file] = {
                'name': db_name,
                'exists': True,
                'size_mb': round(size_mb, 2) if 'size_mb' in locals() else 0,
                'error': str(e),
                'status': '❌ 오류'
            }
    
    # 2. HTML 대시보드 생성
    html_content = generate_html_dashboard(dashboard_data)
    
    # 3. 파일 저장
    output_file = Path('data_dashboard.html')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    # 4. JSON 데이터도 저장 (int64 타입 변환)
    json_file = Path('data_dashboard.json')
    
    # JSON 직렬화를 위해 int64를 int로 변환
    def convert_numpy_types(obj):
        if hasattr(obj, 'item'):
            return obj.item()
        elif isinstance(obj, dict):
            return {k: convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_numpy_types(v) for v in obj]
        else:
            return obj
    
    dashboard_data_json = convert_numpy_types(dashboard_data)
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(dashboard_data_json, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ 대시보드 생성 완료!")
    print(f"📄 HTML 대시보드: {output_file.absolute()}")
    print(f"📊 JSON 데이터: {json_file.absolute()}")
    print(f"💾 총 데이터 크기: {dashboard_data['total_size_mb']:.1f}MB")
    
    # 5. 요약 통계 출력
    print_dashboard_summary(dashboard_data)

def generate_html_dashboard(data):
    """HTML 대시보드 생성"""
    
    # CSS 스타일
    css_style = """
    <style>
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; 
            margin: 20px; 
            background: #f5f7fa; 
        }
        .container { 
            max-width: 1200px; 
            margin: 0 auto; 
            background: white; 
            padding: 30px; 
            border-radius: 10px; 
            box-shadow: 0 2px 10px rgba(0,0,0,0.1); 
        }
        .header { 
            text-align: center; 
            margin-bottom: 40px; 
            padding-bottom: 20px; 
            border-bottom: 2px solid #e1e8ed; 
        }
        .header h1 { 
            color: #1a73e8; 
            margin: 0; 
            font-size: 2.5em; 
        }
        .header p { 
            color: #666; 
            margin: 10px 0; 
            font-size: 1.1em; 
        }
        .stats-grid { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
            gap: 20px; 
            margin-bottom: 40px; 
        }
        .stat-card { 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
            color: white; 
            padding: 25px; 
            border-radius: 10px; 
            text-align: center; 
        }
        .stat-card h3 { 
            margin: 0 0 10px 0; 
            font-size: 2.2em; 
        }
        .stat-card p { 
            margin: 0; 
            opacity: 0.9; 
        }
        .database-grid { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); 
            gap: 25px; 
        }
        .db-card { 
            border: 1px solid #e1e8ed; 
            border-radius: 10px; 
            overflow: hidden; 
            transition: transform 0.2s; 
        }
        .db-card:hover { 
            transform: translateY(-2px); 
            box-shadow: 0 4px 20px rgba(0,0,0,0.1); 
        }
        .db-header { 
            padding: 20px; 
            background: #f8f9fa; 
            border-bottom: 1px solid #e1e8ed; 
        }
        .db-header h3 { 
            margin: 0; 
            color: #333; 
            display: flex; 
            justify-content: space-between; 
            align-items: center; 
        }
        .db-content { 
            padding: 20px; 
        }
        .table-info { 
            background: #f8f9fa; 
            padding: 15px; 
            border-radius: 5px; 
            margin: 10px 0; 
        }
        .table-info h4 { 
            margin: 0 0 10px 0; 
            color: #1a73e8; 
        }
        .metric { 
            display: flex; 
            justify-content: space-between; 
            margin: 5px 0; 
            padding: 5px 0; 
            border-bottom: 1px dotted #ddd; 
        }
        .metric:last-child { 
            border-bottom: none; 
        }
        .status-good { color: #28a745; }
        .status-ok { color: #ffc107; }
        .status-bad { color: #dc3545; }
        .footer { 
            text-align: center; 
            margin-top: 40px; 
            padding-top: 20px; 
            border-top: 1px solid #e1e8ed; 
            color: #666; 
        }
    </style>
    """
    
    # 전체 통계 계산
    total_dbs = len(data['databases'])
    active_dbs = sum(1 for db in data['databases'].values() if db.get('exists'))
    total_records = sum(db.get('total_records', 0) for db in data['databases'].values() if db.get('total_records'))
    
    # HTML 생성
    html = f"""
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>📊 Value Investment System 데이터 현황</title>
        {css_style}
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Value Investment System</h1>
                <p>데이터 현황 대시보드</p>
                <p>생성일시: {data['generated_at']}</p>
            </div>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <h3>{data['total_size_mb']:.1f}MB</h3>
                    <p>총 데이터 크기</p>
                </div>
                <div class="stat-card">
                    <h3>{total_records:,}</h3>
                    <p>총 레코드 수</p>
                </div>
                <div class="stat-card">
                    <h3>{active_dbs}/{total_dbs}</h3>
                    <p>활성 데이터베이스</p>
                </div>
            </div>
            
            <div class="database-grid">
    """
    
    # 각 데이터베이스 카드 생성
    for db_file, db_info in data['databases'].items():
        status_class = "status-good" if "✅" in db_info['status'] else "status-ok" if "🟡" in db_info['status'] else "status-bad"
        
        html += f"""
                <div class="db-card">
                    <div class="db-header">
                        <h3>
                            {db_info['name']}
                            <span class="{status_class}">{db_info['status']}</span>
                        </h3>
                    </div>
                    <div class="db-content">
                        <div class="metric">
                            <span>파일 크기:</span>
                            <strong>{db_info.get('size_mb', 0):.1f}MB</strong>
                        </div>
                        <div class="metric">
                            <span>총 레코드:</span>
                            <strong>{db_info.get('total_records', 0):,}건</strong>
                        </div>
                        <div class="metric">
                            <span>최종 수정:</span>
                            <strong>{db_info.get('modified', 'N/A')}</strong>
                        </div>
        """
        
        # 테이블 정보
        if 'tables' in db_info and db_info['tables']:
            html += "<h4>📋 테이블 현황:</h4>"
            for table_name, table_info in db_info['tables'].items():
                if 'records' in table_info:
                    html += f"""
                        <div class="table-info">
                            <h4>{table_name}</h4>
                            <div class="metric">
                                <span>레코드 수:</span>
                                <strong>{table_info['records']:,}건</strong>
                            </div>
                    """
                    if table_info.get('latest_date'):
                        html += f"""
                            <div class="metric">
                                <span>최신 데이터:</span>
                                <strong>{table_info['latest_date']}</strong>
                            </div>
                        """
                    html += "</div>"
        
        if 'error' in db_info:
            html += f'<div style="color: red; margin-top: 10px;">❌ 오류: {db_info["error"]}</div>'
        
        html += """
                    </div>
                </div>
        """
    
    html += f"""
            </div>
            
            <div class="footer">
                <p>🚀 Value Investment System | 데이터 기반 가치투자 시스템</p>
                <p>📊 뉴스 데이터: 77,729건 | 📈 주가 데이터: 250MB | 📋 재무 데이터: 46MB</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

def print_dashboard_summary(data):
    """대시보드 요약 출력"""
    print(f"\n📋 데이터 현황 요약")
    print("=" * 60)
    
    for db_file, db_info in data['databases'].items():
        print(f"{db_info['status']} {db_info['name']}")
        print(f"   📁 크기: {db_info.get('size_mb', 0):.1f}MB")
        print(f"   📊 레코드: {db_info.get('total_records', 0):,}건")
        
        if db_info.get('tables'):
            main_tables = sorted(db_info['tables'].items(), 
                               key=lambda x: x[1].get('records', 0), reverse=True)[:3]
            for table_name, table_info in main_tables:
                if 'records' in table_info and table_info['records'] > 0:
                    print(f"     - {table_name}: {table_info['records']:,}건")
        print()
    
    print(f"💾 총 데이터 크기: {data['total_size_mb']:.1f}MB")
    total_records = sum(db.get('total_records', 0) for db in data['databases'].values())
    print(f"📊 총 레코드 수: {total_records:,}건")

if __name__ == "__main__":
    try:
        create_data_dashboard()
        
        # 브라우저로 열기 시도
        dashboard_file = Path('data_dashboard.html').absolute()
        print(f"\n🌐 브라우저에서 확인하세요:")
        print(f"   file://{dashboard_file}")
        
        # 운영체제별 브라우저 열기
        import webbrowser
        try:
            webbrowser.open(f'file://{dashboard_file}')
            print("   ✅ 브라우저가 자동으로 열렸습니다!")
        except:
            print("   ⚠️ 브라우저를 수동으로 열어서 위 주소를 입력하세요.")
            
    except Exception as e:
        print(f"❌ 대시보드 생성 실패: {e}")
        import traceback
        traceback.print_exc()
