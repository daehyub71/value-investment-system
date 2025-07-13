#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 데이터베이스 상태 점검 도구
4개 데이터베이스의 모든 테이블 데이터 수집 현황을 종합 분석
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import os
from pathlib import Path
import humanize

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
            'size_human': humanize.naturalsize(stat.st_size),
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
        """데이터 신선도 분석 (fixed_date_investigator.py 로직 적용)"""
        if not latest_data:
            return "❓ 알 수 없음", "N/A"
        
        date_str = str(latest_data).strip()
        parsed_dt = None
        
        # fixed_date_investigator.py의 올바른 파싱 로직 적용
        import re
        
        # 다양한 형식 시도 (정확한 매칭)
        formats_and_patterns = [
            # ISO 8601 variants
            ('%Y-%m-%d %H:%M:%S', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}
    
    def get_database_health_score(self, db_info):
        """데이터베이스 건강도 점수 계산"""
        if not db_info.get('file_info', {}).get('exists'):
            return 0, "파일 없음"
        
        score = 0
        issues = []
        
        # 파일 크기 점수 (10점)
        size = db_info['file_info']['size']
        if size > 1000000:  # 1MB 이상
            score += 10
        elif size > 100000:  # 100KB 이상
            score += 5
        else:
            issues.append("파일 크기 작음")
        
        # 테이블 존재 점수 (30점)
        expected_tables = set(db_info['expected_tables'])
        actual_tables = set(db_info['table_info'].keys())
        table_coverage = len(actual_tables & expected_tables) / len(expected_tables)
        score += int(table_coverage * 30)
        
        if table_coverage < 1.0:
            missing = expected_tables - actual_tables
            issues.append(f"테이블 누락: {', '.join(missing)}")
        
        # 데이터 존재 점수 (40점)
        total_records = sum(info.get('count', 0) for info in db_info['table_info'].values())
        if total_records > 10000:
            score += 40
        elif total_records > 1000:
            score += 30
        elif total_records > 100:
            score += 20
        elif total_records > 0:
            score += 10
        else:
            issues.append("데이터 없음")
        
        # 데이터 신선도 점수 (20점)
        fresh_tables = 0
        total_tables = len(actual_tables)
        
        for table_info in db_info['table_info'].values():
            if table_info.get('latest_data'):
                freshness, _ = self.analyze_data_freshness(table_info['latest_data'])
                if '🟢' in freshness or '🟡' in freshness:
                    fresh_tables += 1
        
        if total_tables > 0:
            freshness_ratio = fresh_tables / total_tables
            score += int(freshness_ratio * 20)
        
        if fresh_tables == 0 and total_tables > 0:
            issues.append("데이터 오래됨")
        
        # 등급 결정
        if score >= 90:
            grade = "A+ 우수"
        elif score >= 80:
            grade = "A 양호"
        elif score >= 70:
            grade = "B+ 보통"
        elif score >= 60:
            grade = "B 미흡"
        elif score >= 40:
            grade = "C 불량"
        else:
            grade = "D 심각"
        
        return score, grade, issues
    
    def check_all_databases(self):
        """모든 데이터베이스 상태 점검"""
        print("🔍 전체 데이터베이스 상태 점검 시작")
        print("=" * 80)
        
        for db_file, db_config in self.databases.items():
            print(f"\n📊 {db_config['name']} ({db_file})")
            print("-" * 60)
            
            # 파일 정보
            file_info = self.get_file_info(db_file)
            print(f"📁 파일 정보:")
            print(f"   존재: {'✅' if file_info['exists'] else '❌'}")
            if file_info['exists']:
                print(f"   크기: {file_info['size_human']}")
                print(f"   수정일: {file_info['modified']}")
            
            # 테이블 정보
            table_info = self.get_table_info(db_file)
            
            if 'error' in table_info:
                print(f"❌ 데이터베이스 접근 오류: {table_info['error']}")
                continue
            
            print(f"\n📋 테이블 현황:")
            if not table_info:
                print("   ❌ 테이블이 없습니다")
                continue
            
            total_records = 0
            for table_name, info in table_info.items():
                count = info.get('count', 0)
                total_records += count
                
                # 데이터 신선도 분석
                freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                
                print(f"   📊 {table_name}: {count:,}개 레코드")
                print(f"      컬럼: {info.get('columns', 0)}개")
                if info.get('latest_data'):
                    print(f"      최신 데이터: {freshness} ({latest})")
                
                # 에러가 있다면 표시
                if 'error' in info:
                    print(f"      ❌ 오류: {info['error']}")
            
            print(f"\n📈 총 레코드 수: {total_records:,}개")
            
            # 건강도 점수 계산
            db_info = {
                'file_info': file_info,
                'table_info': table_info,
                'expected_tables': db_config['expected_tables']
            }
            
            score, grade, issues = self.get_database_health_score(db_info)
            print(f"🏆 건강도: {score}/100점 ({grade})")
            
            if issues:
                print(f"⚠️  문제점:")
                for issue in issues:
                    print(f"     • {issue}")
            
            # 결과 저장
            self.results[db_file] = {
                'config': db_config,
                'file_info': file_info,
                'table_info': table_info,
                'score': score,
                'grade': grade,
                'issues': issues,
                'total_records': total_records
            }
    
    def show_summary(self):
        """종합 요약 보고서"""
        print("\n" + "=" * 80)
        print("📋 종합 요약 보고서")
        print("=" * 80)
        
        # 전체 통계
        total_size = sum(r['file_info']['size'] for r in self.results.values() if r['file_info']['exists'])
        total_records = sum(r['total_records'] for r in self.results.values())
        avg_score = sum(r['score'] for r in self.results.values()) / len(self.results) if self.results else 0
        
        print(f"💾 전체 데이터베이스 크기: {humanize.naturalsize(total_size)}")
        print(f"📊 전체 레코드 수: {total_records:,}개")
        print(f"🏆 평균 건강도: {avg_score:.1f}/100점")
        
        # 데이터베이스별 요약
        print(f"\n📈 데이터베이스별 상태:")
        print(f"{'데이터베이스':<15} {'상태':<8} {'레코드 수':<12} {'건강도':<10} {'주요 문제'}")
        print("-" * 70)
        
        for db_file, result in self.results.items():
            name = result['config']['name'][:12]
            status = "정상" if result['file_info']['exists'] else "없음"
            records = f"{result['total_records']:,}" if result['total_records'] > 0 else "0"
            score = f"{result['score']}/100"
            main_issue = result['issues'][0] if result['issues'] else "없음"
            
            print(f"{name:<15} {status:<8} {records:<12} {score:<10} {main_issue}")
        
        # 권장사항
        print(f"\n💡 권장사항:")
        
        # 심각한 문제가 있는 DB 찾기
        critical_dbs = [db for db, result in self.results.items() if result['score'] < 40]
        if critical_dbs:
            print(f"🚨 긴급 수정 필요:")
            for db in critical_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {', '.join(result['issues'])}")
        
        # 데이터가 적은 DB 찾기
        low_data_dbs = [db for db, result in self.results.items() if result['total_records'] < 1000 and result['file_info']['exists']]
        if low_data_dbs:
            print(f"📈 데이터 수집 권장:")
            for db in low_data_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: 현재 {result['total_records']:,}개 레코드")
        
        # 높은 점수 DB (칭찬)
        good_dbs = [db for db, result in self.results.items() if result['score'] >= 80]
        if good_dbs:
            print(f"✅ 잘 관리된 데이터베이스:")
            for db in good_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {result['score']}/100점")
    
    def show_collection_status(self):
        """데이터 수집 현황 상세 분석"""
        print("\n" + "=" * 80)
        print("📊 데이터 수집 현황 상세 분석")
        print("=" * 80)
        
        # 주요 테이블별 수집 현황
        important_tables = {
            'stock_data.db': {
                'company_info': '기업 기본정보',
                'stock_prices': '주가 데이터',
                'financial_ratios': '재무비율'
            },
            'dart_data.db': {
                'corp_codes': '기업코드',
                'financial_statements': '재무제표',
                'disclosures': '공시정보'
            },
            'news_data.db': {
                'news_articles': '뉴스 기사',
                'sentiment_scores': '감정 점수'
            },
            'kis_data.db': {
                'realtime_quotes': '실시간 시세'
            }
        }
        
        for db_file, tables in important_tables.items():
            if db_file in self.results:
                result = self.results[db_file]
                print(f"\n🗃️  {result['config']['name']}:")
                
                for table_name, description in tables.items():
                    if table_name in result['table_info']:
                        info = result['table_info'][table_name]
                        count = info.get('count', 0)
                        freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                        
                        print(f"   📊 {description} ({table_name})")
                        print(f"      📈 레코드: {count:,}개")
                        print(f"      🕒 최신: {freshness}")
                        
                        # 수집 상태 평가
                        if count == 0:
                            print(f"      🚨 상태: 데이터 없음 - 수집 필요")
                        elif count < 100:
                            print(f"      ⚠️  상태: 데이터 부족 - 추가 수집 권장")
                        elif '🔴' in freshness:
                            print(f"      ⏰ 상태: 데이터 오래됨 - 업데이트 필요")
                        else:
                            print(f"      ✅ 상태: 양호")
                    else:
                        print(f"   📊 {description} ({table_name})")
                        print(f"      ❌ 테이블 없음 - 생성 및 수집 필요")

def main():
    """메인 함수"""
    print("🚀 Finance Data Vibe - 전체 데이터베이스 상태 점검")
    print("=" * 80)
    
    # 데이터베이스 경로 확인
    db_path = Path("data/databases")
    if not db_path.exists():
        # 현재 디렉터리에서 찾기
        current_files = list(Path(".").glob("*.db"))
        if current_files:
            db_path = Path(".")
        else:
            print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
            print("예상 위치: data/databases/")
            return
    
    # 점검 실행
    checker = DatabaseStatusChecker(db_path)
    checker.check_all_databases()
    checker.show_summary()
    checker.show_collection_status()
    
    print(f"\n✅ 전체 점검 완료!")
    print(f"💡 상세한 테이블별 분석이 필요하면 개별 체커를 사용하세요:")
    print(f"   • python company_info_checker.py  # 기업정보 상세 분석")
    print(f"   • python news_data_checker.py     # 뉴스 데이터 상세 분석")

if __name__ == "__main__":
    main()),
            ('%Y-%m-%dT%H:%M:%S', r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}
    
    def get_database_health_score(self, db_info):
        """데이터베이스 건강도 점수 계산"""
        if not db_info.get('file_info', {}).get('exists'):
            return 0, "파일 없음"
        
        score = 0
        issues = []
        
        # 파일 크기 점수 (10점)
        size = db_info['file_info']['size']
        if size > 1000000:  # 1MB 이상
            score += 10
        elif size > 100000:  # 100KB 이상
            score += 5
        else:
            issues.append("파일 크기 작음")
        
        # 테이블 존재 점수 (30점)
        expected_tables = set(db_info['expected_tables'])
        actual_tables = set(db_info['table_info'].keys())
        table_coverage = len(actual_tables & expected_tables) / len(expected_tables)
        score += int(table_coverage * 30)
        
        if table_coverage < 1.0:
            missing = expected_tables - actual_tables
            issues.append(f"테이블 누락: {', '.join(missing)}")
        
        # 데이터 존재 점수 (40점)
        total_records = sum(info.get('count', 0) for info in db_info['table_info'].values())
        if total_records > 10000:
            score += 40
        elif total_records > 1000:
            score += 30
        elif total_records > 100:
            score += 20
        elif total_records > 0:
            score += 10
        else:
            issues.append("데이터 없음")
        
        # 데이터 신선도 점수 (20점)
        fresh_tables = 0
        total_tables = len(actual_tables)
        
        for table_info in db_info['table_info'].values():
            if table_info.get('latest_data'):
                freshness, _ = self.analyze_data_freshness(table_info['latest_data'])
                if '🟢' in freshness or '🟡' in freshness:
                    fresh_tables += 1
        
        if total_tables > 0:
            freshness_ratio = fresh_tables / total_tables
            score += int(freshness_ratio * 20)
        
        if fresh_tables == 0 and total_tables > 0:
            issues.append("데이터 오래됨")
        
        # 등급 결정
        if score >= 90:
            grade = "A+ 우수"
        elif score >= 80:
            grade = "A 양호"
        elif score >= 70:
            grade = "B+ 보통"
        elif score >= 60:
            grade = "B 미흡"
        elif score >= 40:
            grade = "C 불량"
        else:
            grade = "D 심각"
        
        return score, grade, issues
    
    def check_all_databases(self):
        """모든 데이터베이스 상태 점검"""
        print("🔍 전체 데이터베이스 상태 점검 시작")
        print("=" * 80)
        
        for db_file, db_config in self.databases.items():
            print(f"\n📊 {db_config['name']} ({db_file})")
            print("-" * 60)
            
            # 파일 정보
            file_info = self.get_file_info(db_file)
            print(f"📁 파일 정보:")
            print(f"   존재: {'✅' if file_info['exists'] else '❌'}")
            if file_info['exists']:
                print(f"   크기: {file_info['size_human']}")
                print(f"   수정일: {file_info['modified']}")
            
            # 테이블 정보
            table_info = self.get_table_info(db_file)
            
            if 'error' in table_info:
                print(f"❌ 데이터베이스 접근 오류: {table_info['error']}")
                continue
            
            print(f"\n📋 테이블 현황:")
            if not table_info:
                print("   ❌ 테이블이 없습니다")
                continue
            
            total_records = 0
            for table_name, info in table_info.items():
                count = info.get('count', 0)
                total_records += count
                
                # 데이터 신선도 분석
                freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                
                print(f"   📊 {table_name}: {count:,}개 레코드")
                print(f"      컬럼: {info.get('columns', 0)}개")
                if info.get('latest_data'):
                    print(f"      최신 데이터: {freshness} ({latest})")
                
                # 에러가 있다면 표시
                if 'error' in info:
                    print(f"      ❌ 오류: {info['error']}")
            
            print(f"\n📈 총 레코드 수: {total_records:,}개")
            
            # 건강도 점수 계산
            db_info = {
                'file_info': file_info,
                'table_info': table_info,
                'expected_tables': db_config['expected_tables']
            }
            
            score, grade, issues = self.get_database_health_score(db_info)
            print(f"🏆 건강도: {score}/100점 ({grade})")
            
            if issues:
                print(f"⚠️  문제점:")
                for issue in issues:
                    print(f"     • {issue}")
            
            # 결과 저장
            self.results[db_file] = {
                'config': db_config,
                'file_info': file_info,
                'table_info': table_info,
                'score': score,
                'grade': grade,
                'issues': issues,
                'total_records': total_records
            }
    
    def show_summary(self):
        """종합 요약 보고서"""
        print("\n" + "=" * 80)
        print("📋 종합 요약 보고서")
        print("=" * 80)
        
        # 전체 통계
        total_size = sum(r['file_info']['size'] for r in self.results.values() if r['file_info']['exists'])
        total_records = sum(r['total_records'] for r in self.results.values())
        avg_score = sum(r['score'] for r in self.results.values()) / len(self.results) if self.results else 0
        
        print(f"💾 전체 데이터베이스 크기: {humanize.naturalsize(total_size)}")
        print(f"📊 전체 레코드 수: {total_records:,}개")
        print(f"🏆 평균 건강도: {avg_score:.1f}/100점")
        
        # 데이터베이스별 요약
        print(f"\n📈 데이터베이스별 상태:")
        print(f"{'데이터베이스':<15} {'상태':<8} {'레코드 수':<12} {'건강도':<10} {'주요 문제'}")
        print("-" * 70)
        
        for db_file, result in self.results.items():
            name = result['config']['name'][:12]
            status = "정상" if result['file_info']['exists'] else "없음"
            records = f"{result['total_records']:,}" if result['total_records'] > 0 else "0"
            score = f"{result['score']}/100"
            main_issue = result['issues'][0] if result['issues'] else "없음"
            
            print(f"{name:<15} {status:<8} {records:<12} {score:<10} {main_issue}")
        
        # 권장사항
        print(f"\n💡 권장사항:")
        
        # 심각한 문제가 있는 DB 찾기
        critical_dbs = [db for db, result in self.results.items() if result['score'] < 40]
        if critical_dbs:
            print(f"🚨 긴급 수정 필요:")
            for db in critical_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {', '.join(result['issues'])}")
        
        # 데이터가 적은 DB 찾기
        low_data_dbs = [db for db, result in self.results.items() if result['total_records'] < 1000 and result['file_info']['exists']]
        if low_data_dbs:
            print(f"📈 데이터 수집 권장:")
            for db in low_data_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: 현재 {result['total_records']:,}개 레코드")
        
        # 높은 점수 DB (칭찬)
        good_dbs = [db for db, result in self.results.items() if result['score'] >= 80]
        if good_dbs:
            print(f"✅ 잘 관리된 데이터베이스:")
            for db in good_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {result['score']}/100점")
    
    def show_collection_status(self):
        """데이터 수집 현황 상세 분석"""
        print("\n" + "=" * 80)
        print("📊 데이터 수집 현황 상세 분석")
        print("=" * 80)
        
        # 주요 테이블별 수집 현황
        important_tables = {
            'stock_data.db': {
                'company_info': '기업 기본정보',
                'stock_prices': '주가 데이터',
                'financial_ratios': '재무비율'
            },
            'dart_data.db': {
                'corp_codes': '기업코드',
                'financial_statements': '재무제표',
                'disclosures': '공시정보'
            },
            'news_data.db': {
                'news_articles': '뉴스 기사',
                'sentiment_scores': '감정 점수'
            },
            'kis_data.db': {
                'realtime_quotes': '실시간 시세'
            }
        }
        
        for db_file, tables in important_tables.items():
            if db_file in self.results:
                result = self.results[db_file]
                print(f"\n🗃️  {result['config']['name']}:")
                
                for table_name, description in tables.items():
                    if table_name in result['table_info']:
                        info = result['table_info'][table_name]
                        count = info.get('count', 0)
                        freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                        
                        print(f"   📊 {description} ({table_name})")
                        print(f"      📈 레코드: {count:,}개")
                        print(f"      🕒 최신: {freshness}")
                        
                        # 수집 상태 평가
                        if count == 0:
                            print(f"      🚨 상태: 데이터 없음 - 수집 필요")
                        elif count < 100:
                            print(f"      ⚠️  상태: 데이터 부족 - 추가 수집 권장")
                        elif '🔴' in freshness:
                            print(f"      ⏰ 상태: 데이터 오래됨 - 업데이트 필요")
                        else:
                            print(f"      ✅ 상태: 양호")
                    else:
                        print(f"   📊 {description} ({table_name})")
                        print(f"      ❌ 테이블 없음 - 생성 및 수집 필요")

def main():
    """메인 함수"""
    print("🚀 Finance Data Vibe - 전체 데이터베이스 상태 점검")
    print("=" * 80)
    
    # 데이터베이스 경로 확인
    db_path = Path("data/databases")
    if not db_path.exists():
        # 현재 디렉터리에서 찾기
        current_files = list(Path(".").glob("*.db"))
        if current_files:
            db_path = Path(".")
        else:
            print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
            print("예상 위치: data/databases/")
            return
    
    # 점검 실행
    checker = DatabaseStatusChecker(db_path)
    checker.check_all_databases()
    checker.show_summary()
    checker.show_collection_status()
    
    print(f"\n✅ 전체 점검 완료!")
    print(f"💡 상세한 테이블별 분석이 필요하면 개별 체커를 사용하세요:")
    print(f"   • python company_info_checker.py  # 기업정보 상세 분석")
    print(f"   • python news_data_checker.py     # 뉴스 데이터 상세 분석")

if __name__ == "__main__":
    main()),
            ('%Y-%m-%d', r'^\d{4}-\d{2}-\d{2}
    
    def get_database_health_score(self, db_info):
        """데이터베이스 건강도 점수 계산"""
        if not db_info.get('file_info', {}).get('exists'):
            return 0, "파일 없음"
        
        score = 0
        issues = []
        
        # 파일 크기 점수 (10점)
        size = db_info['file_info']['size']
        if size > 1000000:  # 1MB 이상
            score += 10
        elif size > 100000:  # 100KB 이상
            score += 5
        else:
            issues.append("파일 크기 작음")
        
        # 테이블 존재 점수 (30점)
        expected_tables = set(db_info['expected_tables'])
        actual_tables = set(db_info['table_info'].keys())
        table_coverage = len(actual_tables & expected_tables) / len(expected_tables)
        score += int(table_coverage * 30)
        
        if table_coverage < 1.0:
            missing = expected_tables - actual_tables
            issues.append(f"테이블 누락: {', '.join(missing)}")
        
        # 데이터 존재 점수 (40점)
        total_records = sum(info.get('count', 0) for info in db_info['table_info'].values())
        if total_records > 10000:
            score += 40
        elif total_records > 1000:
            score += 30
        elif total_records > 100:
            score += 20
        elif total_records > 0:
            score += 10
        else:
            issues.append("데이터 없음")
        
        # 데이터 신선도 점수 (20점)
        fresh_tables = 0
        total_tables = len(actual_tables)
        
        for table_info in db_info['table_info'].values():
            if table_info.get('latest_data'):
                freshness, _ = self.analyze_data_freshness(table_info['latest_data'])
                if '🟢' in freshness or '🟡' in freshness:
                    fresh_tables += 1
        
        if total_tables > 0:
            freshness_ratio = fresh_tables / total_tables
            score += int(freshness_ratio * 20)
        
        if fresh_tables == 0 and total_tables > 0:
            issues.append("데이터 오래됨")
        
        # 등급 결정
        if score >= 90:
            grade = "A+ 우수"
        elif score >= 80:
            grade = "A 양호"
        elif score >= 70:
            grade = "B+ 보통"
        elif score >= 60:
            grade = "B 미흡"
        elif score >= 40:
            grade = "C 불량"
        else:
            grade = "D 심각"
        
        return score, grade, issues
    
    def check_all_databases(self):
        """모든 데이터베이스 상태 점검"""
        print("🔍 전체 데이터베이스 상태 점검 시작")
        print("=" * 80)
        
        for db_file, db_config in self.databases.items():
            print(f"\n📊 {db_config['name']} ({db_file})")
            print("-" * 60)
            
            # 파일 정보
            file_info = self.get_file_info(db_file)
            print(f"📁 파일 정보:")
            print(f"   존재: {'✅' if file_info['exists'] else '❌'}")
            if file_info['exists']:
                print(f"   크기: {file_info['size_human']}")
                print(f"   수정일: {file_info['modified']}")
            
            # 테이블 정보
            table_info = self.get_table_info(db_file)
            
            if 'error' in table_info:
                print(f"❌ 데이터베이스 접근 오류: {table_info['error']}")
                continue
            
            print(f"\n📋 테이블 현황:")
            if not table_info:
                print("   ❌ 테이블이 없습니다")
                continue
            
            total_records = 0
            for table_name, info in table_info.items():
                count = info.get('count', 0)
                total_records += count
                
                # 데이터 신선도 분석
                freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                
                print(f"   📊 {table_name}: {count:,}개 레코드")
                print(f"      컬럼: {info.get('columns', 0)}개")
                if info.get('latest_data'):
                    print(f"      최신 데이터: {freshness} ({latest})")
                
                # 에러가 있다면 표시
                if 'error' in info:
                    print(f"      ❌ 오류: {info['error']}")
            
            print(f"\n📈 총 레코드 수: {total_records:,}개")
            
            # 건강도 점수 계산
            db_info = {
                'file_info': file_info,
                'table_info': table_info,
                'expected_tables': db_config['expected_tables']
            }
            
            score, grade, issues = self.get_database_health_score(db_info)
            print(f"🏆 건강도: {score}/100점 ({grade})")
            
            if issues:
                print(f"⚠️  문제점:")
                for issue in issues:
                    print(f"     • {issue}")
            
            # 결과 저장
            self.results[db_file] = {
                'config': db_config,
                'file_info': file_info,
                'table_info': table_info,
                'score': score,
                'grade': grade,
                'issues': issues,
                'total_records': total_records
            }
    
    def show_summary(self):
        """종합 요약 보고서"""
        print("\n" + "=" * 80)
        print("📋 종합 요약 보고서")
        print("=" * 80)
        
        # 전체 통계
        total_size = sum(r['file_info']['size'] for r in self.results.values() if r['file_info']['exists'])
        total_records = sum(r['total_records'] for r in self.results.values())
        avg_score = sum(r['score'] for r in self.results.values()) / len(self.results) if self.results else 0
        
        print(f"💾 전체 데이터베이스 크기: {humanize.naturalsize(total_size)}")
        print(f"📊 전체 레코드 수: {total_records:,}개")
        print(f"🏆 평균 건강도: {avg_score:.1f}/100점")
        
        # 데이터베이스별 요약
        print(f"\n📈 데이터베이스별 상태:")
        print(f"{'데이터베이스':<15} {'상태':<8} {'레코드 수':<12} {'건강도':<10} {'주요 문제'}")
        print("-" * 70)
        
        for db_file, result in self.results.items():
            name = result['config']['name'][:12]
            status = "정상" if result['file_info']['exists'] else "없음"
            records = f"{result['total_records']:,}" if result['total_records'] > 0 else "0"
            score = f"{result['score']}/100"
            main_issue = result['issues'][0] if result['issues'] else "없음"
            
            print(f"{name:<15} {status:<8} {records:<12} {score:<10} {main_issue}")
        
        # 권장사항
        print(f"\n💡 권장사항:")
        
        # 심각한 문제가 있는 DB 찾기
        critical_dbs = [db for db, result in self.results.items() if result['score'] < 40]
        if critical_dbs:
            print(f"🚨 긴급 수정 필요:")
            for db in critical_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {', '.join(result['issues'])}")
        
        # 데이터가 적은 DB 찾기
        low_data_dbs = [db for db, result in self.results.items() if result['total_records'] < 1000 and result['file_info']['exists']]
        if low_data_dbs:
            print(f"📈 데이터 수집 권장:")
            for db in low_data_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: 현재 {result['total_records']:,}개 레코드")
        
        # 높은 점수 DB (칭찬)
        good_dbs = [db for db, result in self.results.items() if result['score'] >= 80]
        if good_dbs:
            print(f"✅ 잘 관리된 데이터베이스:")
            for db in good_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {result['score']}/100점")
    
    def show_collection_status(self):
        """데이터 수집 현황 상세 분석"""
        print("\n" + "=" * 80)
        print("📊 데이터 수집 현황 상세 분석")
        print("=" * 80)
        
        # 주요 테이블별 수집 현황
        important_tables = {
            'stock_data.db': {
                'company_info': '기업 기본정보',
                'stock_prices': '주가 데이터',
                'financial_ratios': '재무비율'
            },
            'dart_data.db': {
                'corp_codes': '기업코드',
                'financial_statements': '재무제표',
                'disclosures': '공시정보'
            },
            'news_data.db': {
                'news_articles': '뉴스 기사',
                'sentiment_scores': '감정 점수'
            },
            'kis_data.db': {
                'realtime_quotes': '실시간 시세'
            }
        }
        
        for db_file, tables in important_tables.items():
            if db_file in self.results:
                result = self.results[db_file]
                print(f"\n🗃️  {result['config']['name']}:")
                
                for table_name, description in tables.items():
                    if table_name in result['table_info']:
                        info = result['table_info'][table_name]
                        count = info.get('count', 0)
                        freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                        
                        print(f"   📊 {description} ({table_name})")
                        print(f"      📈 레코드: {count:,}개")
                        print(f"      🕒 최신: {freshness}")
                        
                        # 수집 상태 평가
                        if count == 0:
                            print(f"      🚨 상태: 데이터 없음 - 수집 필요")
                        elif count < 100:
                            print(f"      ⚠️  상태: 데이터 부족 - 추가 수집 권장")
                        elif '🔴' in freshness:
                            print(f"      ⏰ 상태: 데이터 오래됨 - 업데이트 필요")
                        else:
                            print(f"      ✅ 상태: 양호")
                    else:
                        print(f"   📊 {description} ({table_name})")
                        print(f"      ❌ 테이블 없음 - 생성 및 수집 필요")

def main():
    """메인 함수"""
    print("🚀 Finance Data Vibe - 전체 데이터베이스 상태 점검")
    print("=" * 80)
    
    # 데이터베이스 경로 확인
    db_path = Path("data/databases")
    if not db_path.exists():
        # 현재 디렉터리에서 찾기
        current_files = list(Path(".").glob("*.db"))
        if current_files:
            db_path = Path(".")
        else:
            print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
            print("예상 위치: data/databases/")
            return
    
    # 점검 실행
    checker = DatabaseStatusChecker(db_path)
    checker.check_all_databases()
    checker.show_summary()
    checker.show_collection_status()
    
    print(f"\n✅ 전체 점검 완료!")
    print(f"💡 상세한 테이블별 분석이 필요하면 개별 체커를 사용하세요:")
    print(f"   • python company_info_checker.py  # 기업정보 상세 분석")
    print(f"   • python news_data_checker.py     # 뉴스 데이터 상세 분석")

if __name__ == "__main__":
    main()),
            
            # With microseconds
            ('%Y-%m-%d %H:%M:%S.%f', r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+
    
    def get_database_health_score(self, db_info):
        """데이터베이스 건강도 점수 계산"""
        if not db_info.get('file_info', {}).get('exists'):
            return 0, "파일 없음"
        
        score = 0
        issues = []
        
        # 파일 크기 점수 (10점)
        size = db_info['file_info']['size']
        if size > 1000000:  # 1MB 이상
            score += 10
        elif size > 100000:  # 100KB 이상
            score += 5
        else:
            issues.append("파일 크기 작음")
        
        # 테이블 존재 점수 (30점)
        expected_tables = set(db_info['expected_tables'])
        actual_tables = set(db_info['table_info'].keys())
        table_coverage = len(actual_tables & expected_tables) / len(expected_tables)
        score += int(table_coverage * 30)
        
        if table_coverage < 1.0:
            missing = expected_tables - actual_tables
            issues.append(f"테이블 누락: {', '.join(missing)}")
        
        # 데이터 존재 점수 (40점)
        total_records = sum(info.get('count', 0) for info in db_info['table_info'].values())
        if total_records > 10000:
            score += 40
        elif total_records > 1000:
            score += 30
        elif total_records > 100:
            score += 20
        elif total_records > 0:
            score += 10
        else:
            issues.append("데이터 없음")
        
        # 데이터 신선도 점수 (20점)
        fresh_tables = 0
        total_tables = len(actual_tables)
        
        for table_info in db_info['table_info'].values():
            if table_info.get('latest_data'):
                freshness, _ = self.analyze_data_freshness(table_info['latest_data'])
                if '🟢' in freshness or '🟡' in freshness:
                    fresh_tables += 1
        
        if total_tables > 0:
            freshness_ratio = fresh_tables / total_tables
            score += int(freshness_ratio * 20)
        
        if fresh_tables == 0 and total_tables > 0:
            issues.append("데이터 오래됨")
        
        # 등급 결정
        if score >= 90:
            grade = "A+ 우수"
        elif score >= 80:
            grade = "A 양호"
        elif score >= 70:
            grade = "B+ 보통"
        elif score >= 60:
            grade = "B 미흡"
        elif score >= 40:
            grade = "C 불량"
        else:
            grade = "D 심각"
        
        return score, grade, issues
    
    def check_all_databases(self):
        """모든 데이터베이스 상태 점검"""
        print("🔍 전체 데이터베이스 상태 점검 시작")
        print("=" * 80)
        
        for db_file, db_config in self.databases.items():
            print(f"\n📊 {db_config['name']} ({db_file})")
            print("-" * 60)
            
            # 파일 정보
            file_info = self.get_file_info(db_file)
            print(f"📁 파일 정보:")
            print(f"   존재: {'✅' if file_info['exists'] else '❌'}")
            if file_info['exists']:
                print(f"   크기: {file_info['size_human']}")
                print(f"   수정일: {file_info['modified']}")
            
            # 테이블 정보
            table_info = self.get_table_info(db_file)
            
            if 'error' in table_info:
                print(f"❌ 데이터베이스 접근 오류: {table_info['error']}")
                continue
            
            print(f"\n📋 테이블 현황:")
            if not table_info:
                print("   ❌ 테이블이 없습니다")
                continue
            
            total_records = 0
            for table_name, info in table_info.items():
                count = info.get('count', 0)
                total_records += count
                
                # 데이터 신선도 분석
                freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                
                print(f"   📊 {table_name}: {count:,}개 레코드")
                print(f"      컬럼: {info.get('columns', 0)}개")
                if info.get('latest_data'):
                    print(f"      최신 데이터: {freshness} ({latest})")
                
                # 에러가 있다면 표시
                if 'error' in info:
                    print(f"      ❌ 오류: {info['error']}")
            
            print(f"\n📈 총 레코드 수: {total_records:,}개")
            
            # 건강도 점수 계산
            db_info = {
                'file_info': file_info,
                'table_info': table_info,
                'expected_tables': db_config['expected_tables']
            }
            
            score, grade, issues = self.get_database_health_score(db_info)
            print(f"🏆 건강도: {score}/100점 ({grade})")
            
            if issues:
                print(f"⚠️  문제점:")
                for issue in issues:
                    print(f"     • {issue}")
            
            # 결과 저장
            self.results[db_file] = {
                'config': db_config,
                'file_info': file_info,
                'table_info': table_info,
                'score': score,
                'grade': grade,
                'issues': issues,
                'total_records': total_records
            }
    
    def show_summary(self):
        """종합 요약 보고서"""
        print("\n" + "=" * 80)
        print("📋 종합 요약 보고서")
        print("=" * 80)
        
        # 전체 통계
        total_size = sum(r['file_info']['size'] for r in self.results.values() if r['file_info']['exists'])
        total_records = sum(r['total_records'] for r in self.results.values())
        avg_score = sum(r['score'] for r in self.results.values()) / len(self.results) if self.results else 0
        
        print(f"💾 전체 데이터베이스 크기: {humanize.naturalsize(total_size)}")
        print(f"📊 전체 레코드 수: {total_records:,}개")
        print(f"🏆 평균 건강도: {avg_score:.1f}/100점")
        
        # 데이터베이스별 요약
        print(f"\n📈 데이터베이스별 상태:")
        print(f"{'데이터베이스':<15} {'상태':<8} {'레코드 수':<12} {'건강도':<10} {'주요 문제'}")
        print("-" * 70)
        
        for db_file, result in self.results.items():
            name = result['config']['name'][:12]
            status = "정상" if result['file_info']['exists'] else "없음"
            records = f"{result['total_records']:,}" if result['total_records'] > 0 else "0"
            score = f"{result['score']}/100"
            main_issue = result['issues'][0] if result['issues'] else "없음"
            
            print(f"{name:<15} {status:<8} {records:<12} {score:<10} {main_issue}")
        
        # 권장사항
        print(f"\n💡 권장사항:")
        
        # 심각한 문제가 있는 DB 찾기
        critical_dbs = [db for db, result in self.results.items() if result['score'] < 40]
        if critical_dbs:
            print(f"🚨 긴급 수정 필요:")
            for db in critical_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {', '.join(result['issues'])}")
        
        # 데이터가 적은 DB 찾기
        low_data_dbs = [db for db, result in self.results.items() if result['total_records'] < 1000 and result['file_info']['exists']]
        if low_data_dbs:
            print(f"📈 데이터 수집 권장:")
            for db in low_data_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: 현재 {result['total_records']:,}개 레코드")
        
        # 높은 점수 DB (칭찬)
        good_dbs = [db for db, result in self.results.items() if result['score'] >= 80]
        if good_dbs:
            print(f"✅ 잘 관리된 데이터베이스:")
            for db in good_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {result['score']}/100점")
    
    def show_collection_status(self):
        """데이터 수집 현황 상세 분석"""
        print("\n" + "=" * 80)
        print("📊 데이터 수집 현황 상세 분석")
        print("=" * 80)
        
        # 주요 테이블별 수집 현황
        important_tables = {
            'stock_data.db': {
                'company_info': '기업 기본정보',
                'stock_prices': '주가 데이터',
                'financial_ratios': '재무비율'
            },
            'dart_data.db': {
                'corp_codes': '기업코드',
                'financial_statements': '재무제표',
                'disclosures': '공시정보'
            },
            'news_data.db': {
                'news_articles': '뉴스 기사',
                'sentiment_scores': '감정 점수'
            },
            'kis_data.db': {
                'realtime_quotes': '실시간 시세'
            }
        }
        
        for db_file, tables in important_tables.items():
            if db_file in self.results:
                result = self.results[db_file]
                print(f"\n🗃️  {result['config']['name']}:")
                
                for table_name, description in tables.items():
                    if table_name in result['table_info']:
                        info = result['table_info'][table_name]
                        count = info.get('count', 0)
                        freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                        
                        print(f"   📊 {description} ({table_name})")
                        print(f"      📈 레코드: {count:,}개")
                        print(f"      🕒 최신: {freshness}")
                        
                        # 수집 상태 평가
                        if count == 0:
                            print(f"      🚨 상태: 데이터 없음 - 수집 필요")
                        elif count < 100:
                            print(f"      ⚠️  상태: 데이터 부족 - 추가 수집 권장")
                        elif '🔴' in freshness:
                            print(f"      ⏰ 상태: 데이터 오래됨 - 업데이트 필요")
                        else:
                            print(f"      ✅ 상태: 양호")
                    else:
                        print(f"   📊 {description} ({table_name})")
                        print(f"      ❌ 테이블 없음 - 생성 및 수집 필요")

def main():
    """메인 함수"""
    print("🚀 Finance Data Vibe - 전체 데이터베이스 상태 점검")
    print("=" * 80)
    
    # 데이터베이스 경로 확인
    db_path = Path("data/databases")
    if not db_path.exists():
        # 현재 디렉터리에서 찾기
        current_files = list(Path(".").glob("*.db"))
        if current_files:
            db_path = Path(".")
        else:
            print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
            print("예상 위치: data/databases/")
            return
    
    # 점검 실행
    checker = DatabaseStatusChecker(db_path)
    checker.check_all_databases()
    checker.show_summary()
    checker.show_collection_status()
    
    print(f"\n✅ 전체 점검 완료!")
    print(f"💡 상세한 테이블별 분석이 필요하면 개별 체커를 사용하세요:")
    print(f"   • python company_info_checker.py  # 기업정보 상세 분석")
    print(f"   • python news_data_checker.py     # 뉴스 데이터 상세 분석")

if __name__ == "__main__":
    main()),
            ('%Y-%m-%dT%H:%M:%S.%f', r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+
    
    def get_database_health_score(self, db_info):
        """데이터베이스 건강도 점수 계산"""
        if not db_info.get('file_info', {}).get('exists'):
            return 0, "파일 없음"
        
        score = 0
        issues = []
        
        # 파일 크기 점수 (10점)
        size = db_info['file_info']['size']
        if size > 1000000:  # 1MB 이상
            score += 10
        elif size > 100000:  # 100KB 이상
            score += 5
        else:
            issues.append("파일 크기 작음")
        
        # 테이블 존재 점수 (30점)
        expected_tables = set(db_info['expected_tables'])
        actual_tables = set(db_info['table_info'].keys())
        table_coverage = len(actual_tables & expected_tables) / len(expected_tables)
        score += int(table_coverage * 30)
        
        if table_coverage < 1.0:
            missing = expected_tables - actual_tables
            issues.append(f"테이블 누락: {', '.join(missing)}")
        
        # 데이터 존재 점수 (40점)
        total_records = sum(info.get('count', 0) for info in db_info['table_info'].values())
        if total_records > 10000:
            score += 40
        elif total_records > 1000:
            score += 30
        elif total_records > 100:
            score += 20
        elif total_records > 0:
            score += 10
        else:
            issues.append("데이터 없음")
        
        # 데이터 신선도 점수 (20점)
        fresh_tables = 0
        total_tables = len(actual_tables)
        
        for table_info in db_info['table_info'].values():
            if table_info.get('latest_data'):
                freshness, _ = self.analyze_data_freshness(table_info['latest_data'])
                if '🟢' in freshness or '🟡' in freshness:
                    fresh_tables += 1
        
        if total_tables > 0:
            freshness_ratio = fresh_tables / total_tables
            score += int(freshness_ratio * 20)
        
        if fresh_tables == 0 and total_tables > 0:
            issues.append("데이터 오래됨")
        
        # 등급 결정
        if score >= 90:
            grade = "A+ 우수"
        elif score >= 80:
            grade = "A 양호"
        elif score >= 70:
            grade = "B+ 보통"
        elif score >= 60:
            grade = "B 미흡"
        elif score >= 40:
            grade = "C 불량"
        else:
            grade = "D 심각"
        
        return score, grade, issues
    
    def check_all_databases(self):
        """모든 데이터베이스 상태 점검"""
        print("🔍 전체 데이터베이스 상태 점검 시작")
        print("=" * 80)
        
        for db_file, db_config in self.databases.items():
            print(f"\n📊 {db_config['name']} ({db_file})")
            print("-" * 60)
            
            # 파일 정보
            file_info = self.get_file_info(db_file)
            print(f"📁 파일 정보:")
            print(f"   존재: {'✅' if file_info['exists'] else '❌'}")
            if file_info['exists']:
                print(f"   크기: {file_info['size_human']}")
                print(f"   수정일: {file_info['modified']}")
            
            # 테이블 정보
            table_info = self.get_table_info(db_file)
            
            if 'error' in table_info:
                print(f"❌ 데이터베이스 접근 오류: {table_info['error']}")
                continue
            
            print(f"\n📋 테이블 현황:")
            if not table_info:
                print("   ❌ 테이블이 없습니다")
                continue
            
            total_records = 0
            for table_name, info in table_info.items():
                count = info.get('count', 0)
                total_records += count
                
                # 데이터 신선도 분석
                freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                
                print(f"   📊 {table_name}: {count:,}개 레코드")
                print(f"      컬럼: {info.get('columns', 0)}개")
                if info.get('latest_data'):
                    print(f"      최신 데이터: {freshness} ({latest})")
                
                # 에러가 있다면 표시
                if 'error' in info:
                    print(f"      ❌ 오류: {info['error']}")
            
            print(f"\n📈 총 레코드 수: {total_records:,}개")
            
            # 건강도 점수 계산
            db_info = {
                'file_info': file_info,
                'table_info': table_info,
                'expected_tables': db_config['expected_tables']
            }
            
            score, grade, issues = self.get_database_health_score(db_info)
            print(f"🏆 건강도: {score}/100점 ({grade})")
            
            if issues:
                print(f"⚠️  문제점:")
                for issue in issues:
                    print(f"     • {issue}")
            
            # 결과 저장
            self.results[db_file] = {
                'config': db_config,
                'file_info': file_info,
                'table_info': table_info,
                'score': score,
                'grade': grade,
                'issues': issues,
                'total_records': total_records
            }
    
    def show_summary(self):
        """종합 요약 보고서"""
        print("\n" + "=" * 80)
        print("📋 종합 요약 보고서")
        print("=" * 80)
        
        # 전체 통계
        total_size = sum(r['file_info']['size'] for r in self.results.values() if r['file_info']['exists'])
        total_records = sum(r['total_records'] for r in self.results.values())
        avg_score = sum(r['score'] for r in self.results.values()) / len(self.results) if self.results else 0
        
        print(f"💾 전체 데이터베이스 크기: {humanize.naturalsize(total_size)}")
        print(f"📊 전체 레코드 수: {total_records:,}개")
        print(f"🏆 평균 건강도: {avg_score:.1f}/100점")
        
        # 데이터베이스별 요약
        print(f"\n📈 데이터베이스별 상태:")
        print(f"{'데이터베이스':<15} {'상태':<8} {'레코드 수':<12} {'건강도':<10} {'주요 문제'}")
        print("-" * 70)
        
        for db_file, result in self.results.items():
            name = result['config']['name'][:12]
            status = "정상" if result['file_info']['exists'] else "없음"
            records = f"{result['total_records']:,}" if result['total_records'] > 0 else "0"
            score = f"{result['score']}/100"
            main_issue = result['issues'][0] if result['issues'] else "없음"
            
            print(f"{name:<15} {status:<8} {records:<12} {score:<10} {main_issue}")
        
        # 권장사항
        print(f"\n💡 권장사항:")
        
        # 심각한 문제가 있는 DB 찾기
        critical_dbs = [db for db, result in self.results.items() if result['score'] < 40]
        if critical_dbs:
            print(f"🚨 긴급 수정 필요:")
            for db in critical_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {', '.join(result['issues'])}")
        
        # 데이터가 적은 DB 찾기
        low_data_dbs = [db for db, result in self.results.items() if result['total_records'] < 1000 and result['file_info']['exists']]
        if low_data_dbs:
            print(f"📈 데이터 수집 권장:")
            for db in low_data_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: 현재 {result['total_records']:,}개 레코드")
        
        # 높은 점수 DB (칭찬)
        good_dbs = [db for db, result in self.results.items() if result['score'] >= 80]
        if good_dbs:
            print(f"✅ 잘 관리된 데이터베이스:")
            for db in good_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {result['score']}/100점")
    
    def show_collection_status(self):
        """데이터 수집 현황 상세 분석"""
        print("\n" + "=" * 80)
        print("📊 데이터 수집 현황 상세 분석")
        print("=" * 80)
        
        # 주요 테이블별 수집 현황
        important_tables = {
            'stock_data.db': {
                'company_info': '기업 기본정보',
                'stock_prices': '주가 데이터',
                'financial_ratios': '재무비율'
            },
            'dart_data.db': {
                'corp_codes': '기업코드',
                'financial_statements': '재무제표',
                'disclosures': '공시정보'
            },
            'news_data.db': {
                'news_articles': '뉴스 기사',
                'sentiment_scores': '감정 점수'
            },
            'kis_data.db': {
                'realtime_quotes': '실시간 시세'
            }
        }
        
        for db_file, tables in important_tables.items():
            if db_file in self.results:
                result = self.results[db_file]
                print(f"\n🗃️  {result['config']['name']}:")
                
                for table_name, description in tables.items():
                    if table_name in result['table_info']:
                        info = result['table_info'][table_name]
                        count = info.get('count', 0)
                        freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                        
                        print(f"   📊 {description} ({table_name})")
                        print(f"      📈 레코드: {count:,}개")
                        print(f"      🕒 최신: {freshness}")
                        
                        # 수집 상태 평가
                        if count == 0:
                            print(f"      🚨 상태: 데이터 없음 - 수집 필요")
                        elif count < 100:
                            print(f"      ⚠️  상태: 데이터 부족 - 추가 수집 권장")
                        elif '🔴' in freshness:
                            print(f"      ⏰ 상태: 데이터 오래됨 - 업데이트 필요")
                        else:
                            print(f"      ✅ 상태: 양호")
                    else:
                        print(f"   📊 {description} ({table_name})")
                        print(f"      ❌ 테이블 없음 - 생성 및 수집 필요")

def main():
    """메인 함수"""
    print("🚀 Finance Data Vibe - 전체 데이터베이스 상태 점검")
    print("=" * 80)
    
    # 데이터베이스 경로 확인
    db_path = Path("data/databases")
    if not db_path.exists():
        # 현재 디렉터리에서 찾기
        current_files = list(Path(".").glob("*.db"))
        if current_files:
            db_path = Path(".")
        else:
            print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
            print("예상 위치: data/databases/")
            return
    
    # 점검 실행
    checker = DatabaseStatusChecker(db_path)
    checker.check_all_databases()
    checker.show_summary()
    checker.show_collection_status()
    
    print(f"\n✅ 전체 점검 완료!")
    print(f"💡 상세한 테이블별 분석이 필요하면 개별 체커를 사용하세요:")
    print(f"   • python company_info_checker.py  # 기업정보 상세 분석")
    print(f"   • python news_data_checker.py     # 뉴스 데이터 상세 분석")

if __name__ == "__main__":
    main()),
            
            # Compact formats
            ('%Y%m%d', r'^\d{8}
    
    def get_database_health_score(self, db_info):
        """데이터베이스 건강도 점수 계산"""
        if not db_info.get('file_info', {}).get('exists'):
            return 0, "파일 없음"
        
        score = 0
        issues = []
        
        # 파일 크기 점수 (10점)
        size = db_info['file_info']['size']
        if size > 1000000:  # 1MB 이상
            score += 10
        elif size > 100000:  # 100KB 이상
            score += 5
        else:
            issues.append("파일 크기 작음")
        
        # 테이블 존재 점수 (30점)
        expected_tables = set(db_info['expected_tables'])
        actual_tables = set(db_info['table_info'].keys())
        table_coverage = len(actual_tables & expected_tables) / len(expected_tables)
        score += int(table_coverage * 30)
        
        if table_coverage < 1.0:
            missing = expected_tables - actual_tables
            issues.append(f"테이블 누락: {', '.join(missing)}")
        
        # 데이터 존재 점수 (40점)
        total_records = sum(info.get('count', 0) for info in db_info['table_info'].values())
        if total_records > 10000:
            score += 40
        elif total_records > 1000:
            score += 30
        elif total_records > 100:
            score += 20
        elif total_records > 0:
            score += 10
        else:
            issues.append("데이터 없음")
        
        # 데이터 신선도 점수 (20점)
        fresh_tables = 0
        total_tables = len(actual_tables)
        
        for table_info in db_info['table_info'].values():
            if table_info.get('latest_data'):
                freshness, _ = self.analyze_data_freshness(table_info['latest_data'])
                if '🟢' in freshness or '🟡' in freshness:
                    fresh_tables += 1
        
        if total_tables > 0:
            freshness_ratio = fresh_tables / total_tables
            score += int(freshness_ratio * 20)
        
        if fresh_tables == 0 and total_tables > 0:
            issues.append("데이터 오래됨")
        
        # 등급 결정
        if score >= 90:
            grade = "A+ 우수"
        elif score >= 80:
            grade = "A 양호"
        elif score >= 70:
            grade = "B+ 보통"
        elif score >= 60:
            grade = "B 미흡"
        elif score >= 40:
            grade = "C 불량"
        else:
            grade = "D 심각"
        
        return score, grade, issues
    
    def check_all_databases(self):
        """모든 데이터베이스 상태 점검"""
        print("🔍 전체 데이터베이스 상태 점검 시작")
        print("=" * 80)
        
        for db_file, db_config in self.databases.items():
            print(f"\n📊 {db_config['name']} ({db_file})")
            print("-" * 60)
            
            # 파일 정보
            file_info = self.get_file_info(db_file)
            print(f"📁 파일 정보:")
            print(f"   존재: {'✅' if file_info['exists'] else '❌'}")
            if file_info['exists']:
                print(f"   크기: {file_info['size_human']}")
                print(f"   수정일: {file_info['modified']}")
            
            # 테이블 정보
            table_info = self.get_table_info(db_file)
            
            if 'error' in table_info:
                print(f"❌ 데이터베이스 접근 오류: {table_info['error']}")
                continue
            
            print(f"\n📋 테이블 현황:")
            if not table_info:
                print("   ❌ 테이블이 없습니다")
                continue
            
            total_records = 0
            for table_name, info in table_info.items():
                count = info.get('count', 0)
                total_records += count
                
                # 데이터 신선도 분석
                freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                
                print(f"   📊 {table_name}: {count:,}개 레코드")
                print(f"      컬럼: {info.get('columns', 0)}개")
                if info.get('latest_data'):
                    print(f"      최신 데이터: {freshness} ({latest})")
                
                # 에러가 있다면 표시
                if 'error' in info:
                    print(f"      ❌ 오류: {info['error']}")
            
            print(f"\n📈 총 레코드 수: {total_records:,}개")
            
            # 건강도 점수 계산
            db_info = {
                'file_info': file_info,
                'table_info': table_info,
                'expected_tables': db_config['expected_tables']
            }
            
            score, grade, issues = self.get_database_health_score(db_info)
            print(f"🏆 건강도: {score}/100점 ({grade})")
            
            if issues:
                print(f"⚠️  문제점:")
                for issue in issues:
                    print(f"     • {issue}")
            
            # 결과 저장
            self.results[db_file] = {
                'config': db_config,
                'file_info': file_info,
                'table_info': table_info,
                'score': score,
                'grade': grade,
                'issues': issues,
                'total_records': total_records
            }
    
    def show_summary(self):
        """종합 요약 보고서"""
        print("\n" + "=" * 80)
        print("📋 종합 요약 보고서")
        print("=" * 80)
        
        # 전체 통계
        total_size = sum(r['file_info']['size'] for r in self.results.values() if r['file_info']['exists'])
        total_records = sum(r['total_records'] for r in self.results.values())
        avg_score = sum(r['score'] for r in self.results.values()) / len(self.results) if self.results else 0
        
        print(f"💾 전체 데이터베이스 크기: {humanize.naturalsize(total_size)}")
        print(f"📊 전체 레코드 수: {total_records:,}개")
        print(f"🏆 평균 건강도: {avg_score:.1f}/100점")
        
        # 데이터베이스별 요약
        print(f"\n📈 데이터베이스별 상태:")
        print(f"{'데이터베이스':<15} {'상태':<8} {'레코드 수':<12} {'건강도':<10} {'주요 문제'}")
        print("-" * 70)
        
        for db_file, result in self.results.items():
            name = result['config']['name'][:12]
            status = "정상" if result['file_info']['exists'] else "없음"
            records = f"{result['total_records']:,}" if result['total_records'] > 0 else "0"
            score = f"{result['score']}/100"
            main_issue = result['issues'][0] if result['issues'] else "없음"
            
            print(f"{name:<15} {status:<8} {records:<12} {score:<10} {main_issue}")
        
        # 권장사항
        print(f"\n💡 권장사항:")
        
        # 심각한 문제가 있는 DB 찾기
        critical_dbs = [db for db, result in self.results.items() if result['score'] < 40]
        if critical_dbs:
            print(f"🚨 긴급 수정 필요:")
            for db in critical_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {', '.join(result['issues'])}")
        
        # 데이터가 적은 DB 찾기
        low_data_dbs = [db for db, result in self.results.items() if result['total_records'] < 1000 and result['file_info']['exists']]
        if low_data_dbs:
            print(f"📈 데이터 수집 권장:")
            for db in low_data_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: 현재 {result['total_records']:,}개 레코드")
        
        # 높은 점수 DB (칭찬)
        good_dbs = [db for db, result in self.results.items() if result['score'] >= 80]
        if good_dbs:
            print(f"✅ 잘 관리된 데이터베이스:")
            for db in good_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {result['score']}/100점")
    
    def show_collection_status(self):
        """데이터 수집 현황 상세 분석"""
        print("\n" + "=" * 80)
        print("📊 데이터 수집 현황 상세 분석")
        print("=" * 80)
        
        # 주요 테이블별 수집 현황
        important_tables = {
            'stock_data.db': {
                'company_info': '기업 기본정보',
                'stock_prices': '주가 데이터',
                'financial_ratios': '재무비율'
            },
            'dart_data.db': {
                'corp_codes': '기업코드',
                'financial_statements': '재무제표',
                'disclosures': '공시정보'
            },
            'news_data.db': {
                'news_articles': '뉴스 기사',
                'sentiment_scores': '감정 점수'
            },
            'kis_data.db': {
                'realtime_quotes': '실시간 시세'
            }
        }
        
        for db_file, tables in important_tables.items():
            if db_file in self.results:
                result = self.results[db_file]
                print(f"\n🗃️  {result['config']['name']}:")
                
                for table_name, description in tables.items():
                    if table_name in result['table_info']:
                        info = result['table_info'][table_name]
                        count = info.get('count', 0)
                        freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                        
                        print(f"   📊 {description} ({table_name})")
                        print(f"      📈 레코드: {count:,}개")
                        print(f"      🕒 최신: {freshness}")
                        
                        # 수집 상태 평가
                        if count == 0:
                            print(f"      🚨 상태: 데이터 없음 - 수집 필요")
                        elif count < 100:
                            print(f"      ⚠️  상태: 데이터 부족 - 추가 수집 권장")
                        elif '🔴' in freshness:
                            print(f"      ⏰ 상태: 데이터 오래됨 - 업데이트 필요")
                        else:
                            print(f"      ✅ 상태: 양호")
                    else:
                        print(f"   📊 {description} ({table_name})")
                        print(f"      ❌ 테이블 없음 - 생성 및 수집 필요")

def main():
    """메인 함수"""
    print("🚀 Finance Data Vibe - 전체 데이터베이스 상태 점검")
    print("=" * 80)
    
    # 데이터베이스 경로 확인
    db_path = Path("data/databases")
    if not db_path.exists():
        # 현재 디렉터리에서 찾기
        current_files = list(Path(".").glob("*.db"))
        if current_files:
            db_path = Path(".")
        else:
            print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
            print("예상 위치: data/databases/")
            return
    
    # 점검 실행
    checker = DatabaseStatusChecker(db_path)
    checker.check_all_databases()
    checker.show_summary()
    checker.show_collection_status()
    
    print(f"\n✅ 전체 점검 완료!")
    print(f"💡 상세한 테이블별 분석이 필요하면 개별 체커를 사용하세요:")
    print(f"   • python company_info_checker.py  # 기업정보 상세 분석")
    print(f"   • python news_data_checker.py     # 뉴스 데이터 상세 분석")

if __name__ == "__main__":
    main()),
            ('%Y%m%d%H%M%S', r'^\d{14}
    
    def get_database_health_score(self, db_info):
        """데이터베이스 건강도 점수 계산"""
        if not db_info.get('file_info', {}).get('exists'):
            return 0, "파일 없음"
        
        score = 0
        issues = []
        
        # 파일 크기 점수 (10점)
        size = db_info['file_info']['size']
        if size > 1000000:  # 1MB 이상
            score += 10
        elif size > 100000:  # 100KB 이상
            score += 5
        else:
            issues.append("파일 크기 작음")
        
        # 테이블 존재 점수 (30점)
        expected_tables = set(db_info['expected_tables'])
        actual_tables = set(db_info['table_info'].keys())
        table_coverage = len(actual_tables & expected_tables) / len(expected_tables)
        score += int(table_coverage * 30)
        
        if table_coverage < 1.0:
            missing = expected_tables - actual_tables
            issues.append(f"테이블 누락: {', '.join(missing)}")
        
        # 데이터 존재 점수 (40점)
        total_records = sum(info.get('count', 0) for info in db_info['table_info'].values())
        if total_records > 10000:
            score += 40
        elif total_records > 1000:
            score += 30
        elif total_records > 100:
            score += 20
        elif total_records > 0:
            score += 10
        else:
            issues.append("데이터 없음")
        
        # 데이터 신선도 점수 (20점)
        fresh_tables = 0
        total_tables = len(actual_tables)
        
        for table_info in db_info['table_info'].values():
            if table_info.get('latest_data'):
                freshness, _ = self.analyze_data_freshness(table_info['latest_data'])
                if '🟢' in freshness or '🟡' in freshness:
                    fresh_tables += 1
        
        if total_tables > 0:
            freshness_ratio = fresh_tables / total_tables
            score += int(freshness_ratio * 20)
        
        if fresh_tables == 0 and total_tables > 0:
            issues.append("데이터 오래됨")
        
        # 등급 결정
        if score >= 90:
            grade = "A+ 우수"
        elif score >= 80:
            grade = "A 양호"
        elif score >= 70:
            grade = "B+ 보통"
        elif score >= 60:
            grade = "B 미흡"
        elif score >= 40:
            grade = "C 불량"
        else:
            grade = "D 심각"
        
        return score, grade, issues
    
    def check_all_databases(self):
        """모든 데이터베이스 상태 점검"""
        print("🔍 전체 데이터베이스 상태 점검 시작")
        print("=" * 80)
        
        for db_file, db_config in self.databases.items():
            print(f"\n📊 {db_config['name']} ({db_file})")
            print("-" * 60)
            
            # 파일 정보
            file_info = self.get_file_info(db_file)
            print(f"📁 파일 정보:")
            print(f"   존재: {'✅' if file_info['exists'] else '❌'}")
            if file_info['exists']:
                print(f"   크기: {file_info['size_human']}")
                print(f"   수정일: {file_info['modified']}")
            
            # 테이블 정보
            table_info = self.get_table_info(db_file)
            
            if 'error' in table_info:
                print(f"❌ 데이터베이스 접근 오류: {table_info['error']}")
                continue
            
            print(f"\n📋 테이블 현황:")
            if not table_info:
                print("   ❌ 테이블이 없습니다")
                continue
            
            total_records = 0
            for table_name, info in table_info.items():
                count = info.get('count', 0)
                total_records += count
                
                # 데이터 신선도 분석
                freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                
                print(f"   📊 {table_name}: {count:,}개 레코드")
                print(f"      컬럼: {info.get('columns', 0)}개")
                if info.get('latest_data'):
                    print(f"      최신 데이터: {freshness} ({latest})")
                
                # 에러가 있다면 표시
                if 'error' in info:
                    print(f"      ❌ 오류: {info['error']}")
            
            print(f"\n📈 총 레코드 수: {total_records:,}개")
            
            # 건강도 점수 계산
            db_info = {
                'file_info': file_info,
                'table_info': table_info,
                'expected_tables': db_config['expected_tables']
            }
            
            score, grade, issues = self.get_database_health_score(db_info)
            print(f"🏆 건강도: {score}/100점 ({grade})")
            
            if issues:
                print(f"⚠️  문제점:")
                for issue in issues:
                    print(f"     • {issue}")
            
            # 결과 저장
            self.results[db_file] = {
                'config': db_config,
                'file_info': file_info,
                'table_info': table_info,
                'score': score,
                'grade': grade,
                'issues': issues,
                'total_records': total_records
            }
    
    def show_summary(self):
        """종합 요약 보고서"""
        print("\n" + "=" * 80)
        print("📋 종합 요약 보고서")
        print("=" * 80)
        
        # 전체 통계
        total_size = sum(r['file_info']['size'] for r in self.results.values() if r['file_info']['exists'])
        total_records = sum(r['total_records'] for r in self.results.values())
        avg_score = sum(r['score'] for r in self.results.values()) / len(self.results) if self.results else 0
        
        print(f"💾 전체 데이터베이스 크기: {humanize.naturalsize(total_size)}")
        print(f"📊 전체 레코드 수: {total_records:,}개")
        print(f"🏆 평균 건강도: {avg_score:.1f}/100점")
        
        # 데이터베이스별 요약
        print(f"\n📈 데이터베이스별 상태:")
        print(f"{'데이터베이스':<15} {'상태':<8} {'레코드 수':<12} {'건강도':<10} {'주요 문제'}")
        print("-" * 70)
        
        for db_file, result in self.results.items():
            name = result['config']['name'][:12]
            status = "정상" if result['file_info']['exists'] else "없음"
            records = f"{result['total_records']:,}" if result['total_records'] > 0 else "0"
            score = f"{result['score']}/100"
            main_issue = result['issues'][0] if result['issues'] else "없음"
            
            print(f"{name:<15} {status:<8} {records:<12} {score:<10} {main_issue}")
        
        # 권장사항
        print(f"\n💡 권장사항:")
        
        # 심각한 문제가 있는 DB 찾기
        critical_dbs = [db for db, result in self.results.items() if result['score'] < 40]
        if critical_dbs:
            print(f"🚨 긴급 수정 필요:")
            for db in critical_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {', '.join(result['issues'])}")
        
        # 데이터가 적은 DB 찾기
        low_data_dbs = [db for db, result in self.results.items() if result['total_records'] < 1000 and result['file_info']['exists']]
        if low_data_dbs:
            print(f"📈 데이터 수집 권장:")
            for db in low_data_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: 현재 {result['total_records']:,}개 레코드")
        
        # 높은 점수 DB (칭찬)
        good_dbs = [db for db, result in self.results.items() if result['score'] >= 80]
        if good_dbs:
            print(f"✅ 잘 관리된 데이터베이스:")
            for db in good_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {result['score']}/100점")
    
    def show_collection_status(self):
        """데이터 수집 현황 상세 분석"""
        print("\n" + "=" * 80)
        print("📊 데이터 수집 현황 상세 분석")
        print("=" * 80)
        
        # 주요 테이블별 수집 현황
        important_tables = {
            'stock_data.db': {
                'company_info': '기업 기본정보',
                'stock_prices': '주가 데이터',
                'financial_ratios': '재무비율'
            },
            'dart_data.db': {
                'corp_codes': '기업코드',
                'financial_statements': '재무제표',
                'disclosures': '공시정보'
            },
            'news_data.db': {
                'news_articles': '뉴스 기사',
                'sentiment_scores': '감정 점수'
            },
            'kis_data.db': {
                'realtime_quotes': '실시간 시세'
            }
        }
        
        for db_file, tables in important_tables.items():
            if db_file in self.results:
                result = self.results[db_file]
                print(f"\n🗃️  {result['config']['name']}:")
                
                for table_name, description in tables.items():
                    if table_name in result['table_info']:
                        info = result['table_info'][table_name]
                        count = info.get('count', 0)
                        freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                        
                        print(f"   📊 {description} ({table_name})")
                        print(f"      📈 레코드: {count:,}개")
                        print(f"      🕒 최신: {freshness}")
                        
                        # 수집 상태 평가
                        if count == 0:
                            print(f"      🚨 상태: 데이터 없음 - 수집 필요")
                        elif count < 100:
                            print(f"      ⚠️  상태: 데이터 부족 - 추가 수집 권장")
                        elif '🔴' in freshness:
                            print(f"      ⏰ 상태: 데이터 오래됨 - 업데이트 필요")
                        else:
                            print(f"      ✅ 상태: 양호")
                    else:
                        print(f"   📊 {description} ({table_name})")
                        print(f"      ❌ 테이블 없음 - 생성 및 수집 필요")

def main():
    """메인 함수"""
    print("🚀 Finance Data Vibe - 전체 데이터베이스 상태 점검")
    print("=" * 80)
    
    # 데이터베이스 경로 확인
    db_path = Path("data/databases")
    if not db_path.exists():
        # 현재 디렉터리에서 찾기
        current_files = list(Path(".").glob("*.db"))
        if current_files:
            db_path = Path(".")
        else:
            print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
            print("예상 위치: data/databases/")
            return
    
    # 점검 실행
    checker = DatabaseStatusChecker(db_path)
    checker.check_all_databases()
    checker.show_summary()
    checker.show_collection_status()
    
    print(f"\n✅ 전체 점검 완료!")
    print(f"💡 상세한 테이블별 분석이 필요하면 개별 체커를 사용하세요:")
    print(f"   • python company_info_checker.py  # 기업정보 상세 분석")
    print(f"   • python news_data_checker.py     # 뉴스 데이터 상세 분석")

if __name__ == "__main__":
    main()),
        ]
        
        for fmt, pattern in formats_and_patterns:
            if re.match(pattern, date_str):
                try:
                    parsed_dt = datetime.strptime(date_str, fmt)
                    break
                except Exception as e:
                    continue
        
        # RFC 2822 형식 (뉴스 pubDate)
        if not parsed_dt and re.match(r'^[A-Za-z]{3}, \d{1,2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2}', date_str):
            try:
                # 타임존 제거하고 파싱
                date_part = date_str.split(' +')[0] if ' +' in date_str else date_str
                parsed_dt = datetime.strptime(date_part, '%a, %d %b %Y %H:%M:%S')
            except Exception as e:
                pass
        
        # Unix timestamp 시도
        if not parsed_dt:
            try:
                if date_str.isdigit() and len(date_str) in [10, 13]:
                    timestamp = int(date_str)
                    if len(date_str) == 13:  # 밀리초
                        timestamp = timestamp / 1000
                    parsed_dt = datetime.fromtimestamp(timestamp)
            except:
                pass
        
        if not parsed_dt:
            return "❓ 파싱 실패", latest_data
        
        # 신선도 계산
        now = datetime.now()
        diff = now - parsed_dt
        
        if diff.days < 0:  # 미래 날짜
            return "🔮 미래 데이터", latest_data
        elif diff.days == 0:
            return "🟢 오늘", latest_data
        elif diff.days == 1:
            return "🟡 어제", latest_data
        elif diff.days <= 7:
            return f"🟠 {diff.days}일 전", latest_data
        elif diff.days <= 30:
            return f"🟠 {diff.days}일 전", latest_data
        else:
            return f"🔴 {diff.days}일 전", latest_data
    
    def get_database_health_score(self, db_info):
        """데이터베이스 건강도 점수 계산"""
        if not db_info.get('file_info', {}).get('exists'):
            return 0, "파일 없음"
        
        score = 0
        issues = []
        
        # 파일 크기 점수 (10점)
        size = db_info['file_info']['size']
        if size > 1000000:  # 1MB 이상
            score += 10
        elif size > 100000:  # 100KB 이상
            score += 5
        else:
            issues.append("파일 크기 작음")
        
        # 테이블 존재 점수 (30점)
        expected_tables = set(db_info['expected_tables'])
        actual_tables = set(db_info['table_info'].keys())
        table_coverage = len(actual_tables & expected_tables) / len(expected_tables)
        score += int(table_coverage * 30)
        
        if table_coverage < 1.0:
            missing = expected_tables - actual_tables
            issues.append(f"테이블 누락: {', '.join(missing)}")
        
        # 데이터 존재 점수 (40점)
        total_records = sum(info.get('count', 0) for info in db_info['table_info'].values())
        if total_records > 10000:
            score += 40
        elif total_records > 1000:
            score += 30
        elif total_records > 100:
            score += 20
        elif total_records > 0:
            score += 10
        else:
            issues.append("데이터 없음")
        
        # 데이터 신선도 점수 (20점)
        fresh_tables = 0
        total_tables = len(actual_tables)
        
        for table_info in db_info['table_info'].values():
            if table_info.get('latest_data'):
                freshness, _ = self.analyze_data_freshness(table_info['latest_data'])
                if '🟢' in freshness or '🟡' in freshness:
                    fresh_tables += 1
        
        if total_tables > 0:
            freshness_ratio = fresh_tables / total_tables
            score += int(freshness_ratio * 20)
        
        if fresh_tables == 0 and total_tables > 0:
            issues.append("데이터 오래됨")
        
        # 등급 결정
        if score >= 90:
            grade = "A+ 우수"
        elif score >= 80:
            grade = "A 양호"
        elif score >= 70:
            grade = "B+ 보통"
        elif score >= 60:
            grade = "B 미흡"
        elif score >= 40:
            grade = "C 불량"
        else:
            grade = "D 심각"
        
        return score, grade, issues
    
    def check_all_databases(self):
        """모든 데이터베이스 상태 점검"""
        print("🔍 전체 데이터베이스 상태 점검 시작")
        print("=" * 80)
        
        for db_file, db_config in self.databases.items():
            print(f"\n📊 {db_config['name']} ({db_file})")
            print("-" * 60)
            
            # 파일 정보
            file_info = self.get_file_info(db_file)
            print(f"📁 파일 정보:")
            print(f"   존재: {'✅' if file_info['exists'] else '❌'}")
            if file_info['exists']:
                print(f"   크기: {file_info['size_human']}")
                print(f"   수정일: {file_info['modified']}")
            
            # 테이블 정보
            table_info = self.get_table_info(db_file)
            
            if 'error' in table_info:
                print(f"❌ 데이터베이스 접근 오류: {table_info['error']}")
                continue
            
            print(f"\n📋 테이블 현황:")
            if not table_info:
                print("   ❌ 테이블이 없습니다")
                continue
            
            total_records = 0
            for table_name, info in table_info.items():
                count = info.get('count', 0)
                total_records += count
                
                # 데이터 신선도 분석
                freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                
                print(f"   📊 {table_name}: {count:,}개 레코드")
                print(f"      컬럼: {info.get('columns', 0)}개")
                if info.get('latest_data'):
                    print(f"      최신 데이터: {freshness} ({latest})")
                
                # 에러가 있다면 표시
                if 'error' in info:
                    print(f"      ❌ 오류: {info['error']}")
            
            print(f"\n📈 총 레코드 수: {total_records:,}개")
            
            # 건강도 점수 계산
            db_info = {
                'file_info': file_info,
                'table_info': table_info,
                'expected_tables': db_config['expected_tables']
            }
            
            score, grade, issues = self.get_database_health_score(db_info)
            print(f"🏆 건강도: {score}/100점 ({grade})")
            
            if issues:
                print(f"⚠️  문제점:")
                for issue in issues:
                    print(f"     • {issue}")
            
            # 결과 저장
            self.results[db_file] = {
                'config': db_config,
                'file_info': file_info,
                'table_info': table_info,
                'score': score,
                'grade': grade,
                'issues': issues,
                'total_records': total_records
            }
    
    def show_summary(self):
        """종합 요약 보고서"""
        print("\n" + "=" * 80)
        print("📋 종합 요약 보고서")
        print("=" * 80)
        
        # 전체 통계
        total_size = sum(r['file_info']['size'] for r in self.results.values() if r['file_info']['exists'])
        total_records = sum(r['total_records'] for r in self.results.values())
        avg_score = sum(r['score'] for r in self.results.values()) / len(self.results) if self.results else 0
        
        print(f"💾 전체 데이터베이스 크기: {humanize.naturalsize(total_size)}")
        print(f"📊 전체 레코드 수: {total_records:,}개")
        print(f"🏆 평균 건강도: {avg_score:.1f}/100점")
        
        # 데이터베이스별 요약
        print(f"\n📈 데이터베이스별 상태:")
        print(f"{'데이터베이스':<15} {'상태':<8} {'레코드 수':<12} {'건강도':<10} {'주요 문제'}")
        print("-" * 70)
        
        for db_file, result in self.results.items():
            name = result['config']['name'][:12]
            status = "정상" if result['file_info']['exists'] else "없음"
            records = f"{result['total_records']:,}" if result['total_records'] > 0 else "0"
            score = f"{result['score']}/100"
            main_issue = result['issues'][0] if result['issues'] else "없음"
            
            print(f"{name:<15} {status:<8} {records:<12} {score:<10} {main_issue}")
        
        # 권장사항
        print(f"\n💡 권장사항:")
        
        # 심각한 문제가 있는 DB 찾기
        critical_dbs = [db for db, result in self.results.items() if result['score'] < 40]
        if critical_dbs:
            print(f"🚨 긴급 수정 필요:")
            for db in critical_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {', '.join(result['issues'])}")
        
        # 데이터가 적은 DB 찾기
        low_data_dbs = [db for db, result in self.results.items() if result['total_records'] < 1000 and result['file_info']['exists']]
        if low_data_dbs:
            print(f"📈 데이터 수집 권장:")
            for db in low_data_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: 현재 {result['total_records']:,}개 레코드")
        
        # 높은 점수 DB (칭찬)
        good_dbs = [db for db, result in self.results.items() if result['score'] >= 80]
        if good_dbs:
            print(f"✅ 잘 관리된 데이터베이스:")
            for db in good_dbs:
                result = self.results[db]
                print(f"   • {result['config']['name']}: {result['score']}/100점")
    
    def show_collection_status(self):
        """데이터 수집 현황 상세 분석"""
        print("\n" + "=" * 80)
        print("📊 데이터 수집 현황 상세 분석")
        print("=" * 80)
        
        # 주요 테이블별 수집 현황
        important_tables = {
            'stock_data.db': {
                'company_info': '기업 기본정보',
                'stock_prices': '주가 데이터',
                'financial_ratios': '재무비율'
            },
            'dart_data.db': {
                'corp_codes': '기업코드',
                'financial_statements': '재무제표',
                'disclosures': '공시정보'
            },
            'news_data.db': {
                'news_articles': '뉴스 기사',
                'sentiment_scores': '감정 점수'
            },
            'kis_data.db': {
                'realtime_quotes': '실시간 시세'
            }
        }
        
        for db_file, tables in important_tables.items():
            if db_file in self.results:
                result = self.results[db_file]
                print(f"\n🗃️  {result['config']['name']}:")
                
                for table_name, description in tables.items():
                    if table_name in result['table_info']:
                        info = result['table_info'][table_name]
                        count = info.get('count', 0)
                        freshness, latest = self.analyze_data_freshness(info.get('latest_data'))
                        
                        print(f"   📊 {description} ({table_name})")
                        print(f"      📈 레코드: {count:,}개")
                        print(f"      🕒 최신: {freshness}")
                        
                        # 수집 상태 평가
                        if count == 0:
                            print(f"      🚨 상태: 데이터 없음 - 수집 필요")
                        elif count < 100:
                            print(f"      ⚠️  상태: 데이터 부족 - 추가 수집 권장")
                        elif '🔴' in freshness:
                            print(f"      ⏰ 상태: 데이터 오래됨 - 업데이트 필요")
                        else:
                            print(f"      ✅ 상태: 양호")
                    else:
                        print(f"   📊 {description} ({table_name})")
                        print(f"      ❌ 테이블 없음 - 생성 및 수집 필요")

def main():
    """메인 함수"""
    print("🚀 Finance Data Vibe - 전체 데이터베이스 상태 점검")
    print("=" * 80)
    
    # 데이터베이스 경로 확인
    db_path = Path("data/databases")
    if not db_path.exists():
        # 현재 디렉터리에서 찾기
        current_files = list(Path(".").glob("*.db"))
        if current_files:
            db_path = Path(".")
        else:
            print("❌ 데이터베이스 파일을 찾을 수 없습니다.")
            print("예상 위치: data/databases/")
            return
    
    # 점검 실행
    checker = DatabaseStatusChecker(db_path)
    checker.check_all_databases()
    checker.show_summary()
    checker.show_collection_status()
    
    print(f"\n✅ 전체 점검 완료!")
    print(f"💡 상세한 테이블별 분석이 필요하면 개별 체커를 사용하세요:")
    print(f"   • python company_info_checker.py  # 기업정보 상세 분석")
    print(f"   • python news_data_checker.py     # 뉴스 데이터 상세 분석")

if __name__ == "__main__":
    main()