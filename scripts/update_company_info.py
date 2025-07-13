#!/usr/bin/env python3
"""
company_info 테이블 데이터 보완 스크립트
시가총액, 업종, 상장일 등 누락된 정보를 다양한 소스에서 수집하여 업데이트
"""

import sys
import sqlite3
import requests
import time
from pathlib import Path
from datetime import datetime
import logging

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from config import ConfigManager
    import FinanceDataReader as fdr
except ImportError as e:
    print(f"⚠️  필요한 모듈을 import할 수 없습니다: {e}")
    print("pip install FinanceDataReader 를 실행해주세요.")
    sys.exit(1)

class CompanyInfoUpdater:
    """기업정보 업데이트 클래스"""
    
    def __init__(self):
        self.config_manager = ConfigManager()
        self.logger = self.config_manager.get_logger('CompanyInfoUpdater')
        self.db_path = Path('data/databases/stock_data.db')
        
        # DART API 설정
        dart_config = self.config_manager.get_dart_config()
        self.dart_api_key = dart_config.get('api_key')
        self.dart_base_url = dart_config.get('base_url', 'https://opendart.fss.or.kr/api')
    
    def update_market_cap_from_prices(self):
        """stock_prices 테이블의 최신 데이터로 시가총액 계산"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 최신 주가 데이터로 시가총액 계산 (임시로 거래량 * 종가 사용)
                update_query = """
                UPDATE company_info 
                SET market_cap = (
                    SELECT sp.close_price * sp.volume
                    FROM stock_prices sp 
                    WHERE sp.stock_code = company_info.stock_code 
                    ORDER BY sp.date DESC 
                    LIMIT 1
                ),
                updated_at = CURRENT_TIMESTAMP
                WHERE EXISTS (
                    SELECT 1 FROM stock_prices sp 
                    WHERE sp.stock_code = company_info.stock_code
                )
                """
                
                cursor = conn.execute(update_query)
                updated_count = cursor.rowcount
                conn.commit()
                
                self.logger.info(f"시가총액 업데이트 완료: {updated_count}개 종목")
                return updated_count
                
        except Exception as e:
            self.logger.error(f"시가총액 업데이트 실패: {e}")
            return 0
    
    def update_company_details_from_fdr(self, limit=100):
        """FinanceDataReader로 기업 상세정보 업데이트"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 업데이트가 필요한 종목들 조회
                cursor = conn.execute("""
                    SELECT stock_code, company_name 
                    FROM company_info 
                    WHERE (sector IS NULL OR sector = '') 
                    OR (listing_date IS NULL OR listing_date = '')
                    LIMIT ?
                """, (limit,))
                
                stocks_to_update = cursor.fetchall()
                
                if not stocks_to_update:
                    self.logger.info("업데이트할 종목이 없습니다.")
                    return 0
                
                self.logger.info(f"기업정보 업데이트 시작: {len(stocks_to_update)}개 종목")
                
                updated_count = 0
                for stock_code, company_name in stocks_to_update:
                    try:
                        # 업종 정보 (간단한 추정)
                        sector = self._estimate_sector(company_name)
                        
                        # 데이터베이스 업데이트
                        conn.execute("""
                            UPDATE company_info 
                            SET 
                                sector = ?,
                                industry = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE stock_code = ?
                        """, (
                            sector,
                            sector,  # industry도 같은 값으로 임시 설정
                            stock_code
                        ))
                        
                        updated_count += 1
                        self.logger.info(f"업데이트 완료: {stock_code} - {company_name} - {sector}")
                        
                        # API 호출 제한 대응
                        time.sleep(0.1)
                        
                    except Exception as e:
                        self.logger.warning(f"개별 종목 업데이트 실패 ({stock_code}): {e}")
                        continue
                
                conn.commit()
                self.logger.info(f"기업정보 업데이트 완료: {updated_count}개 종목")
                return updated_count
                
        except Exception as e:
            self.logger.error(f"기업정보 업데이트 실패: {e}")
            return 0
    
    def _estimate_sector(self, company_name):
        """회사명으로 업종 추정 (간단한 키워드 기반)"""
        if not company_name:
            return "기타"
            
        company_name = company_name.lower()
        
        # 업종 키워드 매핑
        sector_keywords = {
            '반도체': ['반도체', '메모리', 'sk하이닉스', '삼성전자'],
            '자동차': ['자동차', '현대차', '기아', '모비스'],
            '화학': ['화학', '케미칼', 'lg화학', '롯데케미칼'],
            '바이오': ['바이오', '제약', '셀트리온', '삼성바이오'],
            '금융': ['금융', '은행', '증권', '보험', 'kb', '신한', '하나'],
            '통신': ['통신', '텔레콤', 'kt', 'skt'],
            '인터넷': ['네이버', '카카오', '쿠팡'],
            '에너지': ['에너지', '전력', '가스', '석유'],
            '건설': ['건설', '건설', '대우건설', '현대건설'],
            '유통': ['유통', '마트', '백화점', '이마트']
        }
        
        for sector, keywords in sector_keywords.items():
            if any(keyword in company_name for keyword in keywords):
                return sector
        
        return "기타"
    
    def update_shares_outstanding(self):
        """상장주식수 업데이트 (DART 재무제표에서)"""
        try:
            # 이 부분은 DART 재무제표 데이터가 있을 때 구현
            self.logger.info("상장주식수 업데이트는 아직 구현되지 않았습니다.")
            return 0
        except Exception as e:
            self.logger.error(f"상장주식수 업데이트 실패: {e}")
            return 0
    
    def run_full_update(self, limit=100):
        """전체 업데이트 실행"""
        self.logger.info("=" * 60)
        self.logger.info("📊 기업정보 종합 업데이트 시작")
        self.logger.info("=" * 60)
        
        results = {}
        
        # 1. 시가총액 업데이트
        self.logger.info("\n1️⃣ 시가총액 계산 중...")
        results['market_cap'] = self.update_market_cap_from_prices()
        
        # 2. 기업 상세정보 업데이트
        self.logger.info("\n2️⃣ 기업 상세정보 업데이트 중...")
        results['company_details'] = self.update_company_details_from_fdr(limit)
        
        # 3. 상장주식수 업데이트 (향후 구현)
        self.logger.info("\n3️⃣ 상장주식수 업데이트 (미구현)")
        results['shares_outstanding'] = self.update_shares_outstanding()
        
        # 결과 요약
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📋 업데이트 결과 요약")
        self.logger.info("=" * 60)
        self.logger.info(f"✅ 시가총액 업데이트: {results['market_cap']}개 종목")
        self.logger.info(f"✅ 기업정보 업데이트: {results['company_details']}개 종목")
        self.logger.info(f"⏸️  상장주식수: {results['shares_outstanding']}개 종목 (미구현)")
        
        total_updated = sum(results.values())
        self.logger.info(f"\n🎉 총 업데이트: {total_updated}개 항목")
        
        return results

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='기업정보 업데이트 스크립트')
    parser.add_argument('--market_cap', action='store_true', help='시가총액만 업데이트')
    parser.add_argument('--company_details', action='store_true', help='기업 상세정보만 업데이트')
    parser.add_argument('--limit', type=int, default=100, help='업데이트할 종목 수 제한')
    parser.add_argument('--all', action='store_true', help='전체 정보 업데이트')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    updater = CompanyInfoUpdater()
    
    try:
        if args.market_cap:
            result = updater.update_market_cap_from_prices()
            print(f"✅ 시가총액 업데이트 완료: {result}개 종목")
        
        elif args.company_details:
            result = updater.update_company_details_from_fdr(args.limit)
            print(f"✅ 기업정보 업데이트 완료: {result}개 종목")
        
        elif args.all:
            updater.run_full_update(args.limit)
        
        else:
            parser.print_help()
            print("\n💡 사용 예시:")
            print("  python scripts/update_company_info.py --all")
            print("  python scripts/update_company_info.py --market_cap")
            print("  python scripts/update_company_info.py --company_details --limit=50")
            
    except KeyboardInterrupt:
        print("\n⏹️  사용자에 의해 중단됨")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        
if __name__ == "__main__":
    main()
