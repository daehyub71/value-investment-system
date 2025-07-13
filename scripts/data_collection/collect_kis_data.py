#!/usr/bin/env python3
"""
KIS API 데이터 수집 스크립트
DART 재무정보의 시차를 극복하기 위한 실시간 데이터 수집

실행 방법:
python scripts/data_collection/collect_kis_data.py --realtime_quotes --stock_code=005930
python scripts/data_collection/collect_kis_data.py --market_indicators --all_stocks
python scripts/data_collection/collect_kis_data.py --update_financial_ratios --limit=50
"""

import sys
import os
import argparse
import sqlite3
import requests
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from config import ConfigManager
except ImportError:
    print("⚠️  ConfigManager를 찾을 수 없습니다. 기본 설정으로 진행합니다.")
    ConfigManager = None

class KisDataCollector:
    """KIS API 데이터 수집 클래스"""
    
    def __init__(self):
        # ConfigManager를 통한 통합 설정 관리
        if ConfigManager:
            self.config_manager = ConfigManager()
            self.logger = self.config_manager.get_logger('KisDataCollector')
            
            # KIS API 설정 가져오기
            kis_config = self.config_manager.get_kis_config()
            self.app_key = kis_config.get('app_key')
            self.app_secret = kis_config.get('app_secret')
            self.environment = kis_config.get('environment', 'VIRTUAL')
            self.base_url = kis_config.get('url_base')
            self.access_token = kis_config.get('access_token')
            self.cano = kis_config.get('cano')
            self.request_delay = kis_config.get('request_delay', 0.05)
        else:
            # 기본 설정
            self.logger = logging.getLogger(__name__)
            self.app_key = os.getenv('KIS_APP_KEY', '').strip('"')
            self.app_secret = os.getenv('KIS_APP_SECRET', '').strip('"')
            self.environment = os.getenv('KIS_ENVIRONMENT', 'VIRTUAL')
            self.cano = os.getenv('KIS_CANO', '').strip('"')
            self.access_token = os.getenv('KIS_ACCESS_TOKEN', '')
            self.request_delay = float(os.getenv('KIS_REQUEST_DELAY', '0.05'))
            
            # URL 설정
            if self.environment == 'REAL':
                self.base_url = 'https://openapi.koreainvestment.com:9443'
            else:
                self.base_url = 'https://openapivts.koreainvestment.com:29443'
        
        self.db_path = Path('data/databases/kis_data.db')
        
        # API 엔드포인트 정의
        self.endpoints = {
            'realtime_quote': '/uapi/domestic-stock/v1/quotations/inquire-price',
            'market_indicators': '/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice',
            'financial_info': '/uapi/domestic-stock/v1/quotations/inquire-daily-price',
            'trading_volume': '/uapi/domestic-stock/v1/quotations/inquire-daily-chartprice'
        }
        
        if not self.app_key or not self.app_secret:
            raise ValueError("KIS API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")
        
        # 강화된 토큰 가져오기 로직
        self.access_token = self.get_or_create_access_token()
    
    def get_or_create_access_token(self) -> str:
        """강화된 토큰 가져오기/생성 로직"""
        # 1순위: 기존 access_token
        if self.access_token:
            self.logger.info("기존 ACCESS_TOKEN 사용")
            return self.access_token
        
        # 2순위: 임시 토큰 (KIS_ACCESS_TOKEN_TEMP)
        temp_token = os.getenv('KIS_ACCESS_TOKEN_TEMP', '').strip()
        if temp_token and temp_token != '':
            self.logger.info("임시 토큰 재사용 (KIS_ACCESS_TOKEN_TEMP)")
            return temp_token
        
        # 3순위: 새 토큰 요청 (위험하지만 시도)
        try:
            self.logger.warning("새 토큰 요청 시도 (시간 제한 위험)")
            return self.get_access_token()
        except Exception as e:
            self.logger.error(f"토큰 요청 실패: {e}")
            raise ValueError(
                "KIS API 토큰을 가져올 수 없습니다.\n"
                "해결 방법:\n"
                "1. .env 파일에 KIS_ACCESS_TOKEN_TEMP에 유효한 토큰 입력\n"
                "2. 또는 30분 후 다시 시도\n"
                "3. 또는 FinanceDataReader 대안 사용"
            )
    
    def get_access_token(self) -> str:
        """KIS API 인증 토큰 획득"""
        try:
            url = f"{self.base_url}/oauth2/tokenP"
            headers = {
                'content-type': 'application/json; charset=utf-8'
            }
            data = {
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "appsecret": self.app_secret
            }
            
            response = requests.post(url, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            access_token = result.get('access_token')
            
            if access_token:
                self.logger.info("KIS API 인증 토큰 획득 성공")
                return access_token
            else:
                raise ValueError("인증 토큰을 받을 수 없습니다.")
                
        except Exception as e:
            self.logger.error(f"KIS API 인증 실패: {e}")
            raise
    
    def make_api_request(self, endpoint: str, tr_id: str, params: Dict[str, str]) -> Optional[Dict]:
        """KIS API 요청"""
        try:
            url = f"{self.base_url}{endpoint}"
            headers = {
                'content-type': 'application/json; charset=utf-8',
                'authorization': f'Bearer {self.access_token}',
                'appkey': self.app_key,
                'appsecret': self.app_secret,
                'tr_id': tr_id,
                'custtype': 'P'
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            
            # API 호출 제한 대응
            time.sleep(self.request_delay)
            
            if result.get('rt_cd') == '0':  # 성공
                return result
            else:
                self.logger.warning(f"API 호출 경고: {result.get('msg1', 'Unknown error')}")
                return result  # 경고여도 데이터가 있을 수 있음
                
        except Exception as e:
            self.logger.error(f"API 요청 실패 ({endpoint}): {e}")
            return None
    
    def collect_realtime_quote(self, stock_code: str) -> Optional[Dict]:
        """실시간 주가 정보 수집"""
        try:
            params = {
                'FID_COND_MRKT_DIV_CODE': 'J',  # 시장 구분 (J: 주식)
                'FID_INPUT_ISCD': stock_code
            }
            
            result = self.make_api_request(
                self.endpoints['realtime_quote'],
                'FHKST01010100',  # 실시간 시세 조회 TR ID
                params
            )
            
            if not result or 'output' not in result:
                return None
            
            output = result['output']
            
            quote_data = {
                'stock_code': stock_code,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'current_price': float(output.get('stck_prpr', 0)),  # 현재가
                'change_price': float(output.get('prdy_vrss', 0)),   # 전일대비
                'change_rate': float(output.get('prdy_ctrt', 0)),    # 등락률
                'volume': int(output.get('acml_vol', 0)),            # 누적거래량
                'high_price': float(output.get('stck_hgpr', 0)),     # 최고가
                'low_price': float(output.get('stck_lwpr', 0)),      # 최저가
                'open_price': float(output.get('stck_oprc', 0)),     # 시가
                'market_cap': int(output.get('hts_avls', 0)) if output.get('hts_avls') else None,  # 시가총액
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.logger.info(f"실시간 주가 수집 완료: {stock_code} - {quote_data['current_price']}원")
            return quote_data
            
        except Exception as e:
            self.logger.error(f"실시간 주가 수집 실패 ({stock_code}): {e}")
            return None
    
    def collect_market_indicators(self) -> List[Dict]:
        """시장 지표 수집 (KOSPI, KOSDAQ)"""
        try:
            indicators = []
            market_codes = [
                ('0001', 'KOSPI'),
                ('1001', 'KOSDAQ')
            ]
            
            for code, name in market_codes:
                params = {
                    'FID_COND_MRKT_DIV_CODE': 'U',
                    'FID_INPUT_ISCD': code,
                    'FID_INPUT_DATE_1': '',
                    'FID_INPUT_DATE_2': '',
                    'FID_PERIOD_DIV_CODE': 'D'  # 일봉
                }
                
                result = self.make_api_request(
                    self.endpoints['market_indicators'],
                    'FHKUP03500100',  # 지수 차트 조회 TR ID
                    params
                )
                
                if result and 'output2' in result and result['output2']:
                    latest_data = result['output2'][0]
                    indicator_data = {
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'index_name': name,
                        'index_code': code,
                        'close_price': float(latest_data.get('stck_clpr', 0)),
                        'change_price': float(latest_data.get('prdy_vrss', 0)),
                        'change_rate': float(latest_data.get('prdy_ctrt', 0)),
                        'volume': int(latest_data.get('acml_vol', 0)),
                        'high_price': float(latest_data.get('stck_hgpr', 0)),
                        'low_price': float(latest_data.get('stck_lwpr', 0)),
                        'open_price': float(latest_data.get('stck_oprc', 0)),
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    indicators.append(indicator_data)
                    
                    self.logger.info(f"시장지표 수집 완료: {name} - {indicator_data['close_price']}")
                else:
                    self.logger.warning(f"시장지표 데이터가 없습니다: {name}")
            
            return indicators
            
        except Exception as e:
            self.logger.error(f"시장 지표 수집 실패: {e}")
            return []
    
    def save_to_database(self, realtime_quotes=None, market_indicators=None):
        """데이터베이스에 저장"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 실시간 주가 저장
                if realtime_quotes:
                    for quote in realtime_quotes:
                        conn.execute('''
                            INSERT OR REPLACE INTO realtime_quotes 
                            (stock_code, timestamp, current_price, change_price, change_rate,
                             volume, high_price, low_price, open_price, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            quote['stock_code'], quote['timestamp'], quote['current_price'],
                            quote['change_price'], quote['change_rate'], quote['volume'],
                            quote['high_price'], quote['low_price'], quote['open_price'],
                            quote['created_at']
                        ))
                    
                    self.logger.info(f"실시간 주가 저장 완료: {len(realtime_quotes)}건")
                
                # 시장 지표 저장
                if market_indicators:
                    for indicator in market_indicators:
                        conn.execute('''
                            INSERT OR REPLACE INTO market_indicators
                            (date, index_name, index_code, close_price, change_price, change_rate,
                             volume, high_price, low_price, open_price, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            indicator['date'], indicator['index_name'], indicator['index_code'],
                            indicator['close_price'], indicator['change_price'], indicator['change_rate'],
                            indicator['volume'], indicator['high_price'], indicator['low_price'],
                            indicator['open_price'], indicator['created_at']
                        ))
                    
                    self.logger.info(f"시장 지표 저장 완료: {len(market_indicators)}건")
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"데이터베이스 저장 실패: {e}")
            return False
    
    def collect_stock_realtime_data(self, stock_code: str):
        """개별 종목 실시간 데이터 수집"""
        try:
            self.logger.info(f"실시간 데이터 수집 시작: {stock_code}")
            
            # 실시간 주가 수집
            quote_data = self.collect_realtime_quote(stock_code)
            if not quote_data:
                return False
            
            # 데이터베이스 저장
            success = self.save_to_database(realtime_quotes=[quote_data])
            
            if success:
                self.logger.info(f"실시간 데이터 수집 완료: {stock_code}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"실시간 데이터 수집 실패 ({stock_code}): {e}")
            return False
    
    def collect_all_market_data(self):
        """전체 시장 데이터 수집"""
        try:
            self.logger.info("전체 시장 데이터 수집 시작")
            
            # 시장 지표 수집
            market_indicators = self.collect_market_indicators()
            
            if not market_indicators:
                self.logger.warning("시장 지표를 수집할 수 없습니다")
                return False
            
            # 데이터베이스 저장
            success = self.save_to_database(market_indicators=market_indicators)
            
            if success:
                self.logger.info("전체 시장 데이터 수집 완료")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"전체 시장 데이터 수집 실패: {e}")
            return False
    
    def update_top_stocks_realtime(self, limit: int = 50):
        """상위 종목 실시간 데이터 업데이트"""
        try:
            # 시가총액 상위 종목 조회
            stock_db_path = Path('data/databases/stock_data.db')
            if not stock_db_path.exists():
                self.logger.error("주식 데이터베이스를 찾을 수 없습니다.")
                return False
                
            with sqlite3.connect(stock_db_path) as conn:
                cursor = conn.execute(f"""
                    SELECT stock_code, company_name
                    FROM company_info 
                    WHERE market_cap IS NOT NULL AND market_cap > 0
                    ORDER BY market_cap DESC 
                    LIMIT {limit}
                """)
                stock_list = cursor.fetchall()
            
            if not stock_list:
                self.logger.error("종목 리스트를 찾을 수 없습니다.")
                return False
            
            self.logger.info(f"상위 {len(stock_list)}개 종목 실시간 데이터 업데이트 시작")
            
            success_count = 0
            for idx, (stock_code, company_name) in enumerate(stock_list):
                self.logger.info(f"진행률: {idx+1}/{len(stock_list)} - {company_name}({stock_code})")
                
                if self.collect_stock_realtime_data(stock_code):
                    success_count += 1
                
                # API 호출 제한 대응
                time.sleep(self.request_delay)
            
            self.logger.info(f"실시간 데이터 업데이트 완료: {success_count}/{len(stock_list)} 성공")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"상위 종목 실시간 데이터 업데이트 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='KIS API 데이터 수집 스크립트')
    parser.add_argument('--stock_code', type=str, help='실시간 데이터 수집할 종목코드')
    parser.add_argument('--realtime_quotes', action='store_true', help='실시간 주가 수집')
    parser.add_argument('--market_indicators', action='store_true', help='시장 지표 수집')
    parser.add_argument('--update_financial_ratios', action='store_true', help='실시간 재무비율 업데이트')
    parser.add_argument('--all_stocks', action='store_true', help='상위 종목 전체 업데이트')
    parser.add_argument('--limit', type=int, default=50, help='처리할 종목 수 제한')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 기본 로깅 설정
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # 수집기 초기화
    try:
        collector = KisDataCollector()
    except ValueError as e:
        logger.error(f"초기화 실패: {e}")
        sys.exit(1)
    
    try:
        if args.stock_code and args.realtime_quotes:
            # 특정 종목 실시간 주가 수집
            if collector.collect_stock_realtime_data(args.stock_code):
                logger.info("✅ 실시간 주가 데이터 수집 성공")
            else:
                logger.error("❌ 실시간 주가 데이터 수집 실패")
                sys.exit(1)
                
        elif args.market_indicators:
            # 시장 지표 수집
            if collector.collect_all_market_data():
                logger.info("✅ 시장 지표 데이터 수집 성공")
            else:
                logger.error("❌ 시장 지표 데이터 수집 실패")
                sys.exit(1)
                
        elif args.all_stocks or args.update_financial_ratios:
            # 상위 종목 실시간 데이터 업데이트
            if collector.update_top_stocks_realtime(args.limit):
                logger.info("✅ 상위 종목 실시간 데이터 업데이트 성공")
            else:
                logger.error("❌ 상위 종목 실시간 데이터 업데이트 실패")
                sys.exit(1)
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
