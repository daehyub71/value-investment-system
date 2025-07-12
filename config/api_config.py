"""
API 설정 파일
모든 API 관련 설정을 통합 관리
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

class APIConfig:
    """API 설정 관리 클래스"""
    
    def __init__(self):
        self.dart = self._load_dart_config()
        self.naver_news = self._load_naver_news_config()
        self.kis = self._load_kis_config()
        self.common = self._load_common_config()
    
    def _load_dart_config(self) -> Dict[str, Any]:
        """DART API 설정 로드"""
        return {
            'api_key': os.getenv('DART_API_KEY', '').strip('"'),
            'base_url': 'https://opendart.fss.or.kr/api',
            'endpoints': {
                'corp_code': 'corpCode.xml',
                'financial_stmt': 'fnlttSinglAcntAll.json',
                'disclosure': 'list.json',
                'corp_outline': 'company.json',
                'stock_info': 'stockTotqySttus.json'
            },
            'request_delay': 1.0,  # 초당 1회 요청 제한
            'timeout': 30,
            'retry_count': 3,
            'retry_delay': 5,
            'params': {
                'corp_code': {
                    'required': ['api_key'],
                    'optional': []
                },
                'financial_stmt': {
                    'required': ['api_key', 'corp_code', 'bsns_year', 'reprt_code'],
                    'optional': ['fs_div']
                },
                'disclosure': {
                    'required': ['api_key'],
                    'optional': ['corp_code', 'bgn_de', 'end_de', 'last_reprt_at', 'pblntf_ty', 'corp_cls', 'sort', 'sort_mth', 'page_no', 'page_count']
                }
            }
        }
    
    def _load_naver_news_config(self) -> Dict[str, Any]:
        """네이버 뉴스 API 설정 로드"""
        return {
            'client_id': os.getenv('NAVER_CLIENT_ID', ''),
            'client_secret': os.getenv('NAVER_CLIENT_SECRET', ''),
            'base_url': 'https://openapi.naver.com/v1/search/news.json',
            'daily_limit': 25000,
            'request_delay': 0.1,  # 초당 10회 요청 제한
            'timeout': 10,
            'retry_count': 3,
            'retry_delay': 2,
            'params': {
                'query': {
                    'required': ['query'],
                    'optional': ['display', 'start', 'sort']
                }
            },
            'headers': {
                'X-Naver-Client-Id': os.getenv('NAVER_CLIENT_ID', ''),
                'X-Naver-Client-Secret': os.getenv('NAVER_CLIENT_SECRET', '')
            },
            'search_options': {
                'display': 100,  # 검색 결과 출력 건수 (1~100)
                'start': 1,      # 검색 시작 위치 (1~1000)
                'sort': 'date'   # 정렬 옵션 (sim:유사도, date:날짜)
            }
        }
    
    def _load_kis_config(self) -> Dict[str, Any]:
        """KIS API 설정 로드"""
        environment = os.getenv('KIS_ENVIRONMENT', 'VIRTUAL')
        
        config = {
            'app_key': os.getenv('KIS_APP_KEY', '').strip('"'),
            'app_secret': os.getenv('KIS_APP_SECRET', '').strip('"'),
            'environment': environment,
            'url_base_real': os.getenv('KIS_URL_BASE_REAL', 'https://openapi.koreainvestment.com:9443'),
            'url_base_virtual': os.getenv('KIS_URL_BASE_VIRTUAL', 'https://openapivts.koreainvestment.com:29443'),
            'cano': os.getenv('KIS_CANO', '').strip('"'),
            'acnt_prdt_cd': '01',  # 계좌상품코드 (기본값)
            'access_token': os.getenv('KIS_ACCESS_TOKEN', ''),
            'request_delay': float(os.getenv('KIS_REQUEST_DELAY', '0.05')),
            'timeout': 30,
            'retry_count': 3,
            'retry_delay': 1
        }
        
        # 현재 환경에 따른 기본 URL 설정
        config['url_base'] = config['url_base_real'] if environment == 'REAL' else config['url_base_virtual']
        
        return config
    
    def _load_common_config(self) -> Dict[str, Any]:
        """공통 API 설정 로드"""
        return {
            'user_agent': 'Finance Data Vibe/1.0 (https://github.com/your-repo)',
            'accept_encoding': 'gzip, deflate',
            'connection_timeout': 10,
            'read_timeout': 30,
            'max_retries': 3,
            'backoff_factor': 0.3,
            'status_forcelist': [500, 502, 503, 504],
            'session_pool_connections': 10,
            'session_pool_maxsize': 20
        }
    
    def get_dart_headers(self) -> Dict[str, str]:
        """DART API 요청 헤더 반환"""
        return {
            'User-Agent': self.common['user_agent'],
            'Accept': 'application/json',
            'Accept-Encoding': self.common['accept_encoding']
        }
    
    def get_naver_news_headers(self) -> Dict[str, str]:
        """네이버 뉴스 API 요청 헤더 반환"""
        return {
            'User-Agent': self.common['user_agent'],
            'Accept': 'application/json',
            'Accept-Encoding': self.common['accept_encoding'],
            'X-Naver-Client-Id': self.naver_news['client_id'],
            'X-Naver-Client-Secret': self.naver_news['client_secret']
        }
    
    def get_kis_headers(self, tr_id: str, custtype: str = 'P') -> Dict[str, str]:
        """KIS API 요청 헤더 반환"""
        return {
            'content-type': 'application/json; charset=utf-8',
            'authorization': f'Bearer {self.kis["access_token"]}',
            'appkey': self.kis['app_key'],
            'appsecret': self.kis['app_secret'],
            'tr_id': tr_id,
            'custtype': custtype,
            'User-Agent': self.common['user_agent']
        }
    
    def validate_dart_config(self) -> list:
        """DART API 설정 유효성 검사"""
        errors = []
        
        if not self.dart['api_key']:
            errors.append("DART_API_KEY가 설정되지 않았습니다.")
        
        if len(self.dart['api_key']) != 40:
            errors.append("DART_API_KEY 형식이 올바르지 않습니다. (40자 필요)")
        
        return errors
    
    def validate_naver_news_config(self) -> list:
        """네이버 뉴스 API 설정 유효성 검사"""
        errors = []
        
        if not self.naver_news['client_id']:
            errors.append("NAVER_CLIENT_ID가 설정되지 않았습니다.")
        
        if not self.naver_news['client_secret']:
            errors.append("NAVER_CLIENT_SECRET이 설정되지 않았습니다.")
        
        return errors
    
    def validate_kis_config(self) -> list:
        """KIS API 설정 유효성 검사"""
        errors = []
        
        if not self.kis['app_key']:
            errors.append("KIS_APP_KEY가 설정되지 않았습니다.")
        
        if not self.kis['app_secret']:
            errors.append("KIS_APP_SECRET이 설정되지 않았습니다.")
        
        if not self.kis['cano']:
            errors.append("KIS_CANO (계좌번호)가 설정되지 않았습니다.")
        
        if self.kis['environment'] not in ['REAL', 'VIRTUAL']:
            errors.append("KIS_ENVIRONMENT는 'REAL' 또는 'VIRTUAL'이어야 합니다.")
        
        return errors
    
    def validate_all_configs(self) -> list:
        """모든 API 설정 유효성 검사"""
        errors = []
        errors.extend(self.validate_dart_config())
        errors.extend(self.validate_naver_news_config())
        errors.extend(self.validate_kis_config())
        return errors

# 글로벌 API 설정 인스턴스
api_config = APIConfig()

# 편의 함수들
def get_dart_config() -> Dict[str, Any]:
    """DART API 설정 반환"""
    return api_config.dart

def get_naver_news_config() -> Dict[str, Any]:
    """네이버 뉴스 API 설정 반환"""
    return api_config.naver_news

def get_kis_config() -> Dict[str, Any]:
    """KIS API 설정 반환"""
    return api_config.kis

def get_api_headers(api_type: str, **kwargs) -> Dict[str, str]:
    """API 타입별 헤더 반환"""
    if api_type == 'dart':
        return api_config.get_dart_headers()
    elif api_type == 'naver_news':
        return api_config.get_naver_news_headers()
    elif api_type == 'kis':
        return api_config.get_kis_headers(**kwargs)
    else:
        raise ValueError(f"지원하지 않는 API 타입: {api_type}")

def validate_api_configs() -> list:
    """모든 API 설정 유효성 검사"""
    return api_config.validate_all_configs()

# 사용 예시
if __name__ == "__main__":
    print("📊 API 설정 확인")
    print("=" * 50)
    
    # 각 API 설정 확인
    dart_errors = api_config.validate_dart_config()
    naver_errors = api_config.validate_naver_news_config()
    kis_errors = api_config.validate_kis_config()
    
    print(f"DART API: {'✅ 정상' if not dart_errors else '❌ 오류'}")
    if dart_errors:
        for error in dart_errors:
            print(f"  - {error}")
    
    print(f"네이버 뉴스 API: {'✅ 정상' if not naver_errors else '❌ 오류'}")
    if naver_errors:
        for error in naver_errors:
            print(f"  - {error}")
    
    print(f"KIS API: {'✅ 정상' if not kis_errors else '❌ 오류'}")
    if kis_errors:
        for error in kis_errors:
            print(f"  - {error}")
    
    print("\n📋 설정 요약:")
    print(f"DART API Key: {api_config.dart['api_key'][:10]}...")
    print(f"네이버 Client ID: {api_config.naver_news['client_id']}")
    print(f"KIS 환경: {api_config.kis['environment']}")
    print(f"KIS 계좌: {api_config.kis['cano']}")