"""
API 유틸리티
HTTP 요청, API 호출, 에러 처리 등 API 관련 유틸리티 함수들
"""

import time
import requests
import json
import hashlib
from typing import Dict, Any, Optional, List, Union, Callable
from urllib.parse import urljoin, urlencode
from datetime import datetime, timedelta
import logging
from functools import wraps
import threading
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

class APIError(Exception):
    """API 호출 오류"""
    pass

class RateLimitError(APIError):
    """레이트 제한 오류"""
    pass

class APITimeoutError(APIError):
    """API 타임아웃 오류"""
    pass

class RateLimiter:
    """API 요청 속도 제한 클래스"""
    
    def __init__(self, calls_per_second: float = 1.0, burst_size: int = 5):
        self.calls_per_second = calls_per_second
        self.burst_size = burst_size
        self.calls = deque()
        self.lock = threading.Lock()
    
    def acquire(self):
        """요청 허용 여부 확인 및 대기"""
        with self.lock:
            now = time.time()
            
            # 오래된 호출 기록 제거
            while self.calls and self.calls[0] <= now - 1.0:
                self.calls.popleft()
            
            # 버스트 크기 확인
            if len(self.calls) >= self.burst_size:
                sleep_time = 1.0 / self.calls_per_second
                time.sleep(sleep_time)
                return self.acquire()
            
            # 초당 호출 수 확인
            if len(self.calls) >= self.calls_per_second:
                sleep_time = self.calls[0] + 1.0 - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    return self.acquire()
            
            self.calls.append(now)

class APICache:
    """API 응답 캐시 클래스"""
    
    def __init__(self, ttl: int = 300, max_size: int = 1000):
        self.ttl = ttl
        self.max_size = max_size
        self.cache = {}
        self.timestamps = {}
        self.lock = threading.Lock()
    
    def _generate_key(self, url: str, params: Dict = None, headers: Dict = None) -> str:
        """캐시 키 생성"""
        key_data = {
            'url': url,
            'params': params or {},
            'headers': headers or {}
        }
        key_string = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def get(self, url: str, params: Dict = None, headers: Dict = None) -> Optional[Any]:
        """캐시에서 응답 조회"""
        key = self._generate_key(url, params, headers)
        
        with self.lock:
            if key in self.cache:
                # TTL 확인
                if time.time() - self.timestamps[key] < self.ttl:
                    return self.cache[key]
                else:
                    # 만료된 항목 제거
                    del self.cache[key]
                    del self.timestamps[key]
        
        return None
    
    def set(self, url: str, response: Any, params: Dict = None, headers: Dict = None):
        """캐시에 응답 저장"""
        key = self._generate_key(url, params, headers)
        
        with self.lock:
            # 캐시 크기 확인
            if len(self.cache) >= self.max_size:
                self._evict_oldest()
            
            self.cache[key] = response
            self.timestamps[key] = time.time()
    
    def _evict_oldest(self):
        """가장 오래된 항목 제거"""
        if not self.timestamps:
            return
        
        oldest_key = min(self.timestamps, key=self.timestamps.get)
        del self.cache[oldest_key]
        del self.timestamps[oldest_key]
    
    def clear(self):
        """캐시 전체 삭제"""
        with self.lock:
            self.cache.clear()
            self.timestamps.clear()

class APIClient:
    """API 클라이언트 클래스"""
    
    def __init__(self, base_url: str = "", 
                 timeout: int = 30,
                 retry_count: int = 3,
                 retry_delay: float = 1.0,
                 rate_limit: float = 1.0,
                 use_cache: bool = True,
                 cache_ttl: int = 300):
        
        self.base_url = base_url
        self.timeout = timeout
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.rate_limiter = RateLimiter(rate_limit)
        self.cache = APICache(cache_ttl) if use_cache else None
        
        # 세션 설정
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Finance-Data-Vibe/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
    
    def _make_url(self, endpoint: str) -> str:
        """완전한 URL 생성"""
        if endpoint.startswith('http'):
            return endpoint
        return urljoin(self.base_url, endpoint)
    
    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """응답 처리"""
        try:
            response.raise_for_status()
            
            # Content-Type 확인
            content_type = response.headers.get('Content-Type', '')
            
            if 'application/json' in content_type:
                return response.json()
            elif 'text/xml' in content_type or 'application/xml' in content_type:
                return {'xml_content': response.text}
            else:
                return {'text_content': response.text}
        
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                raise RateLimitError(f"Rate limit exceeded: {e}")
            elif response.status_code >= 500:
                raise APIError(f"Server error: {e}")
            else:
                raise APIError(f"HTTP error: {e}")
        
        except requests.exceptions.Timeout:
            raise APITimeoutError("Request timeout")
        
        except requests.exceptions.RequestException as e:
            raise APIError(f"Request error: {e}")
    
    def get(self, endpoint: str, params: Dict = None, headers: Dict = None,
            use_cache: bool = True) -> Dict[str, Any]:
        """GET 요청"""
        url = self._make_url(endpoint)
        
        # 캐시 확인
        if self.cache and use_cache:
            cached_response = self.cache.get(url, params, headers)
            if cached_response:
                logger.debug(f"캐시에서 응답 반환: {url}")
                return cached_response
        
        # 레이트 제한 적용
        self.rate_limiter.acquire()
        
        # 요청 헤더 병합
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        # 재시도 로직
        for attempt in range(self.retry_count + 1):
            try:
                logger.debug(f"API 요청: GET {url} (시도 {attempt + 1})")
                
                response = self.session.get(
                    url, 
                    params=params, 
                    headers=request_headers,
                    timeout=self.timeout
                )
                
                result = self._handle_response(response)
                
                # 캐시에 저장
                if self.cache and use_cache:
                    self.cache.set(url, result, params, headers)
                
                logger.info(f"API 요청 성공: {url}")
                return result
            
            except (RateLimitError, APITimeoutError, APIError) as e:
                if attempt == self.retry_count:
                    logger.error(f"API 요청 최종 실패: {url} - {e}")
                    raise
                
                logger.warning(f"API 요청 실패 (재시도 {attempt + 1}): {url} - {e}")
                time.sleep(self.retry_delay * (2 ** attempt))  # 지수 백오프
    
    def post(self, endpoint: str, data: Dict = None, json_data: Dict = None,
             headers: Dict = None) -> Dict[str, Any]:
        """POST 요청"""
        url = self._make_url(endpoint)
        
        # 레이트 제한 적용
        self.rate_limiter.acquire()
        
        # 요청 헤더 병합
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        # 재시도 로직
        for attempt in range(self.retry_count + 1):
            try:
                logger.debug(f"API 요청: POST {url} (시도 {attempt + 1})")
                
                response = self.session.post(
                    url,
                    data=data,
                    json=json_data,
                    headers=request_headers,
                    timeout=self.timeout
                )
                
                result = self._handle_response(response)
                
                logger.info(f"API 요청 성공: {url}")
                return result
            
            except (RateLimitError, APITimeoutError, APIError) as e:
                if attempt == self.retry_count:
                    logger.error(f"API 요청 최종 실패: {url} - {e}")
                    raise
                
                logger.warning(f"API 요청 실패 (재시도 {attempt + 1}): {url} - {e}")
                time.sleep(self.retry_delay * (2 ** attempt))
    
    def put(self, endpoint: str, data: Dict = None, json_data: Dict = None,
            headers: Dict = None) -> Dict[str, Any]:
        """PUT 요청"""
        url = self._make_url(endpoint)
        
        # 레이트 제한 적용
        self.rate_limiter.acquire()
        
        # 요청 헤더 병합
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        # 재시도 로직
        for attempt in range(self.retry_count + 1):
            try:
                logger.debug(f"API 요청: PUT {url} (시도 {attempt + 1})")
                
                response = self.session.put(
                    url,
                    data=data,
                    json=json_data,
                    headers=request_headers,
                    timeout=self.timeout
                )
                
                result = self._handle_response(response)
                
                logger.info(f"API 요청 성공: {url}")
                return result
            
            except (RateLimitError, APITimeoutError, APIError) as e:
                if attempt == self.retry_count:
                    logger.error(f"API 요청 최종 실패: {url} - {e}")
                    raise
                
                logger.warning(f"API 요청 실패 (재시도 {attempt + 1}): {url} - {e}")
                time.sleep(self.retry_delay * (2 ** attempt))
    
    def delete(self, endpoint: str, headers: Dict = None) -> Dict[str, Any]:
        """DELETE 요청"""
        url = self._make_url(endpoint)
        
        # 레이트 제한 적용
        self.rate_limiter.acquire()
        
        # 요청 헤더 병합
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        # 재시도 로직
        for attempt in range(self.retry_count + 1):
            try:
                logger.debug(f"API 요청: DELETE {url} (시도 {attempt + 1})")
                
                response = self.session.delete(
                    url,
                    headers=request_headers,
                    timeout=self.timeout
                )
                
                result = self._handle_response(response)
                
                logger.info(f"API 요청 성공: {url}")
                return result
            
            except (RateLimitError, APITimeoutError, APIError) as e:
                if attempt == self.retry_count:
                    logger.error(f"API 요청 최종 실패: {url} - {e}")
                    raise
                
                logger.warning(f"API 요청 실패 (재시도 {attempt + 1}): {url} - {e}")
                time.sleep(self.retry_delay * (2 ** attempt))
    
    def clear_cache(self):
        """캐시 정리"""
        if self.cache:
            self.cache.clear()
            logger.info("API 캐시 정리 완료")

class APIBatchProcessor:
    """API 배치 처리 클래스"""
    
    def __init__(self, api_client: APIClient, batch_size: int = 10,
                 batch_delay: float = 1.0, max_workers: int = 1):
        self.api_client = api_client
        self.batch_size = batch_size
        self.batch_delay = batch_delay
        self.max_workers = max_workers
    
    def process_batch(self, requests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """배치 요청 처리"""
        results = []
        
        for i in range(0, len(requests), self.batch_size):
            batch = requests[i:i + self.batch_size]
            batch_results = []
            
            for request in batch:
                try:
                    method = request.get('method', 'GET').upper()
                    endpoint = request['endpoint']
                    params = request.get('params', {})
                    headers = request.get('headers', {})
                    data = request.get('data', {})
                    
                    if method == 'GET':
                        result = self.api_client.get(endpoint, params, headers)
                    elif method == 'POST':
                        result = self.api_client.post(endpoint, data, headers=headers)
                    elif method == 'PUT':
                        result = self.api_client.put(endpoint, data, headers=headers)
                    elif method == 'DELETE':
                        result = self.api_client.delete(endpoint, headers)
                    else:
                        raise ValueError(f"지원하지 않는 HTTP 메서드: {method}")
                    
                    batch_results.append({
                        'success': True,
                        'request': request,
                        'response': result
                    })
                
                except Exception as e:
                    batch_results.append({
                        'success': False,
                        'request': request,
                        'error': str(e)
                    })
            
            results.extend(batch_results)
            
            # 배치 간 대기
            if i + self.batch_size < len(requests):
                time.sleep(self.batch_delay)
        
        return results

class APIMonitor:
    """API 모니터링 클래스"""
    
    def __init__(self):
        self.stats = defaultdict(lambda: {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_response_time': 0,
            'avg_response_time': 0,
            'last_request_time': None,
            'errors': []
        })
        self.lock = threading.Lock()
    
    def record_request(self, url: str, success: bool, response_time: float, error: str = None):
        """요청 기록"""
        with self.lock:
            stats = self.stats[url]
            stats['total_requests'] += 1
            stats['last_request_time'] = datetime.now()
            
            if success:
                stats['successful_requests'] += 1
            else:
                stats['failed_requests'] += 1
                if error:
                    stats['errors'].append({
                        'timestamp': datetime.now(),
                        'error': error
                    })
            
            stats['total_response_time'] += response_time
            stats['avg_response_time'] = stats['total_response_time'] / stats['total_requests']
    
    def get_stats(self, url: str = None) -> Dict[str, Any]:
        """통계 조회"""
        with self.lock:
            if url:
                return dict(self.stats[url])
            else:
                return dict(self.stats)
    
    def get_summary(self) -> Dict[str, Any]:
        """요약 통계"""
        with self.lock:
            total_requests = sum(stats['total_requests'] for stats in self.stats.values())
            successful_requests = sum(stats['successful_requests'] for stats in self.stats.values())
            failed_requests = sum(stats['failed_requests'] for stats in self.stats.values())
            
            return {
                'total_apis': len(self.stats),
                'total_requests': total_requests,
                'successful_requests': successful_requests,
                'failed_requests': failed_requests,
                'success_rate': successful_requests / total_requests * 100 if total_requests > 0 else 0,
                'failure_rate': failed_requests / total_requests * 100 if total_requests > 0 else 0
            }

# 데코레이터들
def api_retry(retry_count: int = 3, retry_delay: float = 1.0):
    """API 재시도 데코레이터"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retry_count + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == retry_count:
                        logger.error(f"API 호출 최종 실패: {func.__name__} - {e}")
                        raise
                    
                    logger.warning(f"API 호출 실패 (재시도 {attempt + 1}): {func.__name__} - {e}")
                    time.sleep(retry_delay * (2 ** attempt))
        
        return wrapper
    return decorator

def api_timeout(timeout: int = 30):
    """API 타임아웃 데코레이터"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            import signal
            
            def timeout_handler(signum, frame):
                raise APITimeoutError(f"API 호출 타임아웃: {func.__name__}")
            
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
            
            try:
                result = func(*args, **kwargs)
                signal.alarm(0)
                return result
            except APITimeoutError:
                signal.alarm(0)
                raise
        
        return wrapper
    return decorator

def api_rate_limit(calls_per_second: float = 1.0):
    """API 레이트 제한 데코레이터"""
    rate_limiter = RateLimiter(calls_per_second)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            rate_limiter.acquire()
            return func(*args, **kwargs)
        
        return wrapper
    return decorator

# 전역 인스턴스
api_monitor = APIMonitor()

# 편의 함수들
def create_api_client(base_url: str, rate_limit: float = 1.0, **kwargs) -> APIClient:
    """API 클라이언트 생성"""
    return APIClient(base_url, rate_limit=rate_limit, **kwargs)

def get_api_stats(url: str = None) -> Dict[str, Any]:
    """API 통계 조회"""
    return api_monitor.get_stats(url)

def get_api_summary() -> Dict[str, Any]:
    """API 요약 통계"""
    return api_monitor.get_summary()

def build_query_string(params: Dict[str, Any]) -> str:
    """쿼리 문자열 생성"""
    return urlencode(params)

def parse_response_headers(headers: Dict[str, str]) -> Dict[str, Any]:
    """응답 헤더 파싱"""
    parsed = {}
    
    # 레이트 제한 정보
    if 'X-RateLimit-Limit' in headers:
        parsed['rate_limit'] = int(headers['X-RateLimit-Limit'])
    if 'X-RateLimit-Remaining' in headers:
        parsed['rate_remaining'] = int(headers['X-RateLimit-Remaining'])
    if 'X-RateLimit-Reset' in headers:
        parsed['rate_reset'] = int(headers['X-RateLimit-Reset'])
    
    # 페이지네이션 정보
    if 'X-Total-Count' in headers:
        parsed['total_count'] = int(headers['X-Total-Count'])
    if 'Link' in headers:
        parsed['pagination'] = headers['Link']
    
    return parsed

# 사용 예시
if __name__ == "__main__":
    print("🌐 API 유틸리티 테스트")
    print("=" * 50)
    
    # API 클라이언트 생성
    client = create_api_client('https://httpbin.org', rate_limit=2.0)
    
    # GET 요청 테스트
    print("📡 GET 요청 테스트:")
    try:
        response = client.get('/get', params={'test': 'value'})
        print(f"응답 받음: {response.get('args', {})}")
    except Exception as e:
        print(f"GET 요청 실패: {e}")
    
    # POST 요청 테스트
    print("\n📤 POST 요청 테스트:")
    try:
        response = client.post('/post', json_data={'key': 'value'})
        print(f"응답 받음: {response.get('json', {})}")
    except Exception as e:
        print(f"POST 요청 실패: {e}")
    
    # 레이트 제한 테스트
    print("\n⏱️ 레이트 제한 테스트:")
    start_time = time.time()
    
    for i in range(3):
        try:
            response = client.get('/get', params={'request': i})
            print(f"요청 {i+1} 완료")
        except Exception as e:
            print(f"요청 {i+1} 실패: {e}")
    
    elapsed = time.time() - start_time
    print(f"총 소요 시간: {elapsed:.2f}초")
    
    # 캐시 테스트
    print("\n💾 캐시 테스트:")
    start_time = time.time()
    
    # 첫 번째 요청 (캐시 없음)
    try:
        response1 = client.get('/get', params={'cache_test': 'first'})
        print("첫 번째 요청 완료")
    except Exception as e:
        print(f"첫 번째 요청 실패: {e}")
    
    # 두 번째 요청 (캐시 사용)
    try:
        response2 = client.get('/get', params={'cache_test': 'first'})
        print("두 번째 요청 완료 (캐시 사용)")
    except Exception as e:
        print(f"두 번째 요청 실패: {e}")
    
    elapsed = time.time() - start_time
    print(f"캐시 테스트 소요 시간: {elapsed:.2f}초")
    
    # 배치 처리 테스트
    print("\n📦 배치 처리 테스트:")
    batch_processor = APIBatchProcessor(client, batch_size=2)
    
    batch_requests = [
        {'endpoint': '/get', 'params': {'batch': 1}},
        {'endpoint': '/get', 'params': {'batch': 2}},
        {'endpoint': '/get', 'params': {'batch': 3}},
    ]
    
    try:
        batch_results = batch_processor.process_batch(batch_requests)
        successful = sum(1 for r in batch_results if r['success'])
        print(f"배치 처리 완료: {successful}/{len(batch_requests)} 성공")
    except Exception as e:
        print(f"배치 처리 실패: {e}")
    
    # API 통계 조회
    print("\n📊 API 통계:")
    summary = get_api_summary()
    print(f"총 요청: {summary['total_requests']}")
    print(f"성공률: {summary['success_rate']:.1f}%")
    
    # 데코레이터 테스트
    print("\n⚡ 데코레이터 테스트:")
    
    @api_retry(retry_count=2)
    @api_rate_limit(calls_per_second=1.0)
    def test_api_call():
        return client.get('/get', params={'decorator_test': True})
    
    try:
        result = test_api_call()
        print("데코레이터 테스트 성공")
    except Exception as e:
        print(f"데코레이터 테스트 실패: {e}")
    
    print("\n✅ 모든 API 유틸리티 테스트 완료!")