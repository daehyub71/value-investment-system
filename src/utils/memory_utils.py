"""
메모리 관리 유틸리티
메모리 사용량 모니터링 및 최적화 관련 유틸리티 함수들
"""

import gc
import psutil
import os
import sys
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from functools import wraps
from contextlib import contextmanager
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class MemoryMonitor:
    """메모리 모니터링 클래스"""
    
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.initial_memory = self.get_memory_usage()
        
    def get_memory_usage(self) -> Dict[str, float]:
        """현재 메모리 사용량 반환"""
        try:
            memory_info = self.process.memory_info()
            virtual_memory = psutil.virtual_memory()
            
            return {
                'rss': memory_info.rss / 1024 / 1024,  # MB
                'vms': memory_info.vms / 1024 / 1024,  # MB
                'percent': self.process.memory_percent(),
                'available': virtual_memory.available / 1024 / 1024,  # MB
                'total': virtual_memory.total / 1024 / 1024,  # MB
                'used': virtual_memory.used / 1024 / 1024,  # MB
                'free': virtual_memory.free / 1024 / 1024,  # MB
            }
        except Exception as e:
            logger.error(f"메모리 사용량 조회 실패: {e}")
            return {}
    
    def get_memory_info(self) -> Dict[str, Any]:
        """상세한 메모리 정보 반환"""
        try:
            usage = self.get_memory_usage()
            
            return {
                'current_usage': usage,
                'peak_usage': self.process.memory_info().peak_wset / 1024 / 1024 if hasattr(self.process.memory_info(), 'peak_wset') else 0,
                'memory_increase': usage.get('rss', 0) - self.initial_memory.get('rss', 0),
                'gc_stats': {
                    'collections': gc.get_stats(),
                    'count': gc.get_count(),
                    'threshold': gc.get_threshold()
                }
            }
        except Exception as e:
            logger.error(f"메모리 정보 조회 실패: {e}")
            return {}
    
    def print_memory_info(self):
        """메모리 정보 출력"""
        info = self.get_memory_info()
        usage = info.get('current_usage', {})
        
        print("🧠 메모리 사용량 정보")
        print("=" * 40)
        print(f"RSS (물리 메모리): {usage.get('rss', 0):.2f} MB")
        print(f"VMS (가상 메모리): {usage.get('vms', 0):.2f} MB")
        print(f"메모리 사용률: {usage.get('percent', 0):.2f}%")
        print(f"시스템 전체 메모리: {usage.get('total', 0):.2f} MB")
        print(f"사용 가능한 메모리: {usage.get('available', 0):.2f} MB")
        print(f"초기 대비 메모리 증가: {info.get('memory_increase', 0):.2f} MB")
        
        gc_stats = info.get('gc_stats', {})
        gc_count = gc_stats.get('count', [0, 0, 0])
        print(f"GC 카운트: Gen0={gc_count[0]}, Gen1={gc_count[1]}, Gen2={gc_count[2]}")

class MemoryOptimizer:
    """메모리 최적화 클래스"""
    
    @staticmethod
    def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """데이터프레임 메모리 최적화"""
        try:
            original_memory = df.memory_usage(deep=True).sum()
            optimized_df = df.copy()
            
            # 숫자 타입 최적화
            for col in optimized_df.select_dtypes(include=['int']).columns:
                col_min = optimized_df[col].min()
                col_max = optimized_df[col].max()
                
                if col_min >= -128 and col_max <= 127:
                    optimized_df[col] = optimized_df[col].astype('int8')
                elif col_min >= -32768 and col_max <= 32767:
                    optimized_df[col] = optimized_df[col].astype('int16')
                elif col_min >= -2147483648 and col_max <= 2147483647:
                    optimized_df[col] = optimized_df[col].astype('int32')
            
            # 실수 타입 최적화
            for col in optimized_df.select_dtypes(include=['float']).columns:
                optimized_df[col] = pd.to_numeric(optimized_df[col], downcast='float')
            
            # 문자열 타입 최적화
            for col in optimized_df.select_dtypes(include=['object']).columns:
                if optimized_df[col].nunique() / len(optimized_df) < 0.5:
                    optimized_df[col] = optimized_df[col].astype('category')
            
            # 날짜 타입 최적화
            for col in optimized_df.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    try:
                        optimized_df[col] = pd.to_datetime(optimized_df[col], errors='ignore')
                    except:
                        pass
            
            optimized_memory = optimized_df.memory_usage(deep=True).sum()
            reduction = (original_memory - optimized_memory) / original_memory * 100
            
            logger.info(f"데이터프레임 메모리 최적화 완료: {reduction:.2f}% 감소")
            
            return optimized_df
        except Exception as e:
            logger.error(f"데이터프레임 최적화 실패: {e}")
            return df
    
    @staticmethod
    def clear_memory():
        """메모리 정리"""
        try:
            # 가비지 컬렉션 실행
            gc.collect()
            
            # 캐시 정리
            if hasattr(pd, 'core'):
                pd.core.common.clear_cache()
            
            logger.info("메모리 정리 완료")
        except Exception as e:
            logger.error(f"메모리 정리 실패: {e}")
    
    @staticmethod
    def optimize_numpy_array(arr: np.ndarray) -> np.ndarray:
        """NumPy 배열 메모리 최적화"""
        try:
            if arr.dtype == np.float64:
                # float32로 변환 가능한지 확인
                if np.allclose(arr, arr.astype(np.float32)):
                    return arr.astype(np.float32)
            
            elif arr.dtype == np.int64:
                # 더 작은 int 타입으로 변환 가능한지 확인
                if arr.min() >= -128 and arr.max() <= 127:
                    return arr.astype(np.int8)
                elif arr.min() >= -32768 and arr.max() <= 32767:
                    return arr.astype(np.int16)
                elif arr.min() >= -2147483648 and arr.max() <= 2147483647:
                    return arr.astype(np.int32)
            
            return arr
        except Exception as e:
            logger.error(f"NumPy 배열 최적화 실패: {e}")
            return arr
    
    @staticmethod
    def batch_process(data: List[Any], batch_size: int = 1000,
                     processor: Callable = None) -> List[Any]:
        """배치 처리로 메모리 사용량 최적화"""
        if processor is None:
            processor = lambda x: x
        
        results = []
        
        try:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                batch_result = processor(batch)
                results.extend(batch_result if isinstance(batch_result, list) else [batch_result])
                
                # 배치 처리 후 메모리 정리
                if i % (batch_size * 10) == 0:
                    gc.collect()
            
            return results
        except Exception as e:
            logger.error(f"배치 처리 실패: {e}")
            return data

class MemoryCache:
    """메모리 캐시 클래스"""
    
    def __init__(self, max_size: int = 100, ttl: int = 3600):
        self.max_size = max_size
        self.ttl = ttl
        self.cache = {}
        self.access_times = {}
        self.lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        """캐시에서 값 조회"""
        with self.lock:
            if key in self.cache:
                # TTL 확인
                if time.time() - self.access_times[key] < self.ttl:
                    self.access_times[key] = time.time()
                    return self.cache[key]
                else:
                    # 만료된 항목 제거
                    del self.cache[key]
                    del self.access_times[key]
        
        return None
    
    def set(self, key: str, value: Any):
        """캐시에 값 저장"""
        with self.lock:
            # 캐시 크기 확인
            if len(self.cache) >= self.max_size:
                self._evict_oldest()
            
            self.cache[key] = value
            self.access_times[key] = time.time()
    
    def _evict_oldest(self):
        """가장 오래된 항목 제거"""
        if not self.access_times:
            return
        
        oldest_key = min(self.access_times, key=self.access_times.get)
        del self.cache[oldest_key]
        del self.access_times[oldest_key]
    
    def clear(self):
        """캐시 전체 삭제"""
        with self.lock:
            self.cache.clear()
            self.access_times.clear()
    
    def size(self) -> int:
        """캐시 크기 반환"""
        return len(self.cache)
    
    def get_stats(self) -> Dict[str, Any]:
        """캐시 통계 반환"""
        with self.lock:
            return {
                'size': len(self.cache),
                'max_size': self.max_size,
                'ttl': self.ttl,
                'keys': list(self.cache.keys())
            }

# 데코레이터들
def memory_monitor(func):
    """메모리 사용량 모니터링 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        monitor = MemoryMonitor()
        initial_memory = monitor.get_memory_usage()
        
        logger.info(f"함수 '{func.__name__}' 실행 전 메모리: {initial_memory.get('rss', 0):.2f} MB")
        
        try:
            result = func(*args, **kwargs)
            
            final_memory = monitor.get_memory_usage()
            memory_diff = final_memory.get('rss', 0) - initial_memory.get('rss', 0)
            
            logger.info(f"함수 '{func.__name__}' 실행 후 메모리: {final_memory.get('rss', 0):.2f} MB (차이: {memory_diff:+.2f} MB)")
            
            return result
        except Exception as e:
            logger.error(f"함수 '{func.__name__}' 실행 중 오류: {e}")
            raise
    
    return wrapper

def memory_limit(max_memory_mb: int = 1000):
    """메모리 사용량 제한 데코레이터"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            monitor = MemoryMonitor()
            current_memory = monitor.get_memory_usage()
            
            if current_memory.get('rss', 0) > max_memory_mb:
                logger.warning(f"메모리 사용량 초과: {current_memory.get('rss', 0):.2f} MB > {max_memory_mb} MB")
                MemoryOptimizer.clear_memory()
                
                # 메모리 정리 후 다시 확인
                current_memory = monitor.get_memory_usage()
                if current_memory.get('rss', 0) > max_memory_mb:
                    raise MemoryError(f"메모리 사용량이 제한을 초과했습니다: {current_memory.get('rss', 0):.2f} MB")
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator

def auto_cleanup(func):
    """자동 메모리 정리 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            MemoryOptimizer.clear_memory()
    
    return wrapper

# 컨텍스트 매니저
@contextmanager
def memory_context(max_memory_mb: int = 1000):
    """메모리 관리 컨텍스트 매니저"""
    monitor = MemoryMonitor()
    initial_memory = monitor.get_memory_usage()
    
    try:
        yield monitor
    finally:
        final_memory = monitor.get_memory_usage()
        memory_diff = final_memory.get('rss', 0) - initial_memory.get('rss', 0)
        
        if memory_diff > max_memory_mb * 0.5:  # 50% 이상 증가 시
            logger.warning(f"메모리 사용량 크게 증가: {memory_diff:.2f} MB")
            MemoryOptimizer.clear_memory()

# 전역 메모리 모니터 및 캐시 인스턴스
memory_monitor_instance = MemoryMonitor()
memory_cache = MemoryCache()

# 편의 함수들
def get_memory_usage() -> Dict[str, float]:
    """현재 메모리 사용량 반환"""
    return memory_monitor_instance.get_memory_usage()

def print_memory_info():
    """메모리 정보 출력"""
    memory_monitor_instance.print_memory_info()

def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """데이터프레임 메모리 최적화"""
    return MemoryOptimizer.optimize_dataframe(df)

def clear_memory():
    """메모리 정리"""
    MemoryOptimizer.clear_memory()

def cache_get(key: str) -> Optional[Any]:
    """캐시에서 값 조회"""
    return memory_cache.get(key)

def cache_set(key: str, value: Any):
    """캐시에 값 저장"""
    memory_cache.set(key, value)

def cache_clear():
    """캐시 전체 삭제"""
    memory_cache.clear()

def get_cache_stats() -> Dict[str, Any]:
    """캐시 통계 반환"""
    return memory_cache.get_stats()

# 사용 예시
if __name__ == "__main__":
    print("🧠 메모리 관리 유틸리티 테스트")
    print("=" * 50)
    
    # 메모리 모니터링 테스트
    print("📊 메모리 모니터링 테스트:")
    print_memory_info()
    
    # 데이터프레임 최적화 테스트
    print("\n📈 데이터프레임 최적화 테스트:")
    
    # 테스트 데이터 생성
    test_data = pd.DataFrame({
        'int_col': np.random.randint(1, 100, 1000),
        'float_col': np.random.random(1000),
        'str_col': np.random.choice(['A', 'B', 'C'], 1000),
        'date_col': pd.date_range('2024-01-01', periods=1000)
    })
    
    print(f"원본 데이터프레임 메모리: {test_data.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    optimized_df = optimize_dataframe(test_data)
    print(f"최적화된 데이터프레임 메모리: {optimized_df.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    # 캐시 테스트
    print("\n💾 캐시 테스트:")
    cache_set('test_key', 'test_value')
    cached_value = cache_get('test_key')
    print(f"캐시된 값: {cached_value}")
    
    cache_stats = get_cache_stats()
    print(f"캐시 통계: {cache_stats}")
    
    # 메모리 컨텍스트 테스트
    print("\n🔄 메모리 컨텍스트 테스트:")
    with memory_context(max_memory_mb=500) as monitor:
        # 메모리를 많이 사용하는 작업 시뮬레이션
        large_data = np.random.random((1000, 1000))
        current_usage = monitor.get_memory_usage()
        print(f"컨텍스트 내 메모리 사용량: {current_usage.get('rss', 0):.2f} MB")
    
    # 데코레이터 테스트
    print("\n⚡ 데코레이터 테스트:")
    
    @memory_monitor
    @auto_cleanup
    def test_function():
        """테스트 함수"""
        data = np.random.random((100, 100))
        df = pd.DataFrame(data)
        return df.sum().sum()
    
    result = test_function()
    print(f"테스트 함수 결과: {result:.2f}")
    
    # 배치 처리 테스트
    print("\n📦 배치 처리 테스트:")
    large_list = list(range(10000))
    
    def square_batch(batch):
        return [x**2 for x in batch]
    
    squared_results = MemoryOptimizer.batch_process(large_list, batch_size=1000, processor=square_batch)
    print(f"배치 처리 결과 길이: {len(squared_results)}")
    
    # 최종 메모리 상태
    print("\n🏁 최종 메모리 상태:")
    print_memory_info()
    
    print("\n✅ 모든 메모리 관리 테스트 완료!")