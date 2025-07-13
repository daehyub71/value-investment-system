#!/usr/bin/env python3
"""
JSON 안전 저장 유틸리티
Pandas/Numpy 타입 자동 변환으로 JSON 직렬화 오류 방지
"""

import json
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Union

def safe_json_serializer(obj: Any) -> Any:
    """JSON 직렬화를 위한 안전한 커스텀 함수"""
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return str(obj)
    elif hasattr(obj, 'item'):  # pandas scalar
        return obj.item()
    elif hasattr(obj, 'isoformat'):  # datetime objects
        return obj.isoformat()
    else:
        raise TypeError(f'Object of type {type(obj).__name__} is not JSON serializable')

def convert_to_python_types(data: Any) -> Any:
    """Pandas/Numpy 타입을 재귀적으로 Python 기본 타입으로 변환"""
    if isinstance(data, dict):
        return {key: convert_to_python_types(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_to_python_types(item) for item in data]
    elif isinstance(data, tuple):
        return tuple(convert_to_python_types(item) for item in data)
    elif isinstance(data, (np.integer, np.int64, np.int32)):
        return int(data)
    elif isinstance(data, (np.floating, np.float64, np.float32)):
        return float(data)
    elif isinstance(data, np.ndarray):
        return data.tolist()
    elif isinstance(data, (pd.Timestamp, pd.Timedelta)):
        return str(data)
    elif hasattr(data, 'item'):  # pandas scalar
        return data.item()
    elif hasattr(data, 'isoformat'):  # datetime objects
        return data.isoformat()
    else:
        return data

def safe_json_dump(data: Dict[str, Any], filepath: str, **kwargs) -> bool:
    """안전한 JSON 저장 (타입 변환 자동 처리)"""
    try:
        # 1단계: 데이터 타입 변환
        safe_data = convert_to_python_types(data)
        
        # 2단계: JSON 저장 (커스텀 직렬화 함수 적용)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(
                safe_data, 
                f, 
                ensure_ascii=False, 
                indent=2, 
                default=safe_json_serializer,
                **kwargs
            )
        
        print(f"✅ JSON 저장 완료: {filepath}")
        return True
        
    except Exception as e:
        print(f"❌ JSON 저장 실패: {e}")
        return False

# 사용 예시
if __name__ == "__main__":
    # 테스트 데이터 (다양한 numpy/pandas 타입 포함)
    test_data = {
        'numpy_int': np.int64(42),
        'numpy_float': np.float64(3.14159),
        'numpy_array': np.array([1, 2, 3]),
        'pandas_series': pd.Series([1, 2, 3]).iloc[0],  # pandas scalar
        'normal_data': {'string': 'hello', 'number': 123}
    }
    
    # 안전한 JSON 저장
    safe_json_dump(test_data, 'test_output.json')
