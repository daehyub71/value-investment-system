#!/usr/bin/env python3
"""
기술분석 모듈 import 테스트
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

print(f"프로젝트 루트: {project_root}")
print(f"Python 경로에 추가됨: {str(project_root)}")

try:
    from src.analysis.technical.technical_analysis import TechnicalAnalyzer
    print("✅ TechnicalAnalyzer import 성공!")
    
    # 간단한 테스트
    analyzer = TechnicalAnalyzer()
    print("✅ TechnicalAnalyzer 인스턴스 생성 성공!")
    
    print("\n🎉 모든 import 테스트 통과!")
    
except ImportError as e:
    print(f"❌ Import 실패: {e}")
    print(f"현재 작업 디렉토리: {os.getcwd()}")
    print(f"Python 경로: {sys.path}")

except Exception as e:
    print(f"❌ 예상치 못한 오류: {e}")
