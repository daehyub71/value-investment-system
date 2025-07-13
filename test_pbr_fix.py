#!/usr/bin/env python3
"""
PBR 수정 테스트 스크립트
삼성전자와 SK하이닉스로 수정된 코드 테스트
"""

import sys
import os
from pathlib import Path

# 현재 디렉토리를 경로에 추가
sys.path.insert(0, str(Path(__file__).parent))

try:
    from batch_buffett_scorecard import BatchBuffettScorecard
    print("✅ batch_buffett_scorecard 모듈 로드 성공")
except ImportError as e:
    print(f"❌ 모듈 로드 실패: {e}")
    sys.exit(1)

def test_pbr_fix():
    """PBR 수정 테스트"""
    
    print("🧪 PBR 수정 테스트 시작")
    print("=" * 60)
    
    # 테스트할 종목들
    test_stocks = ['005930', '000660']  # 삼성전자, SK하이닉스
    
    batch_processor = BatchBuffettScorecard(batch_size=10, delay=1.0)
    
    for i, stock_code in enumerate(test_stocks, 1):
        print(f"\n[{i}/2] {stock_code} 테스트:")
        
        try:
            # 수정된 collect_yahoo_data 메서드 테스트
            data = batch_processor.collect_yahoo_data(stock_code)
            
            if data:
                company_name = data.get('company_name', 'Unknown')
                pbr = data.get('price_to_book')
                
                print(f"  ✅ {company_name}")
                if pbr:
                    print(f"     PBR: {pbr:.3f} ✅")
                    
                    # PBR 평가
                    if pbr < 0.8:
                        evaluation = "매우 저평가"
                    elif pbr < 1.0:
                        evaluation = "저평가"
                    elif pbr < 1.5:
                        evaluation = "적정가치"
                    elif pbr < 2.5:
                        evaluation = "약간 고평가"
                    else:
                        evaluation = "고평가"
                    
                    print(f"     평가: {evaluation}")
                else:
                    print(f"     PBR: ❌ 여전히 누락")
                
                # 기타 지표들
                print(f"     Forward PE: {data.get('forward_pe', 'N/A')}")
                print(f"     ROE: {data.get('return_on_equity', 'N/A')}%")
                print(f"     부채비율: {data.get('debt_to_equity', 'N/A')}")
                
            else:
                print(f"  ❌ 데이터 수집 실패")
        
        except Exception as e:
            print(f"  ❌ 오류: {e}")
    
    print(f"\n" + "=" * 60)
    print("🎯 테스트 결과 요약")
    print("=" * 60)
    print("✅ 수정된 batch_buffett_scorecard.py 테스트 완료")
    print("📊 PBR 데이터 수집 상태 확인됨")
    print(f"\n💡 다음 단계:")
    print(f"   1. PBR 현황 확인: python fix_pbr_data.py --status")
    print(f"   2. 특정 종목 수정: python fix_pbr_data.py --codes 005930,000660")
    print(f"   3. 전체 배치 테스트: python batch_buffett_scorecard.py --test")

if __name__ == "__main__":
    test_pbr_fix()
