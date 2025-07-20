#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BuffettScorecard110 클래스 메소드 확인 및 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_buffett_scorecard_methods():
    """BuffettScorecard110 클래스의 실제 메소드들 확인"""
    print("🔍 BuffettScorecard110 클래스 메소드 분석")
    print("=" * 60)
    
    try:
        from src.analysis.fundamental.buffett_scorecard_110_complete import BuffettScorecard110
        
        # 인스턴스 생성
        scorecard = BuffettScorecard110()
        print("✅ BuffettScorecard110 클래스 로드 성공")
        
        # 모든 메소드 확인
        all_methods = [method for method in dir(scorecard) if not method.startswith('_')]
        print(f"\n📋 전체 메소드 수: {len(all_methods)}개")
        
        # 카테고리별 메소드 분류
        calculate_methods = [m for m in all_methods if m.startswith('calculate')]
        analyze_methods = [m for m in all_methods if 'analyze' in m]
        get_methods = [m for m in all_methods if m.startswith('get')]
        other_methods = [m for m in all_methods if m not in calculate_methods + analyze_methods + get_methods]
        
        print(f"\n📊 calculate 메소드들 ({len(calculate_methods)}개):")
        for method in calculate_methods:
            print(f"   - {method}")
        
        print(f"\n🔬 analyze 메소드들 ({len(analyze_methods)}개):")
        for method in analyze_methods:
            print(f"   - {method}")
        
        print(f"\n📥 get 메소드들 ({len(get_methods)}개):")
        for method in get_methods:
            print(f"   - {method}")
        
        print(f"\n🔧 기타 메소드들 ({len(other_methods)}개):")
        for method in other_methods:
            print(f"   - {method}")
        
        # 메인 분석 메소드 확인
        main_methods = []
        if hasattr(scorecard, 'calculate_comprehensive_score'):
            main_methods.append('calculate_comprehensive_score')
        if hasattr(scorecard, 'analyze_stock'):
            main_methods.append('analyze_stock')
        if hasattr(scorecard, 'run_analysis'):
            main_methods.append('run_analysis')
        
        print(f"\n🎯 메인 분석 메소드 후보들:")
        for method in main_methods:
            method_obj = getattr(scorecard, method)
            # 메소드 시그니처 확인
            import inspect
            sig = inspect.signature(method_obj)
            print(f"   - {method}{sig}")
        
        # 테스트해볼 메소드 추천
        if 'calculate_comprehensive_score' in main_methods:
            print("\n✅ 추천 메소드: calculate_comprehensive_score")
            return 'calculate_comprehensive_score'
        elif calculate_methods:
            print(f"\n💡 대안 메소드: {calculate_methods[0]}")
            return calculate_methods[0]
        else:
            print("\n❌ 적절한 분석 메소드를 찾을 수 없습니다.")
            return None
            
    except ImportError as e:
        print(f"❌ BuffettScorecard110 import 실패: {e}")
        return None
    except Exception as e:
        print(f"❌ 메소드 분석 실패: {e}")
        return None

def test_scorecard_method(method_name):
    """특정 메소드 테스트"""
    print(f"\n🧪 {method_name} 메소드 테스트")
    print("=" * 60)
    
    try:
        from src.analysis.fundamental.buffett_scorecard_110_complete import BuffettScorecard110
        scorecard = BuffettScorecard110()
        
        if hasattr(scorecard, method_name):
            method = getattr(scorecard, method_name)
            
            # 메소드 시그니처 확인
            import inspect
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            
            print(f"📋 메소드 시그니처: {method_name}{sig}")
            print(f"📥 필요한 파라미터: {params}")
            
            # 파라미터에 따라 테스트 데이터 준비
            if 'financial_data' in params and 'market_data' in params:
                print("🧮 financial_data와 market_data가 필요한 메소드")
                
                # 샘플 데이터 생성
                financial_data = {
                    'stock_code': '005930',
                    'company_name': '삼성전자',
                    'revenue': 279600000000000,
                    'net_income': 26900000000000,
                    'total_assets': 400000000000000,
                    'shareholders_equity': 286700000000000,
                }
                
                market_data = {
                    'stock_price': 72000,
                    'market_cap': 400000000000000
                }
                
                print("📊 샘플 데이터로 테스트 실행...")
                result = method(financial_data, market_data)
                
                print(f"✅ 테스트 성공!")
                print(f"📈 결과 타입: {type(result)}")
                
                # 결과 구조 분석
                if hasattr(result, '__dict__'):
                    print("📋 결과 속성들:")
                    for attr, value in result.__dict__.items():
                        print(f"   - {attr}: {type(value)}")
                elif isinstance(result, dict):
                    print("📋 결과 키들:")
                    for key, value in result.items():
                        print(f"   - {key}: {type(value)}")
                
                return result
                
            elif 'stock_code' in params:
                print("🏷️ stock_code가 필요한 메소드")
                
                print("📊 삼성전자(005930)로 테스트 실행...")
                result = method('005930')
                
                print(f"✅ 테스트 성공!")
                print(f"📈 결과: {result}")
                
                return result
                
            else:
                print("❓ 파라미터가 명확하지 않은 메소드")
                print("💡 파라미터 없이 호출 시도...")
                
                try:
                    result = method()
                    print(f"✅ 테스트 성공! 결과: {result}")
                    return result
                except Exception as e:
                    print(f"❌ 파라미터 없는 호출 실패: {e}")
                    return None
        else:
            print(f"❌ {method_name} 메소드가 존재하지 않습니다.")
            return None
            
    except Exception as e:
        print(f"❌ 메소드 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """메인 실행"""
    print("🎯 BuffettScorecard110 클래스 완전 분석")
    print("=" * 70)
    
    # 1단계: 메소드 확인
    recommended_method = check_buffett_scorecard_methods()
    
    # 2단계: 추천 메소드 테스트
    if recommended_method:
        result = test_scorecard_method(recommended_method)
        
        if result:
            print(f"\n🎉 {recommended_method} 메소드 사용 가능!")
            print("💡 이 메소드를 실제 분석 시스템에 적용하세요.")
        else:
            print(f"\n❌ {recommended_method} 메소드 테스트 실패")
    
    print("\n" + "=" * 70)
    print("🔍 BuffettScorecard110 분석 완료")

if __name__ == "__main__":
    main()
