"""
간단한 테스트 실행 스크립트
"""

import os
import sys

# 현재 디렉토리 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

def simple_test():
    """간단한 테스트"""
    print("🧪 워런 버핏 스코어카드 110점 체계 - 간단 테스트")
    print("="*60)
    
    try:
        # 모듈 import
        from buffett_scorecard_110_complete import BuffettScorecard110, create_sample_data
        
        print("✅ 모듈 import 성공")
        
        # 스코어카드 초기화
        scorecard = BuffettScorecard110()
        print("✅ 스코어카드 초기화 성공")
        
        # 샘플 데이터 생성
        financial_data, market_data = create_sample_data()
        print("✅ 샘플 데이터 생성 성공")
        
        # 분석 실행
        analysis = scorecard.calculate_comprehensive_score(financial_data, market_data)
        print("✅ 분석 실행 성공")
        
        # 결과 출력
        print(f"\n📊 {analysis.company_name} ({analysis.stock_code})")
        print(f"🏆 총점: {analysis.total_score:.1f}/110점 ({analysis.score_percentage:.1f}%)")
        print(f"📈 등급: {analysis.overall_grade}")
        print(f"💰 투자등급: {analysis.investment_grade.value}")
        print(f"⚠️ 리스크: {analysis.risk_level.value}")
        
        print(f"\n🎯 카테고리별 점수:")
        print(f"  수익성: {analysis.profitability.actual_score:.1f}/30점")
        print(f"  성장성: {analysis.growth.actual_score:.1f}/25점")
        print(f"  안정성: {analysis.stability.actual_score:.1f}/25점")
        print(f"  효율성: {analysis.efficiency.actual_score:.1f}/10점")
        print(f"  가치평가: {analysis.valuation.actual_score:.1f}/20점")
        print(f"  품질: {analysis.quality.actual_score:.1f}/10점")
        
        print(f"\n💡 {analysis.investment_thesis}")
        
        return True
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = simple_test()
    if success:
        print("\n🎉 테스트 완료!")
    else:
        print("\n💥 테스트 실패!")
