"""
워런 버핏 스코어카드 110점 체계 - 테스트 및 실행 스크립트
간단한 테스트를 위한 스크립트

사용법:
1. 단순 테스트: python test_buffett_110.py
2. 특정 종목 테스트: python test_buffett_110.py --stock-code 005930
3. 샘플 데이터 테스트: python test_buffett_110.py --sample
"""

import sys
import json
from pathlib import Path
from typing import Dict, Any

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

try:
    from src.analysis.fundamental.buffett_scorecard_110_complete import (
        BuffettScorecard110, create_sample_data, test_buffett_scorecard
    )
    from src.analysis.fundamental.buffett_batch_processor import BuffettBatchProcessor
except ImportError as e:
    print(f"Import 오류: {e}")
    print("현재 디렉토리에서 직접 import 시도...")
    try:
        from buffett_scorecard_110_complete import (
            BuffettScorecard110, create_sample_data, test_buffett_scorecard
        )
        from buffett_batch_processor import BuffettBatchProcessor
    except ImportError as e2:
        print(f"Import 실패: {e2}")
        sys.exit(1)

def test_single_analysis():
    """단일 분석 테스트"""
    print("🧪 워런 버핏 스코어카드 110점 체계 - 단일 분석 테스트")
    print("=" * 60)
    
    # 스코어카드 초기화
    scorecard = BuffettScorecard110()
    
    # 샘플 데이터 생성
    financial_data, market_data = create_sample_data()
    
    print("📊 입력 데이터:")
    print(f"  기업명: {financial_data['company_name']}")
    print(f"  종목코드: {financial_data['stock_code']}")
    print(f"  매출: {financial_data['revenue']:,}원")
    print(f"  순이익: {financial_data['net_income']:,}원")
    print(f"  주가: {market_data['stock_price']:,}원")
    print()
    
    # 분석 실행
    analysis = scorecard.calculate_comprehensive_score(financial_data, market_data)
    
    # 결과 출력
    print("🎯 분석 결과:")
    print(f"  총점: {analysis.total_score:.1f}/110점 ({analysis.score_percentage:.1f}%)")
    print(f"  종합등급: {analysis.overall_grade}")
    print(f"  투자등급: {analysis.investment_grade.value}")
    print(f"  리스크: {analysis.risk_level.value}")
    print(f"  품질등급: {analysis.quality_rating.value}")
    print()
    
    # 카테고리별 점수
    print("📈 카테고리별 점수:")
    categories = [
        analysis.profitability, analysis.growth, analysis.stability,
        analysis.efficiency, analysis.valuation, analysis.quality
    ]
    
    for category in categories:
        print(f"  {category.category}: {category.actual_score:.1f}/{category.max_score}점 "
              f"({category.percentage:.1f}% - {category.grade})")
    print()
    
    # 강점/약점
    if analysis.key_strengths:
        print("✅ 주요 강점:")
        for strength in analysis.key_strengths[:3]:
            print(f"  • {strength}")
        print()
    
    if analysis.key_weaknesses:
        print("⚠️ 주요 약점:")
        for weakness in analysis.key_weaknesses[:3]:
            print(f"  • {weakness}")
        print()
    
    # 투자 논리
    print("💡 투자 논리:")
    print(f"  {analysis.investment_thesis}")
    print()
    
    return analysis

def test_batch_processing(limit: int = 5):
    """배치 처리 테스트"""
    print(f"🔄 배치 처리 테스트 (최대 {limit}개 종목)")
    print("=" * 60)
    
    try:
        # 배치 처리기 초기화
        processor = BuffettBatchProcessor()
        
        # 종목 목록 조회
        stock_list = processor.get_stock_list()
        print(f"📊 전체 종목 수: {len(stock_list)}개")
        
        if not stock_list:
            print("❌ 분석 가능한 종목이 없습니다.")
            return
        
        # 제한된 수만큼 처리
        test_stocks = stock_list[:limit]
        print(f"🎯 테스트 대상: {len(test_stocks)}개")
        print()
        
        results = []
        for i, stock_info in enumerate(test_stocks, 1):
            stock_code = stock_info['stock_code']
            company_name = stock_info['company_name']
            
            print(f"[{i}/{len(test_stocks)}] 분석 중: {company_name} ({stock_code})")
            
            try:
                analysis = processor.process_single_stock(stock_code, company_name)
                if analysis:
                    results.append(analysis)
                    print(f"  ✅ 완료 - 점수: {analysis.total_score:.1f}/110점")
                else:
                    print(f"  ❌ 실패 - 데이터 부족")
            except Exception as e:
                print(f"  ❌ 오류: {e}")
        
        print()
        print(f"🎉 배치 테스트 완료: {len(results)}/{len(test_stocks)}개 성공")
        
        if results:
            # 결과 정렬
            results.sort(key=lambda x: x.total_score, reverse=True)
            
            print("\n🏆 상위 종목:")
            for i, result in enumerate(results[:3], 1):
                print(f"  {i}. {result.company_name} ({result.stock_code}) - {result.total_score:.1f}점")
        
        return results
        
    except Exception as e:
        print(f"❌ 배치 테스트 오류: {e}")
        return []

def test_specific_stock(stock_code: str):
    """특정 종목 테스트"""
    print(f"🔍 특정 종목 테스트: {stock_code}")
    print("=" * 60)
    
    try:
        processor = BuffettBatchProcessor()
        
        # 종목 정보 조회
        stock_list = processor.get_stock_list()
        stock_info = next((s for s in stock_list if s['stock_code'] == stock_code), None)
        
        if not stock_info:
            print(f"❌ 종목을 찾을 수 없습니다: {stock_code}")
            return None
        
        company_name = stock_info['company_name']
        print(f"📊 기업명: {company_name}")
        
        # 분석 실행
        analysis = processor.process_single_stock(stock_code, company_name)
        
        if analysis:
            print(f"✅ 분석 완료!")
            print(f"  총점: {analysis.total_score:.1f}/110점")
            print(f"  등급: {analysis.overall_grade}")
            print(f"  추천: {analysis.investment_grade.value}")
            print(f"  리스크: {analysis.risk_level.value}")
            
            # 세부 분석 결과
            print(f"\n📈 카테고리별 상세:")
            categories = [
                analysis.profitability, analysis.growth, analysis.stability,
                analysis.efficiency, analysis.valuation, analysis.quality
            ]
            
            for category in categories:
                print(f"  {category.category}: {category.actual_score:.1f}/{category.max_score}점 ({category.percentage:.1f}%)")
                
                # 상위 3개 세부 지표
                top_details = sorted(category.details, key=lambda x: x.score, reverse=True)[:3]
                for detail in top_details:
                    print(f"    • {detail.name}: {detail.score:.1f}/{detail.max_score}점")
            
            if analysis.investment_thesis:
                print(f"\n💡 투자 논리:")
                print(f"  {analysis.investment_thesis}")
            
            return analysis
        else:
            print(f"❌ 분석 실패: 데이터 부족")
            return None
            
    except Exception as e:
        print(f"❌ 종목 분석 오류: {e}")
        return None

def save_test_results(results: list, filename: str = "test_results_110.json"):
    """테스트 결과 저장"""
    try:
        # 결과 정리
        output_data = {
            "test_date": str(pd.Timestamp.now()),
            "total_tested": len(results),
            "results": []
        }
        
        for result in results:
            output_data["results"].append({
                "stock_code": result.stock_code,
                "company_name": result.company_name,
                "total_score": result.total_score,
                "grade": result.overall_grade,
                "investment_grade": result.investment_grade.value,
                "categories": {
                    "profitability": result.profitability.actual_score,
                    "growth": result.growth.actual_score,
                    "stability": result.stability.actual_score,
                    "efficiency": result.efficiency.actual_score,
                    "valuation": result.valuation.actual_score,
                    "quality": result.quality.actual_score
                }
            })
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"📁 테스트 결과 저장: {filename}")
        
    except Exception as e:
        print(f"❌ 결과 저장 오류: {e}")

def main():
    """메인 실행 함수"""
    import argparse
    import pandas as pd
    
    parser = argparse.ArgumentParser(description='워런 버핏 스코어카드 110점 테스트')
    parser.add_argument('--sample', action='store_true', help='샘플 데이터로 테스트')
    parser.add_argument('--stock-code', type=str, help='특정 종목 테스트')
    parser.add_argument('--batch', type=int, default=5, help='배치 테스트할 종목 수')
    parser.add_argument('--save', action='store_true', help='결과 저장')
    
    args = parser.parse_args()
    
    results = []
    
    try:
        if args.sample:
            # 샘플 데이터 테스트
            print("🧪 샘플 데이터로 테스트 실행")
            result = test_buffett_scorecard()
            if result:
                results.append(result)
                
        elif args.stock_code:
            # 특정 종목 테스트
            result = test_specific_stock(args.stock_code)
            if result:
                results.append(result)
                
        else:
            # 일반 테스트
            print("📝 워런 버핏 스코어카드 110점 체계 종합 테스트")
            print("=" * 60)
            
            # 1. 샘플 데이터 테스트
            print("\n1️⃣ 샘플 데이터 테스트:")
            sample_result = test_single_analysis()
            if sample_result:
                results.append(sample_result)
            
            # 2. 배치 처리 테스트
            print(f"\n2️⃣ 배치 처리 테스트 ({args.batch}개 종목):")
            batch_results = test_batch_processing(args.batch)
            results.extend(batch_results)
        
        # 결과 저장
        if args.save and results:
            save_test_results(results)
        
        # 최종 요약
        if results:
            print(f"\n🎯 테스트 완료 요약:")
            print(f"  처리된 종목: {len(results)}개")
            scores = [r.total_score for r in results]
            print(f"  평균 점수: {sum(scores)/len(scores):.1f}점")
            print(f"  최고 점수: {max(scores):.1f}점")
            print(f"  최저 점수: {min(scores):.1f}점")
            
            # 등급 분포
            grades = {}
            for result in results:
                grade = result.investment_grade.value
                grades[grade] = grades.get(grade, 0) + 1
            
            print(f"\n📊 투자 등급 분포:")
            for grade, count in grades.items():
                print(f"  {grade}: {count}개")
        else:
            print("❌ 테스트 결과가 없습니다.")
            
    except KeyboardInterrupt:
        print("\n⏹️ 테스트가 중단되었습니다.")
    except Exception as e:
        print(f"❌ 테스트 실행 오류: {e}")

if __name__ == "__main__":
    main()
