"""
워런 버핏 스코어카드 110점 체계 - 간단한 배치 실행기
기본 기능만 구현한 버전
"""

import sqlite3
import logging
import json
import os
from datetime import date
from typing import Dict, List, Optional, Any
from pathlib import Path

# 현재 디렉토리 설정
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent.parent

try:
    from buffett_scorecard_110_complete import BuffettScorecard110, BuffettAnalysis
except ImportError as e:
    print(f"Import 오류: {e}")
    print("buffett_scorecard_110_complete.py 파일이 같은 디렉토리에 있는지 확인하세요.")
    exit(1)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleBuffettProcessor:
    """간단한 워런 버핏 스코어카드 처리기"""
    
    def __init__(self, data_dir: str = "data"):
        """초기화"""
        self.data_dir = Path(project_root) / data_dir
        self.scorecard = BuffettScorecard110()
        
        logger.info("간단한 워런 버핏 처리기 초기화 완료")
    
    def create_sample_stocks(self) -> List[Dict]:
        """샘플 종목 데이터 생성"""
        return [
            {
                'stock_code': '005930',
                'company_name': '삼성전자',
                'net_income': 26900000000000,
                'shareholders_equity': 286700000000000,
                'total_assets': 400000000000000,
                'revenue': 279600000000000,
            },
            {
                'stock_code': '000660',
                'company_name': 'SK하이닉스',
                'net_income': 15000000000000,
                'shareholders_equity': 180000000000000,
                'total_assets': 250000000000000,
                'revenue': 120000000000000,
            },
            {
                'stock_code': '035420',
                'company_name': 'NAVER',
                'net_income': 2500000000000,
                'shareholders_equity': 25000000000000,
                'total_assets': 35000000000000,
                'revenue': 8500000000000,
            }
        ]
    
    def process_sample_stocks(self) -> List[BuffettAnalysis]:
        """샘플 종목들 처리"""
        logger.info("샘플 종목 워런 버핏 분석 시작")
        
        sample_stocks = self.create_sample_stocks()
        results = []
        
        market_data = {'stock_price': 50000}  # 임시 주가
        
        for stock_data in sample_stocks:
            try:
                logger.info(f"분석 중: {stock_data['company_name']}")
                
                # 분석 실행
                analysis = self.scorecard.calculate_comprehensive_score(stock_data, market_data)
                results.append(analysis)
                
                print(f"✅ {analysis.company_name}: {analysis.total_score:.1f}/110점 ({analysis.overall_grade})")
                
            except Exception as e:
                logger.error(f"분석 오류 ({stock_data['company_name']}): {e}")
                continue
        
        logger.info(f"샘플 분석 완료: {len(results)}개 성공")
        return results
    
    def save_results(self, results: List[BuffettAnalysis], filename: str = "sample_buffett_results.json"):
        """결과를 JSON으로 저장"""
        try:
            output_data = {
                "analysis_date": str(date.today()),
                "total_analyzed": len(results),
                "results": []
            }
            
            for result in results:
                output_data["results"].append({
                    "stock_code": result.stock_code,
                    "company_name": result.company_name,
                    "total_score": result.total_score,
                    "score_percentage": result.score_percentage,
                    "overall_grade": result.overall_grade,
                    "investment_grade": result.investment_grade.value,
                    "risk_level": result.risk_level.value,
                    "quality_rating": result.quality_rating.value,
                    "investment_thesis": result.investment_thesis,
                    "categories": {
                        "profitability": result.profitability.actual_score,
                        "growth": result.growth.actual_score,
                        "stability": result.stability.actual_score,
                        "efficiency": result.efficiency.actual_score,
                        "valuation": result.valuation.actual_score,
                        "quality": result.quality.actual_score
                    }
                })
            
            # 결과 정렬 (점수 순)
            output_data["results"].sort(key=lambda x: x["total_score"], reverse=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"결과 저장: {filename}")
            
            # 요약 출력
            print(f"\n🎯 워런 버핏 스코어카드 110점 분석 결과")
            print("=" * 50)
            print(f"📊 분석 종목: {len(results)}개")
            
            if results:
                scores = [r.total_score for r in results]
                print(f"📈 평균 점수: {sum(scores)/len(scores):.1f}점")
                print(f"🏆 최고 점수: {max(scores):.1f}점")
                print()
                
                print("🥇 상위 종목:")
                sorted_results = sorted(results, key=lambda x: x.total_score, reverse=True)
                for i, result in enumerate(sorted_results, 1):
                    print(f"  {i}. {result.company_name} ({result.stock_code}) "
                          f"- {result.total_score:.1f}점 ({result.overall_grade})")
            
            return True
            
        except Exception as e:
            logger.error(f"결과 저장 오류: {e}")
            return False

def main():
    """메인 실행 함수"""
    print("🎯 워런 버핏 스코어카드 110점 체계 - 간단 배치 처리")
    print("=" * 60)
    
    try:
        # 처리기 초기화
        processor = SimpleBuffettProcessor()
        
        # 샘플 분석 실행
        results = processor.process_sample_stocks()
        
        if results:
            # 결과 저장
            success = processor.save_results(results)
            
            if success:
                print(f"\n🎉 처리 완료!")
                print(f"분석된 종목: {len(results)}개")
                print(f"결과 파일: sample_buffett_results.json")
            else:
                print(f"\n⚠️ 결과 저장 실패")
        else:
            print(f"\n❌ 분석된 종목이 없습니다.")
            
    except Exception as e:
        logger.error(f"실행 오류: {e}")
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()
