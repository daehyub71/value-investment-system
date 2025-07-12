#!/usr/bin/env python3
"""
워런 버핏 스코어카드 분석 실행 스크립트

실행 방법:
python scripts/analysis/run_buffett_analysis.py --stock_code=005930
python scripts/analysis/run_buffett_analysis.py --stock_code=005930 --save_to_db
python scripts/analysis/run_buffett_analysis.py --all_stocks --top=50
"""

import sys
import os
import argparse
import json
from pathlib import Path
import logging

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.database_config import DatabaseConfig
from config.logging_config import setup_logging
from src.analysis.fundamental.buffett_scorecard import BuffettScorecard

def analyze_single_stock(stock_code: str, save_to_db: bool = False) -> dict:
    """단일 종목 워런 버핏 스코어카드 분석"""
    scorecard = BuffettScorecard()
    
    print(f"\n🎯 워런 버핏 스코어카드 분석: {stock_code}")
    print("=" * 60)
    
    # 분석 실행
    result = scorecard.calculate_total_score(stock_code)
    
    if 'error' in result:
        print(f"❌ 분석 실패: {result['error']}")
        return result
    
    # 결과 출력
    print(f"📊 기업명: {result['company_name']}")
    print(f"📅 분석일: {result['analysis_date']}")
    print()
    
    print("🏆 종합 점수")
    print(f"총점: {result['total_score']:.1f}/{result['max_score']}점")
    print(f"등급: {result['grade']}")
    print(f"투자추천: {result['recommendation']}")
    print()
    
    print("📈 카테고리별 점수")
    scores = result['scores']
    print(f"• 수익성 지표: {scores['profitability']:.1f}/30점")
    print(f"• 성장성 지표: {scores['growth']:.1f}/25점")
    print(f"• 안정성 지표: {scores['stability']:.1f}/25점")
    print(f"• 효율성 지표: {scores['efficiency']:.1f}/10점")
    print(f"• 가치평가 지표: {scores['valuation']:.1f}/20점")
    print(f"• 보너스 점수: {scores['bonus']:.1f}/10점")
    print()
    
    # 재무 요약
    if 'financial_summary' in result:
        summary = result['financial_summary']
        print("💰 재무 요약")
        print(f"• 매출액: {summary.get('revenue', 0):,}백만원")
        print(f"• 순이익: {summary.get('net_income', 0):,}백만원")
        print(f"• 총자산: {summary.get('total_assets', 0):,}백만원")
        print(f"• 자기자본: {summary.get('total_equity', 0):,}백만원")
        print(f"• 부채비율: {summary.get('debt_ratio', 0):.1%}")
        print(f"• ROE: {summary.get('roe', 0):.1f}%")
        print()
    
    # 주가 정보
    if 'stock_info' in result:
        stock_info = result['stock_info']
        print("📊 주가 정보")
        print(f"• 현재가: {stock_info.get('current_price', 0):,}원")
        print(f"• 52주 최고가: {stock_info.get('high_52w', 0):,}원")
        print(f"• 52주 최저가: {stock_info.get('low_52w', 0):,}원")
        print(f"• 시가총액: {stock_info.get('market_cap', 0):,}억원")
        print()
    
    # 상세 점수 (옵션)
    print("🔍 상세 점수 분석")
    if 'score_details' in result:
        details = result['score_details']
        
        if 'profitability' in details:
            prof = details['profitability']
            print("수익성 지표:")
            for key, value in prof.items():
                print(f"  - {key}: {value:.1f}점")
        
        if 'growth' in details:
            growth = details['growth']
            print("성장성 지표:")
            for key, value in growth.items():
                print(f"  - {key}: {value:.1f}점")
        
        if 'stability' in details:
            stability = details['stability']
            print("안정성 지표:")
            for key, value in stability.items():
                print(f"  - {key}: {value:.1f}점")
    
    # 데이터베이스 저장
    if save_to_db:
        success = scorecard.save_to_database(result)
        if success:
            print("✅ 결과가 데이터베이스에 저장되었습니다.")
        else:
            print("❌ 데이터베이스 저장에 실패했습니다.")
    
    return result

def analyze_multiple_stocks(limit: int = 50, save_to_db: bool = False) -> list:
    """다중 종목 분석 (시가총액 상위 종목)"""
    db_config = DatabaseConfig()
    
    try:
        # 분석할 종목 리스트 조회
        with db_config.get_connection('stock') as conn:
            query = """
            SELECT stock_code, company_name, market_cap
            FROM company_info 
            WHERE market_cap IS NOT NULL AND market_cap > 0
            ORDER BY market_cap DESC 
            LIMIT ?
            """
            
            import pandas as pd
            stocks_df = pd.read_sql(query, conn, params=(limit,))
        
        if stocks_df.empty:
            print("❌ 분석할 종목을 찾을 수 없습니다.")
            return []
        
        print(f"\n🎯 다중 종목 워런 버핏 스코어카드 분석 (상위 {len(stocks_df)}개 종목)")
        print("=" * 80)
        
        scorecard = BuffettScorecard()
        results = []
        
        for idx, row in stocks_df.iterrows():
            stock_code = row['stock_code']
            company_name = row['company_name']
            
            print(f"\n진행률: {idx+1}/{len(stocks_df)} - {company_name}({stock_code})")
            
            try:
                result = scorecard.calculate_total_score(stock_code)
                
                if 'error' not in result:
                    # 간단한 결과 출력
                    print(f"  점수: {result['total_score']:.1f}점 ({result['grade']}) - {result['recommendation']}")
                    
                    # 데이터베이스 저장
                    if save_to_db:
                        scorecard.save_to_database(result)
                    
                    results.append(result)
                else:
                    print(f"  ❌ 분석 실패: {result['error']}")
                
            except Exception as e:
                print(f"  ❌ 오류 발생: {e}")
                continue
        
        # 결과 요약
        if results:
            print(f"\n📊 분석 결과 요약")
            print("=" * 50)
            
            # 점수순 정렬
            sorted_results = sorted(results, key=lambda x: x['total_score'], reverse=True)
            
            print("🏆 상위 10개 종목:")
            for i, result in enumerate(sorted_results[:10], 1):
                print(f"{i:2d}. {result['company_name']:<15} {result['total_score']:>6.1f}점 ({result['grade']})")
            
            print(f"\n📈 전체 통계:")
            scores = [r['total_score'] for r in results]
            print(f"• 분석 완료: {len(results)}개 종목")
            print(f"• 평균 점수: {sum(scores)/len(scores):.1f}점")
            print(f"• 최고 점수: {max(scores):.1f}점")
            print(f"• 최저 점수: {min(scores):.1f}점")
            
            # 등급별 분포
            grades = {}
            for result in results:
                grade = result['grade'].split()[0]  # 'A+', 'A', 'B+' 등에서 첫 부분만
                grades[grade] = grades.get(grade, 0) + 1
            
            print(f"\n📊 등급별 분포:")
            for grade, count in sorted(grades.items()):
                print(f"• {grade}등급: {count}개 종목")
        
        return results
        
    except Exception as e:
        print(f"❌ 다중 종목 분석 실패: {e}")
        return []

def generate_report(results: list, output_file: str = None):
    """분석 결과 리포트 생성"""
    if not results:
        print("생성할 리포트 데이터가 없습니다.")
        return
    
    # JSON 리포트 생성
    report = {
        'analysis_date': results[0]['analysis_date'],
        'total_analyzed': len(results),
        'summary': {
            'avg_score': sum(r['total_score'] for r in results) / len(results),
            'max_score': max(r['total_score'] for r in results),
            'min_score': min(r['total_score'] for r in results)
        },
        'top_10': sorted(results, key=lambda x: x['total_score'], reverse=True)[:10],
        'detailed_results': results
    }
    
    # 파일 저장
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"📄 리포트가 저장되었습니다: {output_file}")
    else:
        # 콘솔 출력
        print("\n📋 JSON 리포트:")
        print(json.dumps(report['summary'], ensure_ascii=False, indent=2))

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='워런 버핏 스코어카드 분석')
    parser.add_argument('--stock_code', type=str, help='분석할 종목코드 (예: 005930)')
    parser.add_argument('--all_stocks', action='store_true', help='전체 종목 분석')
    parser.add_argument('--top', type=int, default=50, help='분석할 상위 종목 수 (기본값: 50)')
    parser.add_argument('--save_to_db', action='store_true', help='결과를 데이터베이스에 저장')
    parser.add_argument('--output', type=str, help='결과를 JSON 파일로 저장')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 로깅 설정
    setup_logging(level=args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        if args.stock_code:
            # 단일 종목 분석
            result = analyze_single_stock(args.stock_code, args.save_to_db)
            
            if args.output:
                generate_report([result], args.output)
            
        elif args.all_stocks:
            # 다중 종목 분석
            results = analyze_multiple_stocks(args.top, args.save_to_db)
            
            if args.output:
                generate_report(results, args.output)
            
        else:
            parser.print_help()
            print(f"\n💡 사용 예시:")
            print(f"  {sys.argv[0]} --stock_code=005930")
            print(f"  {sys.argv[0]} --stock_code=005930 --save_to_db")
            print(f"  {sys.argv[0]} --all_stocks --top=20 --save_to_db")
            print(f"  {sys.argv[0]} --all_stocks --output=buffett_analysis.json")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()