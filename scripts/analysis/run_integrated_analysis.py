#!/usr/bin/env python3
"""
통합 분석 실행 스크립트
워런 버핏 스코어카드 + 기술분석 + 감정분석을 통합한 최종 투자 분석

실행 방법:
python scripts/analysis/run_integrated_analysis.py --stock_code=005930
python scripts/analysis/run_integrated_analysis.py --all_stocks --top=30
python scripts/analysis/run_integrated_analysis.py --stock_code=005930 --save_to_db
"""

import sys
import os
import argparse
import json
import pandas as pd
from pathlib import Path
import logging

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.database_config import DatabaseConfig
from config.logging_config import setup_logging
from src.analysis.integrated.integrated_analyzer import IntegratedAnalyzer

def analyze_single_stock(stock_code: str, save_to_db: bool = False, 
                        technical_days: int = 252, sentiment_days: int = 30) -> dict:
    """단일 종목 통합 분석"""
    analyzer = IntegratedAnalyzer()
    
    print(f"\n🔬 통합 분석: {stock_code}")
    print("=" * 80)
    
    # 분석 실행
    result = analyzer.analyze_stock(stock_code, technical_days, sentiment_days)
    
    if 'error' in result:
        print(f"❌ 분석 실패: {result['error']}")
        return result
    
    # 기본 정보
    print(f"📊 기업명: {result['company_name']}")
    print(f"📅 분석일: {result['analysis_date']}")
    print()
    
    # 🏆 종합 결과
    print("🏆 종합 투자 평가")
    total_score = result['total_score']
    final_grade = result['final_grade']
    recommendation = result['investment_recommendation']
    risk_level = result['risk_level']
    data_quality = result['data_quality']
    
    # 등급별 이모티콘
    grade_icons = {
        'S+': '🌟', 'S': '⭐', 'A+': '🥇', 'A': '🏆',
        'B+': '🥈', 'B': '🏅', 'C+': '🥉', 'C': '📊',
        'D': '📉', 'F': '❌'
    }
    
    # 추천별 이모티콘
    recommendation_icons = {
        'STRONG_BUY': '🟢🟢', 'BUY': '🟢', 'WEAK_BUY': '🟡',
        'HOLD': '🟡', 'WEAK_SELL': '🟠', 'SELL': '🔴', 'STRONG_SELL': '🔴🔴'
    }
    
    # 위험도별 이모티콘
    risk_icons = {'LOW': '🟢', 'MEDIUM': '🟡', 'HIGH': '🔴'}
    
    grade_icon = grade_icons.get(final_grade, '📊')
    rec_icon = recommendation_icons.get(recommendation, '🟡')
    risk_icon = risk_icons.get(risk_level, '🟡')
    
    print(f"최종 점수: {total_score:.1f}/100점")
    print(f"투자 등급: {grade_icon} {final_grade}")
    print(f"투자 추천: {rec_icon} {recommendation}")
    print(f"위험 수준: {risk_icon} {risk_level}")
    print(f"데이터 품질: {data_quality:.1f}%")
    print()
    
    # 📊 세부 점수 분석
    print("📊 세부 분석 점수")
    analysis_scores = result['analysis_scores']
    
    # 기본분석 (워런 버핏 스코어카드)
    fundamental = analysis_scores['fundamental']
    if fundamental['available']:
        print(f"🎯 기본분석: {fundamental['weighted_score']:.1f}점 (가중치: {fundamental['weight']:.1f}%)")
        print(f"   워런 버핏 스코어: {fundamental['raw_score']:.1f}/110점")
    else:
        print("🎯 기본분석: ❌ 데이터 없음")
    
    # 기술분석
    technical = analysis_scores['technical']
    if technical['available']:
        print(f"📈 기술분석: {technical['weighted_score']:.1f}점 (가중치: {technical['weight']:.1f}%)")
        print(f"   신호 강도: {technical['raw_score']:.0f}/100")
    else:
        print("📈 기술분석: ❌ 데이터 없음")
    
    # 감정분석
    sentiment = analysis_scores['sentiment']
    if sentiment['available']:
        print(f"💭 감정분석: {sentiment['weighted_score']:.1f}점 (가중치: {sentiment['weight']:.1f}%)")
        print(f"   감정 지수: {sentiment['raw_score']:+.3f}")
    else:
        print("💭 감정분석: ❌ 데이터 없음")
    print()
    
    # 💡 투자 포인트
    highlights = result.get('investment_highlights', [])
    if highlights:
        print("💡 투자 포인트")
        for i, highlight in enumerate(highlights, 1):
            print(f"  {i}. {highlight}")
        print()
    
    # ⚠️ 주의사항
    risk_factors = result.get('risk_factors', [])
    if risk_factors:
        print("⚠️ 주의사항")
        for i, risk in enumerate(risk_factors, 1):
            print(f"  {i}. {risk}")
        print()
    
    # 📋 상세 분석 요약
    if 'detailed_analysis' in result:
        detailed = result['detailed_analysis']
        
        print("📋 상세 분석 요약")
        
        # 기본분석 요약
        if fundamental['available'] and 'error' not in detailed['fundamental']:
            fund_detail = detailed['fundamental']
            scores = fund_detail.get('scores', {})
            print(f"🎯 기본분석 상세:")
            print(f"   수익성: {scores.get('profitability', 0):.1f}/30점")
            print(f"   성장성: {scores.get('growth', 0):.1f}/25점")
            print(f"   안정성: {scores.get('stability', 0):.1f}/25점")
            print(f"   효율성: {scores.get('efficiency', 0):.1f}/10점")
            print(f"   가치평가: {scores.get('valuation', 0):.1f}/20점")
        
        # 기술분석 요약
        if technical['available'] and 'error' not in detailed['technical']:
            tech_detail = detailed['technical']
            print(f"📈 기술분석 상세:")
            print(f"   종합신호: {tech_detail.get('overall_signal', 'N/A')}")
            print(f"   RSI: {tech_detail.get('rsi', 0):.1f}")
            print(f"   현재가: {tech_detail.get('current_price', 0):,}원")
        
        # 감정분석 요약
        if sentiment['available'] and 'error' not in detailed['sentiment']:
            sent_detail = detailed['sentiment']
            print(f"💭 감정분석 상세:")
            print(f"   감정등급: {sent_detail.get('sentiment_grade', 'N/A')}")
            print(f"   뉴스 수: {sent_detail.get('total_news_count', 0)}건")
            print(f"   신뢰도: {sent_detail.get('avg_confidence', 0):.1%}")
        print()
    
    # 💼 투자 제안
    print("💼 투자 제안")
    if recommendation == 'STRONG_BUY':
        print("🟢 강력 매수 추천")
        print("   여러 분석이 일치하여 강한 상승 신호를 보이고 있습니다.")
        print("   적극적인 매수를 고려할 수 있습니다.")
    elif recommendation == 'BUY':
        print("🟢 매수 추천")
        print("   전반적으로 긍정적인 신호가 우세합니다.")
        print("   매수 타이밍으로 판단됩니다.")
    elif recommendation == 'WEAK_BUY':
        print("🟡 약한 매수")
        print("   일부 긍정적 신호가 있으나 신중한 접근이 필요합니다.")
    elif recommendation == 'HOLD':
        print("🟡 보유 권장")
        print("   현재로서는 특별한 방향성이 없습니다.")
        print("   기존 보유 종목이라면 보유하며 관망하세요.")
    elif recommendation == 'WEAK_SELL':
        print("🟠 약한 매도")
        print("   일부 부정적 신호가 감지됩니다.")
        print("   포지션 축소를 고려해보세요.")
    elif recommendation == 'SELL':
        print("🔴 매도 추천")
        print("   여러 부정적 신호가 우세합니다.")
        print("   매도를 고려하는 것이 좋겠습니다.")
    elif recommendation == 'STRONG_SELL':
        print("🔴 강력 매도 추천")
        print("   모든 분석이 부정적입니다.")
        print("   즉시 매도를 검토하세요.")
    
    # 위험도별 추가 조언
    if risk_level == 'HIGH':
        print("\n⚠️ 고위험 종목입니다. 투자 시 각별한 주의가 필요합니다.")
    elif risk_level == 'MEDIUM':
        print("\n🟡 중간 위험 종목입니다. 적절한 포지션 관리가 필요합니다.")
    else:
        print("\n🟢 저위험 종목입니다. 상대적으로 안전한 투자가 가능합니다.")
    
    # 데이터베이스 저장
    if save_to_db:
        success = analyzer.save_to_database(result)
        if success:
            print("\n✅ 분석 결과가 데이터베이스에 저장되었습니다.")
        else:
            print("\n❌ 데이터베이스 저장에 실패했습니다.")
    
    return result

def analyze_multiple_stocks(limit: int = 30, save_to_db: bool = False, 
                           technical_days: int = 252, sentiment_days: int = 30) -> list:
    """다중 종목 통합 분석"""
    db_config = DatabaseConfig()
    
    try:
        # 분석할 종목 리스트 조회 (시가총액 상위)
        with db_config.get_connection('stock') as conn:
            query = """
            SELECT stock_code, company_name, market_cap
            FROM company_info 
            WHERE market_cap IS NOT NULL AND market_cap > 0
            ORDER BY market_cap DESC 
            LIMIT ?
            """
            
            stocks_df = pd.read_sql(query, conn, params=(limit,))
        
        if stocks_df.empty:
            print("❌ 분석할 종목을 찾을 수 없습니다.")
            return []
        
        print(f"\n🔬 다중 종목 통합 분석 (상위 {len(stocks_df)}개 종목)")
        print("=" * 80)
        
        analyzer = IntegratedAnalyzer()
        results = []
        
        for idx, row in stocks_df.iterrows():
            stock_code = row['stock_code']
            company_name = row['company_name']
            
            print(f"\n진행률: {idx+1}/{len(stocks_df)} - {company_name}({stock_code})")
            
            try:
                result = analyzer.analyze_stock(stock_code, technical_days, sentiment_days)
                
                if 'error' not in result:
                    # 간단한 결과 출력
                    score = result['total_score']
                    grade = result['final_grade']
                    recommendation = result['investment_recommendation']
                    print(f"  점수: {score:.1f}점 ({grade}) - {recommendation}")
                    
                    # 데이터베이스 저장
                    if save_to_db:
                        analyzer.save_to_database(result)
                    
                    results.append(result)
                else:
                    print(f"  ❌ 분석 실패: {result['error']}")
                
            except Exception as e:
                print(f"  ❌ 오류 발생: {e}")
                continue
        
        # 결과 요약
        if results:
            print(f"\n📊 통합 분석 결과 요약")
            print("=" * 60)
            
            # 점수순 정렬
            sorted_results = sorted(results, key=lambda x: x['total_score'], reverse=True)
            
            print("🏆 투자 추천 상위 10개 종목:")
            for i, result in enumerate(sorted_results[:10], 1):
                stock_code = result['stock_code']
                company_name = result['company_name']
                score = result['total_score']
                grade = result['final_grade']
                recommendation = result['investment_recommendation']
                
                # 추천별 아이콘
                rec_icon = '🟢' if 'BUY' in recommendation else '🟡' if 'HOLD' in recommendation else '🔴'
                
                print(f"{i:2d}. {company_name:<15} {score:>6.1f}점 ({grade}) {rec_icon} {recommendation}")
            
            # 통계
            scores = [r['total_score'] for r in results]
            grades = {}
            recommendations = {}
            
            for result in results:
                grade = result['final_grade']
                rec = result['investment_recommendation']
                grades[grade] = grades.get(grade, 0) + 1
                recommendations[rec] = recommendations.get(rec, 0) + 1
            
            print(f"\n📈 전체 통계:")
            print(f"• 분석 완료: {len(results)}개 종목")
            print(f"• 평균 점수: {sum(scores)/len(scores):.1f}점")
            print(f"• 최고 점수: {max(scores):.1f}점")
            print(f"• 최저 점수: {min(scores):.1f}점")
            
            print(f"\n🏆 등급별 분포:")
            for grade, count in sorted(grades.items()):
                print(f"• {grade}등급: {count}개 종목")
            
            print(f"\n💼 추천별 분포:")
            rec_order = ['STRONG_BUY', 'BUY', 'WEAK_BUY', 'HOLD', 'WEAK_SELL', 'SELL', 'STRONG_SELL']
            for rec in rec_order:
                if rec in recommendations:
                    count = recommendations[rec]
                    icon = '🟢' if 'BUY' in rec else '🟡' if 'HOLD' in rec else '🔴'
                    print(f"• {icon} {rec}: {count}개 종목")
            
            # 우수 종목 별도 표시
            excellent_stocks = [r for r in sorted_results if r['total_score'] >= 80]
            if excellent_stocks:
                print(f"\n⭐ 우수 종목 (80점 이상): {len(excellent_stocks)}개")
                for result in excellent_stocks:
                    print(f"   {result['company_name']}: {result['total_score']:.1f}점")
        
        return results
        
    except Exception as e:
        print(f"❌ 다중 종목 통합 분석 실패: {e}")
        return []

def generate_integrated_report(results: list, output_file: str = None):
    """통합 분석 결과 리포트 생성"""
    if not results:
        print("생성할 리포트 데이터가 없습니다.")
        return
    
    # 통계 계산
    scores = [r['total_score'] for r in results if 'total_score' in r]
    
    if scores:
        avg_score = sum(scores) / len(scores)
        max_score = max(scores)
        min_score = min(scores)
    else:
        avg_score = max_score = min_score = 0
    
    # 등급별 분포
    grade_dist = {}
    rec_dist = {}
    risk_dist = {}
    
    for result in results:
        grade = result.get('final_grade', 'N/A')
        rec = result.get('investment_recommendation', 'N/A')
        risk = result.get('risk_level', 'N/A')
        
        grade_dist[grade] = grade_dist.get(grade, 0) + 1
        rec_dist[rec] = rec_dist.get(rec, 0) + 1
        risk_dist[risk] = risk_dist.get(risk, 0) + 1
    
    # 상위 종목 추출
    sorted_results = sorted(results, key=lambda x: x.get('total_score', 0), reverse=True)
    top_10 = sorted_results[:10]
    
    # 투자 추천 종목 (매수 추천)
    buy_recommendations = [
        r for r in results 
        if r.get('investment_recommendation', '') in ['STRONG_BUY', 'BUY', 'WEAK_BUY']
    ]
    buy_recommendations.sort(key=lambda x: x.get('total_score', 0), reverse=True)
    
    # 리포트 생성
    report = {
        'analysis_date': results[0].get('analysis_date', ''),
        'total_analyzed': len(results),
        'summary_statistics': {
            'average_score': round(avg_score, 1),
            'maximum_score': round(max_score, 1),
            'minimum_score': round(min_score, 1),
            'excellent_count': len([r for r in results if r.get('total_score', 0) >= 80]),
            'good_count': len([r for r in results if 70 <= r.get('total_score', 0) < 80]),
            'fair_count': len([r for r in results if 60 <= r.get('total_score', 0) < 70]),
            'poor_count': len([r for r in results if r.get('total_score', 0) < 60])
        },
        'distributions': {
            'grades': grade_dist,
            'recommendations': rec_dist,
            'risk_levels': risk_dist
        },
        'top_10_stocks': top_10,
        'buy_recommendations': buy_recommendations[:15],  # 상위 15개
        'investment_insights': {
            'strong_buy_count': rec_dist.get('STRONG_BUY', 0),
            'buy_count': rec_dist.get('BUY', 0),
            'hold_count': rec_dist.get('HOLD', 0),
            'sell_count': rec_dist.get('SELL', 0) + rec_dist.get('STRONG_SELL', 0),
            'high_risk_count': risk_dist.get('HIGH', 0),
            'low_risk_count': risk_dist.get('LOW', 0)
        },
        'detailed_results': results
    }
    
    # 파일 저장
    if output_file:
        # 디렉토리 생성
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"📄 통합 분석 리포트가 저장되었습니다: {output_file}")
    else:
        # 콘솔 출력
        print("\n📋 통합 분석 요약:")
        print(f"분석 종목 수: {report['total_analyzed']}")
        print(f"평균 점수: {report['summary_statistics']['average_score']:.1f}점")
        print(f"우수 종목 (80점 이상): {report['summary_statistics']['excellent_count']}개")
        print(f"매수 추천 종목: {len(buy_recommendations)}개")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='통합 분석 실행')
    parser.add_argument('--stock_code', type=str, help='분석할 종목코드 (예: 005930)')
    parser.add_argument('--all_stocks', action='store_true', help='전체 종목 분석')
    parser.add_argument('--top', type=int, default=30, help='분석할 상위 종목 수 (기본값: 30)')
    parser.add_argument('--technical_days', type=int, default=252, help='기술분석 기간 (일수, 기본값: 252)')
    parser.add_argument('--sentiment_days', type=int, default=30, help='감정분석 기간 (일수, 기본값: 30)')
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
            result = analyze_single_stock(
                args.stock_code, 
                args.save_to_db, 
                args.technical_days, 
                args.sentiment_days
            )
            
            if args.output:
                generate_integrated_report([result], args.output)
            
        elif args.all_stocks:
            # 다중 종목 분석
            results = analyze_multiple_stocks(
                args.top, 
                args.save_to_db, 
                args.technical_days, 
                args.sentiment_days
            )
            
            if args.output:
                generate_integrated_report(results, args.output)
            
        else:
            parser.print_help()
            print(f"\n💡 사용 예시:")
            print(f"  {sys.argv[0]} --stock_code=005930")
            print(f"  {sys.argv[0]} --stock_code=005930 --save_to_db")
            print(f"  {sys.argv[0]} --all_stocks --top=20")
            print(f"  {sys.argv[0]} --all_stocks --output=reports/integrated_analysis.json")
            print(f"  {sys.argv[0]} --stock_code=005930 --technical_days=126 --sentiment_days=14")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()