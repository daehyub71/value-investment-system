#!/usr/bin/env python3
"""
기술분석 실행 스크립트

실행 방법:
python scripts/analysis/run_technical_analysis.py --stock_code=005930
python scripts/analysis/run_technical_analysis.py --stock_code=005930 --period=6M
python scripts/analysis/run_technical_analysis.py --all_stocks --top=50
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
from src.analysis.technical.technical_analysis import TechnicalAnalyzer

def analyze_single_stock(stock_code: str, days: int = 252) -> dict:
    """단일 종목 기술분석"""
    analyzer = TechnicalAnalyzer()
    
    print(f"\n📈 기술분석: {stock_code}")
    print("=" * 60)
    
    # 분석 실행
    result = analyzer.analyze_stock(stock_code, days)
    
    if 'error' in result:
        print(f"❌ 분석 실패: {result['error']}")
        return result
    
    # 결과 출력
    print(f"📊 종목코드: {stock_code}")
    print(f"📅 분석일: {result['analysis_date']}")
    print(f"📋 분석기간: {days}일")
    print()
    
    # 종합 신호
    print("🎯 매매신호")
    signal_strength = result.get('signal_strength', 0)
    overall_signal = result.get('overall_signal', 'HOLD')
    
    signal_color = "🟢" if signal_strength > 0 else "🔴" if signal_strength < 0 else "🟡"
    print(f"종합신호: {signal_color} {overall_signal}")
    print(f"신호강도: {signal_strength}/100")
    print()
    
    # 매수/매도 신호 상세
    buy_signals = result.get('buy_signals', '')
    sell_signals = result.get('sell_signals', '')
    
    if buy_signals:
        print("📈 매수신호:")
        for signal in buy_signals.split(', '):
            if signal.strip():
                print(f"  • {signal}")
        print()
    
    if sell_signals:
        print("📉 매도신호:")
        for signal in sell_signals.split(', '):
            if signal.strip():
                print(f"  • {signal}")
        print()
    
    # 주요 지표 현재값
    print("📊 주요 기술지표")
    current_price = result.get('current_price', 0)
    print(f"현재가: {current_price:,}원")
    print()
    
    # 추세 지표
    print("📈 추세 지표:")
    sma_20 = result.get('sma_20')
    sma_60 = result.get('sma_60')
    sma_120 = result.get('sma_120')
    ema_12 = result.get('ema_12')
    ema_26 = result.get('ema_26')
    
    if sma_20: print(f"  SMA(20): {sma_20:,.0f}원")
    if sma_60: print(f"  SMA(60): {sma_60:,.0f}원")
    if sma_120: print(f"  SMA(120): {sma_120:,.0f}원")
    if ema_12: print(f"  EMA(12): {ema_12:,.0f}원")
    if ema_26: print(f"  EMA(26): {ema_26:,.0f}원")
    print()
    
    # 모멘텀 지표
    print("⚡ 모멘텀 지표:")
    rsi = result.get('rsi')
    macd = result.get('macd')
    macd_signal = result.get('macd_signal')
    stoch_k = result.get('stoch_k')
    
    if rsi is not None:
        rsi_status = "과매수" if rsi > 70 else "과매도" if rsi < 30 else "중립"
        print(f"  RSI(14): {rsi:.1f} ({rsi_status})")
    
    if macd is not None and macd_signal is not None:
        macd_status = "상승" if macd > macd_signal else "하락"
        print(f"  MACD: {macd:.2f} / Signal: {macd_signal:.2f} ({macd_status})")
    
    if stoch_k is not None:
        stoch_status = "과매수" if stoch_k > 80 else "과매도" if stoch_k < 20 else "중립"
        print(f"  Stochastic %K: {stoch_k:.1f} ({stoch_status})")
    print()
    
    # 변동성 지표
    print("📊 변동성 지표:")
    bb_upper = result.get('bb_upper')
    bb_lower = result.get('bb_lower')
    bb_middle = result.get('bb_middle')
    bb_percent = result.get('bb_percent')
    atr = result.get('atr')
    
    if bb_upper and bb_lower and bb_middle:
        print(f"  볼린저 밴드:")
        print(f"    상단: {bb_upper:,.0f}원")
        print(f"    중간: {bb_middle:,.0f}원")
        print(f"    하단: {bb_lower:,.0f}원")
        if bb_percent is not None:
            bb_status = "상단 근접" if bb_percent > 0.8 else "하단 근접" if bb_percent < 0.2 else "중앙 위치"
            print(f"    %B: {bb_percent:.2f} ({bb_status})")
    
    if atr is not None:
        print(f"  ATR(14): {atr:,.0f}원 (변동성)")
    print()
    
    # 추세 강도
    adx = result.get('adx')
    if adx is not None:
        trend_strength = "강한 추세" if adx > 25 else "약한 추세" if adx > 20 else "횡보"
        print(f"📈 추세강도: ADX {adx:.1f} ({trend_strength})")
        print()
    
    # 투자 제안
    print("💡 투자 제안")
    if overall_signal == 'STRONG_BUY':
        print("🟢 강력 매수 추천 - 여러 지표가 강한 상승 신호를 보이고 있습니다.")
    elif overall_signal == 'BUY':
        print("🟢 매수 추천 - 상승 신호가 우세합니다.")
    elif overall_signal == 'WEAK_BUY':
        print("🟡 약한 매수 - 일부 긍정적 신호가 있으나 신중하게 접근하세요.")
    elif overall_signal == 'HOLD':
        print("🟡 보유 권장 - 명확한 방향성이 없습니다. 관망하세요.")
    elif overall_signal == 'WEAK_SELL':
        print("🟠 약한 매도 - 일부 부정적 신호가 있습니다.")
    elif overall_signal == 'SELL':
        print("🔴 매도 추천 - 하락 신호가 우세합니다.")
    elif overall_signal == 'STRONG_SELL':
        print("🔴 강력 매도 추천 - 여러 지표가 강한 하락 신호를 보이고 있습니다.")
    
    return result

def analyze_multiple_stocks(limit: int = 50, days: int = 252) -> list:
    """다중 종목 기술분석"""
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
            
            stocks_df = pd.read_sql(query, conn, params=(limit,))
        
        if stocks_df.empty:
            print("❌ 분석할 종목을 찾을 수 없습니다.")
            return []
        
        print(f"\n📈 다중 종목 기술분석 (상위 {len(stocks_df)}개 종목)")
        print("=" * 80)
        
        analyzer = TechnicalAnalyzer()
        results = []
        
        for idx, row in stocks_df.iterrows():
            stock_code = row['stock_code']
            company_name = row['company_name']
            
            print(f"\n진행률: {idx+1}/{len(stocks_df)} - {company_name}({stock_code})")
            
            try:
                result = analyzer.analyze_stock(stock_code, days)
                
                if 'error' not in result:
                    # 간단한 결과 출력
                    signal = result.get('overall_signal', 'HOLD')
                    strength = result.get('signal_strength', 0)
                    print(f"  신호: {signal} (강도: {strength})")
                    
                    results.append(result)
                else:
                    print(f"  ❌ 분석 실패: {result['error']}")
                
            except Exception as e:
                print(f"  ❌ 오류 발생: {e}")
                continue
        
        # 결과 요약
        if results:
            print(f"\n📊 기술분석 결과 요약")
            print("=" * 50)
            
            # 신호별 분류
            signals = {}
            for result in results:
                signal = result.get('overall_signal', 'HOLD')
                if signal not in signals:
                    signals[signal] = []
                signals[signal].append(result)
            
            print("📈 신호별 분포:")
            signal_order = ['STRONG_BUY', 'BUY', 'WEAK_BUY', 'HOLD', 'WEAK_SELL', 'SELL', 'STRONG_SELL']
            for signal in signal_order:
                if signal in signals:
                    count = len(signals[signal])
                    print(f"• {signal}: {count}개 종목")
            
            # 매수 추천 상위 종목
            buy_stocks = []
            for signal in ['STRONG_BUY', 'BUY', 'WEAK_BUY']:
                if signal in signals:
                    buy_stocks.extend(signals[signal])
            
            if buy_stocks:
                # 신호 강도순 정렬
                buy_stocks.sort(key=lambda x: x.get('signal_strength', 0), reverse=True)
                
                print(f"\n🟢 매수 추천 상위 10개 종목:")
                for i, result in enumerate(buy_stocks[:10], 1):
                    stock_code = result.get('stock_code', '')
                    signal = result.get('overall_signal', '')
                    strength = result.get('signal_strength', 0)
                    
                    # 회사명 조회
                    try:
                        company_name = stocks_df[stocks_df['stock_code'] == stock_code]['company_name'].iloc[0]
                    except:
                        company_name = stock_code
                    
                    print(f"{i:2d}. {company_name:<15} {signal:<12} (강도: {strength:>3d})")
            
            # 매도 추천 종목
            sell_stocks = []
            for signal in ['STRONG_SELL', 'SELL', 'WEAK_SELL']:
                if signal in signals:
                    sell_stocks.extend(signals[signal])
            
            if sell_stocks:
                sell_stocks.sort(key=lambda x: x.get('signal_strength', 0))  # 낮은 순
                
                print(f"\n🔴 매도 주의 종목:")
                for i, result in enumerate(sell_stocks[:5], 1):
                    stock_code = result.get('stock_code', '')
                    signal = result.get('overall_signal', '')
                    strength = result.get('signal_strength', 0)
                    
                    try:
                        company_name = stocks_df[stocks_df['stock_code'] == stock_code]['company_name'].iloc[0]
                    except:
                        company_name = stock_code
                    
                    print(f"{i:2d}. {company_name:<15} {signal:<12} (강도: {strength:>3d})")
        
        return results
        
    except Exception as e:
        print(f"❌ 다중 종목 기술분석 실패: {e}")
        return []

def generate_technical_report(results: list, output_file: str = None):
    """기술분석 결과 리포트 생성"""
    if not results:
        print("생성할 리포트 데이터가 없습니다.")
        return
    
    # 신호별 통계
    signal_stats = {}
    for result in results:
        signal = result.get('overall_signal', 'HOLD')
        signal_stats[signal] = signal_stats.get(signal, 0) + 1
    
    # 평균 신호 강도
    strengths = [r.get('signal_strength', 0) for r in results if 'signal_strength' in r]
    avg_strength = sum(strengths) / len(strengths) if strengths else 0
    
    # 리포트 생성
    report = {
        'analysis_date': results[0].get('analysis_date', ''),
        'total_analyzed': len(results),
        'signal_distribution': signal_stats,
        'avg_signal_strength': round(avg_strength, 1),
        'top_buy_signals': [
            r for r in results 
            if r.get('overall_signal') in ['STRONG_BUY', 'BUY'] 
               and r.get('signal_strength', 0) > 30
        ][:10],
        'top_sell_signals': [
            r for r in results 
            if r.get('overall_signal') in ['STRONG_SELL', 'SELL'] 
               and r.get('signal_strength', 0) < -30
        ][:10],
        'detailed_results': results
    }
    
    # 파일 저장
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"📄 기술분석 리포트가 저장되었습니다: {output_file}")
    else:
        # 콘솔 출력
        print("\n📋 기술분석 요약:")
        print(f"분석 종목 수: {report['total_analyzed']}")
        print(f"평균 신호강도: {report['avg_signal_strength']}")
        print("신호 분포:", json.dumps(report['signal_distribution'], ensure_ascii=False, indent=2))

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='기술분석 실행')
    parser.add_argument('--stock_code', type=str, help='분석할 종목코드 (예: 005930)')
    parser.add_argument('--all_stocks', action='store_true', help='전체 종목 분석')
    parser.add_argument('--top', type=int, default=50, help='분석할 상위 종목 수 (기본값: 50)')
    parser.add_argument('--period', type=str, default='1Y', 
                       help='분석 기간 (1M, 3M, 6M, 1Y, 2Y, 기본값: 1Y)')
    parser.add_argument('--output', type=str, help='결과를 JSON 파일로 저장')
    parser.add_argument('--log_level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='로그 레벨')
    
    args = parser.parse_args()
    
    # 기간을 일수로 변환
    period_mapping = {
        '1M': 22, '3M': 66, '6M': 126, '1Y': 252, '2Y': 504
    }
    days = period_mapping.get(args.period.upper(), 252)
    
    # 로깅 설정
    setup_logging(level=args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        if args.stock_code:
            # 단일 종목 분석
            result = analyze_single_stock(args.stock_code, days)
            
            if args.output:
                generate_technical_report([result], args.output)
            
        elif args.all_stocks:
            # 다중 종목 분석
            results = analyze_multiple_stocks(args.top, days)
            
            if args.output:
                generate_technical_report(results, args.output)
            
        else:
            parser.print_help()
            print(f"\n💡 사용 예시:")
            print(f"  {sys.argv[0]} --stock_code=005930")
            print(f"  {sys.argv[0]} --stock_code=005930 --period=6M")
            print(f"  {sys.argv[0]} --all_stocks --top=20 --period=3M")
            print(f"  {sys.argv[0]} --all_stocks --output=technical_analysis.json")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()