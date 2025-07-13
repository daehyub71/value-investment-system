#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
워런 버핏 투자 분석 통합 시스템
모든 기능을 한 곳에서 사용할 수 있는 메인 인터페이스
"""

import os
import sys
import json
from datetime import datetime
import argparse

def display_welcome():
    """환영 메시지 및 메뉴 표시"""
    print("💰 워런 버핏 스타일 가치투자 분석 시스템")
    print("=" * 60)
    print("🎯 Finance Data Vibe v2.0")
    print("📊 110점 만점 워런 버핏 스코어카드 시스템")
    print()
    print("🔍 사용 가능한 기능:")
    print("1️⃣  단일 종목 분석 (워런 버핏 스코어카드)")
    print("2️⃣  여러 종목 일괄 분석")
    print("3️⃣  우량주 스크리닝 (3단계 필터링)")
    print("4️⃣  주요 종목 순위표")
    print("5️⃣  데이터베이스 종목 현황")
    print("0️⃣  종료")
    print()

def analyze_single_stock():
    """단일 종목 분석"""
    print("🔍 단일 종목 분석")
    print("=" * 40)
    
    stock_code = input("📝 종목코드를 입력하세요 (예: 005930): ").strip()
    
    if not stock_code:
        print("❌ 종목코드를 입력해주세요.")
        return
    
    if len(stock_code) != 6 or not stock_code.isdigit():
        print("❌ 올바른 6자리 종목코드를 입력해주세요.")
        return
    
    print(f"🚀 {stock_code} 종목 분석을 시작합니다...")
    
    # 범용 분석기 실행
    try:
        from buffett_scorecard_calculator import BuffettScorecard
        
        analyzer = BuffettScorecard()
        result = analyzer.calculate_total_score()
        
        if result:
            print(f"\n✅ 분석 완료!")
            save_option = input(f"\n💾 결과를 파일로 저장하시겠습니까? (y/n): ").strip().lower()
            
            if save_option == 'y':
                filename = f"analysis_{stock_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                print(f"💾 결과가 {filename}에 저장되었습니다.")
        
    except ImportError:
        print("❌ 분석 모듈을 찾을 수 없습니다. buffett_scorecard_calculator.py가 있는지 확인해주세요.")
    except Exception as e:
        print(f"❌ 분석 중 오류 발생: {e}")

def batch_analysis():
    """여러 종목 일괄 분석"""
    print("📊 여러 종목 일괄 분석")
    print("=" * 40)
    
    print("📝 분석할 종목들을 입력하세요 (쉼표로 구분):")
    print("예시: 005930,000660,035420")
    
    stock_input = input("종목코드 입력: ").strip()
    
    if not stock_input:
        print("❌ 종목코드를 입력해주세요.")
        return
    
    stock_codes = [code.strip() for code in stock_input.split(',')]
    
    # 종목코드 유효성 검사
    valid_codes = []
    for code in stock_codes:
        if len(code) == 6 and code.isdigit():
            valid_codes.append(code)
        else:
            print(f"⚠️  {code}는 올바른 종목코드가 아닙니다. (6자리 숫자)")
    
    if not valid_codes:
        print("❌ 유효한 종목코드가 없습니다.")
        return
    
    print(f"🚀 {len(valid_codes)}개 종목 일괄 분석을 시작합니다...")
    
    # 주요 종목 추가 옵션
    add_major = input("🏆 주요 대형주도 함께 분석하시겠습니까? (y/n): ").strip().lower()
    
    if add_major == 'y':
        major_codes = ['005930', '000660', '035420', '005380', '051910']
        valid_codes.extend([code for code in major_codes if code not in valid_codes])
        print(f"📈 주요 대형주 {len(major_codes)}개 추가됨")
    
    print(f"📊 총 {len(valid_codes)}개 종목 분석")
    
    # 실제 일괄 분석은 추후 구현
    print("🚧 일괄 분석 기능은 곧 완성됩니다.")
    print("💡 현재는 단일 종목 분석을 여러 번 수행해주세요.")

def screening_analysis():
    """우량주 스크리닝"""
    print("🎯 워런 버핏 우량주 스크리닝")
    print("=" * 40)
    
    print("📋 스크리닝 옵션:")
    print("1. 빠른 스크리닝 (상위 50개 종목)")
    print("2. 전체 스크리닝 (모든 상장기업)")
    print("3. 커스텀 스크리닝 (조건 직접 설정)")
    
    option = input("옵션을 선택하세요 (1-3): ").strip()
    
    if option == '1':
        max_stocks = 50
        print("🚀 빠른 스크리닝을 시작합니다...")
    elif option == '2':
        max_stocks = None
        print("🚀 전체 스크리닝을 시작합니다 (시간이 오래 걸릴 수 있습니다)...")
    elif option == '3':
        print("🚧 커스텀 스크리닝은 곧 구현됩니다.")
        return
    else:
        print("❌ 올바른 옵션을 선택해주세요.")
        return
    
    try:
        os.system(f"python buffett_screening_system.py")
        print("✅ 스크리닝 완료!")
        
    except Exception as e:
        print(f"❌ 스크리닝 중 오류 발생: {e}")

def ranking_analysis():
    """주요 종목 순위표"""
    print("🏆 주요 종목 워런 버핏 스코어 순위표")
    print("=" * 40)
    
    major_stocks = {
        '005930': '삼성전자',
        '000660': 'SK하이닉스',
        '035420': 'NAVER',
        '005380': '현대차',
        '051910': 'LG화학',
        '006400': '삼성SDI',
        '035720': '카카오',
        '068270': '셀트리온',
        '000270': '기아',
        '105560': 'KB금융'
    }
    
    print(f"📊 분석 대상: {len(major_stocks)}개 주요 종목")
    
    for i, (code, name) in enumerate(major_stocks.items(), 1):
        print(f"{i:2d}. {code}: {name}")
    
    print()
    start_analysis = input("🚀 순위표 분석을 시작하시겠습니까? (y/n): ").strip().lower()
    
    if start_analysis == 'y':
        print("🚀 주요 종목 분석을 시작합니다...")
        # 실제 순위 분석은 추후 구현
        print("🚧 순위표 기능은 곧 완성됩니다.")
        print("💡 현재는 개별 종목 분석을 사용해주세요.")
    else:
        print("📊 분석을 취소했습니다.")

def check_database():
    """데이터베이스 현황 확인"""
    print("🔍 데이터베이스 종목 현황")
    print("=" * 40)
    
    try:
        os.system("python check_available_stocks.py")
        print("✅ 데이터베이스 현황 확인 완료!")
        
    except Exception as e:
        print(f"❌ 데이터베이스 확인 중 오류 발생: {e}")

def show_help():
    """도움말 표시"""
    print("📖 워런 버핏 투자 분석 시스템 사용법")
    print("=" * 50)
    print()
    print("🎯 주요 기능:")
    print()
    print("1️⃣ 단일 종목 분석")
    print("   - 6자리 종목코드 입력 (예: 005930)")
    print("   - 110점 만점 워런 버핏 스코어카드")
    print("   - 5개 영역 세부 분석")
    print("   - S~D등급 투자 의견 제공")
    print()
    print("2️⃣ 여러 종목 일괄 분석")
    print("   - 쉼표로 구분된 종목코드 입력")
    print("   - 모든 종목에 대한 종합 분석")
    print("   - 순위표 자동 생성")
    print()
    print("3️⃣ 우량주 스크리닝")
    print("   - 워런 버핏 3단계 필터링")
    print("   - 1차: 필수 조건 (ROE, 부채비율 등)")
    print("   - 2차: 우대 조건 (성장성, 배당 등)")
    print("   - 3차: 가치평가 (PER, PBR 등)")
    print()
    print("💡 사용 팁:")
    print("   - 종목코드는 6자리 숫자로 입력")
    print("   - 분석 결과는 JSON 파일로 저장 가능")
    print("   - 데이터가 없는 종목은 업종별 추정치 사용")
    print()
    print("📞 문의사항이 있으시면 개발팀에 연락해주세요!")

def main():
    """메인 실행 함수"""
    while True:
        display_welcome()
        
        choice = input("🔢 메뉴를 선택하세요 (0-5): ").strip()
        
        print()  # 빈 줄 추가
        
        if choice == '1':
            analyze_single_stock()
        elif choice == '2':
            batch_analysis()
        elif choice == '3':
            screening_analysis()
        elif choice == '4':
            ranking_analysis()
        elif choice == '5':
            check_database()
        elif choice == '0':
            print("👋 워런 버핏 투자 분석 시스템을 종료합니다.")
            print("📈 성공적인 투자하세요!")
            break
        elif choice.lower() == 'help' or choice == 'h':
            show_help()
        else:
            print("❌ 올바른 메뉴 번호를 선택해주세요.")
        
        print()
        input("⏸️  계속하려면 Enter 키를 누르세요...")
        print()

if __name__ == "__main__":
    # 명령줄 인수 처리
    parser = argparse.ArgumentParser(description='워런 버핏 투자 분석 통합 시스템')
    parser.add_argument('--stock', '-s', type=str, help='단일 종목 코드 분석')
    parser.add_argument('--screening', '-sc', action='store_true', help='우량주 스크리닝 실행')
    parser.add_argument('--ranking', '-r', action='store_true', help='주요 종목 순위표')
    parser.add_argument('--check', '-c', action='store_true', help='데이터베이스 현황 확인')
    
    args = parser.parse_args()
    
    if args.stock:
        print(f"🔍 {args.stock} 종목 분석")
        # 직접 분석 실행
        from buffett_scorecard_calculator import BuffettScorecard
        analyzer = BuffettScorecard()
        analyzer.calculate_total_score()
    elif args.screening:
        os.system("python buffett_screening_system.py")
    elif args.ranking:
        print("🏆 주요 종목 순위표 (구현 예정)")
    elif args.check:
        os.system("python check_available_stocks.py")
    else:
        # 대화형 메뉴 실행
        main()
