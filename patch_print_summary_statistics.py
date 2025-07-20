#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
print_summary_statistics 함수 자동 패치 프로그램
============================================

run_technical_analysis_all_stocks.py 파일에서 None 값 처리 오류를 
자동으로 수정하는 패치 프로그램입니다.

실행 방법:
python patch_print_summary_statistics.py

기능:
- 기존 파일 자동 백업
- print_summary_statistics 함수만 교체
- 안전한 None 값 처리 로직 적용
"""

import os
import re
import shutil
from datetime import datetime
from pathlib import Path

class PrintSummaryStatisticsPatcher:
    """print_summary_statistics 함수 패치 클래스"""
    
    def __init__(self, target_file: str = "run_technical_analysis_all_stocks.py"):
        self.target_file = Path(target_file)
        self.backup_file = None
        
    def create_backup(self) -> bool:
        """원본 파일 백업"""
        if not self.target_file.exists():
            print(f"❌ 대상 파일이 존재하지 않습니다: {self.target_file}")
            return False
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.backup_file = self.target_file.with_suffix(f".backup_{timestamp}.py")
        
        try:
            shutil.copy2(self.target_file, self.backup_file)
            print(f"✅ 백업 완료: {self.backup_file}")
            return True
        except Exception as e:
            print(f"❌ 백업 실패: {e}")
            return False
    
    def get_fixed_function(self) -> str:
        """수정된 print_summary_statistics 함수 반환"""
        return '''    def print_summary_statistics(self, results: Dict[str, Dict], stock_list: List[Dict[str, str]]):
        """결과 요약 통계 - None 값 안전 처리"""
        successful_results = [r for r in results.values() if 'error' not in r]
        failed_results = [r for r in results.values() if 'error' in r]
        
        print(f"\\n📊 분석 결과 요약")
        print("=" * 80)
        print(f"✅ 성공: {len(successful_results)}개")
        print(f"❌ 실패: {len(failed_results)}개")
        print(f"📈 성공률: {len(successful_results)/len(results)*100:.1f}%")
        
        if successful_results:
            # 데이터 소스별 분류
            data_sources = {}
            for result in successful_results:
                source = result.get('data_source', 'unknown')
                data_sources[source] = data_sources.get(source, 0) + 1
            
            print(f"\\n📊 데이터 소스별 분포:")
            for source, count in data_sources.items():
                emoji = "🌐" if source == "real_data" else "🎲"
                name = "실제 데이터" if source == "real_data" else "샘플 데이터"
                print(f"  {emoji} {name}: {count}개 ({count/len(successful_results)*100:.1f}%)")
            
            # 추천도별 분류
            recommendations = {}
            for result in successful_results:
                rec = result.get('recommendation', 'NEUTRAL')
                recommendations[rec] = recommendations.get(rec, 0) + 1
            
            print(f"\\n📈 추천도 분포:")
            for rec, count in sorted(recommendations.items()):
                emoji = "🟢" if "BUY" in rec else "🔴" if "SELL" in rec else "🟡"
                print(f"  {emoji} {rec}: {count}개 ({count/len(successful_results)*100:.1f}%)")
            
            # 시장별 분류
            if stock_list:
                market_stats = {}
                for stock_info in stock_list:
                    market = stock_info.get('market_type', 'UNKNOWN')
                    market_stats[market] = market_stats.get(market, 0) + 1
                
                print(f"\\n📊 시장별 분포:")
                for market, count in market_stats.items():
                    print(f"  📈 {market}: {count}개")
            
            # 상위 추천 종목 (상위 10개) - None 값 안전 처리
            buy_recommendations = [r for r in successful_results 
                                 if r.get('recommendation') in ['STRONG_BUY', 'BUY']]
            
            if buy_recommendations:
                buy_recommendations.sort(key=lambda x: x.get('overall_score', 0), reverse=True)
                print(f"\\n🟢 상위 매수 추천 종목 (Top 10):")
                for i, result in enumerate(buy_recommendations[:10], 1):
                    stock_code = result.get('stock_code', 'N/A')
                    score = result.get('overall_score', 0)
                    rec = result.get('recommendation', 'NEUTRAL')
                    price = result.get('current_price', 0)
                    name = result.get('company_name', stock_code)
                    market = result.get('market_type', 'N/A')
                    source_emoji = "🌐" if result.get('data_source') == "real_data" else "🎲"
                    
                    # None 값 안전 처리
                    safe_name = name if name is not None else 'N/A'
                    safe_market = market if market is not None else 'N/A'
                    safe_rec = rec if rec is not None else 'N/A'
                    safe_score = score if score is not None else 0.0
                    safe_price = price if price is not None else 0.0
                    
                    try:
                        print(f"  {i:2d}. {safe_name[:15]:15s}({stock_code}) {safe_market:6s}: {safe_rec:12s} (점수: {safe_score:5.1f}, 가격: {safe_price:8,.0f}원) {source_emoji}")
                    except (ValueError, TypeError) as e:
                        # 포맷팅 실패 시 안전한 출력
                        print(f"  {i:2d}. {safe_name[:15]}({stock_code}) {safe_market}: {safe_rec} (점수: {safe_score}, 가격: {safe_price}원) {source_emoji}")
            else:
                print(f"\\n🟡 매수 추천 종목이 없습니다.")'''
    
    def find_function_boundaries(self, content: str) -> tuple:
        """print_summary_statistics 함수의 시작과 끝 위치 찾기"""
        lines = content.split('\n')
        start_line = -1
        end_line = -1
        indent_level = None
        
        # 함수 시작 찾기
        for i, line in enumerate(lines):
            if 'def print_summary_statistics(' in line:
                start_line = i
                # 함수의 들여쓰기 레벨 확인
                indent_level = len(line) - len(line.lstrip())
                break
        
        if start_line == -1:
            print("❌ print_summary_statistics 함수를 찾을 수 없습니다.")
            return -1, -1
        
        # 함수 끝 찾기 (다음 함수나 클래스가 시작되는 지점)
        for i in range(start_line + 1, len(lines)):
            line = lines[i]
            
            # 빈 줄은 건너뛰기
            if not line.strip():
                continue
            
            # 현재 줄의 들여쓰기 레벨
            current_indent = len(line) - len(line.lstrip())
            
            # 같은 레벨 이하의 들여쓰기가 나오고, def나 class로 시작하면 함수 끝
            if current_indent <= indent_level and (line.strip().startswith('def ') or line.strip().startswith('class ')):
                end_line = i - 1
                break
        
        # 파일 끝까지 함수가 계속되는 경우
        if end_line == -1:
            end_line = len(lines) - 1
        
        print(f"📍 함수 위치: {start_line + 1}줄 ~ {end_line + 1}줄")
        return start_line, end_line
    
    def apply_patch(self) -> bool:
        """패치 적용"""
        print(f"🔧 패치 적용 시작: {self.target_file}")
        
        # 원본 파일 읽기
        try:
            with open(self.target_file, 'r', encoding='utf-8') as f:
                original_content = f.read()
        except Exception as e:
            print(f"❌ 파일 읽기 실패: {e}")
            return False
        
        # 함수 위치 찾기
        start_line, end_line = self.find_function_boundaries(original_content)
        if start_line == -1:
            return False
        
        # 파일을 줄 단위로 분할
        lines = original_content.split('\n')
        
        # 기존 함수 제거하고 새 함수 삽입
        before_function = lines[:start_line]
        after_function = lines[end_line + 1:]
        new_function_lines = self.get_fixed_function().split('\n')
        
        # 새로운 내용 조합
        new_content = '\n'.join(before_function + new_function_lines + after_function)
        
        # 수정된 파일 저장
        try:
            with open(self.target_file, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"✅ 패치 적용 완료: {self.target_file}")
            return True
        except Exception as e:
            print(f"❌ 파일 저장 실패: {e}")
            return False
    
    def verify_patch(self) -> bool:
        """패치 적용 확인"""
        try:
            with open(self.target_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 수정된 코드가 포함되어 있는지 확인
            if 'safe_name = name if name is not None else' in content:
                print("✅ 패치 검증 성공: None 값 안전 처리 코드가 적용되었습니다.")
                return True
            else:
                print("❌ 패치 검증 실패: 수정된 코드를 찾을 수 없습니다.")
                return False
        except Exception as e:
            print(f"❌ 패치 검증 중 오류: {e}")
            return False
    
    def rollback(self) -> bool:
        """백업에서 복원"""
        if not self.backup_file or not self.backup_file.exists():
            print("❌ 백업 파일이 없어 복원할 수 없습니다.")
            return False
        
        try:
            shutil.copy2(self.backup_file, self.target_file)
            print(f"✅ 백업에서 복원 완료: {self.backup_file} -> {self.target_file}")
            return True
        except Exception as e:
            print(f"❌ 복원 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    print("🔧 print_summary_statistics 함수 자동 패치 프로그램")
    print("=" * 60)
    
    # 대상 파일 확인
    target_files = [
        "run_technical_analysis_all_stocks.py",
        "./run_technical_analysis_all_stocks.py",
        "../run_technical_analysis_all_stocks.py"
    ]
    
    target_file = None
    for file_path in target_files:
        if os.path.exists(file_path):
            target_file = file_path
            break
    
    if not target_file:
        print("❌ run_technical_analysis_all_stocks.py 파일을 찾을 수 없습니다.")
        print("현재 디렉토리에서 실행해주세요.")
        return
    
    print(f"🎯 대상 파일: {target_file}")
    
    # 패치 실행
    patcher = PrintSummaryStatisticsPatcher(target_file)
    
    try:
        # 1. 백업 생성
        if not patcher.create_backup():
            return
        
        # 2. 사용자 확인
        print(f"\n❓ {target_file} 파일의 print_summary_statistics 함수를 수정하시겠습니까?")
        print("   수정 내용: None 값 안전 처리 로직 추가")
        confirm = input("   계속하려면 'y'를 입력하세요 (y/N): ")
        
        if confirm.lower() != 'y':
            print("⏹️  패치 취소됨")
            return
        
        # 3. 패치 적용
        if not patcher.apply_patch():
            print("❌ 패치 적용 실패")
            return
        
        # 4. 패치 검증
        if not patcher.verify_patch():
            print("❌ 패치 검증 실패")
            rollback_confirm = input("백업에서 복원하시겠습니까? (y/N): ")
            if rollback_confirm.lower() == 'y':
                patcher.rollback()
            return
        
        # 5. 완료 메시지
        print(f"\n🎉 패치 완료!")
        print(f"✅ {target_file} 파일의 print_summary_statistics 함수가 수정되었습니다.")
        print(f"📁 백업 파일: {patcher.backup_file}")
        print(f"\n🚀 이제 다음 명령어를 실행할 수 있습니다:")
        print(f"   python {target_file} --all_stocks --save all_stocks_results.json")
        
        # 6. 테스트 실행 제안
        test_confirm = input(f"\n❓ 패치가 제대로 적용되었는지 간단한 테스트를 실행하시겠습니까? (y/N): ")
        if test_confirm.lower() == 'y':
            print(f"🧪 테스트 실행 중...")
            os.system(f"python {target_file} --sample_analysis")
        
    except KeyboardInterrupt:
        print(f"\n⏹️  사용자에 의해 중단됨")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()