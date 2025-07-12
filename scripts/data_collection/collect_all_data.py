#!/usr/bin/env python3
"""
전체 데이터 수집 통합 실행 스크립트
실행: python scripts/data_collection/collect_all_data.py
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import subprocess
import logging
from datetime import datetime
from config import ConfigManager

def run_script(script_path, logger):
    """스크립트 실행"""
    try:
        logger.info(f"스크립트 실행: {script_path}")
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, check=True)
        
        logger.info(f"스크립트 완료: {script_path}")
        print(result.stdout)
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"스크립트 실행 실패: {script_path}")
        print(f"오류: {e}")
        print(f"stderr: {e.stderr}")
        return False

def main():
    """메인 실행 함수"""
    print("🚀 Finance Data Vibe - 전체 데이터 수집 시작")
    print("=" * 60)
    
    # ConfigManager를 통한 로깅 설정
    try:
        config_manager = ConfigManager()
        logger = config_manager.get_logger('DataCollector')
        logger.info("전체 데이터 수집 프로세스 시작")
    except Exception as e:
        # 기본 로깅 설정으로 fallback
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logger = logging.getLogger(__name__)
        logger.warning(f"ConfigManager 초기화 실패, 기본 로깅 사용: {e}")
    
    start_time = datetime.now()
    
    # 스크립트 경로 설정
    scripts_dir = Path(__file__).parent
    scripts = [
        {
            'path': scripts_dir / "collect_stock_info.py",
            'name': "전종목 기본정보 수집",
            'estimated_time': "5분"
        },
        {
            'path': scripts_dir / "collect_dart_data.py",
            'name': "DART 재무데이터 수집",
            'estimated_time': "2-3시간",
            'args': ['--corp_codes']  # 기업코드만 먼저 수집
        },
        {
            'path': scripts_dir / "collect_stock_prices.py",
            'name': "주가 데이터 수집",
            'estimated_time': "1-2시간"
        },
        {
            'path': scripts_dir / "collect_news_data.py",
            'name': "뉴스 데이터 수집",
            'estimated_time': "30분-1시간"
        }
    ]
    
    # 사용자 확인
    print("다음 작업을 순차적으로 실행합니다:")
    for i, script_info in enumerate(scripts, 1):
        print(f"{i}. {script_info['name']} ({script_info['estimated_time']})")
    
    print("\n⚠️  전체 작업은 4-6시간이 소요될 수 있습니다.")
    
    user_input = input("\n전체 데이터 수집을 시작하시겠습니까? (y/N): ")
    if user_input.lower() != 'y':
        print("데이터 수집을 취소합니다.")
        logger.info("사용자에 의해 데이터 수집 취소됨")
        return False
    
    # 각 스크립트 실행
    success_count = 0
    
    for i, script_info in enumerate(scripts, 1):
        print(f"\n{'='*60}")
        print(f"단계 {i}/{len(scripts)}: {script_info['name']}")
        print(f"예상 소요시간: {script_info['estimated_time']}")
        print("=" * 60)
        
        script_path = script_info['path']
        
        # 파일 존재 확인
        if not script_path.exists():
            logger.warning(f"스크립트 파일이 존재하지 않음: {script_path}")
            print(f"⚠️  스크립트 파일이 없습니다: {script_path.name}")
            continue
        
        # 스크립트 실행 (추가 인자가 있는 경우)
        if 'args' in script_info:
            script_cmd = [sys.executable, str(script_path)] + script_info['args']
            try:
                logger.info(f"스크립트 실행: {script_path} {script_info['args']}")
                result = subprocess.run(script_cmd, capture_output=True, text=True, check=True)
                logger.info(f"스크립트 완료: {script_path}")
                print(result.stdout)
                success_count += 1
            except subprocess.CalledProcessError as e:
                logger.error(f"스크립트 실행 실패: {script_path}")
                print(f"❌ {script_path.name} 실행 실패!")
                print(f"오류: {e}")
                print(f"stderr: {e.stderr}")
        else:
            if run_script(script_path, logger):
                success_count += 1
            else:
                print(f"❌ {script_path.name} 실행 실패!")
        
        # 실패 시 사용자 확인
        if success_count != i:
            user_input = input("계속 진행하시겠습니까? (y/N): ")
            if user_input.lower() != 'y':
                logger.info("사용자에 의해 데이터 수집 중단됨")
                break
    
    # 결과 출력
    end_time = datetime.now()
    elapsed = end_time - start_time
    
    print(f"\n{'='*60}")
    print("🎉 전체 데이터 수집 완료!")
    print("=" * 60)
    print(f"성공: {success_count}/{len(scripts)} 개 스크립트")
    print(f"소요 시간: {elapsed}")
    print(f"완료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if success_count == len(scripts):
        print("\n✅ 모든 데이터 수집이 성공적으로 완료되었습니다!")
        print("이제 워런 버핏 스코어카드 분석을 시작할 수 있습니다.")
        logger.info("전체 데이터 수집 프로세스 성공적으로 완료")
        
        # 다음 단계 안내
        print("\n📊 다음 단계:")
        print("1. 워런 버핏 스코어카드 분석:")
        print("   python scripts/analysis/run_buffett_analysis.py --stock_code=005930")
        print("2. 기술분석 실행:")
        print("   python scripts/analysis/run_technical_analysis.py")
        print("3. 웹 앱 실행:")
        print("   streamlit run src/web/app.py")
        
        return True
    else:
        print(f"\n⚠️  {len(scripts) - success_count}개 스크립트가 실패했습니다.")
        print("실패한 스크립트를 개별적으로 다시 실행해주세요.")
        logger.warning(f"데이터 수집 부분 실패: {success_count}/{len(scripts)} 성공")
        return False

def collect_sample_data():
    """샘플 데이터만 빠르게 수집 (테스트용)"""
    print("🔬 샘플 데이터 수집 모드")
    print("=" * 40)
    
    try:
        config_manager = ConfigManager()
        logger = config_manager.get_logger('DataCollector')
    except:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
    
    scripts_dir = Path(__file__).parent
    
    # 빠른 테스트용 스크립트 실행
    sample_scripts = [
        {
            'path': scripts_dir / "collect_dart_data.py",
            'args': ['--corp_codes'],
            'name': "기업코드 수집"
        }
    ]
    
    for script_info in sample_scripts:
        script_path = script_info['path']
        if script_path.exists():
            script_cmd = [sys.executable, str(script_path)] + script_info['args']
            try:
                print(f"실행 중: {script_info['name']}")
                result = subprocess.run(script_cmd, capture_output=True, text=True, check=True)
                print(f"✅ {script_info['name']} 완료")
                print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
            except subprocess.CalledProcessError as e:
                print(f"❌ {script_info['name']} 실패: {e}")
                return False
        else:
            print(f"⚠️  스크립트 파일 없음: {script_path.name}")
            return False
    
    print("\n✅ 샘플 데이터 수집 완료!")
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='전체 데이터 수집 스크립트')
    parser.add_argument('--sample', action='store_true', help='샘플 데이터만 수집')
    args = parser.parse_args()
    
    try:
        if args.sample:
            success = collect_sample_data()
        else:
            success = main()
        
        if not success:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  사용자에 의해 중단됨")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 예기치 못한 오류: {e}")
        sys.exit(1)