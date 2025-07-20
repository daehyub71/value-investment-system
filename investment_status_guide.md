# 투자 가능 여부 포함 워런 버핏 분석 시스템 실행 가이드

## 🚀 빠른 시작

### 1단계: 데이터베이스 마이그레이션
```bash
python db_migration_script.py
```

### 2단계: 투자 가능 여부 업데이트
```bash
python investment_status_updater.py
```

### 3단계: 기존 분석 프로그램 실행 (업데이트 버전)

#### 방법 1: 전체 종목 분석 (권장)
```bash
python buffett_all_stocks_analyzer.py
```

#### 방법 2: 배치 분석
```bash
python batch_buffett_analyzer.py
```

#### 방법 3: 신뢰 종목만 분석
```bash
python get_reliable_stocks_updated.py
```

---

## 📊 결과 파일 확인

분석 완료 후 `results/buffett_analysis/` 폴더에서 다음 파일들을 확인할 수 있습니다:

### 📈 투자 가능 종목
- `buffett_investable_recommendations_YYYYMMDD_HHMMSS.csv` - 투자 추천 종목
- `buffett_investable_stocks_YYYYMMDD_HHMMSS.csv` - 모든 투자 가능 종목

### ❌ 투자 불가 종목
- `buffett_non_investable_YYYYMMDD_HHMMSS.csv` - 투자 불가 종목 (상장폐지, 관리종목 등)

### 📋 전체 결과
- `buffett_all_stocks_with_status_YYYYMMDD_HHMMSS.csv` - 투자 가능 여부 포함 전체 결과

---

## 🎯 주요 변경사항

### 1. 새로 추가된 필드
- `is_investable`: 투자 가능 여부 (True/False)
- `investment_warning`: 투자 경고 수준 (NONE/CAUTION/ALERT/DESIGNATED)
- `listing_status`: 상장 상태 (LISTED/DELISTED/SUSPENDED)

### 2. 투자 경고 수준
- **NONE**: 정상, 투자 가능
- **CAUTION**: 주의, 투자 가능
- **ALERT**: 경고, 투자 가능하지만 위험
- **DESIGNATED**: 관리종목 수준, 투자 불가

### 3. 자동 필터링
- 총점 20점 미만 → 투자 불가 (관리종목 수준)
- 안정성/수익성 5점 미만 → 투자 경고
- 상장폐지 종목 → 자동 제외

---

## 💡 사용 팁

1. **첫 실행**: 반드시 마이그레이션부터 실행
2. **정기 업데이트**: 주간 단위로 투자 가능 여부 업데이트 권장
3. **결과 해석**: `is_investable=True`인 종목만 실제 투자 고려
4. **위험 관리**: `investment_warning`이 ALERT 이상인 종목은 신중히 검토

---

## 🔄 명령어 옵션

### 테스트 실행
```bash
python buffett_all_stocks_analyzer.py --test  # 10개 종목만
python buffett_all_stocks_analyzer.py --max-stocks 50  # 50개 종목만
```

### 투자 가능 여부 업데이트 생략
```bash
python buffett_all_stocks_analyzer.py --no-investment-status
```

이제 기존 분석 프로그램들이 모두 **실제 투자 가능한 종목만** 추천하도록 업데이트되었습니다! 🎉
