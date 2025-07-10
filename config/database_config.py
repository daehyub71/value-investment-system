"""
워런 버핏 스코어카드 110점 평가 시스템을 위한 완전한 데이터베이스 스키마 개선안
"""

def _define_table_schemas(self) -> Dict[str, str]:
    """테이블 스키마 정의 - 워런 버핏 스코어카드 110점 평가 시스템 지원"""
    return {
        # 1. 기존 테이블들 (유지)
        'stock_prices': '''
            CREATE TABLE IF NOT EXISTS stock_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date TEXT NOT NULL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume INTEGER,
                amount INTEGER,
                adjusted_close REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, date)
            )
        ''',
        
        'company_info': '''
            CREATE TABLE IF NOT EXISTS company_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT UNIQUE NOT NULL,
                company_name TEXT NOT NULL,
                market_type TEXT,
                sector TEXT,
                industry TEXT,
                listing_date TEXT,
                market_cap INTEGER,
                shares_outstanding INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''',
        
        # 2. 확장된 financial_ratios 테이블 (워런 버핏 스코어카드 110점 평가)
        'financial_ratios': '''
            CREATE TABLE IF NOT EXISTS financial_ratios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                year INTEGER NOT NULL,
                quarter INTEGER,
                
                -- 기본 재무 데이터
                revenue REAL,
                operating_income REAL,
                net_income REAL,
                total_assets REAL,
                total_equity REAL,
                total_debt REAL,
                current_assets REAL,
                current_liabilities REAL,
                cash_and_equivalents REAL,
                inventory REAL,
                accounts_receivable REAL,
                ebitda REAL,
                free_cash_flow REAL,
                dividend_paid REAL,
                shares_outstanding INTEGER,
                
                -- 🏆 수익성 지표 (30점)
                roe REAL,                    -- 자기자본이익률
                roa REAL,                    -- 총자산이익률
                operating_margin REAL,       -- 영업이익률
                net_margin REAL,             -- 순이익률
                gross_margin REAL,           -- 매출총이익률
                ebitda_margin REAL,          -- EBITDA 마진
                roic REAL,                   -- 투하자본이익률
                
                -- 📈 성장성 지표 (25점)
                revenue_growth_1y REAL,      -- 1년 매출 성장률
                revenue_growth_3y REAL,      -- 3년 매출 성장률 (CAGR)
                revenue_growth_5y REAL,      -- 5년 매출 성장률 (CAGR)
                net_income_growth_1y REAL,   -- 1년 순이익 성장률
                net_income_growth_3y REAL,   -- 3년 순이익 성장률 (CAGR)
                net_income_growth_5y REAL,   -- 5년 순이익 성장률 (CAGR)
                eps_growth_1y REAL,          -- 1년 EPS 성장률
                eps_growth_3y REAL,          -- 3년 EPS 성장률 (CAGR)
                eps_growth_5y REAL,          -- 5년 EPS 성장률 (CAGR)
                equity_growth_3y REAL,       -- 3년 자기자본 성장률
                dividend_growth_3y REAL,     -- 3년 배당 성장률
                fcf_growth_3y REAL,          -- 3년 잉여현금흐름 성장률
                
                -- 🛡️ 안정성 지표 (25점)
                debt_ratio REAL,             -- 부채비율
                current_ratio REAL,          -- 유동비율
                quick_ratio REAL,            -- 당좌비율
                cash_ratio REAL,             -- 현금비율
                interest_coverage_ratio REAL, -- 이자보상배율
                net_debt_ratio REAL,         -- 순부채비율
                altman_z_score REAL,         -- 알트만 Z-Score
                
                -- 🔄 효율성 지표 (10점)
                inventory_turnover REAL,     -- 재고회전율
                receivables_turnover REAL,   -- 매출채권회전율
                total_asset_turnover REAL,   -- 총자산회전율
                cash_conversion_cycle REAL,  -- 현금전환주기
                
                -- 💰 가치평가 지표 (20점)
                per REAL,                    -- 주가수익비율
                pbr REAL,                    -- 주가순자산비율
                peg REAL,                    -- PER/성장률
                ev_ebitda REAL,              -- EV/EBITDA
                psr REAL,                    -- 주가매출비율
                pcr REAL,                    -- 주가현금흐름비율
                dividend_yield REAL,         -- 배당수익률
                eps REAL,                    -- 주당순이익
                bps REAL,                    -- 주당순자산
                
                -- 워런 버핏 스코어카드 점수
                profitability_score REAL,    -- 수익성 점수 (30점)
                growth_score REAL,           -- 성장성 점수 (25점)
                stability_score REAL,        -- 안정성 점수 (25점)
                efficiency_score REAL,       -- 효율성 점수 (10점)
                valuation_score REAL,        -- 가치평가 점수 (20점)
                total_buffett_score REAL,    -- 총합 점수 (110점)
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, year, quarter)
            )
        ''',
        
        # 3. 기술적 분석 지표 테이블 (30% 비중)
        'technical_indicators': '''
            CREATE TABLE IF NOT EXISTS technical_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date TEXT NOT NULL,
                
                -- 📊 추세 지표
                sma_5 REAL,                  -- 5일 단순이동평균
                sma_20 REAL,                 -- 20일 단순이동평균
                sma_60 REAL,                 -- 60일 단순이동평균
                sma_120 REAL,                -- 120일 단순이동평균
                sma_200 REAL,                -- 200일 단순이동평균
                ema_12 REAL,                 -- 12일 지수이동평균
                ema_26 REAL,                 -- 26일 지수이동평균
                parabolic_sar REAL,          -- 파라볼릭 SAR
                adx REAL,                    -- 평균방향지수
                plus_di REAL,                -- +DI
                minus_di REAL,               -- -DI
                
                -- ⚡ 모멘텀 지표
                rsi REAL,                    -- RSI (14일)
                macd REAL,                   -- MACD
                macd_signal REAL,            -- MACD 신호선
                macd_histogram REAL,         -- MACD 히스토그램
                stochastic_k REAL,           -- 스토캐스틱 %K
                stochastic_d REAL,           -- 스토캐스틱 %D
                williams_r REAL,             -- Williams %R
                cci REAL,                    -- 상품채널지수
                mfi REAL,                    -- 자금흐름지수
                momentum REAL,               -- 모멘텀 오실레이터
                
                -- 📈 변동성 지표
                bollinger_upper REAL,        -- 볼린저 밴드 상한
                bollinger_middle REAL,       -- 볼린저 밴드 중간
                bollinger_lower REAL,        -- 볼린저 밴드 하한
                bollinger_width REAL,        -- 볼린저 밴드 폭
                atr REAL,                    -- 평균진실범위
                keltner_upper REAL,          -- 켈트너 채널 상한
                keltner_lower REAL,          -- 켈트너 채널 하한
                donchian_upper REAL,         -- 도너찬 채널 상한
                donchian_lower REAL,         -- 도너찬 채널 하한
                
                -- 📊 거래량 지표
                obv REAL,                    -- 누적거래량
                vwap REAL,                   -- 거래량가중평균가
                cmf REAL,                    -- 차이킨자금흐름
                volume_ratio REAL,           -- 거래량 비율
                
                -- 🎯 종합 신호
                trend_signal INTEGER,        -- 추세 신호 (-1: 하락, 0: 보합, 1: 상승)
                momentum_signal INTEGER,     -- 모멘텀 신호
                volatility_signal INTEGER,   -- 변동성 신호
                volume_signal INTEGER,       -- 거래량 신호
                technical_score REAL,        -- 기술적 분석 종합 점수 (0-100)
                
                -- 52주 관련 지표
                week_52_high REAL,           -- 52주 최고가
                week_52_low REAL,            -- 52주 최저가
                week_52_high_ratio REAL,     -- 52주 최고가 대비 비율
                week_52_low_ratio REAL,      -- 52주 최저가 대비 비율
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, date)
            )
        ''',
        
        # 4. 감정분석 관련 테이블들 (25% 비중)
        'news_articles': '''
            CREATE TABLE IF NOT EXISTS news_articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                originallink TEXT,
                link TEXT,
                pubDate TEXT NOT NULL,
                source TEXT,
                category TEXT,               -- 'fundamental', 'technical', 'general'
                
                -- 뉴스 분류
                news_type TEXT,              -- 'earnings', 'expansion', 'management', 'dividend', 'industry'
                importance_score REAL,       -- 중요도 점수 (0-1)
                
                -- 감정분석 결과
                sentiment_score REAL,        -- 감정 점수 (-1 ~ 1)
                sentiment_label TEXT,        -- 'positive', 'negative', 'neutral'
                confidence_score REAL,       -- 신뢰도 점수 (0-1)
                
                -- 키워드 분석
                keywords TEXT,               -- JSON 형태의 키워드 목록
                entities TEXT,               -- JSON 형태의 개체명 목록
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''',
        
        'sentiment_scores': '''
            CREATE TABLE IF NOT EXISTS sentiment_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date TEXT NOT NULL,
                
                -- 일별 감정분석 점수
                daily_sentiment REAL,        -- 일별 감정 점수
                weekly_sentiment REAL,       -- 주별 감정 점수
                monthly_sentiment REAL,      -- 월별 감정 점수
                
                -- 뉴스 통계
                total_news_count INTEGER,    -- 총 뉴스 개수
                positive_news_count INTEGER, -- 긍정 뉴스 개수
                negative_news_count INTEGER, -- 부정 뉴스 개수
                neutral_news_count INTEGER,  -- 중립 뉴스 개수
                
                -- 펀더멘털 뉴스 분석
                fundamental_news_count INTEGER,  -- 펀더멘털 뉴스 개수
                fundamental_sentiment REAL,      -- 펀더멘털 감정 점수
                
                -- 감정분석 종합 점수
                sentiment_momentum REAL,     -- 감정 모멘텀
                sentiment_volatility REAL,   -- 감정 변동성
                sentiment_final_score REAL,  -- 최종 감정분석 점수 (0-100)
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, date)
            )
        ''',
        
        'market_sentiment': '''
            CREATE TABLE IF NOT EXISTS market_sentiment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                
                -- 시장 전체 감정
                market_sentiment_index REAL,     -- 시장 감정 지수
                fear_greed_index REAL,           -- 공포탐욕지수
                vix_level REAL,                  -- VIX 수준
                
                -- 섹터별 감정
                sector_sentiment TEXT,           -- JSON 형태의 섹터별 감정
                
                -- 뉴스 통계
                total_market_news INTEGER,       -- 시장 전체 뉴스 개수
                positive_ratio REAL,             -- 긍정 뉴스 비율
                negative_ratio REAL,             -- 부정 뉴스 비율
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date)
            )
        ''',
        
        # 5. DART 관련 테이블들
        'corp_codes': '''
            CREATE TABLE IF NOT EXISTS corp_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                corp_code TEXT UNIQUE NOT NULL,
                corp_name TEXT NOT NULL,
                stock_code TEXT,
                modify_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''',
        
        'financial_statements': '''
            CREATE TABLE IF NOT EXISTS financial_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                corp_code TEXT NOT NULL,
                bsns_year TEXT NOT NULL,
                reprt_code TEXT NOT NULL,
                account_nm TEXT NOT NULL,
                thstrm_amount INTEGER,
                frmtrm_amount INTEGER,
                bfefrmtrm_amount INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(corp_code, bsns_year, reprt_code, account_nm)
            )
        ''',
        
        'disclosures': '''
            CREATE TABLE IF NOT EXISTS disclosures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                corp_code TEXT NOT NULL,
                corp_name TEXT,
                stock_code TEXT,
                rcept_no TEXT UNIQUE NOT NULL,
                report_nm TEXT,
                rcept_dt TEXT,
                flr_nm TEXT,
                rm TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''',
        
        'company_outlines': '''
            CREATE TABLE IF NOT EXISTS company_outlines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                corp_code TEXT UNIQUE NOT NULL,
                corp_name TEXT,
                corp_name_eng TEXT,
                stock_name TEXT,
                stock_code TEXT,
                ceo_nm TEXT,
                corp_cls TEXT,
                jurir_no TEXT,
                bizr_no TEXT,
                adres TEXT,
                hm_url TEXT,
                ir_url TEXT,
                phn_no TEXT,
                fax_no TEXT,
                induty_code TEXT,
                est_dt TEXT,
                acc_mt TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''',
        
        # 6. KIS API 관련 테이블들
        'realtime_quotes': '''
            CREATE TABLE IF NOT EXISTS realtime_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                current_price REAL,
                change_price REAL,
                change_rate REAL,
                volume INTEGER,
                high_price REAL,
                low_price REAL,
                open_price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, timestamp)
            )
        ''',
        
        'account_balance': '''
            CREATE TABLE IF NOT EXISTS account_balance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_no TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT,
                holding_qty INTEGER,
                avg_price REAL,
                current_price REAL,
                evaluation_amount REAL,
                profit_loss REAL,
                profit_loss_rate REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_no, stock_code)
            )
        ''',
        
        'order_history': '''
            CREATE TABLE IF NOT EXISTS order_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_no TEXT UNIQUE NOT NULL,
                account_no TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                order_type TEXT,
                order_qty INTEGER,
                order_price REAL,
                executed_qty INTEGER,
                executed_price REAL,
                order_status TEXT,
                order_time TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''',
        
        'market_indicators': '''
            CREATE TABLE IF NOT EXISTS market_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                kospi_index REAL,
                kosdaq_index REAL,
                kospi_volume INTEGER,
                kosdaq_volume INTEGER,
                advance_decline_ratio REAL,
                new_high_low_ratio REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date)
            )
        ''',
        
        # 7. 통합 분석 결과 테이블
        'investment_scores': '''
            CREATE TABLE IF NOT EXISTS investment_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date TEXT NOT NULL,
                
                -- 3개 분석 영역 점수 (기본분석 45% + 기술분석 30% + 감정분석 25%)
                fundamental_score REAL,      -- 기본분석 점수 (0-100)
                technical_score REAL,        -- 기술분석 점수 (0-100)
                sentiment_score REAL,        -- 감정분석 점수 (0-100)
                
                -- 가중 평균 최종 점수
                weighted_fundamental REAL,   -- 기본분석 가중점수 (×0.45)
                weighted_technical REAL,     -- 기술분석 가중점수 (×0.30)
                weighted_sentiment REAL,     -- 감정분석 가중점수 (×0.25)
                
                -- 최종 결과
                total_investment_score REAL, -- 총 투자 점수 (0-100)
                recommendation TEXT,         -- 'Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell'
                risk_level TEXT,            -- 'Low', 'Medium', 'High'
                confidence_level REAL,      -- 신뢰도 (0-1)
                
                -- 내재가치 관련
                intrinsic_value REAL,       -- 내재가치 (5개 모델 평균)
                current_price REAL,         -- 현재가
                discount_rate REAL,         -- 할인율
                margin_of_safety REAL,      -- 안전마진
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(stock_code, date)
            )
        '''
    }

# 추가 인덱스 정의
def _create_enhanced_indexes(self, conn: sqlite3.Connection, db_name: str):
    """확장된 인덱스 생성"""
    enhanced_index_queries = {
        'stock': [
            # 기존 인덱스
            'CREATE INDEX IF NOT EXISTS idx_stock_prices_code_date ON stock_prices(stock_code, date)',
            'CREATE INDEX IF NOT EXISTS idx_company_info_code ON company_info(stock_code)',
            'CREATE INDEX IF NOT EXISTS idx_financial_ratios_code_year ON financial_ratios(stock_code, year)',
            'CREATE INDEX IF NOT EXISTS idx_technical_indicators_code_date ON technical_indicators(stock_code, date)',
            
            # 워런 버핏 스코어카드 관련 인덱스
            'CREATE INDEX IF NOT EXISTS idx_financial_ratios_roe ON financial_ratios(roe)',
            'CREATE INDEX IF NOT EXISTS idx_financial_ratios_debt_ratio ON financial_ratios(debt_ratio)',
            'CREATE INDEX IF NOT EXISTS idx_financial_ratios_buffett_score ON financial_ratios(total_buffett_score)',
            'CREATE INDEX IF NOT EXISTS idx_financial_ratios_growth ON financial_ratios(revenue_growth_3y, net_income_growth_3y)',
            
            # 기술적 분석 관련 인덱스
            'CREATE INDEX IF NOT EXISTS idx_technical_indicators_rsi ON technical_indicators(rsi)',
            'CREATE INDEX IF NOT EXISTS idx_technical_indicators_macd ON technical_indicators(macd)',
            'CREATE INDEX IF NOT EXISTS idx_technical_indicators_score ON technical_indicators(technical_score)',
            
            # 통합 점수 관련 인덱스
            'CREATE INDEX IF NOT EXISTS idx_investment_scores_total ON investment_scores(total_investment_score)',
            'CREATE INDEX IF NOT EXISTS idx_investment_scores_recommendation ON investment_scores(recommendation)',
            'CREATE INDEX IF NOT EXISTS idx_investment_scores_risk ON investment_scores(risk_level)'
        ],
        
        'news': [
            # 기존 인덱스
            'CREATE INDEX IF NOT EXISTS idx_news_articles_stock_date ON news_articles(stock_code, pubDate)',
            'CREATE INDEX IF NOT EXISTS idx_news_articles_source ON news_articles(source)',
            'CREATE INDEX IF NOT EXISTS idx_sentiment_scores_stock ON sentiment_scores(stock_code)',
            
            # 감정분석 관련 인덱스
            'CREATE INDEX IF NOT EXISTS idx_news_articles_sentiment ON news_articles(sentiment_score)',
            'CREATE INDEX IF NOT EXISTS idx_news_articles_category ON news_articles(category)',
            'CREATE INDEX IF NOT EXISTS idx_news_articles_type ON news_articles(news_type)',
            'CREATE INDEX IF NOT EXISTS idx_sentiment_scores_date ON sentiment_scores(date)',
            'CREATE INDEX IF NOT EXISTS idx_sentiment_scores_final ON sentiment_scores(sentiment_final_score)',
            'CREATE INDEX IF NOT EXISTS idx_market_sentiment_date ON market_sentiment(date)',
            'CREATE INDEX IF NOT EXISTS idx_market_sentiment_index ON market_sentiment(market_sentiment_index)'
        ],
        
        'dart': [
            # 기존 인덱스
            'CREATE INDEX IF NOT EXISTS idx_corp_codes_code ON corp_codes(corp_code)',
            'CREATE INDEX IF NOT EXISTS idx_corp_codes_stock ON corp_codes(stock_code)',
            'CREATE INDEX IF NOT EXISTS idx_financial_statements_corp_year ON financial_statements(corp_code, bsns_year)',
            'CREATE INDEX IF NOT EXISTS idx_disclosures_corp_date ON disclosures(corp_code, rcept_dt)',
            
            # 추가 인덱스
            'CREATE INDEX IF NOT EXISTS idx_financial_statements_account ON financial_statements(account_nm)',
            'CREATE INDEX IF NOT EXISTS idx_disclosures_report ON disclosures(report_nm)',
            'CREATE INDEX IF NOT EXISTS idx_company_outlines_stock ON company_outlines(stock_code)',
            'CREATE INDEX IF NOT EXISTS idx_company_outlines_industry ON company_outlines(induty_code)'
        ],
        
        'kis': [
            # 실시간 데이터 관련 인덱스
            'CREATE INDEX IF NOT EXISTS idx_realtime_quotes_stock_time ON realtime_quotes(stock_code, timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_account_balance_account ON account_balance(account_no)',
            'CREATE INDEX IF NOT EXISTS idx_order_history_account ON order_history(account_no)',
            'CREATE INDEX IF NOT EXISTS idx_order_history_stock ON order_history(stock_code)',
            'CREATE INDEX IF NOT EXISTS idx_market_indicators_date ON market_indicators(date)'
        ]
    }
    
    if db_name in enhanced_index_queries:
        for query in enhanced_index_queries[db_name]:
            try:
                conn.execute(query)
            except Exception as e:
                print(f"인덱스 생성 실패: {query} - {e}")

# 성장률 계산을 위한 뷰 생성
def create_growth_calculation_views(self, conn: sqlite3.Connection):
    """성장률 계산을 위한 뷰 생성"""
    
    # 3년/5년/10년 매출 성장률 계산 뷰
    revenue_growth_view = '''
        CREATE VIEW IF NOT EXISTS v_revenue_growth AS
        SELECT 
            stock_code,
            year,
            revenue,
            LAG(revenue, 1) OVER (PARTITION BY stock_code ORDER BY year) as prev_1y_revenue,
            LAG(revenue, 3) OVER (PARTITION BY stock_code ORDER BY year) as prev_3y_revenue,
            LAG(revenue, 5) OVER (PARTITION BY stock_code ORDER BY year) as prev_5y_revenue,
            
            -- 성장률 계산
            CASE 
                WHEN LAG(revenue, 1) OVER (PARTITION BY stock_code ORDER BY year) > 0 
                THEN (revenue - LAG(revenue, 1) OVER (PARTITION BY stock_code ORDER BY year)) / LAG(revenue, 1) OVER (PARTITION BY stock_code ORDER BY year) * 100
                ELSE NULL 
            END as revenue_growth_1y,
            
            CASE 
                WHEN LAG(revenue, 3) OVER (PARTITION BY stock_code ORDER BY year) > 0 
                THEN (POWER(revenue / LAG(revenue, 3) OVER (PARTITION BY stock_code ORDER BY year), 1.0/3) - 1) * 100
                ELSE NULL 
            END as revenue_growth_3y_cagr,
            
            CASE 
                WHEN LAG(revenue, 5) OVER (PARTITION BY stock_code ORDER BY year) > 0 
                THEN (POWER(revenue / LAG(revenue, 5) OVER (PARTITION BY stock_code ORDER BY year), 1.0/5) - 1) * 100
                ELSE NULL 
            END as revenue_growth_5y_cagr
            
        FROM financial_ratios
        WHERE quarter IS NULL  -- 연간 데이터만
        ORDER BY stock_code, year
    '''
    
    # 순이익 성장률 계산 뷰
    earnings_growth_view = '''
        CREATE VIEW IF NOT EXISTS v_earnings_growth AS
        SELECT 
            stock_code,
            year,
            net_income,
            LAG(net_income, 1) OVER (PARTITION BY stock_code ORDER BY year) as prev_1y_earnings,
            LAG(net_income, 3) OVER (PARTITION BY stock_code ORDER BY year) as prev_3y_earnings,
            LAG(net_income, 5) OVER (PARTITION BY stock_code ORDER BY year) as prev_5y_earnings,
            
            -- 성장률 계산
            CASE 
                WHEN LAG(net_income, 1) OVER (PARTITION BY stock_code ORDER BY year) > 0 
                THEN (net_income - LAG(net_income, 1) OVER (PARTITION BY stock_code ORDER BY year)) / LAG(net_income, 1) OVER (PARTITION BY stock_code ORDER BY year) * 100
                ELSE NULL 
            END as earnings_growth_1y,
            
            CASE 
                WHEN LAG(net_income, 3) OVER (PARTITION BY stock_code ORDER BY year) > 0 
                THEN (POWER(net_income / LAG(net_income, 3) OVER (PARTITION BY stock_code ORDER BY year), 1.0/3) - 1) * 100
                ELSE NULL 
            END as earnings_growth_3y_cagr,
            
            CASE 
                WHEN LAG(net_income, 5) OVER (PARTITION BY stock_code ORDER BY year) > 0 
                THEN (POWER(net_income / LAG(net_income, 5) OVER (PARTITION BY stock_code ORDER BY year), 1.0/5) - 1) * 100
                ELSE NULL 
            END as earnings_growth_5y_cagr
            
        FROM financial_ratios
        WHERE quarter IS NULL  -- 연간 데이터만
        ORDER BY stock_code, year
    '''
    
    # 워런 버핏 스코어카드 종합 뷰
    buffett_scorecard_view = '''
        CREATE VIEW IF NOT EXISTS v_buffett_scorecard AS
        SELECT 
            fr.stock_code,
            fr.year,
            ci.company_name,
            ci.sector,
            
            -- 🏆 수익성 지표 (30점)
            fr.roe,
            fr.roa,
            fr.operating_margin,
            fr.net_margin,
            fr.roic,
            fr.profitability_score,
            
            -- 📈 성장성 지표 (25점)
            fr.revenue_growth_3y,
            fr.net_income_growth_3y,
            fr.eps_growth_3y,
            fr.growth_score,
            
            -- 🛡️ 안정성 지표 (25점)
            fr.debt_ratio,
            fr.current_ratio,
            fr.interest_coverage_ratio,
            fr.altman_z_score,
            fr.stability_score,
            
            -- 🔄 효율성 지표 (10점)
            fr.inventory_turnover,
            fr.receivables_turnover,
            fr.total_asset_turnover,
            fr.efficiency_score,
            
            -- 💰 가치평가 지표 (20점)
            fr.per,
            fr.pbr,
            fr.peg,
            fr.dividend_yield,
            fr.valuation_score,
            
            -- 최종 점수
            fr.total_buffett_score,
            
            -- 등급 계산
            CASE 
                WHEN fr.total_buffett_score >= 90 THEN 'Excellent'
                WHEN fr.total_buffett_score >= 80 THEN 'Very Good'
                WHEN fr.total_buffett_score >= 70 THEN 'Good'
                WHEN fr.total_buffett_score >= 60 THEN 'Fair'
                WHEN fr.total_buffett_score >= 50 THEN 'Poor'
                ELSE 'Very Poor'
            END as buffett_grade
            
        FROM financial_ratios fr
        JOIN company_info ci ON fr.stock_code = ci.stock_code
        WHERE fr.quarter IS NULL  -- 연간 데이터만
        ORDER BY fr.total_buffett_score DESC
    '''
    
    # 뷰 생성 실행
    views = [revenue_growth_view, earnings_growth_view, buffett_scorecard_view]
    
    for view in views:
        try:
            conn.execute(view)
        except Exception as e:
            print(f"뷰 생성 실패: {e}")

# 데이터베이스 초기화 함수 업데이트
def create_all_databases_enhanced(self) -> Dict[str, bool]:
    """모든 데이터베이스 생성 (확장 버전)"""
    results = {}
    
    for db_name in self.databases.keys():
        try:
            with self.get_connection(db_name) as conn:
                # 테이블 생성
                for table_name in self.databases[db_name]['tables']:
                    if table_name in self.table_schemas:
                        conn.execute(self.table_schemas[table_name])
                
                # 확장된 인덱스 생성
                self._create_enhanced_indexes(conn, db_name)
                
                # 성장률 계산 뷰 생성 (stock 데이터베이스만)
                if db_name == 'stock':
                    self.create_growth_calculation_views(conn)
                
                conn.commit()
                results[db_name] = True
                
        except Exception as e:
            print(f"데이터베이스 생성 실패 ({db_name}): {e}")
            results[db_name] = False
    
    return results