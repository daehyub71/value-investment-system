"""
이메일 유틸리티
이메일 전송 및 알림 관련 유틸리티 함수들
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
from pathlib import Path
from typing import List, Dict, Optional, Any, Union
from datetime import datetime, timedelta
import logging
from jinja2 import Template
import json

logger = logging.getLogger(__name__)

class EmailError(Exception):
    """이메일 전송 오류"""
    pass

class EmailConfig:
    """이메일 설정 클래스"""
    
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.username = os.getenv('SMTP_USERNAME', '')
        self.password = os.getenv('SMTP_PASSWORD', '')
        self.from_email = os.getenv('SMTP_USERNAME', '')
        self.to_email = os.getenv('SMTP_USERNAME', '')
        self.use_tls = True
        self.timeout = 30
    
    def validate(self) -> List[str]:
        """설정 유효성 검사"""
        errors = []
        
        if not self.smtp_server:
            errors.append("SMTP 서버가 설정되지 않았습니다.")
        
        if not self.username:
            errors.append("이메일 사용자명이 설정되지 않았습니다.")
        
        if not self.password:
            errors.append("이메일 비밀번호가 설정되지 않았습니다.")
        
        if not self.from_email:
            errors.append("발신자 이메일이 설정되지 않았습니다.")
        
        return errors

class EmailTemplate:
    """이메일 템플릿 클래스"""
    
    def __init__(self):
        self.templates_dir = Path(__file__).parent.parent / 'templates' / 'email'
        self.templates_dir.mkdir(parents=True, exist_ok=True)
    
    def get_template(self, template_name: str) -> str:
        """템플릿 파일 로드"""
        template_path = self.templates_dir / f"{template_name}.html"
        
        if template_path.exists():
            with open(template_path, 'r', encoding='utf-8') as f:
                return f.read()
        else:
            return self.get_default_template(template_name)
    
    def get_default_template(self, template_name: str) -> str:
        """기본 템플릿 반환"""
        templates = {
            'stock_alert': '''
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    .header { background-color: #f4f4f4; padding: 20px; border-radius: 5px; }
                    .content { margin: 20px 0; }
                    .stock-info { background-color: #e7f3ff; padding: 15px; border-radius: 5px; margin: 10px 0; }
                    .positive { color: #28a745; }
                    .negative { color: #dc3545; }
                    .footer { font-size: 12px; color: #666; margin-top: 30px; }
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>📊 Finance Data Vibe 주식 알림</h2>
                </div>
                
                <div class="content">
                    <p>안녕하세요! 주식 알림을 전송합니다.</p>
                    
                    <div class="stock-info">
                        <h3>{{ stock_name }} ({{ stock_code }})</h3>
                        <p><strong>현재가:</strong> {{ current_price | format_currency }}</p>
                        <p><strong>변동률:</strong> 
                            <span class="{% if change_rate > 0 %}positive{% else %}negative{% endif %}">
                                {{ change_rate | format_percentage }}
                            </span>
                        </p>
                        <p><strong>알림 조건:</strong> {{ alert_condition }}</p>
                    </div>
                    
                    {% if additional_info %}
                    <div class="stock-info">
                        <h4>추가 정보</h4>
                        <ul>
                        {% for key, value in additional_info.items() %}
                            <li><strong>{{ key }}:</strong> {{ value }}</li>
                        {% endfor %}
                        </ul>
                    </div>
                    {% endif %}
                </div>
                
                <div class="footer">
                    <p>이 메시지는 Finance Data Vibe 시스템에서 자동으로 전송되었습니다.</p>
                    <p>전송 시간: {{ timestamp }}</p>
                </div>
            </body>
            </html>
            ''',
            
            'portfolio_report': '''
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    .header { background-color: #f4f4f4; padding: 20px; border-radius: 5px; }
                    .content { margin: 20px 0; }
                    .summary { background-color: #e7f3ff; padding: 15px; border-radius: 5px; margin: 10px 0; }
                    .stock-table { width: 100%; border-collapse: collapse; margin: 20px 0; }
                    .stock-table th, .stock-table td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                    .stock-table th { background-color: #f2f2f2; }
                    .positive { color: #28a745; }
                    .negative { color: #dc3545; }
                    .footer { font-size: 12px; color: #666; margin-top: 30px; }
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>📈 포트폴리오 리포트</h2>
                </div>
                
                <div class="content">
                    <div class="summary">
                        <h3>포트폴리오 요약</h3>
                        <p><strong>총 평가액:</strong> {{ total_value | format_currency }}</p>
                        <p><strong>총 수익률:</strong> 
                            <span class="{% if total_return > 0 %}positive{% else %}negative{% endif %}">
                                {{ total_return | format_percentage }}
                            </span>
                        </p>
                        <p><strong>일일 변동:</strong> 
                            <span class="{% if daily_change > 0 %}positive{% else %}negative{% endif %}">
                                {{ daily_change | format_currency }}
                            </span>
                        </p>
                    </div>
                    
                    <h3>보유 종목</h3>
                    <table class="stock-table">
                        <thead>
                            <tr>
                                <th>종목명</th>
                                <th>보유수량</th>
                                <th>평균단가</th>
                                <th>현재가</th>
                                <th>평가액</th>
                                <th>수익률</th>
                            </tr>
                        </thead>
                        <tbody>
                        {% for stock in stocks %}
                            <tr>
                                <td>{{ stock.name }}</td>
                                <td>{{ stock.quantity }}</td>
                                <td>{{ stock.avg_price | format_currency }}</td>
                                <td>{{ stock.current_price | format_currency }}</td>
                                <td>{{ stock.value | format_currency }}</td>
                                <td class="{% if stock.return > 0 %}positive{% else %}negative{% endif %}">
                                    {{ stock.return | format_percentage }}
                                </td>
                            </tr>
                        {% endfor %}
                        </tbody>
                    </table>
                </div>
                
                <div class="footer">
                    <p>이 리포트는 Finance Data Vibe 시스템에서 자동으로 생성되었습니다.</p>
                    <p>생성 시간: {{ timestamp }}</p>
                </div>
            </body>
            </html>
            ''',
            
            'analysis_report': '''
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    .header { background-color: #f4f4f4; padding: 20px; border-radius: 5px; }
                    .content { margin: 20px 0; }
                    .analysis-section { background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 10px 0; }
                    .score { font-size: 24px; font-weight: bold; }
                    .good { color: #28a745; }
                    .warning { color: #ffc107; }
                    .danger { color: #dc3545; }
                    .footer { font-size: 12px; color: #666; margin-top: 30px; }
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>📊 종목 분석 리포트</h2>
                </div>
                
                <div class="content">
                    <h3>{{ stock_name }} ({{ stock_code }}) 분석 결과</h3>
                    
                    <div class="analysis-section">
                        <h4>워런 버핏 스코어</h4>
                        <div class="score {% if buffett_score >= 80 %}good{% elif buffett_score >= 60 %}warning{% else %}danger{% endif %}">
                            {{ buffett_score }}/100
                        </div>
                        <p>{{ buffett_grade }}</p>
                    </div>
                    
                    <div class="analysis-section">
                        <h4>재무 지표</h4>
                        <ul>
                            <li><strong>ROE:</strong> {{ roe | format_percentage }}</li>
                            <li><strong>PER:</strong> {{ per }}</li>
                            <li><strong>PBR:</strong> {{ pbr }}</li>
                            <li><strong>부채비율:</strong> {{ debt_ratio | format_percentage }}</li>
                        </ul>
                    </div>
                    
                    <div class="analysis-section">
                        <h4>기술적 분석</h4>
                        <ul>
                            <li><strong>RSI:</strong> {{ rsi }}</li>
                            <li><strong>MACD:</strong> {{ macd_signal }}</li>
                            <li><strong>이동평균:</strong> {{ ma_signal }}</li>
                        </ul>
                    </div>
                    
                    <div class="analysis-section">
                        <h4>투자 추천</h4>
                        <p><strong>등급:</strong> {{ investment_grade }}</p>
                        <p><strong>내재가치:</strong> {{ intrinsic_value | format_currency }}</p>
                        <p><strong>추천 사유:</strong> {{ recommendation_reason }}</p>
                    </div>
                </div>
                
                <div class="footer">
                    <p>이 분석은 Finance Data Vibe 시스템에서 자동으로 생성되었습니다.</p>
                    <p>생성 시간: {{ timestamp }}</p>
                    <p>※ 이 분석은 참고용이며 투자 결정에 대한 책임은 투자자 본인에게 있습니다.</p>
                </div>
            </body>
            </html>
            '''
        }
        
        return templates.get(template_name, '<p>{{content}}</p>')
    
    def render_template(self, template_name: str, data: Dict[str, Any]) -> str:
        """템플릿 렌더링"""
        try:
            template_str = self.get_template(template_name)
            template = Template(template_str)
            
            # 커스텀 필터 추가
            def format_currency(value):
                try:
                    return f"₩{value:,.0f}"
                except:
                    return str(value)
            
            def format_percentage(value):
                try:
                    return f"{value:.2f}%"
                except:
                    return str(value)
            
            template.globals['format_currency'] = format_currency
            template.globals['format_percentage'] = format_percentage
            
            # 타임스탬프 추가
            data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return template.render(data)
        except Exception as e:
            logger.error(f"템플릿 렌더링 실패: {e}")
            raise EmailError(f"템플릿을 렌더링할 수 없습니다: {e}")

class EmailSender:
    """이메일 전송 클래스"""
    
    def __init__(self, config: EmailConfig = None):
        self.config = config or EmailConfig()
        self.template_manager = EmailTemplate()
    
    def send_email(self, to_emails: Union[str, List[str]], 
                   subject: str, 
                   body: str, 
                   html_body: str = None,
                   attachments: List[str] = None) -> bool:
        """이메일 전송"""
        try:
            # 설정 검증
            config_errors = self.config.validate()
            if config_errors:
                raise EmailError(f"이메일 설정 오류: {', '.join(config_errors)}")
            
            # 수신자 목록 처리
            if isinstance(to_emails, str):
                to_emails = [to_emails]
            
            # 메시지 생성
            msg = MIMEMultipart('alternative')
            msg['From'] = self.config.from_email
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = subject
            
            # 텍스트 본문 추가
            text_part = MIMEText(body, 'plain', 'utf-8')
            msg.attach(text_part)
            
            # HTML 본문 추가
            if html_body:
                html_part = MIMEText(html_body, 'html', 'utf-8')
                msg.attach(html_part)
            
            # 첨부파일 추가
            if attachments:
                for file_path in attachments:
                    self._attach_file(msg, file_path)
            
            # SMTP 서버 연결 및 전송
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.config.username, self.config.password)
                
                text = msg.as_string()
                server.sendmail(self.config.from_email, to_emails, text)
            
            logger.info(f"이메일 전송 완료: {subject} -> {', '.join(to_emails)}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 전송 실패: {e}")
            return False
    
    def _attach_file(self, msg: MIMEMultipart, file_path: str):
        """파일 첨부"""
        try:
            with open(file_path, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {Path(file_path).name}'
            )
            
            msg.attach(part)
        except Exception as e:
            logger.error(f"파일 첨부 실패: {e}")
            raise EmailError(f"파일을 첨부할 수 없습니다: {file_path}")
    
    def send_stock_alert(self, stock_data: Dict[str, Any], 
                        alert_condition: str,
                        to_emails: Union[str, List[str]] = None) -> bool:
        """주식 알림 전송"""
        try:
            if to_emails is None:
                to_emails = [self.config.to_email]
            
            subject = f"[주식 알림] {stock_data['name']} - {alert_condition}"
            
            # 템플릿 데이터 준비
            template_data = {
                'stock_name': stock_data.get('name', ''),
                'stock_code': stock_data.get('code', ''),
                'current_price': stock_data.get('current_price', 0),
                'change_rate': stock_data.get('change_rate', 0),
                'alert_condition': alert_condition,
                'additional_info': stock_data.get('additional_info', {})
            }
            
            # HTML 본문 생성
            html_body = self.template_manager.render_template('stock_alert', template_data)
            
            # 텍스트 본문 생성
            text_body = f"""
주식 알림: {stock_data['name']} ({stock_data['code']})

현재가: ₩{stock_data.get('current_price', 0):,.0f}
변동률: {stock_data.get('change_rate', 0):.2f}%
알림 조건: {alert_condition}

전송 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            return self.send_email(to_emails, subject, text_body, html_body)
            
        except Exception as e:
            logger.error(f"주식 알림 전송 실패: {e}")
            return False
    
    def send_portfolio_report(self, portfolio_data: Dict[str, Any],
                            to_emails: Union[str, List[str]] = None) -> bool:
        """포트폴리오 리포트 전송"""
        try:
            if to_emails is None:
                to_emails = [self.config.to_email]
            
            subject = f"[포트폴리오 리포트] {datetime.now().strftime('%Y-%m-%d')}"
            
            # HTML 본문 생성
            html_body = self.template_manager.render_template('portfolio_report', portfolio_data)
            
            # 텍스트 본문 생성
            text_body = f"""
포트폴리오 리포트

총 평가액: ₩{portfolio_data.get('total_value', 0):,.0f}
총 수익률: {portfolio_data.get('total_return', 0):.2f}%
일일 변동: ₩{portfolio_data.get('daily_change', 0):,.0f}

보유 종목 수: {len(portfolio_data.get('stocks', []))}

생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            return self.send_email(to_emails, subject, text_body, html_body)
            
        except Exception as e:
            logger.error(f"포트폴리오 리포트 전송 실패: {e}")
            return False
    
    def send_analysis_report(self, analysis_data: Dict[str, Any],
                           to_emails: Union[str, List[str]] = None) -> bool:
        """분석 리포트 전송"""
        try:
            if to_emails is None:
                to_emails = [self.config.to_email]
            
            subject = f"[분석 리포트] {analysis_data['stock_name']} - {analysis_data['investment_grade']}"
            
            # HTML 본문 생성
            html_body = self.template_manager.render_template('analysis_report', analysis_data)
            
            # 텍스트 본문 생성
            text_body = f"""
종목 분석 리포트: {analysis_data['stock_name']} ({analysis_data['stock_code']})

워런 버핏 스코어: {analysis_data.get('buffett_score', 0)}/100
투자 등급: {analysis_data.get('investment_grade', 'N/A')}
내재가치: ₩{analysis_data.get('intrinsic_value', 0):,.0f}

주요 재무지표:
- ROE: {analysis_data.get('roe', 0):.2f}%
- PER: {analysis_data.get('per', 0):.2f}
- PBR: {analysis_data.get('pbr', 0):.2f}

생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            return self.send_email(to_emails, subject, text_body, html_body)
            
        except Exception as e:
            logger.error(f"분석 리포트 전송 실패: {e}")
            return False

class EmailScheduler:
    """이메일 스케줄러 클래스"""
    
    def __init__(self, email_sender: EmailSender):
        self.email_sender = email_sender
        self.scheduled_emails = []
    
    def schedule_daily_report(self, time_str: str = "09:00"):
        """일일 리포트 스케줄링"""
        # 실제 구현에서는 스케줄링 라이브러리 사용
        pass
    
    def schedule_weekly_report(self, day: str = "monday", time_str: str = "09:00"):
        """주간 리포트 스케줄링"""
        # 실제 구현에서는 스케줄링 라이브러리 사용
        pass
    
    def schedule_alert(self, condition: str, check_interval: int = 300):
        """알림 조건 스케줄링"""
        # 실제 구현에서는 스케줄링 라이브러리 사용
        pass

# 전역 이메일 전송기 인스턴스
email_config = EmailConfig()
email_sender = EmailSender(email_config)

# 편의 함수들
def send_email(to_emails: Union[str, List[str]], subject: str, body: str, 
               html_body: str = None, attachments: List[str] = None) -> bool:
    """이메일 전송"""
    return email_sender.send_email(to_emails, subject, body, html_body, attachments)

def send_stock_alert(stock_data: Dict[str, Any], alert_condition: str,
                    to_emails: Union[str, List[str]] = None) -> bool:
    """주식 알림 전송"""
    return email_sender.send_stock_alert(stock_data, alert_condition, to_emails)

def send_portfolio_report(portfolio_data: Dict[str, Any],
                         to_emails: Union[str, List[str]] = None) -> bool:
    """포트폴리오 리포트 전송"""
    return email_sender.send_portfolio_report(portfolio_data, to_emails)

def send_analysis_report(analysis_data: Dict[str, Any],
                        to_emails: Union[str, List[str]] = None) -> bool:
    """분석 리포트 전송"""
    return email_sender.send_analysis_report(analysis_data, to_emails)

def validate_email_config() -> List[str]:
    """이메일 설정 유효성 검사"""
    return email_config.validate()

def test_email_connection() -> bool:
    """이메일 연결 테스트"""
    try:
        config_errors = validate_email_config()
        if config_errors:
            logger.error(f"이메일 설정 오류: {', '.join(config_errors)}")
            return False
        
        # 테스트 메시지 전송
        test_data = {
            'name': '테스트 종목',
            'code': '000000',
            'current_price': 50000,
            'change_rate': 2.5
        }
        
        return send_stock_alert(test_data, "연결 테스트")
        
    except Exception as e:
        logger.error(f"이메일 연결 테스트 실패: {e}")
        return False

# 사용 예시
if __name__ == "__main__":
    print("📧 이메일 유틸리티 테스트")
    print("=" * 50)
    
    # 이메일 설정 검증
    print("🔍 이메일 설정 검증:")
    config_errors = validate_email_config()
    if config_errors:
        print("❌ 설정 오류:")
        for error in config_errors:
            print(f"  - {error}")
    else:
        print("✅ 이메일 설정 정상")
    
    # 테스트 데이터 생성
    print("\n📊 테스트 데이터 생성:")
    
    # 주식 알림 테스트 데이터
    stock_alert_data = {
        'name': '삼성전자',
        'code': '005930',
        'current_price': 75000,
        'change_rate': 3.2,
        'additional_info': {
            'volume': '1,234,567',
            'market_cap': '448조원',
            'per': '12.5',
            'pbr': '1.2'
        }
    }
    
    # 포트폴리오 리포트 테스트 데이터
    portfolio_data = {
        'total_value': 10000000,
        'total_return': 15.8,
        'daily_change': 150000,
        'stocks': [
            {
                'name': '삼성전자',
                'quantity': 100,
                'avg_price': 70000,
                'current_price': 75000,
                'value': 7500000,
                'return': 7.14
            },
            {
                'name': 'SK하이닉스',
                'quantity': 50,
                'avg_price': 45000,
                'current_price': 50000,
                'value': 2500000,
                'return': 11.11
            }
        ]
    }
    
    # 분석 리포트 테스트 데이터
    analysis_data = {
        'stock_name': '삼성전자',
        'stock_code': '005930',
        'buffett_score': 82,
        'buffett_grade': 'A등급 (우수)',
        'investment_grade': '★★★★☆ (우수)',
        'intrinsic_value': 85000,
        'roe': 12.5,
        'per': 11.8,
        'pbr': 1.2,
        'debt_ratio': 35.2,
        'rsi': 65.4,
        'macd_signal': '매수',
        'ma_signal': '상승 추세',
        'recommendation_reason': '안정적인 재무구조와 지속적인 성장성을 바탕으로 한 우량주'
    }
    
    print("테스트 데이터 준비 완료")
    
    # 템플릿 렌더링 테스트
    print("\n🎨 템플릿 렌더링 테스트:")
    try:
        template_manager = EmailTemplate()
        
        # 주식 알림 템플릿 테스트
        stock_html = template_manager.render_template('stock_alert', stock_alert_data)
        print("✅ 주식 알림 템플릿 렌더링 성공")
        
        # 포트폴리오 리포트 템플릿 테스트
        portfolio_html = template_manager.render_template('portfolio_report', portfolio_data)
        print("✅ 포트폴리오 리포트 템플릿 렌더링 성공")
        
        # 분석 리포트 템플릿 테스트
        analysis_html = template_manager.render_template('analysis_report', analysis_data)
        print("✅ 분석 리포트 템플릿 렌더링 성공")
        
    except Exception as e:
        print(f"❌ 템플릿 렌더링 실패: {e}")
    
    # 이메일 전송 테스트 (실제 전송하지 않음)
    print("\n📤 이메일 전송 테스트:")
    print("실제 이메일 전송을 테스트하려면 올바른 SMTP 설정이 필요합니다.")
    
    if not config_errors:
        print("이메일 설정이 올바르다면 다음 함수들을 사용할 수 있습니다:")
        print("- send_stock_alert(stock_data, '목표가 달성')")
        print("- send_portfolio_report(portfolio_data)")
        print("- send_analysis_report(analysis_data)")
    
    print("\n✅ 모든 이메일 유틸리티 테스트 완료!")
    print("💡 실제 이메일 전송을 위해서는 .env 파일에 올바른 SMTP 설정이 필요합니다.")