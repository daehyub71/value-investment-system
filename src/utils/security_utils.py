"""
보안 유틸리티
암호화, 해싱, 토큰 생성 등 보안 관련 유틸리티 함수들
"""

import hashlib
import hmac
import secrets
import base64
import json
import time
from typing import Dict, Any, Optional, Union, List
from datetime import datetime, timedelta
import jwt
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import os
import logging

logger = logging.getLogger(__name__)

class SecurityError(Exception):
    """보안 관련 오류"""
    pass

class PasswordHasher:
    """비밀번호 해싱 클래스"""
    
    def __init__(self, algorithm: str = 'pbkdf2'):
        self.algorithm = algorithm
        self.iterations = 100000
        self.hash_length = 32
    
    def hash_password(self, password: str, salt: str = None) -> Dict[str, str]:
        """비밀번호 해싱"""
        try:
            if salt is None:
                salt = secrets.token_hex(16)
            
            if self.algorithm == 'pbkdf2':
                # PBKDF2 해싱
                kdf = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=self.hash_length,
                    salt=salt.encode(),
                    iterations=self.iterations,
                )
                key = kdf.derive(password.encode())
                hashed = base64.urlsafe_b64encode(key).decode()
            
            elif self.algorithm == 'bcrypt':
                # bcrypt 해싱 (bcrypt 라이브러리 필요)
                import bcrypt
                hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
                salt = ''  # bcrypt는 자체적으로 salt 포함
            
            else:
                raise SecurityError(f"지원하지 않는 해싱 알고리즘: {self.algorithm}")
            
            return {
                'hash': hashed,
                'salt': salt,
                'algorithm': self.algorithm,
                'iterations': self.iterations
            }
            
        except Exception as e:
            logger.error(f"비밀번호 해싱 실패: {e}")
            raise SecurityError(f"비밀번호를 해싱할 수 없습니다: {e}")
    
    def verify_password(self, password: str, hash_info: Dict[str, str]) -> bool:
        """비밀번호 검증"""
        try:
            if hash_info['algorithm'] == 'pbkdf2':
                # PBKDF2 검증
                kdf = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=self.hash_length,
                    salt=hash_info['salt'].encode(),
                    iterations=hash_info.get('iterations', self.iterations),
                )
                key = kdf.derive(password.encode())
                new_hash = base64.urlsafe_b64encode(key).decode()
                return new_hash == hash_info['hash']
            
            elif hash_info['algorithm'] == 'bcrypt':
                # bcrypt 검증
                import bcrypt
                return bcrypt.checkpw(password.encode(), hash_info['hash'].encode())
            
            else:
                raise SecurityError(f"지원하지 않는 해싱 알고리즘: {hash_info['algorithm']}")
                
        except Exception as e:
            logger.error(f"비밀번호 검증 실패: {e}")
            return False

class DataEncryption:
    """데이터 암호화 클래스"""
    
    def __init__(self, key: str = None):
        if key is None:
            key = os.getenv('ENCRYPTION_KEY', self._generate_key())
        
        self.key = key
        self.cipher = Fernet(key.encode() if isinstance(key, str) else key)
    
    @staticmethod
    def _generate_key() -> str:
        """암호화 키 생성"""
        return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
    
    def encrypt(self, data: Union[str, Dict, List]) -> str:
        """데이터 암호화"""
        try:
            if isinstance(data, (dict, list)):
                data = json.dumps(data)
            
            encrypted = self.cipher.encrypt(data.encode())
            return base64.urlsafe_b64encode(encrypted).decode()
            
        except Exception as e:
            logger.error(f"데이터 암호화 실패: {e}")
            raise SecurityError(f"데이터를 암호화할 수 없습니다: {e}")
    
    def decrypt(self, encrypted_data: str) -> str:
        """데이터 복호화"""
        try:
            encrypted = base64.urlsafe_b64decode(encrypted_data.encode())
            decrypted = self.cipher.decrypt(encrypted)
            return decrypted.decode()
            
        except Exception as e:
            logger.error(f"데이터 복호화 실패: {e}")
            raise SecurityError(f"데이터를 복호화할 수 없습니다: {e}")
    
    def decrypt_json(self, encrypted_data: str) -> Union[Dict, List]:
        """JSON 데이터 복호화"""
        try:
            decrypted = self.decrypt(encrypted_data)
            return json.loads(decrypted)
            
        except Exception as e:
            logger.error(f"JSON 복호화 실패: {e}")
            raise SecurityError(f"JSON 데이터를 복호화할 수 없습니다: {e}")

class TokenManager:
    """토큰 관리 클래스"""
    
    def __init__(self, secret_key: str = None, algorithm: str = 'HS256'):
        self.secret_key = secret_key or os.getenv('JWT_SECRET_KEY', self._generate_secret())
        self.algorithm = algorithm
        self.default_expiry = int(os.getenv('JWT_EXPIRATION_TIME', '3600'))
    
    @staticmethod
    def _generate_secret() -> str:
        """JWT 시크릿 키 생성"""
        return secrets.token_urlsafe(32)
    
    def generate_token(self, payload: Dict[str, Any], 
                      expires_in: int = None) -> str:
        """JWT 토큰 생성"""
        try:
            if expires_in is None:
                expires_in = self.default_expiry
            
            # 토큰 페이로드 준비
            now = datetime.utcnow()
            token_payload = {
                'iat': now,
                'exp': now + timedelta(seconds=expires_in),
                'jti': secrets.token_urlsafe(16),  # JWT ID
                **payload
            }
            
            # 토큰 생성
            token = jwt.encode(token_payload, self.secret_key, algorithm=self.algorithm)
            
            logger.info(f"JWT 토큰 생성 완료: {token[:20]}...")
            return token
            
        except Exception as e:
            logger.error(f"JWT 토큰 생성 실패: {e}")
            raise SecurityError(f"토큰을 생성할 수 없습니다: {e}")
    
    def verify_token(self, token: str) -> Dict[str, Any]:
        """JWT 토큰 검증"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            logger.info(f"JWT 토큰 검증 성공: {payload.get('jti', 'unknown')}")
            return payload
            
        except jwt.ExpiredSignatureError:
            logger.warning("JWT 토큰 만료")
            raise SecurityError("토큰이 만료되었습니다")
        
        except jwt.InvalidTokenError as e:
            logger.error(f"JWT 토큰 검증 실패: {e}")
            raise SecurityError(f"유효하지 않은 토큰입니다: {e}")
    
    def refresh_token(self, token: str) -> str:
        """토큰 갱신"""
        try:
            # 만료 시간 체크 무시하고 페이로드 추출
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm], 
                               options={"verify_exp": False})
            
            # 새 토큰 생성 (jti 제외)
            new_payload = {k: v for k, v in payload.items() 
                          if k not in ['iat', 'exp', 'jti']}
            
            return self.generate_token(new_payload)
            
        except Exception as e:
            logger.error(f"토큰 갱신 실패: {e}")
            raise SecurityError(f"토큰을 갱신할 수 없습니다: {e}")

class APIKeyManager:
    """API 키 관리 클래스"""
    
    def __init__(self, encryption: DataEncryption = None):
        self.encryption = encryption or DataEncryption()
        self.api_keys = {}
    
    def generate_api_key(self, name: str, permissions: List[str] = None) -> str:
        """API 키 생성"""
        try:
            # API 키 생성
            api_key = f"fd_{secrets.token_urlsafe(32)}"
            
            # 키 정보
            key_info = {
                'name': name,
                'key': api_key,
                'permissions': permissions or ['read'],
                'created_at': datetime.utcnow().isoformat(),
                'last_used': None,
                'usage_count': 0
            }
            
            # 암호화하여 저장
            encrypted_info = self.encryption.encrypt(key_info)
            self.api_keys[api_key] = encrypted_info
            
            logger.info(f"API 키 생성 완료: {name}")
            return api_key
            
        except Exception as e:
            logger.error(f"API 키 생성 실패: {e}")
            raise SecurityError(f"API 키를 생성할 수 없습니다: {e}")
    
    def verify_api_key(self, api_key: str) -> Dict[str, Any]:
        """API 키 검증"""
        try:
            if api_key not in self.api_keys:
                raise SecurityError("유효하지 않은 API 키입니다")
            
            # 키 정보 복호화
            encrypted_info = self.api_keys[api_key]
            key_info = self.encryption.decrypt_json(encrypted_info)
            
            # 사용 횟수 업데이트
            key_info['usage_count'] += 1
            key_info['last_used'] = datetime.utcnow().isoformat()
            
            # 다시 암호화하여 저장
            self.api_keys[api_key] = self.encryption.encrypt(key_info)
            
            logger.info(f"API 키 검증 성공: {key_info['name']}")
            return key_info
            
        except Exception as e:
            logger.error(f"API 키 검증 실패: {e}")
            raise SecurityError(f"API 키를 검증할 수 없습니다: {e}")
    
    def revoke_api_key(self, api_key: str) -> bool:
        """API 키 폐기"""
        try:
            if api_key in self.api_keys:
                del self.api_keys[api_key]
                logger.info(f"API 키 폐기 완료: {api_key[:20]}...")
                return True
            return False
            
        except Exception as e:
            logger.error(f"API 키 폐기 실패: {e}")
            return False

class SecurityAudit:
    """보안 감사 클래스"""
    
    def __init__(self):
        self.audit_log = []
        self.failed_attempts = {}
        self.max_failed_attempts = 5
        self.lockout_time = 300  # 5분
    
    def log_security_event(self, event_type: str, details: Dict[str, Any]):
        """보안 이벤트 로그"""
        try:
            event = {
                'timestamp': datetime.utcnow().isoformat(),
                'event_type': event_type,
                'details': details,
                'ip_address': details.get('ip_address', 'unknown'),
                'user_agent': details.get('user_agent', 'unknown')
            }
            
            self.audit_log.append(event)
            logger.info(f"보안 이벤트 로그: {event_type}")
            
            # 실패 시도 추적
            if event_type in ['login_failed', 'token_invalid', 'api_key_invalid']:
                self._track_failed_attempt(details.get('ip_address', 'unknown'))
            
        except Exception as e:
            logger.error(f"보안 이벤트 로그 실패: {e}")
    
    def _track_failed_attempt(self, ip_address: str):
        """실패 시도 추적"""
        now = time.time()
        
        if ip_address not in self.failed_attempts:
            self.failed_attempts[ip_address] = []
        
        # 오래된 시도 제거
        self.failed_attempts[ip_address] = [
            timestamp for timestamp in self.failed_attempts[ip_address]
            if now - timestamp < self.lockout_time
        ]
        
        # 새 시도 추가
        self.failed_attempts[ip_address].append(now)
        
        # 잠금 확인
        if len(self.failed_attempts[ip_address]) >= self.max_failed_attempts:
            logger.warning(f"IP 주소 잠금: {ip_address}")
    
    def is_ip_locked(self, ip_address: str) -> bool:
        """IP 주소 잠금 확인"""
        if ip_address not in self.failed_attempts:
            return False
        
        now = time.time()
        recent_attempts = [
            timestamp for timestamp in self.failed_attempts[ip_address]
            if now - timestamp < self.lockout_time
        ]
        
        return len(recent_attempts) >= self.max_failed_attempts
    
    def get_audit_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        """감사 로그 조회"""
        return self.audit_log[-limit:]
    
    def get_security_summary(self) -> Dict[str, Any]:
        """보안 요약 정보"""
        now = time.time()
        recent_events = [
            event for event in self.audit_log
            if now - time.mktime(datetime.fromisoformat(event['timestamp']).timetuple()) < 86400
        ]
        
        event_counts = {}
        for event in recent_events:
            event_type = event['event_type']
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
        
        locked_ips = [
            ip for ip, attempts in self.failed_attempts.items()
            if len([t for t in attempts if now - t < self.lockout_time]) >= self.max_failed_attempts
        ]
        
        return {
            'total_events_24h': len(recent_events),
            'event_counts': event_counts,
            'locked_ips': locked_ips,
            'total_locked_ips': len(locked_ips)
        }

class InputSanitizer:
    """입력 데이터 정제 클래스"""
    
    @staticmethod
    def sanitize_string(input_str: str, max_length: int = 1000) -> str:
        """문자열 정제"""
        if not isinstance(input_str, str):
            return str(input_str)
        
        # 길이 제한
        sanitized = input_str[:max_length]
        
        # HTML 태그 제거
        import re
        sanitized = re.sub(r'<[^>]+>', '', sanitized)
        
        # 특수 문자 제거 (기본적인 것들만)
        sanitized = re.sub(r'[<>\"\'&]', '', sanitized)
        
        return sanitized.strip()
    
    @staticmethod
    def sanitize_sql_input(input_str: str) -> str:
        """SQL 인젝션 방지"""
        if not isinstance(input_str, str):
            return str(input_str)
        
        # 위험한 SQL 키워드 제거
        dangerous_keywords = [
            'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE',
            'EXEC', 'EXECUTE', 'UNION', 'SELECT', 'TRUNCATE'
        ]
        
        sanitized = input_str
        for keyword in dangerous_keywords:
            sanitized = sanitized.replace(keyword, '')
            sanitized = sanitized.replace(keyword.lower(), '')
        
        return sanitized
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """이메일 형식 검증"""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
    
    @staticmethod
    def validate_stock_code(stock_code: str) -> bool:
        """종목코드 검증"""
        import re
        return re.match(r'^\d{6}$', stock_code) is not None
    
    @staticmethod
    def sanitize_file_name(filename: str) -> str:
        """파일명 정제"""
        import re
        # 위험한 문자 제거
        sanitized = re.sub(r'[<>:"/\\|?*]', '', filename)
        
        # 연속된 점 제거
        sanitized = re.sub(r'\.+', '.', sanitized)
        
        # 파일명 길이 제한
        if len(sanitized) > 255:
            name, ext = os.path.splitext(sanitized)
            sanitized = name[:255-len(ext)] + ext
        
        return sanitized

# 전역 인스턴스
password_hasher = PasswordHasher()
data_encryption = DataEncryption()
token_manager = TokenManager()
api_key_manager = APIKeyManager()
security_audit = SecurityAudit()
input_sanitizer = InputSanitizer()

# 편의 함수들
def hash_password(password: str) -> Dict[str, str]:
    """비밀번호 해싱"""
    return password_hasher.hash_password(password)

def verify_password(password: str, hash_info: Dict[str, str]) -> bool:
    """비밀번호 검증"""
    return password_hasher.verify_password(password, hash_info)

def encrypt_data(data: Union[str, Dict, List]) -> str:
    """데이터 암호화"""
    return data_encryption.encrypt(data)

def decrypt_data(encrypted_data: str) -> str:
    """데이터 복호화"""
    return data_encryption.decrypt(encrypted_data)

def generate_token(payload: Dict[str, Any], expires_in: int = None) -> str:
    """JWT 토큰 생성"""
    return token_manager.generate_token(payload, expires_in)

def verify_token(token: str) -> Dict[str, Any]:
    """JWT 토큰 검증"""
    return token_manager.verify_token(token)

def generate_api_key(name: str, permissions: List[str] = None) -> str:
    """API 키 생성"""
    return api_key_manager.generate_api_key(name, permissions)

def verify_api_key(api_key: str) -> Dict[str, Any]:
    """API 키 검증"""
    return api_key_manager.verify_api_key(api_key)

def sanitize_string(input_str: str, max_length: int = 1000) -> str:
    """문자열 정제"""
    return input_sanitizer.sanitize_string(input_str, max_length)

def validate_email(email: str) -> bool:
    """이메일 형식 검증"""
    return input_sanitizer.validate_email(email)

def log_security_event(event_type: str, details: Dict[str, Any]):
    """보안 이벤트 로그"""
    security_audit.log_security_event(event_type, details)

def is_ip_locked(ip_address: str) -> bool:
    """IP 주소 잠금 확인"""
    return security_audit.is_ip_locked(ip_address)

def get_security_summary() -> Dict[str, Any]:
    """보안 요약 정보"""
    return security_audit.get_security_summary()

# 사용 예시
if __name__ == "__main__":
    print("🔐 보안 유틸리티 테스트")
    print("=" * 50)
    
    # 비밀번호 해싱 테스트
    print("🔒 비밀번호 해싱 테스트:")
    test_password = "mySecurePassword123!"
    
    hash_info = hash_password(test_password)
    print(f"해시 생성 완료: {hash_info['hash'][:20]}...")
    
    # 비밀번호 검증
    is_valid = verify_password(test_password, hash_info)
    print(f"비밀번호 검증: {'✅ 성공' if is_valid else '❌ 실패'}")
    
    is_invalid = verify_password("wrongPassword", hash_info)
    print(f"잘못된 비밀번호: {'❌ 성공' if is_invalid else '✅ 실패'}")
    
    # 데이터 암호화 테스트
    print("\n🔐 데이터 암호화 테스트:")
    test_data = {"api_key": "secret_key_123", "user_id": 12345}
    
    encrypted = encrypt_data(test_data)
    print(f"암호화 완료: {encrypted[:30]}...")
    
    decrypted = decrypt_data(encrypted)
    print(f"복호화 완료: {decrypted}")
    
    # JWT 토큰 테스트
    print("\n🎫 JWT 토큰 테스트:")
    payload = {"user_id": 123, "role": "admin"}
    
    token = generate_token(payload, expires_in=3600)
    print(f"토큰 생성 완료: {token[:30]}...")
    
    try:
        verified_payload = verify_token(token)
        print(f"토큰 검증 성공: {verified_payload['user_id']}")
    except SecurityError as e:
        print(f"토큰 검증 실패: {e}")
    
    # API 키 테스트
    print("\n🔑 API 키 테스트:")
    api_key = generate_api_key("test_app", ["read", "write"])
    print(f"API 키 생성: {api_key[:20]}...")
    
    try:
        key_info = verify_api_key(api_key)
        print(f"API 키 검증 성공: {key_info['name']}")
    except SecurityError as e:
        print(f"API 키 검증 실패: {e}")
    
    # 입력 데이터 정제 테스트
    print("\n🧹 입력 데이터 정제 테스트:")
    dirty_input = "<script>alert('xss')</script>Hello World!"
    clean_input = sanitize_string(dirty_input)
    print(f"정제 전: {dirty_input}")
    print(f"정제 후: {clean_input}")
    
    # 이메일 검증 테스트
    test_emails = ["user@example.com", "invalid-email", "test@domain.co.kr"]
    for email in test_emails:
        is_valid = validate_email(email)
        print(f"이메일 {email}: {'✅ 유효' if is_valid else '❌ 무효'}")
    
    # 보안 이벤트 로그 테스트
    print("\n📊 보안 이벤트 로그 테스트:")
    log_security_event("login_success", {
        "user_id": 123,
        "ip_address": "192.168.1.1",
        "user_agent": "Mozilla/5.0..."
    })
    
    log_security_event("login_failed", {
        "ip_address": "192.168.1.100",
        "reason": "invalid_password"
    })
    
    summary = get_security_summary()
    print(f"보안 요약: {summary}")
    
    print("\n✅ 모든 보안 유틸리티 테스트 완료!")