"""
파일 처리 유틸리티
파일 읽기, 쓰기, 압축 등 파일 관련 유틸리티 함수들
"""

import os
import json
import csv
import zipfile
import gzip
import shutil
import hashlib
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Generator
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class FileError(Exception):
    """파일 처리 오류"""
    pass

class FileManager:
    """파일 관리 클래스"""
    
    def __init__(self, base_path: Union[str, Path] = None):
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def ensure_directory(self, path: Union[str, Path]) -> Path:
        """디렉토리 존재 확인 및 생성"""
        dir_path = Path(path)
        if not dir_path.is_absolute():
            dir_path = self.base_path / dir_path
        
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path
    
    def get_file_info(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """파일 정보 조회"""
        path = Path(file_path)
        if not path.is_absolute():
            path = self.base_path / path
        
        if not path.exists():
            raise FileError(f"파일이 존재하지 않습니다: {path}")
        
        stat = path.stat()
        
        return {
            'name': path.name,
            'path': str(path),
            'size': stat.st_size,
            'size_mb': stat.st_size / (1024 * 1024),
            'created': datetime.fromtimestamp(stat.st_ctime),
            'modified': datetime.fromtimestamp(stat.st_mtime),
            'accessed': datetime.fromtimestamp(stat.st_atime),
            'extension': path.suffix,
            'is_file': path.is_file(),
            'is_dir': path.is_dir()
        }
    
    def list_files(self, directory: Union[str, Path], 
                   pattern: str = "*", recursive: bool = False) -> List[Path]:
        """디렉토리 내 파일 목록 조회"""
        dir_path = Path(directory)
        if not dir_path.is_absolute():
            dir_path = self.base_path / dir_path
        
        if not dir_path.exists():
            raise FileError(f"디렉토리가 존재하지 않습니다: {dir_path}")
        
        if recursive:
            return list(dir_path.rglob(pattern))
        else:
            return list(dir_path.glob(pattern))
    
    def copy_file(self, source: Union[str, Path], 
                  destination: Union[str, Path], 
                  overwrite: bool = False) -> Path:
        """파일 복사"""
        src_path = Path(source)
        dest_path = Path(destination)
        
        if not src_path.is_absolute():
            src_path = self.base_path / src_path
        if not dest_path.is_absolute():
            dest_path = self.base_path / dest_path
        
        if not src_path.exists():
            raise FileError(f"원본 파일이 존재하지 않습니다: {src_path}")
        
        if dest_path.exists() and not overwrite:
            raise FileError(f"대상 파일이 이미 존재합니다: {dest_path}")
        
        # 대상 디렉토리 생성
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        shutil.copy2(src_path, dest_path)
        logger.info(f"파일 복사 완료: {src_path} -> {dest_path}")
        
        return dest_path
    
    def move_file(self, source: Union[str, Path], 
                  destination: Union[str, Path]) -> Path:
        """파일 이동"""
        src_path = Path(source)
        dest_path = Path(destination)
        
        if not src_path.is_absolute():
            src_path = self.base_path / src_path
        if not dest_path.is_absolute():
            dest_path = self.base_path / dest_path
        
        if not src_path.exists():
            raise FileError(f"원본 파일이 존재하지 않습니다: {src_path}")
        
        # 대상 디렉토리 생성
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        shutil.move(str(src_path), str(dest_path))
        logger.info(f"파일 이동 완료: {src_path} -> {dest_path}")
        
        return dest_path
    
    def delete_file(self, file_path: Union[str, Path], 
                   confirm: bool = True) -> bool:
        """파일 삭제"""
        path = Path(file_path)
        if not path.is_absolute():
            path = self.base_path / path
        
        if not path.exists():
            logger.warning(f"삭제할 파일이 존재하지 않습니다: {path}")
            return False
        
        if confirm:
            response = input(f"파일을 삭제하시겠습니까? {path} (y/N): ")
            if response.lower() != 'y':
                return False
        
        try:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)
            
            logger.info(f"파일 삭제 완료: {path}")
            return True
        except Exception as e:
            logger.error(f"파일 삭제 실패: {e}")
            return False
    
    def get_file_hash(self, file_path: Union[str, Path], 
                     algorithm: str = 'md5') -> str:
        """파일 해시 계산"""
        path = Path(file_path)
        if not path.is_absolute():
            path = self.base_path / path
        
        if not path.exists():
            raise FileError(f"파일이 존재하지 않습니다: {path}")
        
        hash_algo = getattr(hashlib, algorithm)()
        
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_algo.update(chunk)
        
        return hash_algo.hexdigest()
    
    def cleanup_old_files(self, directory: Union[str, Path], 
                         days_old: int = 30, 
                         pattern: str = "*") -> List[Path]:
        """오래된 파일 정리"""
        dir_path = Path(directory)
        if not dir_path.is_absolute():
            dir_path = self.base_path / dir_path
        
        if not dir_path.exists():
            return []
        
        cutoff_date = datetime.now() - timedelta(days=days_old)
        deleted_files = []
        
        for file_path in dir_path.glob(pattern):
            if file_path.is_file():
                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_time < cutoff_date:
                    try:
                        file_path.unlink()
                        deleted_files.append(file_path)
                        logger.info(f"오래된 파일 삭제: {file_path}")
                    except Exception as e:
                        logger.error(f"파일 삭제 실패: {e}")
        
        return deleted_files

class DataFileHandler:
    """데이터 파일 처리 클래스"""
    
    @staticmethod
    def read_json(file_path: Union[str, Path], 
                  encoding: str = 'utf-8') -> Dict[str, Any]:
        """JSON 파일 읽기"""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"JSON 파일 읽기 실패: {e}")
            raise FileError(f"JSON 파일을 읽을 수 없습니다: {file_path}")
    
    @staticmethod
    def write_json(data: Dict[str, Any], file_path: Union[str, Path], 
                   encoding: str = 'utf-8', indent: int = 2) -> bool:
        """JSON 파일 쓰기"""
        try:
            # 디렉토리 생성
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding=encoding) as f:
                json.dump(data, f, indent=indent, ensure_ascii=False)
            
            logger.info(f"JSON 파일 저장 완료: {file_path}")
            return True
        except Exception as e:
            logger.error(f"JSON 파일 쓰기 실패: {e}")
            return False
    
    @staticmethod
    def read_csv(file_path: Union[str, Path], 
                 encoding: str = 'utf-8', **kwargs) -> pd.DataFrame:
        """CSV 파일 읽기"""
        try:
            return pd.read_csv(file_path, encoding=encoding, **kwargs)
        except Exception as e:
            logger.error(f"CSV 파일 읽기 실패: {e}")
            raise FileError(f"CSV 파일을 읽을 수 없습니다: {file_path}")
    
    @staticmethod
    def write_csv(df: pd.DataFrame, file_path: Union[str, Path], 
                  encoding: str = 'utf-8', index: bool = False, **kwargs) -> bool:
        """CSV 파일 쓰기"""
        try:
            # 디렉토리 생성
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(file_path, encoding=encoding, index=index, **kwargs)
            logger.info(f"CSV 파일 저장 완료: {file_path}")
            return True
        except Exception as e:
            logger.error(f"CSV 파일 쓰기 실패: {e}")
            return False
    
    @staticmethod
    def read_excel(file_path: Union[str, Path], 
                   sheet_name: str = None, **kwargs) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Excel 파일 읽기"""
        try:
            return pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
        except Exception as e:
            logger.error(f"Excel 파일 읽기 실패: {e}")
            raise FileError(f"Excel 파일을 읽을 수 없습니다: {file_path}")
    
    @staticmethod
    def write_excel(data: Union[pd.DataFrame, Dict[str, pd.DataFrame]], 
                    file_path: Union[str, Path], 
                    index: bool = False, **kwargs) -> bool:
        """Excel 파일 쓰기"""
        try:
            # 디렉토리 생성
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            if isinstance(data, pd.DataFrame):
                data.to_excel(file_path, index=index, **kwargs)
            elif isinstance(data, dict):
                with pd.ExcelWriter(file_path, **kwargs) as writer:
                    for sheet_name, df in data.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=index)
            
            logger.info(f"Excel 파일 저장 완료: {file_path}")
            return True
        except Exception as e:
            logger.error(f"Excel 파일 쓰기 실패: {e}")
            return False
    
    @staticmethod
    def read_pickle(file_path: Union[str, Path]) -> Any:
        """Pickle 파일 읽기"""
        try:
            with open(file_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.error(f"Pickle 파일 읽기 실패: {e}")
            raise FileError(f"Pickle 파일을 읽을 수 없습니다: {file_path}")
    
    @staticmethod
    def write_pickle(data: Any, file_path: Union[str, Path]) -> bool:
        """Pickle 파일 쓰기"""
        try:
            # 디렉토리 생성
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
            
            logger.info(f"Pickle 파일 저장 완료: {file_path}")
            return True
        except Exception as e:
            logger.error(f"Pickle 파일 쓰기 실패: {e}")
            return False

class CompressionHandler:
    """압축 처리 클래스"""
    
    @staticmethod
    def compress_file(file_path: Union[str, Path], 
                     compression_type: str = 'gzip') -> Path:
        """파일 압축"""
        source_path = Path(file_path)
        
        if not source_path.exists():
            raise FileError(f"압축할 파일이 존재하지 않습니다: {source_path}")
        
        if compression_type == 'gzip':
            compressed_path = source_path.with_suffix(source_path.suffix + '.gz')
            
            with open(source_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        elif compression_type == 'zip':
            compressed_path = source_path.with_suffix('.zip')
            
            with zipfile.ZipFile(compressed_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(source_path, source_path.name)
        
        else:
            raise ValueError(f"지원하지 않는 압축 형식: {compression_type}")
        
        logger.info(f"파일 압축 완료: {source_path} -> {compressed_path}")
        return compressed_path
    
    @staticmethod
    def decompress_file(compressed_path: Union[str, Path], 
                       output_path: Union[str, Path] = None) -> Path:
        """파일 압축 해제"""
        source_path = Path(compressed_path)
        
        if not source_path.exists():
            raise FileError(f"압축 파일이 존재하지 않습니다: {source_path}")
        
        if output_path is None:
            if source_path.suffix == '.gz':
                output_path = source_path.with_suffix('')
            elif source_path.suffix == '.zip':
                output_path = source_path.parent / source_path.stem
            else:
                raise ValueError(f"지원하지 않는 압축 형식: {source_path.suffix}")
        
        output_path = Path(output_path)
        
        if source_path.suffix == '.gz':
            with gzip.open(source_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        elif source_path.suffix == '.zip':
            with zipfile.ZipFile(source_path, 'r') as zipf:
                zipf.extractall(output_path.parent)
        
        else:
            raise ValueError(f"지원하지 않는 압축 형식: {source_path.suffix}")
        
        logger.info(f"파일 압축 해제 완료: {source_path} -> {output_path}")
        return output_path
    
    @staticmethod
    def create_archive(files: List[Union[str, Path]], 
                      archive_path: Union[str, Path], 
                      compression_type: str = 'zip') -> Path:
        """여러 파일을 하나의 압축 파일로 생성"""
        archive_path = Path(archive_path)
        
        if compression_type == 'zip':
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in files:
                    file_path = Path(file_path)
                    if file_path.exists():
                        zipf.write(file_path, file_path.name)
        
        else:
            raise ValueError(f"지원하지 않는 압축 형식: {compression_type}")
        
        logger.info(f"압축 파일 생성 완료: {archive_path}")
        return archive_path

class BackupManager:
    """백업 관리 클래스"""
    
    def __init__(self, backup_dir: Union[str, Path]):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
    
    def create_backup(self, source_path: Union[str, Path], 
                     backup_name: str = None) -> Path:
        """백업 생성"""
        source_path = Path(source_path)
        
        if not source_path.exists():
            raise FileError(f"백업할 파일/디렉토리가 존재하지 않습니다: {source_path}")
        
        if backup_name is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"{source_path.name}_{timestamp}"
        
        backup_path = self.backup_dir / backup_name
        
        if source_path.is_file():
            shutil.copy2(source_path, backup_path)
        elif source_path.is_dir():
            shutil.copytree(source_path, backup_path)
        
        logger.info(f"백업 생성 완료: {source_path} -> {backup_path}")
        return backup_path
    
    def restore_backup(self, backup_name: str, 
                      restore_path: Union[str, Path]) -> Path:
        """백업 복원"""
        backup_path = self.backup_dir / backup_name
        restore_path = Path(restore_path)
        
        if not backup_path.exists():
            raise FileError(f"백업 파일이 존재하지 않습니다: {backup_path}")
        
        if restore_path.exists():
            response = input(f"복원 위치에 이미 파일이 있습니다. 덮어쓰시겠습니까? {restore_path} (y/N): ")
            if response.lower() != 'y':
                return restore_path
        
        if backup_path.is_file():
            shutil.copy2(backup_path, restore_path)
        elif backup_path.is_dir():
            if restore_path.exists():
                shutil.rmtree(restore_path)
            shutil.copytree(backup_path, restore_path)
        
        logger.info(f"백업 복원 완료: {backup_path} -> {restore_path}")
        return restore_path
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """백업 목록 조회"""
        backups = []
        
        for backup_path in self.backup_dir.iterdir():
            if backup_path.is_file() or backup_path.is_dir():
                stat = backup_path.stat()
                backups.append({
                    'name': backup_path.name,
                    'path': str(backup_path),
                    'size': stat.st_size,
                    'created': datetime.fromtimestamp(stat.st_ctime),
                    'modified': datetime.fromtimestamp(stat.st_mtime),
                    'type': 'file' if backup_path.is_file() else 'directory'
                })
        
        return sorted(backups, key=lambda x: x['created'], reverse=True)
    
    def cleanup_old_backups(self, days_old: int = 30) -> List[Path]:
        """오래된 백업 정리"""
        cutoff_date = datetime.now() - timedelta(days=days_old)
        deleted_backups = []
        
        for backup_path in self.backup_dir.iterdir():
            backup_time = datetime.fromtimestamp(backup_path.stat().st_ctime)
            if backup_time < cutoff_date:
                try:
                    if backup_path.is_file():
                        backup_path.unlink()
                    elif backup_path.is_dir():
                        shutil.rmtree(backup_path)
                    
                    deleted_backups.append(backup_path)
                    logger.info(f"오래된 백업 삭제: {backup_path}")
                except Exception as e:
                    logger.error(f"백업 삭제 실패: {e}")
        
        return deleted_backups

# 전역 파일 관리자 인스턴스
file_manager = FileManager()
data_handler = DataFileHandler()
compression_handler = CompressionHandler()

# 편의 함수들
def read_json(file_path: Union[str, Path]) -> Dict[str, Any]:
    """JSON 파일 읽기"""
    return data_handler.read_json(file_path)

def write_json(data: Dict[str, Any], file_path: Union[str, Path]) -> bool:
    """JSON 파일 쓰기"""
    return data_handler.write_json(data, file_path)

def read_csv(file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """CSV 파일 읽기"""
    return data_handler.read_csv(file_path, **kwargs)

def write_csv(df: pd.DataFrame, file_path: Union[str, Path], **kwargs) -> bool:
    """CSV 파일 쓰기"""
    return data_handler.write_csv(df, file_path, **kwargs)

def read_excel(file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """Excel 파일 읽기"""
    return data_handler.read_excel(file_path, **kwargs)

def write_excel(data: pd.DataFrame, file_path: Union[str, Path], **kwargs) -> bool:
    """Excel 파일 쓰기"""
    return data_handler.write_excel(data, file_path, **kwargs)

def compress_file(file_path: Union[str, Path], compression_type: str = 'gzip') -> Path:
    """파일 압축"""
    return compression_handler.compress_file(file_path, compression_type)

def decompress_file(compressed_path: Union[str, Path], output_path: Union[str, Path] = None) -> Path:
    """파일 압축 해제"""
    return compression_handler.decompress_file(compressed_path, output_path)

def get_file_info(file_path: Union[str, Path]) -> Dict[str, Any]:
    """파일 정보 조회"""
    return file_manager.get_file_info(file_path)

def list_files(directory: Union[str, Path], pattern: str = "*") -> List[Path]:
    """파일 목록 조회"""
    return file_manager.list_files(directory, pattern)

def ensure_directory(path: Union[str, Path]) -> Path:
    """디렉토리 생성"""
    return file_manager.ensure_directory(path)

def get_file_hash(file_path: Union[str, Path], algorithm: str = 'md5') -> str:
    """파일 해시 계산"""
    return file_manager.get_file_hash(file_path, algorithm)

# 사용 예시
if __name__ == "__main__":
    print("📁 파일 처리 유틸리티 테스트")
    print("=" * 50)
    
    # 테스트 디렉토리 생성
    test_dir = ensure_directory("test_files")
    print(f"테스트 디렉토리 생성: {test_dir}")
    
    # JSON 파일 테스트
    print("\n📄 JSON 파일 테스트:")
    test_data = {
        "name": "테스트 데이터",
        "values": [1, 2, 3, 4, 5],
        "timestamp": datetime.now().isoformat()
    }
    
    json_file = test_dir / "test.json"
    success = write_json(test_data, json_file)
    print(f"JSON 파일 저장: {'성공' if success else '실패'}")
    
    if success:
        loaded_data = read_json(json_file)
        print(f"JSON 파일 로드: {loaded_data['name']}")
    
    # CSV 파일 테스트
    print("\n📊 CSV 파일 테스트:")
    test_df = pd.DataFrame({
        'A': [1, 2, 3, 4, 5],
        'B': [10, 20, 30, 40, 50],
        'C': ['가', '나', '다', '라', '마']
    })
    
    csv_file = test_dir / "test.csv"
    success = write_csv(test_df, csv_file)
    print(f"CSV 파일 저장: {'성공' if success else '실패'}")
    
    if success:
        loaded_df = read_csv(csv_file)
        print(f"CSV 파일 로드: {len(loaded_df)} 행")
    
    # 파일 정보 조회
    print("\n📋 파일 정보 조회:")
    if json_file.exists():
        file_info = get_file_info(json_file)
        print(f"파일명: {file_info['name']}")
        print(f"크기: {file_info['size']} bytes")
        print(f"수정일: {file_info['modified']}")
    
    # 파일 목록 조회
    print("\n📂 파일 목록 조회:")
    files = list_files(test_dir)
    for file_path in files:
        print(f"  {file_path.name}")
    
    # 파일 해시 계산
    print("\n🔐 파일 해시 계산:")
    if json_file.exists():
        file_hash = get_file_hash(json_file)
        print(f"MD5 해시: {file_hash}")
    
    # 압축 테스트
    print("\n📦 압축 테스트:")
    if json_file.exists():
        try:
            compressed_file = compress_file(json_file, 'gzip')
            print(f"압축 파일 생성: {compressed_file}")
            
            decompressed_file = decompress_file(compressed_file)
            print(f"압축 해제: {decompressed_file}")
        except Exception as e:
            print(f"압축 테스트 실패: {e}")
    
    # 백업 테스트
    print("\n💾 백업 테스트:")
    backup_mgr = BackupManager(test_dir / "backups")
    
    if json_file.exists():
        try:
            backup_path = backup_mgr.create_backup(json_file)
            print(f"백업 생성: {backup_path}")
            
            backups = backup_mgr.list_backups()
            print(f"백업 목록: {len(backups)}개")
        except Exception as e:
            print(f"백업 테스트 실패: {e}")
    
    # 정리
    print("\n🧹 테스트 파일 정리:")
    try:
        if test_dir.exists():
            shutil.rmtree(test_dir)
            print("테스트 디렉토리 삭제 완료")
    except Exception as e:
        print(f"정리 실패: {e}")
    
    print("\n✅ 모든 파일 처리 테스트 완료!")