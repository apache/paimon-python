import os
import logging
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse
from io import BytesIO

from pyarrow._fs import FileSystem
import pyarrow.fs
import pyarrow as pa

from pypaimon.pynative.common.exception import PyNativeNotImplementedError
from pypaimon.pynative.table.core_option import CoreOptions

# 常量定义
S3_ENDPOINT = "s3.endpoint"
S3_ACCESS_KEY_ID = "s3.access.key"
S3_SECRET_ACCESS_KEY = "s3.secret.key"
S3_SESSION_TOKEN = "s3.session.token"
S3_REGION = "s3.region"
S3_PROXY_URI = "s3.proxy.uri"
S3_CONNECT_TIMEOUT = "s3.connect.timeout"
S3_REQUEST_TIMEOUT = "s3.request.timeout"
S3_ROLE_ARN = "s3.role.arn"
S3_ROLE_SESSION_NAME = "s3.role.session.name"
S3_FORCE_VIRTUAL_ADDRESSING = "s3.force.virtual.addressing"

AWS_ROLE_ARN = "aws.role.arn"
AWS_ROLE_SESSION_NAME = "aws.role.session.name"

HDFS_HOST = "hdfs.host"
HDFS_PORT = "hdfs.port"
HDFS_USER = "hdfs.user"
HDFS_KERB_TICKET = "hdfs.kerb.ticket"


class FileIO:
    def __init__(self, warehouse: Path, catalog_options: dict):
        self.properties = catalog_options
        self.logger = logging.getLogger(__name__)
        scheme, netloc, path = self.parse_location(str(warehouse))
        if scheme in {"oss"}:
            self.filesystem = self._initialize_oss_fs()
        elif scheme in {"s3", "s3a", "s3n"}:
            self.filesystem = self._initialize_s3_fs()
        elif scheme in {"hdfs", "viewfs"}:
            self.filesystem = self._initialize_hdfs_fs(scheme, netloc)
        elif scheme in {"file"}:
            self.filesystem = self._initialize_local_fs()
        else:
            raise ValueError(f"Unrecognized filesystem type in URI: {scheme}")

    @staticmethod
    def parse_location(location: str):
        uri = urlparse(location)
        if not uri.scheme:
            return "file", uri.netloc, os.path.abspath(location)
        elif uri.scheme in ("hdfs", "viewfs"):
            return uri.scheme, uri.netloc, uri.path
        else:
            return uri.scheme, uri.netloc, f"{uri.netloc}{uri.path}"

    def _initialize_oss_fs(self) -> FileSystem:
        from pyarrow.fs import S3FileSystem

        client_kwargs = {
            "endpoint_override": self.properties.get(S3_ENDPOINT),
            "access_key": self.properties.get(S3_ACCESS_KEY_ID),
            "secret_key": self.properties.get(S3_SECRET_ACCESS_KEY),
            "session_token": self.properties.get(S3_SESSION_TOKEN),
            "region": self.properties.get(S3_REGION),
            "force_virtual_addressing": self.properties.get(S3_FORCE_VIRTUAL_ADDRESSING, True),
        }

        if proxy_uri := self.properties.get(S3_PROXY_URI):
            client_kwargs["proxy_options"] = proxy_uri

        if connect_timeout := self.properties.get(S3_CONNECT_TIMEOUT):
            client_kwargs["connect_timeout"] = float(connect_timeout)

        if request_timeout := self.properties.get(S3_REQUEST_TIMEOUT):
            client_kwargs["request_timeout"] = float(request_timeout)

        if role_arn := self.properties.get(S3_ROLE_ARN):
            client_kwargs["role_arn"] = role_arn

        if session_name := self.properties.get(S3_ROLE_SESSION_NAME):
            client_kwargs["session_name"] = session_name

        return S3FileSystem(**client_kwargs)

    def _initialize_s3_fs(self) -> FileSystem:
        from pyarrow.fs import S3FileSystem

        client_kwargs = {
            "endpoint_override": self.properties.get(S3_ENDPOINT),
            "access_key": self.properties.get(S3_ACCESS_KEY_ID),
            "secret_key": self.properties.get(S3_SECRET_ACCESS_KEY),
            "session_token": self.properties.get(S3_SESSION_TOKEN),
            "region": self.properties.get(S3_REGION),
        }

        if proxy_uri := self.properties.get(S3_PROXY_URI):
            client_kwargs["proxy_options"] = proxy_uri

        if connect_timeout := self.properties.get(S3_CONNECT_TIMEOUT):
            client_kwargs["connect_timeout"] = float(connect_timeout)

        if request_timeout := self.properties.get(S3_REQUEST_TIMEOUT):
            client_kwargs["request_timeout"] = float(request_timeout)

        if role_arn := self.properties.get(S3_ROLE_ARN, AWS_ROLE_ARN):
            client_kwargs["role_arn"] = role_arn

        if session_name := self.properties.get(S3_ROLE_SESSION_NAME, AWS_ROLE_SESSION_NAME):
            client_kwargs["session_name"] = session_name

        client_kwargs["force_virtual_addressing"] = self.properties.get(S3_FORCE_VIRTUAL_ADDRESSING, False)

        return S3FileSystem(**client_kwargs)

    def _initialize_hdfs_fs(self, scheme: str, netloc: Optional[str]) -> FileSystem:
        from pyarrow.fs import HadoopFileSystem

        hdfs_kwargs = {}
        if netloc:
            return HadoopFileSystem.from_uri(f"{scheme}://{netloc}")
        if host := self.properties.get(HDFS_HOST):
            hdfs_kwargs["host"] = host
        if port := self.properties.get(HDFS_PORT):
            # port should be an integer type
            hdfs_kwargs["port"] = int(port)
        if user := self.properties.get(HDFS_USER):
            hdfs_kwargs["user"] = user
        if kerb_ticket := self.properties.get(HDFS_KERB_TICKET):
            hdfs_kwargs["kerb_ticket"] = kerb_ticket

        return HadoopFileSystem(**hdfs_kwargs)

    def _initialize_local_fs(self) -> FileSystem:
        from pyarrow._fs import LocalFileSystem

        return LocalFileSystem()

    def new_input_stream(self, path: Path):
        """打开指定路径的可定位输入流"""
        return self.filesystem.open_input_file(str(path))

    def new_output_stream(self, path: Path):
        """打开指定路径的输出流"""
        # 确保父目录存在，与Java版本保持一致
        parent_dir = path.parent
        if str(parent_dir) and not self.exists(parent_dir):
            self.mkdirs(parent_dir)
        
        return self.filesystem.open_output_stream(str(path))

    def get_file_status(self, path: Path):
        """获取文件状态信息"""
        file_infos = self.filesystem.get_file_info([str(path)])
        return file_infos[0]

    def list_status(self, path: Path):
        """列出指定路径下的文件和目录状态"""
        # 使用FileSelector来列出目录内容
        selector = pyarrow.fs.FileSelector(str(path), recursive=False, allow_not_found=True)
        return self.filesystem.get_file_info(selector)

    def list_directories(self, path: Path):
        """列出指定路径下的目录状态"""
        file_infos = self.list_status(path)
        return [info for info in file_infos if info.type == pyarrow.fs.FileType.Directory]

    def exists(self, path: Path) -> bool:
        """检查路径是否存在"""
        try:
            file_info = self.filesystem.get_file_info([str(path)])[0]
            return file_info.type != pyarrow.fs.FileType.NotFound
        except Exception:
            return False

    def delete(self, path: Path, recursive: bool = False) -> bool:
        """删除文件或目录"""
        try:
            file_info = self.filesystem.get_file_info([str(path)])[0]
            if file_info.type == pyarrow.fs.FileType.Directory:
                if recursive:
                    self.filesystem.delete_dir_contents(str(path))
                else:
                    self.filesystem.delete_dir(str(path))
            else:
                self.filesystem.delete_file(str(path))
            return True
        except Exception as e:
            self.logger.warning(f"Failed to delete {path}: {e}")
            return False

    def mkdirs(self, path: Path) -> bool:
        """创建目录，包括所有不存在的父目录"""
        try:
            self.filesystem.create_dir(str(path), recursive=True)
            return True
        except Exception as e:
            self.logger.warning(f"Failed to create directory {path}: {e}")
            return False

    def rename(self, src: Path, dst: Path) -> bool:
        """重命名文件或目录"""
        try:
            dst_parent = dst.parent
            if str(dst_parent) and not self.exists(dst_parent):
                self.mkdirs(dst_parent)
            
            self.filesystem.move(str(src), str(dst))
            return True
        except Exception as e:
            self.logger.warning(f"Failed to rename {src} to {dst}: {e}")
            return False

    def delete_quietly(self, path: Path):
        """静默删除文件，不抛出异常"""
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Ready to delete {path}")
        
        try:
            if not self.delete(path, False) and self.exists(path):
                self.logger.warning(f"Failed to delete file {path}")
        except Exception as e:
            self.logger.warning(f"Exception occurs when deleting file {path}", exc_info=True)

    def delete_files_quietly(self, files: List[Path]):
        """静默删除多个文件"""
        for file_path in files:
            self.delete_quietly(file_path)

    def delete_directory_quietly(self, directory: Path):
        """静默删除目录"""
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Ready to delete {directory}")
        
        try:
            if not self.delete(directory, True) and self.exists(directory):
                self.logger.warning(f"Failed to delete directory {directory}")
        except Exception as e:
            self.logger.warning(f"Exception occurs when deleting directory {directory}", exc_info=True)

    def get_file_size(self, path: Path) -> int:
        """获取文件大小"""
        file_info = self.get_file_status(path)
        if file_info.size is None:
            raise ValueError(f"File size not available for {path}")
        return file_info.size

    def is_dir(self, path: Path) -> bool:
        """检查路径是否为目录"""
        file_info = self.get_file_status(path)
        return file_info.type == pyarrow.fs.FileType.Directory

    def check_or_mkdirs(self, path: Path):
        """检查路径是否存在，如果不存在则创建目录"""
        if self.exists(path):
            if not self.is_dir(path):
                raise ValueError(f"The path '{path}' should be a directory.")
        else:
            self.mkdirs(path)

    def read_file_utf8(self, path: Path) -> str:
        """以UTF-8编码读取文件内容"""
        with self.new_input_stream(path) as input_stream:
            return input_stream.read().decode('utf-8')

    def try_to_write_atomic(self, path: Path, content: str) -> bool:
        """尝试原子性地写入文件内容"""
        # 创建临时文件
        temp_path = Path(tempfile.mktemp())
        success = False
        try:
            self.write_file(temp_path, content, False)
            success = self.rename(temp_path, path)
        finally:
            if not success:
                self.delete_quietly(temp_path)
        return success

    def write_file(self, path: Path, content: str, overwrite: bool = False):
        """写入文件内容"""
        with self.new_output_stream(path) as output_stream:
            output_stream.write(content.encode('utf-8'))

    def overwrite_file_utf8(self, path: Path, content: str):
        """覆盖文件内容（UTF-8编码）"""
        with self.new_output_stream(path) as output_stream:
            output_stream.write(content.encode('utf-8'))

    def copy_file(self, source_path: Path, target_path: Path, overwrite: bool = False):
        """复制文件"""
        # 如果目标文件已存在且不允许覆盖，先删除
        if not overwrite and self.exists(target_path):
            raise FileExistsError(f"Target file {target_path} already exists and overwrite=False")
        
        self.filesystem.copy_file(str(source_path), str(target_path))

    def copy_files(self, source_directory: Path, target_directory: Path, overwrite: bool = False):
        """复制目录下的所有文件"""
        file_infos = self.list_status(source_directory)
        for file_info in file_infos:
            if file_info.type == pyarrow.fs.FileType.File:
                source_file = Path(file_info.path)
                target_file = target_directory / source_file.name
                self.copy_file(source_file, target_file, overwrite)

    def read_overwritten_file_utf8(self, path: Path) -> Optional[str]:
        """读取被覆盖的文件内容（UTF-8编码），支持重试机制"""
        retry_number = 0
        exception = None
        while retry_number < 5:
            try:
                return self.read_file_utf8(path)
            except FileNotFoundError:
                return None
            except Exception as e:
                if not self.exists(path):
                    return None
                
                # 检查是否为需要重试的异常
                if (str(type(e).__name__).endswith("RemoteFileChangedException") or
                    (str(e) and "Blocklist for" in str(e) and "has changed" in str(e))):
                    exception = e
                    retry_number += 1
                else:
                    raise e
        
        if exception:
            if isinstance(exception, Exception):
                raise exception
            else:
                raise RuntimeError(exception)
        
        return None

    def write_parquet(self, path: Path, table: pa.Table, compression: str = 'snappy', **kwargs):
        try:
            import pyarrow.parquet as pq
            
            with self.new_output_stream(path) as output_stream:
                pq.write_table(
                    table, 
                    output_stream, 
                    compression=compression,
                    **kwargs
                )
                
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Parquet file {path}: {e}") from e

    def write_orc(self, path: Path, table: pa.Table, compression: str = 'zstd', **kwargs):
        try:
            import pyarrow.orc as orc
            
            with self.new_output_stream(path) as output_stream:
                orc.write_table(
                    table, 
                    output_stream, 
                    compression=compression,
                    **kwargs
                )
                
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write ORC file {path}: {e}") from e

    def write_avro(self, path: Path, table: pa.Table, schema: Optional[Dict[str, Any]] = None, **kwargs):
        raise PyNativeNotImplementedError(CoreOptions.FILE_FORMAT_AVRO)
