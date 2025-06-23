import pyarrow as pa
import uuid
from typing import Dict, Tuple, Optional, List
from abc import ABC, abstractmethod

from pypaimon.pynative.table.core_option import CoreOptions
from pypaimon.pynative.writer.commit_message_impl import CommitMessageImpl
from pypaimon.pynative.writer.data_writer import DataWriter
from pypaimon.api import CommitMessage


class FileStoreWrite(ABC):
    """Base class for file store write operations."""

    def __init__(self, table: 'FileStoreTable', commit_user: str):
        self.table = table
        self.commit_user = commit_user
        self.data_writers: Dict[Tuple, DataWriter] = {}
        
        has_primary_keys = bool(table.table_schema.primary_keys)
        default_target_size = 128 * 1024 * 1024 if has_primary_keys else 256 * 1024 * 1024  # 128MB or 256MB
        target_file_size_str = table.options.get(CoreOptions.TARGET_FILE_SIZE, f"{default_target_size}")
        
        if isinstance(target_file_size_str, str):
            target_file_size_str = target_file_size_str.lower()
            if target_file_size_str.endswith('mb'):
                self.target_file_size = int(target_file_size_str[:-2]) * 1024 * 1024
            elif target_file_size_str.endswith('kb'):
                self.target_file_size = int(target_file_size_str[:-2]) * 1024
            elif target_file_size_str.endswith('gb'):
                self.target_file_size = int(target_file_size_str[:-2]) * 1024 * 1024 * 1024
            else:
                self.target_file_size = int(target_file_size_str)
        else:
            self.target_file_size = int(target_file_size_str)

    @abstractmethod
    def write(self, partition: Tuple, bucket: int, data: pa.Table):
        """Write data to the specified partition and bucket."""

    @abstractmethod
    def _create_data_writer(self, partition: Tuple, bucket: int) -> DataWriter:
        """Create a new data writer for the given partition and bucket."""

    def prepare_commit(self) -> List[CommitMessage]:
        commit_messages = []
        
        for (partition, bucket), writer in self.data_writers.items():
            committed_files = writer.prepare_commit()
            
            if committed_files:
                commit_message = CommitMessageImpl(
                    partition=partition,
                    bucket=bucket,
                    new_files=committed_files
                )
                commit_messages.append(commit_message)
        
        return commit_messages

    def close(self):
        """Close all data writers and clean up resources."""
        for writer in self.data_writers.values():
            writer.close()
        self.data_writers.clear()

    def _get_data_writer(self, partition: Tuple, bucket: int) -> DataWriter:
        """Get or create a data writer for the given partition and bucket."""
        key = (partition, bucket)
        if key not in self.data_writers:
            self.data_writers[key] = self._create_data_writer(partition, bucket)
        return self.data_writers[key]