from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar

from pypaimon.pynative.iterator.file_record_iterator import FileRecordIterator
from pypaimon.pynative.reader.record_reader import RecordReader

T = TypeVar('T')

class FileRecordReader(RecordReader[T]):
    """
    文件记录读取器接口，支持返回FileRecordIterator。
    """

    @abstractmethod
    def read_batch(self) -> Optional[FileRecordIterator]:
        """读取一个批次，返回文件记录迭代器"""
