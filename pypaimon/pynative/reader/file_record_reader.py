from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar

from pypaimon.pynative.iterator.file_record_iterator import FileRecordIterator
from pypaimon.pynative.reader.record_reader import RecordReader

T = TypeVar('T')

class FileRecordReader(RecordReader[T]):

    @abstractmethod
    def read_batch(self) -> Optional[FileRecordIterator]:
        """read a batch of data"""
