from abc import ABC, abstractmethod
from typing import TypeVar

from pypaimon.pynative.iterator.record_iterator import RecordIterator

T = TypeVar('T')

class FileRecordIterator(RecordIterator[T], ABC):

    @abstractmethod
    def returned_position(self) -> int:
        pass

    @abstractmethod
    def file_path(self) -> str:
        pass
