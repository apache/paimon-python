from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional

from pypaimon.pynative.iterator.record_iterator import RecordIterator

T = TypeVar('T')


class RecordReader(Generic[T], ABC):

    @abstractmethod
    def read_batch(self) -> Optional[RecordIterator[T]]:
        """
        read a batch of data
        """

    @abstractmethod
    def close(self):
        """
        close reader and release resource
        """
