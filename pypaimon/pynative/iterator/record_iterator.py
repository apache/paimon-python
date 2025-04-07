from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional, Callable


T = TypeVar('T')

class RecordIterator(Generic[T], ABC):

    @abstractmethod
    def next(self) -> Optional[T]:
        """
        next record
        """

    @abstractmethod
    def release_batch(self):
        """
        release batch
        """
