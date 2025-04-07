from abc import ABC, abstractmethod
from typing import TypeVar

from pypaimon.pynative.iterator.record_iterator import RecordIterator

T = TypeVar('T')

class FileRecordIterator(RecordIterator[T], ABC):
    """
    文件记录迭代器接口，增加了文件位置信息。
    """

    @abstractmethod
    def returned_position(self) -> int:
        """
        获取由next()返回的行在文件中的位置。

        返回：从0到文件中的行数的行位置
        """
        pass

    @abstractmethod
    def file_path(self) -> str:
        """返回文件路径"""
        pass
