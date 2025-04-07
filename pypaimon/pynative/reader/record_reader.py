from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional

from pypaimon.pynative.iterator.record_iterator import RecordIterator

T = TypeVar('T')


class RecordReader(Generic[T], ABC):
    """
    记录读取器接口。
    """

    @abstractmethod
    def read_batch(self) -> Optional[RecordIterator[T]]:
        """
        读取一个批次。当到达输入末尾时应返回None。

        返回：记录迭代器或None（如果没有更多数据）
        """

    @abstractmethod
    def close(self):
        """关闭读取器并释放所有资源"""
