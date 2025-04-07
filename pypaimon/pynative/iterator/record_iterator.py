from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional, Callable


T = TypeVar('T')

class RecordIterator(Generic[T], ABC):
    """
    记录迭代器接口，用于迭代一个批次的记录。
    """

    @abstractmethod
    def next(self) -> Optional[T]:
        """
        获取下一条记录。返回None表示该批次没有更多记录。
        """

    @abstractmethod
    def release_batch(self):
        """
        释放批次资源。这不会关闭读取器，只是表明该迭代器不再使用。
        可用于回收/重用大型对象结构。
        """
