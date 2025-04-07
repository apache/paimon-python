from abc import ABC, abstractmethod
from typing import Any

from pypaimon.pynative.row.row_kind import RowKind


class InternalRow(ABC):
    """
    内部行表示，用于在处理管道中传递数据。
    提供统一的行访问接口，可以适配不同的底层存储（PyArrow、NumPy等）。
    """

    @abstractmethod
    def get_field(self, pos: int) -> Any:
        """ xxx """

    @abstractmethod
    def get_field_by_name(self, name: str) -> Any:
        """ xxx """

    @abstractmethod
    def is_null_at(self, pos: int) -> bool:
        """ xxx """

    @abstractmethod
    def set_field(self, pos: int, value: Any) -> None:
        """ xxx """

    @abstractmethod
    def get_row_kind(self) -> RowKind:
        """ xxx """

    @abstractmethod
    def set_row_kind(self, kind: RowKind) -> None:
        """ xxx """

    @abstractmethod
    def __len__(self) -> int:
        """ xxx """

    def __str__(self) -> str:
        fields = []
        for pos in range(self.__len__()):
            value = self.get_field(pos)
            fields.append(str(value))
        return " ".join(fields)
