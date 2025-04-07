from abc import ABC, abstractmethod
from typing import Any

from pypaimon.pynative.row.row_kind import RowKind


class InternalRow(ABC):

    @abstractmethod
    def get_field(self, pos: int) -> Any:
        """get field by pos"""

    @abstractmethod
    def get_field_by_name(self, name: str) -> Any:
        """get field by name"""

    @abstractmethod
    def is_null_at(self, pos: int) -> bool:
        """is null at pos"""

    @abstractmethod
    def set_field(self, pos: int, value: Any) -> None:
        """set field by pos"""

    @abstractmethod
    def get_row_kind(self) -> RowKind:
        """get row kind"""

    @abstractmethod
    def set_row_kind(self, kind: RowKind) -> None:
        """set row kind"""

    @abstractmethod
    def __len__(self) -> int:
        """len"""

    def __str__(self) -> str:
        fields = []
        for pos in range(self.__len__()):
            value = self.get_field(pos)
            fields.append(str(value))
        return " ".join(fields)
