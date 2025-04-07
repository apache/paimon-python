from typing import Any

from pypaimon.pynative.row.internal_row import InternalRow
from pypaimon.pynative.row.row_kind import RowKind


class OffsetRow(InternalRow):

    def __init__(self, row: InternalRow, offset: int, arity: int):
        self.row = row
        self.offset = offset
        self.arity = arity

    def replace(self, row: InternalRow) -> 'OffsetRow':
        self.row = row
        return self

    def get_field(self, pos: int):
        if pos >= self.arity:
            raise IndexError(f"Position {pos} is out of bounds for arity {self.arity}")
        return self.row.get_field(pos + self.offset)

    def get_field_by_name(self, name: str) -> Any:
        pass # todo，这是个好能力，但需要检查name是否越界，比较麻烦

    def is_null_at(self, pos: int) -> bool:
        if pos >= self.arity:
            raise IndexError(f"Position {pos} is out of bounds for arity {self.arity}")
        return self.row.is_null_at(pos + self.offset)

    def set_field(self, pos: int, value: Any) -> None:
        raise NotImplementedError()

    def get_row_kind(self) -> RowKind:
        return self.row.get_row_kind()

    def set_row_kind(self, kind: RowKind) -> None:
        self.row.set_row_kind(kind)

    def __len__(self) -> int:
        return self.arity