from dataclasses import dataclass

from pypaimon.pynative.row.internal_row import InternalRow
from pypaimon.pynative.row.row_kind import RowKind


@dataclass
class KeyValue:
    key: InternalRow
    sequence_number: int
    value_kind: RowKind
    value: InternalRow
    level: int = -1  # default value is -1, same as Java version

    def set_level(self, level: int) -> 'KeyValue':
        self.level = level
        return self

    def is_add(self) -> bool:
        return self.value_kind.is_add()