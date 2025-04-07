from typing import Any

import pyarrow as pa

from pypaimon.pynative.row.internal_row import InternalRow
from pypaimon.pynative.row.key_value import RowKind


class ColumnarRow(InternalRow):
    """基于PyArrow的ColumnarRow实现"""

    def __init__(self, record_batch: pa.RecordBatch, row_id: int = 0, row_kind: RowKind = RowKind.INSERT):
        self._batch = record_batch
        self._row_id = row_id
        self._row_kind = row_kind

    def get_row_id(self) -> int:
        return self._row_id

    def set_row_id(self, row_id: int) -> None:
        self._row_id = row_id

    def batch(self) -> pa.RecordBatch:
        return self._batch

    def get_field(self, pos: int) -> Any:
        return self._batch.column(pos)[self._row_id].as_py()

    def get_field_by_name(self, name: str) -> Any:
        return self._batch.column(name)[self._row_id]

    def is_null_at(self, pos: int) -> bool:
        return self._batch.column(pos).is_null(self._row_id)

    def set_field(self, pos: int, value: Any) -> None:
        raise NotImplementedError()

    def get_row_kind(self) -> RowKind:
        return self._row_kind

    def set_row_kind(self, kind: RowKind) -> None:
        self._row_kind = kind

    def __len__(self) -> int:
        return self._batch.num_columns

