from typing import Optional, Any

from pypaimon.pynative.iterator.record_iterator import RecordIterator
from pypaimon.pynative.row.key_value import KeyValue
from pypaimon.pynative.row.row_kind import RowKind


class KeyValueUnwrapIterator(RecordIterator):

    def __init__(self, kv_iterator: RecordIterator):
        self.kv_iterator = kv_iterator
        self.key_value: KeyValue = None
        self.pre_value_row_kind: RowKind = None

    def next(self) -> Optional[Any]:
        if self.key_value is not None:
            self.key_value.value.set_row_kind(self.pre_value_row_kind)

        self.key_value = self.kv_iterator.next()
        if self.key_value is None:
            return None

        row_data = self.key_value.value
        self.pre_value_row_kind = row_data.get_row_kind()

        row_data.set_row_kind(self.key_value.value_kind)
        return row_data

    def release_batch(self) -> None:
        self.kv_iterator.release_batch()