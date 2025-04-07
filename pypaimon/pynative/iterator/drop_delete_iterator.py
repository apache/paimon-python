from typing import Optional

from pypaimon.pynative.row.key_value import KeyValue
from pypaimon.pynative.iterator.record_iterator import RecordIterator


class DropDeleteIterator(RecordIterator):
    def __init__(self, batch: RecordIterator):
        self.batch = batch

    def next(self) -> Optional[KeyValue]:
        while True:
            kv = self.batch.next()
            if kv is None:
                return None
            if kv.is_add():
                return kv

    def release_batch(self) -> None:
        self.batch.release_batch()
