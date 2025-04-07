from typing import Optional, Callable, TypeVar

from pypaimon.pynative.iterator.record_iterator import RecordIterator

T = TypeVar('T')
R = TypeVar('R')

class TransformRecordIterator(RecordIterator):

    def __init__(self, iterator: RecordIterator, func: Callable[[T], R]):
        self.iterator = iterator
        self.func = func

    def next(self) -> Optional[R]:
        record = self.iterator.next()
        # TODO
        # if record is None:
        #     return None
        # return self.func(record)
        return record

    def release_batch(self):
        self.iterator.release_batch()
