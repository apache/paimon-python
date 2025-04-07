from typing import Optional

from py4j.java_gateway import JavaObject

from pypaimon.pynative.iterator.key_value_unwrap_iterator import KeyValueUnwrapIterator
from pypaimon.pynative.iterator.record_iterator import RecordIterator
from pypaimon.pynative.reader.record_reader import RecordReader
from pypaimon.pynative.record_reader_wrapper import convert_java_reader


class KeyValueUnwrapReader(RecordReader):

    def __init__(self, java_reader: JavaObject):
        reader_class = java_reader.getClass()

        sub_reader_field = reader_class.getDeclaredField("val$reader")
        sub_reader_field.setAccessible(True)
        java_sub_reader = sub_reader_field.get(java_reader)
        self.reader = convert_java_reader(java_sub_reader)

    def read_batch(self) -> Optional[RecordIterator]:
        batch = self.reader.read_batch()
        if batch is None:
            return None

        return KeyValueUnwrapIterator(batch)

    def close(self) -> None:
        self.reader.close()
