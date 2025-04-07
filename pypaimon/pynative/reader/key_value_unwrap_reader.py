from typing import Optional

from py4j.java_gateway import JavaObject

from pypaimon.pynative.iterator.key_value_unwrap_iterator import KeyValueUnwrapIterator
from pypaimon.pynative.iterator.record_iterator import RecordIterator
from pypaimon.pynative.reader.record_reader import RecordReader
from pypaimon.pynative.record_reader_wrapper import convert_java_reader


class KeyValueUnwrapReader(RecordReader):
    """
    一个RecordReader，它将KeyValue类型的记录读取器转换为InternalRow类型的读取器
    """

    def __init__(self, java_reader: JavaObject):
        reader_class = java_reader.getClass()

        sub_reader_field = reader_class.getDeclaredField("val$reader")
        sub_reader_field.setAccessible(True)
        java_sub_reader = sub_reader_field.get(java_reader)
        self.reader = convert_java_reader(java_sub_reader)

    # def __init__(self, reader: RecordReader):
    #     self.reader = reader

    def read_batch(self) -> Optional[RecordIterator]:
        """读取一个批次的数据"""
        batch = self.reader.read_batch()
        if batch is None:
            return None

        return KeyValueUnwrapIterator(batch)

    def close(self) -> None:
        """关闭读取器"""
        self.reader.close()
