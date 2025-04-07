from typing import Optional

from py4j.java_gateway import JavaObject

from pypaimon.pynative.iterator.record_iterator import RecordIterator
from pypaimon.pynative.iterator.transform_record_iterator import TransformRecordIterator
from pypaimon.pynative.reader.record_reader import RecordReader
from pypaimon.pynative.record_reader_wrapper import convert_java_reader


class TransformRecordReader(RecordReader):
    """转换记录的读取器实现"""

    def __init__(self, java_reader: JavaObject):
        reader_class = java_reader.getClass()

        sub_reader_field = reader_class.getDeclaredField("val$thisReader")
        sub_reader_field.setAccessible(True)
        java_sub_reader = sub_reader_field.get(java_reader)
        self.reader = convert_java_reader(java_sub_reader)

        function_field = reader_class.getDeclaredField("val$function")
        function_field.setAccessible(True)
        java_func = function_field.get(java_reader).toString()
        self.func = java_func

    def read_batch(self) -> Optional[RecordIterator]:
        iterator = self.reader.read_batch()
        if iterator is None:
            return None
        return TransformRecordIterator(iterator, self.func)

    def close(self):
        self.reader.close()
