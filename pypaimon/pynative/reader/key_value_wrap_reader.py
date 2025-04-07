from typing import Optional

from py4j.java_gateway import JavaObject

from pypaimon.pynative.iterator.file_record_iterator import FileRecordIterator
from pypaimon.pynative.iterator.key_value_wrap_iterator import KeyValueWrapIterator
from pypaimon.pynative.reader.file_record_reader import FileRecordReader
from pypaimon.pynative.record_reader_wrapper import convert_java_reader


class KeyValueWrapReader(FileRecordReader):

    def __init__(self, java_reader: JavaObject):
        reader_class = java_reader.getClass()

        sub_reader_field = reader_class.getDeclaredField("reader")
        sub_reader_field.setAccessible(True)
        java_sub_reader = sub_reader_field.get(java_reader)
        self.reader = convert_java_reader(java_sub_reader)

        level_field = reader_class.getDeclaredField("level")
        level_field.setAccessible(True)
        self.level = level_field.get(java_reader)

        serializer_field = reader_class.getDeclaredField("serializer")
        serializer_field.setAccessible(True)
        java_serializer = serializer_field.get(java_reader)
        serializer_class = java_serializer.getClass()
        key_arity_field = serializer_class.getDeclaredField("keyArity")
        key_arity_field.setAccessible(True)
        self.key_arity = key_arity_field.get(java_serializer)

        reused_value_field = serializer_class.getDeclaredField("reusedValue")
        reused_value_field.setAccessible(True)
        java_reused_value = reused_value_field.get(java_serializer)
        offset_row_class = java_reused_value.getClass()
        arity_field = offset_row_class.getDeclaredField("arity")
        arity_field.setAccessible(True)
        self.value_arity = arity_field.get(java_reused_value)

    def read_batch(self) -> Optional[FileRecordIterator]:
        iterator = self.reader.read_batch()
        if iterator is None:
            return None

        return KeyValueWrapIterator(iterator, self.key_arity, self.value_arity, self.level)

    def close(self):
        self.reader.close()
