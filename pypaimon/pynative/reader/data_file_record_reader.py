from typing import Optional

from py4j.java_gateway import JavaObject

from pypaimon.pynative.reader.file_record_reader import FileRecordReader
from pypaimon.pynative.row.key_value import InternalRow
from pypaimon.pynative.iterator.file_record_iterator import FileRecordIterator
from pypaimon.pynative.iterator.columnar_row_iterator import ColumnarRowIterator
from pypaimon.pynative.record_reader_wrapper import convert_java_reader


class DataFileRecordReader(FileRecordReader['InternalRow']):

    def __init__(self, java_reader: JavaObject):
        reader_class = java_reader.getClass()

        sub_reader_field = reader_class.getDeclaredField("reader")
        sub_reader_field.setAccessible(True)
        java_sub_reader = sub_reader_field.get(java_reader)
        self.reader = convert_java_reader(java_sub_reader)

        index_mapping_filed = reader_class.getDeclaredField("indexMapping")
        index_mapping_filed.setAccessible(True)
        self.index_mapping = index_mapping_filed.get(java_reader)

        self.partition_info = None
        self.cast_mapping = None

    def read_batch(self) -> Optional[FileRecordIterator['InternalRow']]:
        iterator = self.reader.read_batch()
        if iterator is None:
            return None

        # 处理列式存储的情况
        if isinstance(iterator, ColumnarRowIterator):
            iterator = iterator.mapping(self.partition_info, self.index_mapping)
        else:
            if self.partition_info is not None:
                partition_setted_row = PartitionSettedRow.from_(self.partition_info)
                iterator = iterator.transform(partition_setted_row.replace_row)

            if self.index_mapping is not None:
                projected_row = ProjectedRow.from_(self.index_mapping)
                iterator = iterator.transform(projected_row.replace_row)

        if self.cast_mapping is not None:
            raise Exception('unsupported type')
            # casted_row = CastedRow.from_(self.cast_mapping)
            # iterator = iterator.transform(casted_row.replace_row)

        return iterator

    def close(self) -> None:
        self.reader.close()
