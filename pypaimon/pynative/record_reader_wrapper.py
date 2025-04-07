from py4j.java_gateway import JavaObject

from pypaimon.pynative.common.exception import PyNativeNotImplementedError
from pypaimon.pynative.reader.record_reader import RecordReader

def _create_concat_record_reader(java_reader):
    from pypaimon.pynative.reader.concat_record_reader import ConcatRecordReader
    return ConcatRecordReader(java_reader)

def _create_data_file_record_reader(java_reader):
    from pypaimon.pynative.reader.data_file_record_reader import DataFileRecordReader
    return DataFileRecordReader(java_reader)

def _create_parquet_reader(java_reader):
    from pypaimon.pynative.reader.parquet_format_reader import ParquetReader
    return ParquetReader(java_reader)

def _create_key_value_unwrap_reader(java_reader):
    from pypaimon.pynative.reader.key_value_unwrap_reader import KeyValueUnwrapReader
    return KeyValueUnwrapReader(java_reader)

def _create_transform_reader(java_reader):
    from pypaimon.pynative.reader.transform_record_reader import TransformRecordReader
    return TransformRecordReader(java_reader)

def _create_drop_delete_reader(java_reader):
    from pypaimon.pynative.reader.drop_delete_reader import DropDeleteReader
    return DropDeleteReader(java_reader)

def _create_sort_merge_reader(java_reader):
    from pypaimon.pynative.reader.sort_merge_reader import SortMergeReader
    return SortMergeReader(java_reader)

def _create_key_value_data_file_record_reader(java_reader):
    from pypaimon.pynative.reader.key_value_wrap_reader import KeyValueWrapReader
    return KeyValueWrapReader(java_reader)

reader_mapping = {
            "org.apache.paimon.mergetree.compact.ConcatRecordReader": _create_concat_record_reader,
            "org.apache.paimon.io.DataFileRecordReader": _create_data_file_record_reader,
            "org.apache.paimon.format.parquet.ParquetReaderFactory$ParquetReader": _create_parquet_reader,
            "org.apache.paimon.table.source.KeyValueTableRead$1": _create_key_value_unwrap_reader,
            "org.apache.paimon.reader.RecordReader$1": _create_transform_reader,
            "org.apache.paimon.mergetree.DropDeleteReader": _create_drop_delete_reader,
            "org.apache.paimon.mergetree.compact.SortMergeReaderWithMinHeap": _create_sort_merge_reader,
            "org.apache.paimon.mergetree.compact.SortMergeReaderWithLoserTree": _create_sort_merge_reader,
            "org.apache.paimon.io.KeyValueDataFileRecordReader": _create_key_value_data_file_record_reader,
            # 可以继续添加其他类型的映射
        }

def convert_java_reader(java_reader: JavaObject) -> RecordReader:
    """
    将Java RecordReader转换为Python RecordReader
    """
    java_class_name = java_reader.getClass().getName()
    if java_class_name in reader_mapping:
        return reader_mapping[java_class_name](java_reader)
    else:
        raise PyNativeNotImplementedError(f"Unsupported RecordReader type: {java_class_name}")
