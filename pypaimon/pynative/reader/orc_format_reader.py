import pyarrow.dataset as ds
from typing import Optional
from py4j.java_gateway import JavaObject

from pypaimon.pynative.reader.file_record_reader import FileRecordReader
from pypaimon.pynative.row.key_value import InternalRow
from pypaimon.pynative.iterator.file_record_iterator import FileRecordIterator
from pypaimon.pynative.iterator.columnar_row_iterator import ColumnarRowIterator


class ParquetReader(FileRecordReader[InternalRow]):

    def __init__(self, java_reader: JavaObject):
        reader_class = java_reader.getClass()

        # resolve parent ParquetReaderFactory properties
        factory_field = reader_class.getDeclaredField("this$0")
        factory_field.setAccessible(True)
        java_factory = factory_field.get(java_reader)
        factory_class = java_factory.getClass()

        batch_size_field = factory_class.getDeclaredField("batchSize")
        batch_size_field.setAccessible(True)
        self._batch_size = batch_size_field.get(java_factory)

        projected_type_field = factory_class.getDeclaredField("projectedType")
        projected_type_field.setAccessible(True)
        java_projected_type = projected_type_field.get(java_factory)
        self._projected_type = None # convert(java_projected_type)

        filter_field = factory_class.getDeclaredField("filter")
        filter_field.setAccessible(True)
        java_filters = filter_field.get(java_factory)
        self._filters = None #convert(java_filters)

        # resolve sub ParquetFileReader properties
        file_reader_field = reader_class.getDeclaredField("reader")
        file_reader_field.setAccessible(True)
        java_file_reader = file_reader_field.get(java_reader)
        file_reader_class = java_file_reader.getClass()

        input_file_field = file_reader_class.getDeclaredField("file")
        input_file_field.setAccessible(True)
        java_input_file = input_file_field.get(java_file_reader)
        self._file_path = java_input_file.getPath().toUri().toString()

        # 创建dataset
        self.dataset = ds.dataset(self._file_path, format='parquet')

        # 创建扫描器，应用列裁剪和过滤
        self.scanner = self.dataset.scanner(
            columns=self._projected_type,
            filter=self._filters,
            batch_size=self._batch_size
        )

        # 创建批次迭代器
        self.batch_iterator = self.scanner.to_batches()

    def read_batch(self) -> Optional[FileRecordIterator[InternalRow]]:
        try:
            record_batch = next(self.batch_iterator, None)
            if record_batch is None:
                return None

            return ColumnarRowIterator(
                self._file_path,
                record_batch
            )
        except Exception as e:
            print(f"Error reading parquet batch: {e}")
            raise

    def close(self):
        pass
