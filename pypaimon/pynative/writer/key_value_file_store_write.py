import pyarrow as pa
from typing import Tuple

from pypaimon.pynative.writer.file_store_write import FileStoreWrite
from pypaimon.pynative.writer.key_value_data_writer import KeyValueDataWriter
from pypaimon.pynative.writer.data_writer import DataWriter
from pypaimon.pynative.table.file_store_table import FileStoreTable
from pypaimon.pynative.util.sequence_generator import SequenceGenerator


class KeyValueFileStoreWrite(FileStoreWrite):

    def write(self, partition: Tuple, bucket: int, data: pa.Table):
        """Write data to primary key table."""
        writer = self._get_data_writer(partition, bucket)
        writer.write(data)

    def _create_data_writer(self, partition: Tuple, bucket: int) -> DataWriter:
        """Create a new data writer for primary key tables."""
        return KeyValueDataWriter(
            partition=partition,
            bucket=bucket,
            file_io=self.table.file_io,
            table_schema=self.table.table_schema,
            table_identifier=self.table.table_identifier,
            target_file_size=self.target_file_size,
            options=self.table.options
        )