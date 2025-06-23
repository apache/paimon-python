import pyarrow as pa
from typing import Tuple

from pypaimon.pynative.writer.file_store_write import FileStoreWrite
from pypaimon.pynative.writer.append_only_data_writer import AppendOnlyDataWriter
from pypaimon.pynative.writer.data_writer import DataWriter


class AppendOnlyFileStoreWrite(FileStoreWrite):
    """FileStoreWrite implementation for append-only tables."""

    def write(self, partition: Tuple, bucket: int, data: pa.Table):
        """Write data to append-only table."""
        writer = self._get_data_writer(partition, bucket)
        writer.write(data)

    def _create_data_writer(self, partition: Tuple, bucket: int) -> DataWriter:
        """Create a new data writer for append-only tables."""
        return AppendOnlyDataWriter(
            partition=partition,
            bucket=bucket,
            file_io=self.table.file_io,
            table_schema=self.table.table_schema,
            table_identifier=self.table.table_identifier,
            target_file_size=self.target_file_size
        )
