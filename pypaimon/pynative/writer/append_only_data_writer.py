import pyarrow as pa
import uuid
from pathlib import Path
from typing import Dict

from pypaimon.pynative.writer.data_writer import DataWriter


class AppendOnlyDataWriter(DataWriter):
    """Data writer for append-only tables."""

    def __init__(self, partition, bucket, file_io, table_schema, table_identifier, 
                 target_file_size: int, options: Dict[str, str], data_file_prefix: str = 'data'):
        super().__init__(partition, bucket, file_io, table_schema, table_identifier, 
                         target_file_size, options)
        self.data_file_prefix = data_file_prefix

    def _process_data(self, data: pa.Table) -> pa.Table:
        """For append-only tables, no processing needed - return data as-is."""
        return data

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        """Simple concatenation for append-only tables."""
        return pa.concat_tables([existing_data, new_data])

    def _generate_file_path(self) -> Path:
        """Generate unique file path for append-only data."""
        # Use the configured file format extension
        file_name = f"{self.data_file_prefix}-{uuid.uuid4()}{self.extension}"

        if self.partition:
            # Create partition path like: table_path/partition_key1=value1/partition_key2=value2/
            partition_path_parts = []
            partition_keys = self.table_schema.partition_keys
            for i, value in enumerate(self.partition):
                if i < len(partition_keys):
                    partition_path_parts.append(f"{partition_keys[i]}={value}")
            partition_path = "/".join(partition_path_parts)
            
            # Add bucket directory if bucket is specified
            if self.bucket is not None:
                return Path(str(self.table_identifier)) / partition_path / f"bucket-{self.bucket}" / file_name
            else:
                return Path(str(self.table_identifier)) / partition_path / file_name
        else:
            # Add bucket directory if bucket is specified
            if self.bucket is not None:
                return Path(str(self.table_identifier)) / f"bucket-{self.bucket}" / file_name
            else:
                return Path(str(self.table_identifier)) / file_name 