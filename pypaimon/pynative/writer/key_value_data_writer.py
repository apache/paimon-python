import pyarrow as pa
import pyarrow.compute as pc
import uuid
from typing import Tuple, Optional, List, Dict
from pathlib import Path

from pypaimon.pynative.writer.data_writer import DataWriter
from pypaimon.pynative.table.core_option import CoreOptions


class KeyValueDataWriter(DataWriter):
    """Data writer for primary key tables with system fields and sorting."""

    def __init__(self, partition: Tuple, bucket: int, file_io, table_schema, table_identifier, 
                 target_file_size: int, options: Dict[str, str], sequence_generator, 
                 data_file_prefix: str = 'data'):
        super().__init__(partition, bucket, file_io, table_schema, table_identifier, 
                         target_file_size, options)
        self.sequence_generator = sequence_generator
        self.row_kind_field = table_schema.options.get(CoreOptions.ROWKIND_FIELD, None)
        self.data_file_prefix = data_file_prefix

    def _process_data(self, data: pa.Table) -> pa.Table:
        """Add system fields and sort by primary key."""
        # Add system fields
        enhanced_data = self._add_system_fields(data)
        
        # Sort by primary key
        return self._sort_by_primary_key(enhanced_data)

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        """Concatenate and re-sort for primary key tables."""
        combined = pa.concat_tables([existing_data, new_data])
        return self._sort_by_primary_key(combined)

    def _generate_file_path(self) -> Path:
        """Generate unique file path for primary key data."""
        # Use the configured file format extension
        file_name = f"{self.data_file_prefix}-{uuid.uuid4()}{self.extension}"

        # Build file path based on partition and bucket
        if self.partition:
            # Create partition path
            partition_path_parts = []
            partition_keys = self.table_schema.partition_keys
            for i, value in enumerate(self.partition):
                if i < len(partition_keys):
                    partition_path_parts.append(f"{partition_keys[i]}={value}")
            partition_path = "/".join(partition_path_parts)
            return Path(str(self.table_identifier)) / partition_path / f"bucket-{self.bucket}" / file_name
        else:
            return Path(str(self.table_identifier)) / f"bucket-{self.bucket}" / file_name

    def _add_system_fields(self, data: pa.Table) -> pa.Table:
        """Add system fields: _KEY_id, _SEQUENCE_NUMBER, _VALUE_KIND."""
        num_rows = data.num_rows

        # Generate sequence numbers
        sequence_numbers = [self.sequence_generator.next() for _ in range(num_rows)]

        # Handle _VALUE_KIND field
        if self.row_kind_field:
            # User defined rowkind field
            if self.row_kind_field in data.column_names:
                # Use existing rowkind field
                value_kind_column = data.column(self.row_kind_field)
            else:
                # Defined but not exists - throw exception
                raise ValueError(f"Rowkind field '{self.row_kind_field}' is configured but not found in data columns: {data.column_names}")
        else:
            # No rowkind field defined - default to INSERT for all rows
            value_kind_column = pa.array(['INSERT'] * num_rows, type=pa.string())

        # Generate key IDs (simplified - in practice this would be more complex)
        key_ids = list(range(num_rows))

        # Create system columns
        key_id_column = pa.array(key_ids, type=pa.int64())
        sequence_column = pa.array(sequence_numbers, type=pa.int64())

        # Add system columns to the table
        enhanced_table = data.add_column(0, '_KEY_id', key_id_column)
        enhanced_table = enhanced_table.add_column(1, '_SEQUENCE_NUMBER', sequence_column)
        enhanced_table = enhanced_table.add_column(2, '_VALUE_KIND', value_kind_column)

        return enhanced_table

    def _sort_by_primary_key(self, data: pa.Table) -> pa.Table:
        """Sort data by primary key fields."""
        if not self.table_schema.primary_keys:
            return data

        # Build sort keys - primary key fields first, then sequence number
        sort_keys = []

        # Add primary key fields
        for pk_field in self.table_schema.primary_keys:
            if pk_field in data.column_names:
                sort_keys.append((pk_field, "ascending"))

        # Add sequence number for stable sort
        if '_SEQUENCE_NUMBER' in data.column_names:
            sort_keys.append(('_SEQUENCE_NUMBER', "ascending"))

        if not sort_keys:
            return data

        # Sort the table
        return pc.sort_indices(data, sort_keys=sort_keys)
