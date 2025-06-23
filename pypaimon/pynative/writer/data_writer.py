import pyarrow as pa
import uuid
from typing import Tuple, Optional, List, Dict
from pathlib import Path
from abc import ABC, abstractmethod

from pypaimon.pynative.table.core_option import CoreOptions


class DataWriter(ABC):
    """Base class for data writers that handle PyArrow tables directly."""

    def __init__(self, partition: Tuple, bucket: int, file_io, table_schema, table_identifier, target_file_size: int, options: Dict[str, str]):
        self.partition = partition
        self.bucket = bucket
        self.file_io = file_io
        self.table_schema = table_schema
        self.table_identifier = table_identifier
        self.target_file_size = target_file_size
        self.file_format = options.get(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET)
        self.compression = options.get(CoreOptions.FILE_COMPRESSION, "zstd")
        extensions = {
            'parquet': '.parquet',
            'orc': '.orc',
            'avro': '.avro'
        }
        self.extension = extensions.get(self.file_format)
        self.pending_data: Optional[pa.Table] = None
        self.committed_files = []

    def write(self, data: pa.Table):
        """Write data with smart file rolling strategy."""
        # Process data (subclass-specific logic)
        processed_data = self._process_data(data)
        
        # Concatenate with existing data
        if self.pending_data is None:
            self.pending_data = processed_data
        else:
            self.pending_data = self._merge_data(self.pending_data, processed_data)

        # Check if we need to roll files (post-check strategy)
        self._check_and_roll_if_needed()

    @abstractmethod
    def _process_data(self, data: pa.Table) -> pa.Table:
        """Process incoming data (e.g., add system fields, sort). Must be implemented by subclasses."""
        pass

    @abstractmethod
    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        """Merge existing data with new data. Must be implemented by subclasses."""
        pass

    def _check_and_roll_if_needed(self):
        """Check if current data exceeds threshold and roll files smartly."""
        if self.pending_data is None:
            return

        current_size = self.pending_data.get_total_buffer_size()

        # If size exceeds threshold, find optimal split point
        if current_size > self.target_file_size:
            split_row = self._find_optimal_split_point(self.pending_data, self.target_file_size)

            if split_row > 0:
                # Split the data
                data_to_write = self.pending_data.slice(0, split_row)
                remaining_data = self.pending_data.slice(split_row)

                # Write the first part
                self._write_data_to_file(data_to_write)

                # Keep the remaining part
                self.pending_data = remaining_data

                # Check if remaining data still needs rolling (recursive)
                self._check_and_roll_if_needed()

    def _find_optimal_split_point(self, data: pa.Table, target_size: int) -> int:
        """Find the optimal row to split at, maximizing file utilization."""
        total_rows = data.num_rows
        if total_rows <= 1:
            return 0

        # Binary search for optimal split point
        left, right = 1, total_rows
        best_split = 0

        while left <= right:
            mid = (left + right) // 2
            slice_data = data.slice(0, mid)
            slice_size = slice_data.get_total_buffer_size()

            if slice_size <= target_size:
                best_split = mid
                left = mid + 1
            else:
                right = mid - 1

        return best_split

    def _write_data_to_file(self, data: pa.Table):
        """Write data to a parquet file."""
        if data.num_rows == 0:
            return

        file_path = self._generate_file_path()
        try:
            if self.file_format == CoreOptions.FILE_FORMAT_PARQUET:
                self.file_io.write_parquet(file_path, data, compression=self.compression)
            elif self.file_format == CoreOptions.FILE_FORMAT_ORC:
                self.file_io.write_orc(file_path, data, compression=self.compression)
            elif self.file_format == CoreOptions.FILE_FORMAT_AVRO:
                self.file_io.write_avro(file_path, data, compression=self.compression)
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")

            self.committed_files.append(str(file_path))

        except Exception as e:
            raise RuntimeError(f"Failed to write {self.file_format} file {file_path}: {e}") from e


    @abstractmethod
    def _generate_file_path(self) -> Path:
        """Generate unique file path for the data. Must be implemented by subclasses."""
        pass

    def prepare_commit(self) -> List[str]:
        """Flush any remaining data and return all committed file paths."""
        # Write any remaining pending data
        if self.pending_data is not None and self.pending_data.num_rows > 0:
            self._write_data_to_file(self.pending_data)
            self.pending_data = None

        return self.committed_files.copy()

    def close(self):
        """Close the writer and clean up resources."""
        self.pending_data = None
        self.committed_files.clear()
