################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from typing import Optional, List, Any
import pyarrow as pa

from pypaimon.pynative.common.exception import PyNativeNotImplementedError
from pypaimon.pynative.common.row.internal_row import InternalRow
from pypaimon.pynative.reader.core.file_record_iterator import FileRecordIterator
from pypaimon.pynative.reader.core.file_record_reader import FileRecordReader
from pypaimon.pynative.reader.core.record_reader import RecordReader
from pypaimon.pynative.reader.core.columnar_row_iterator import ColumnarRowIterator


class PartitionInfo:
    """
    Partition information about how the row mapping of outer row.
    """

    def __init__(self, mapping: List[int], partition_values: List[Any]):
        self.mapping = mapping  # Mapping array similar to Java version
        self.partition_values = partition_values  # Partition values to be injected

    def size(self) -> int:
        return len(self.mapping) - 1

    def in_partition_row(self, pos: int) -> bool:
        return self.mapping[pos] < 0

    def get_real_index(self, pos: int) -> int:
        return abs(self.mapping[pos]) - 1

    def get_partition_value(self, pos: int) -> Any:
        real_index = self.get_real_index(pos)
        return self.partition_values[real_index] if real_index < len(self.partition_values) else None


class MappedColumnarRowIterator(ColumnarRowIterator):
    """
    ColumnarRowIterator with mapping support for partition and index mapping.
    """

    def __init__(self, file_path: str, record_batch: pa.RecordBatch,
                 partition_info: Optional[PartitionInfo] = None,
                 index_mapping: Optional[List[int]] = None):
        mapped_batch = self._apply_mappings(record_batch, partition_info, index_mapping)
        super().__init__(file_path, mapped_batch)

    def _apply_mappings(self, record_batch: pa.RecordBatch,
                        partition_info: Optional[PartitionInfo],
                        index_mapping: Optional[List[int]]) -> pa.RecordBatch:
        arrays = []
        names = []

        if partition_info is not None:
            for i in range(partition_info.size()):
                if partition_info.in_partition_row(i):
                    partition_value = partition_info.get_partition_value(i)
                    const_array = pa.array([partition_value] * record_batch.num_rows)
                    arrays.append(const_array)
                    names.append(f"partition_field_{i}")
                else:
                    real_index = partition_info.get_real_index(i)
                    if real_index < record_batch.num_columns:
                        arrays.append(record_batch.column(real_index))
                        names.append(record_batch.column_names[real_index])
        else:
            arrays = [record_batch.column(i) for i in range(record_batch.num_columns)]
            names = record_batch.column_names[:]

        if index_mapping is not None:
            mapped_arrays = []
            mapped_names = []
            for i, real_index in enumerate(index_mapping):
                if real_index >= 0 and real_index < len(arrays):
                    mapped_arrays.append(arrays[real_index])
                    mapped_names.append(names[real_index] if real_index < len(names) else f"field_{i}")
                else:
                    null_array = pa.array([None] * record_batch.num_rows)
                    mapped_arrays.append(null_array)
                    mapped_names.append(f"null_field_{i}")
            arrays = mapped_arrays
            names = mapped_names

        final_batch = pa.RecordBatch.from_arrays(arrays, names=names)
        return final_batch


class DataFileRecordReader(FileRecordReader[InternalRow]):
    """
    Reads InternalRow from data files.
    """

    def __init__(self, wrapped_reader: RecordReader,
                 index_mapping: Optional[List[int]] = None,
                 partition_info: Optional[PartitionInfo] = None):
        self.wrapped_reader = wrapped_reader
        self.index_mapping = index_mapping
        self.partition_info = partition_info

    def read_batch(self) -> Optional[FileRecordIterator['InternalRow']]:
        iterator = self.wrapped_reader.read_batch()
        if iterator is None:
            return None

        if isinstance(iterator, ColumnarRowIterator):
            if self.partition_info is not None or self.index_mapping is not None:
                iterator = MappedColumnarRowIterator(
                    iterator.file_path,
                    iterator.record_batch,
                    self.partition_info,
                    self.index_mapping
                )
        else:
            raise PyNativeNotImplementedError("partition_info & index_mapping for non ColumnarRowIterator")

        return iterator

    def close(self) -> None:
        self.wrapped_reader.close()
