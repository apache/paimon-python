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

from typing import Any, Dict, List, Optional

import fastavro
import pyarrow as pa

from pypaimon.pynative.reader.row.internal_row import InternalRow
from pypaimon.pynative.reader.common.columnar_row_iterator import ColumnarRowIterator
from pypaimon.pynative.reader.common.file_record_iterator import FileRecordIterator
from pypaimon.pynative.reader.common.file_record_reader import FileRecordReader


class AvroFormatReader(FileRecordReader[InternalRow]):
    """
    A RecordReader implementation for reading Avro files using fastavro.
    The reader converts Avro records to pyarrow.RecordBatch format, which is compatible with
    the ColumnarRowIterator.
    """

    def __init__(self, file_path: str, batch_size: int, projected_type: Optional[List[str]] = None):
        self._file_path = file_path
        self._batch_size = batch_size
        self._projected_type = projected_type

        self._reader = fastavro.reader(open(file_path, 'rb'))
        self._schema = self._reader.schema
        self._current_batch: List[Dict[str, Any]] = []

    def read_batch(self) -> Optional[FileRecordIterator[InternalRow]]:
        try:
            self._current_batch = []
            for _ in range(self._batch_size):
                try:
                    record = next(self._reader)
                    self._current_batch.append(record)
                except StopIteration:
                    break

            if not self._current_batch:
                return None

            # TODO: Temporarily converting results to pyarrow RecordBatch, reusing its logic.
            # TODO: Custom adjustments will follow later.
            record_batch = self._convert_to_record_batch(self._current_batch)
            if record_batch is None:
                return None

            return ColumnarRowIterator(
                self._file_path,
                record_batch
            )
        except Exception as e:
            print(f"Error reading Avro batch: {e}")
            raise

    def _convert_to_record_batch(self, records: List[Dict[str, Any]]) -> pa.RecordBatch:
        if not records:
            return None

        if self._projected_type is not None:
            records = [{k: r[k] for k in self._projected_type} for r in records]

        return pa.RecordBatch.from_pylist(records)

    def close(self):
        pass
