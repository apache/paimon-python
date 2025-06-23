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

from typing import Optional

import pyarrow as pa

from pypaimon.pynative.reader.row.columnar_row import ColumnarRow
from pypaimon.pynative.reader.row.key_value import InternalRow
from pypaimon.pynative.reader.common.file_record_iterator import FileRecordIterator


class ColumnarRowIterator(FileRecordIterator[InternalRow]):
    """
    A RecordIterator that returns InternalRows. The next row is set by ColumnarRow.setRowId.
    """

    def __init__(self, file_path: str, record_batch: pa.RecordBatch):
        self.file_path = file_path
        self._record_batch = record_batch
        self._row = ColumnarRow(record_batch)

        self.num_rows = record_batch.num_rows
        self.next_pos = 0
        self.next_file_pos = 0

    def next(self) -> Optional[InternalRow]:
        if self.next_pos < self.num_rows:
            self._row.set_row_id(self.next_pos)
            self.next_pos += 1
            self.next_file_pos += 1
            return self._row
        return None

    def returned_position(self) -> int:
        return self.next_file_pos - 1

    def file_path(self) -> str:
        return self.file_path

    def reset(self, next_file_pos: int):
        self.next_pos = 0
        self.next_file_pos = next_file_pos

    def release_batch(self):
        del self._record_batch
