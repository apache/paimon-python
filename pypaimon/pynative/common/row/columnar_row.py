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

from typing import Any

import pyarrow as pa

from pypaimon.pynative.common.row.internal_row import InternalRow
from pypaimon.pynative.common.row.key_value import RowKind


class ColumnarRow(InternalRow):
    """
    Columnar row to support access to vector column data. It is a row based on PyArrow RecordBatch
    """

    def __init__(self, record_batch: pa.RecordBatch, row_id: int = 0,
                 row_kind: RowKind = RowKind.INSERT):
        self._batch = record_batch
        self._row_id = row_id
        self._row_kind = row_kind

    def get_row_id(self) -> int:
        return self._row_id

    def set_row_id(self, row_id: int) -> None:
        self._row_id = row_id

    def batch(self) -> pa.RecordBatch:
        return self._batch

    def get_field(self, pos: int) -> Any:
        return self._batch.column(pos)[self._row_id].as_py()

    def is_null_at(self, pos: int) -> bool:
        return self._batch.column(pos).is_null(self._row_id)

    def set_field(self, pos: int, value: Any) -> None:
        raise NotImplementedError()

    def get_row_kind(self) -> RowKind:
        return self._row_kind

    def set_row_kind(self, kind: RowKind) -> None:
        self._row_kind = kind

    def __len__(self) -> int:
        return self._batch.num_columns
