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

from typing import Any, Optional

from pypaimon.pynative.reader.row.internal_row import InternalRow
from pypaimon.pynative.reader.row.key_value import KeyValue
from pypaimon.pynative.reader.row.row_kind import RowKind
from pypaimon.pynative.reader.common.record_iterator import RecordIterator
from pypaimon.pynative.reader.common.record_reader import RecordReader


class KeyValueUnwrapReader(RecordReader[InternalRow]):
    """
    A RecordReader that converts a KeyValue type record reader into an InternalRow type reader
    Corresponds to the KeyValueTableRead$1 in Java version.
    """

    def __init__(self, wrapped_reader: RecordReader[KeyValue]):
        self.wrapped_reader = wrapped_reader

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        batch = self.wrapped_reader.read_batch()
        if batch is None:
            return None

        return KeyValueUnwrapIterator(batch)

    def close(self) -> None:
        self.wrapped_reader.close()


class KeyValueUnwrapIterator(RecordIterator[InternalRow]):
    """
    An Iterator that converts a KeyValue into an InternalRow
    """

    def __init__(self, batch: RecordIterator[KeyValue]):
        self.batch = batch
        self.kv: KeyValue = None
        self.pre_value_row_kind: RowKind = None

    def next(self) -> Optional[Any]:
        # The row_data is reused in iterator, we should set back to real kind
        if self.kv is not None:
            self.kv.value.set_row_kind(self.pre_value_row_kind)

        self.kv = self.batch.next()
        if self.kv is None:
            return None

        row_data = self.kv.value
        self.pre_value_row_kind = row_data.get_row_kind()

        row_data.set_row_kind(self.kv.value_kind)
        return row_data

    def release_batch(self) -> None:
        self.batch.release_batch()
