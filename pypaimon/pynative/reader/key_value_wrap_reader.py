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

from pypaimon.pynative.common.row.internal_row import InternalRow
from pypaimon.pynative.common.row.key_value import KeyValue
from pypaimon.pynative.common.row.offset_row import OffsetRow
from pypaimon.pynative.common.row.row_kind import RowKind
from pypaimon.pynative.reader.core.file_record_iterator import FileRecordIterator
from pypaimon.pynative.reader.core.file_record_reader import FileRecordReader


class KeyValueWrapReader(FileRecordReader[KeyValue]):
    """
    RecordReader for reading KeyValue data files.
    Corresponds to the KeyValueDataFileRecordReader in Java version.
    """

    def __init__(self, wrapped_reader: FileRecordReader[InternalRow],
                 level, key_arity, value_arity):
        self.wrapped_reader = wrapped_reader
        self.level = level
        self.key_arity = key_arity
        self.value_arity = value_arity

    def read_batch(self) -> Optional[FileRecordIterator[KeyValue]]:
        iterator = self.wrapped_reader.read_batch()
        if iterator is None:
            return None
        return KeyValueWrapIterator(iterator, self.key_arity, self.value_arity, self.level)

    def close(self):
        self.wrapped_reader.close()


class KeyValueWrapIterator(FileRecordIterator[KeyValue]):
    """
    An Iterator that converts an PrimaryKey InternalRow into a KeyValue
    """

    def __init__(
            self,
            iterator: FileRecordIterator,
            key_arity: int,
            value_arity: int,
            level: int
    ):
        self.iterator = iterator
        self.key_arity = key_arity
        self.value_arity = value_arity
        self.level = level

        self.reused_key = OffsetRow(None, 0, key_arity)
        self.reused_value = OffsetRow(None, key_arity + 2, value_arity)

    def next(self) -> Optional[KeyValue]:
        row = self.iterator.next()
        if row is None:
            return None

        self.reused_key.replace(row)
        self.reused_value.replace(row)

        sequence_number = row.get_field(self.key_arity)
        value_kind = RowKind(row.get_field(self.key_arity + 1))

        return KeyValue(
            key=self.reused_key,
            sequence_number=sequence_number,
            value_kind=value_kind,
            value=self.reused_value
        ).set_level(self.level)

    def returned_position(self) -> int:
        return self.iterator.returned_position()

    def file_path(self) -> str:
        return self.iterator.file_path()

    def release_batch(self):
        self.iterator.release_batch()
