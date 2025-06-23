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

from pypaimon.pynative.reader.row.internal_row import InternalRow
from pypaimon.pynative.reader.row.row_kind import RowKind


class OffsetRow(InternalRow):
    """
    A InternalRow to wrap row with offset.
    """

    def __init__(self, row: InternalRow, offset: int, arity: int):
        self.row = row
        self.offset = offset
        self.arity = arity

    def replace(self, row: InternalRow) -> 'OffsetRow':
        self.row = row
        return self

    def get_field(self, pos: int):
        if pos >= self.arity:
            raise IndexError(f"Position {pos} is out of bounds for arity {self.arity}")
        return self.row.get_field(pos + self.offset)

    def is_null_at(self, pos: int) -> bool:
        if pos >= self.arity:
            raise IndexError(f"Position {pos} is out of bounds for arity {self.arity}")
        return self.row.is_null_at(pos + self.offset)

    def set_field(self, pos: int, value: Any) -> None:
        raise NotImplementedError()

    def get_row_kind(self) -> RowKind:
        return self.row.get_row_kind()

    def set_row_kind(self, kind: RowKind) -> None:
        self.row.set_row_kind(kind)

    def __len__(self) -> int:
        return self.arity
