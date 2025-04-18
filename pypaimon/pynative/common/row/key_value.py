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

from dataclasses import dataclass

from pypaimon.pynative.common.row.internal_row import InternalRow
from pypaimon.pynative.common.row.row_kind import RowKind

"""
A key value, including user key, sequence number, value kind and value.
"""


@dataclass
class KeyValue:
    key: InternalRow
    sequence_number: int
    value_kind: RowKind
    value: InternalRow
    level: int = -1

    def set_level(self, level: int) -> 'KeyValue':
        self.level = level
        return self

    def is_add(self) -> bool:
        return self.value_kind.is_add()
