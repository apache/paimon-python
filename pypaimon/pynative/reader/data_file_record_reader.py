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
from pypaimon.pynative.reader.core.file_record_iterator import FileRecordIterator
from pypaimon.pynative.reader.core.file_record_reader import FileRecordReader
from pypaimon.pynative.reader.core.record_reader import RecordReader


class DataFileRecordReader(FileRecordReader[InternalRow]):
    """
    Reads InternalRow from data files.
    """

    def __init__(self, wrapped_reader: RecordReader):
        self.wrapped_reader = wrapped_reader

    def read_batch(self) -> Optional[FileRecordIterator['InternalRow']]:
        iterator = self.wrapped_reader.read_batch()
        if iterator is None:
            return None

        # TODO: Handle partition_info, index_mapping, and cast_mapping

        return iterator

    def close(self) -> None:
        self.wrapped_reader.close()
