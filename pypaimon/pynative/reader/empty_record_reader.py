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

from pypaimon.pynative.reader.core.file_record_reader import FileRecordReader
from pypaimon.pynative.reader.core.record_iterator import RecordIterator


class EmptyFileRecordReader(FileRecordReader):
    """
    An empty FileRecordReader.
    """

    def __init__(self):
        pass

    def read_batch(self) -> Optional[RecordIterator]:
        return None

    def close(self) -> None:
        pass
