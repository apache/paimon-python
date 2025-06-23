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

from abc import ABC, abstractmethod
from typing import TypeVar

from pypaimon.pynative.reader.common.record_iterator import RecordIterator

T = TypeVar('T')


class FileRecordIterator(RecordIterator[T], ABC):
    """
    A RecordIterator to support returning the record's row position and file Path.
    """

    @abstractmethod
    def returned_position(self) -> int:
        """
        Get the row position of the row returned by next().
        Returns: the row position from 0 to the number of rows in the file
        """

    @abstractmethod
    def file_path(self) -> str:
        """
        Returns: the file path
        """
