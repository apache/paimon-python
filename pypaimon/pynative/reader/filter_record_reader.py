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

from typing import Optional, TypeVar

from pypaimon.api import Predicate
from pypaimon.pynative.reader.common.record_reader import RecordIterator, RecordReader

T = TypeVar('T')


class FilterRecordReader(RecordReader[T]):
    """
    A RecordReader that implements filtering functionality.
    """

    def __init__(self, reader: RecordReader[T], predicate: Predicate):
        self.reader = reader
        self.predicate = predicate

    def read_batch(self) -> Optional[RecordIterator[T]]:
        iterator = self.reader.read_batch()
        if iterator is None:
            return None
        return FilterRecordIterator(iterator, self.predicate)

    def close(self) -> None:
        self.reader.close()


class FilterRecordIterator(RecordIterator[T]):
    """
    A RecordIterator that implements filtering functionality.
    """

    def __init__(self, iterator: RecordIterator[T], predicate: Predicate):
        self.iterator = iterator
        self.predicate = predicate

    def next(self) -> Optional[T]:
        while True:
            record = self.iterator.next()
            if record is None:
                return None
            if self.predicate.test(record):
                return record

    def release_batch(self) -> None:
        self.iterator.release_batch()
