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

from py4j.java_gateway import JavaObject

from pypaimon.pynative.reader.core.record_iterator import RecordIterator
from pypaimon.pynative.reader.core.record_reader import RecordReader


class ConcatRecordReader(RecordReader):
    """
    This reader is to concatenate a list of RecordReaders and read them sequentially.
    The input list is already sorted by key and sequence number, and the key intervals do not
    overlap each other.
    """

    def __init__(self, converter, j_supplier_queue: JavaObject):
        self.converter = converter
        self.j_supplier_queue = j_supplier_queue
        self.current: Optional[RecordReader] = None

    def read_batch(self) -> Optional[RecordIterator]:
        while True:
            if self.current is not None:
                iterator = self.current.read_batch()
                if iterator is not None:
                    return iterator
                self.current.close()
                self.current = None
            elif not self.j_supplier_queue.isEmpty():
                # If the Java supplier queue is not empty, initialize the reader by using py4j
                j_supplier = self.j_supplier_queue.poll()
                j_reader = j_supplier.get()
                self.current = self.converter.convert_java_reader(j_reader)
            else:
                return None

    def close(self) -> None:
        if self.current is not None:
            self.current.close()
