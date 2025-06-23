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

import heapq
from typing import Any, Callable, List, Optional

import pyarrow as pa

from pypaimon.pynative.reader.row.key_value import KeyValue
from pypaimon.pynative.reader.row.row_kind import RowKind
from pypaimon.pynative.reader.common.record_iterator import RecordIterator
from pypaimon.pynative.reader.common.record_reader import RecordReader


def built_comparator(key_schema: pa.Schema) -> Callable[[Any, Any], int]:
    def comparator(key1, key2) -> int:
        if key1 is None and key2 is None:
            return 0
        if key1 is None:
            return -1
        if key2 is None:
            return 1

        for i, field in enumerate(key_schema):
            field_type = field.type
            val1 = key1.get_field(i)
            val2 = key2.get_field(i)

            if val1 is None and val2 is None:
                continue
            if val1 is None:
                return -1
            if val2 is None:
                return 1

            if (pa.types.is_integer(field_type) or pa.types.is_floating(field_type)
                    or pa.types.is_boolean(field_type)):
                if val1 < val2:
                    return -1
                elif val1 > val2:
                    return 1
            elif pa.types.is_string(field_type) or pa.types.is_binary(field_type):
                if val1 < val2:
                    return -1
                elif val1 > val2:
                    return 1
            elif pa.types.is_timestamp(field_type) or pa.types.is_date(field_type):
                if val1 < val2:
                    return -1
                elif val1 > val2:
                    return 1
            else:
                str_val1 = str(val1)
                str_val2 = str(val2)
                if str_val1 < str_val2:
                    return -1
                elif str_val1 > str_val2:
                    return 1
        return 0
    return comparator


class DeduplicateMergeFunction:
    def __init__(self, ignore_delete: bool = False):
        self.ignore_delete = ignore_delete
        self.latest_kv = None
        self.is_initialized = False
        self.initial_kv = None

    def reset(self) -> None:
        self.latest_kv = None
        self.is_initialized = False
        self.initial_kv = None

    def add(self, kv: KeyValue) -> None:
        if self.initial_kv is None:
            self.initial_kv = kv
            return

        if not self.is_initialized:
            if not self.ignore_delete or not self.initial_kv.value_kind == RowKind.DELETE:
                self.latest_kv = self.initial_kv
            self.is_initialized = True

        if self.ignore_delete and kv.value_kind == RowKind.DELETE:
            return

        self.latest_kv = kv

    def get_result(self) -> Optional[KeyValue]:
        if not self.is_initialized:
            return self.initial_kv
        return self.latest_kv


class Element:
    def __init__(self, kv, iterator: RecordIterator, reader: RecordReader):
        self.kv = kv
        self.iterator = iterator
        self.reader = reader

    def update(self) -> bool:
        next_kv = self.iterator.next()
        if next_kv is None:
            return False
        self.kv = next_kv
        return True


class HeapEntry:
    def __init__(self, key, element: Element, key_comparator):
        self.key = key
        self.element = element
        self.key_comparator = key_comparator

    def __lt__(self, other):
        result = self.key_comparator(self.key, other.key)
        if result < 0:
            return True
        elif result > 0:
            return False

        return self.element.kv.sequence_number < other.element.kv.sequence_number


class SortMergeIterator(RecordIterator):
    def __init__(self, reader, polled: List[Element], min_heap, merge_function,
                 user_key_comparator, next_batch_readers):
        self.reader = reader
        self.polled = polled
        self.min_heap = min_heap
        self.merge_function = merge_function
        self.user_key_comparator = user_key_comparator
        self.next_batch_readers = next_batch_readers
        self.released = False

    def next(self):
        while True:
            has_more = self._next_impl()
            if not has_more:
                return None
            result = self.merge_function.get_result()
            if result is not None:
                return result

    def _next_impl(self):
        if self.released:
            raise RuntimeError("SortMergeIterator.next called after release")

        if not self.next_batch_readers:
            for element in self.polled:
                if element.update():
                    entry = HeapEntry(element.kv.key, element, self.user_key_comparator)
                    heapq.heappush(self.min_heap, entry)
                else:
                    element.iterator.release_batch()
                    self.next_batch_readers.append(element.reader)

            self.polled.clear()

            if self.next_batch_readers:
                return False

            if not self.min_heap:
                return False

            self.merge_function.reset()

            first_entry = self.min_heap[0]
            key = first_entry.key

            while self.min_heap and self.user_key_comparator(key, self.min_heap[0].key) == 0:
                entry = heapq.heappop(self.min_heap)
                self.merge_function.add(entry.element.kv)
                self.polled.append(entry.element)

            return True

    def release_batch(self):
        self.released = True


class SortMergeReader:
    def __init__(self, readers, primary_keys):
        self.next_batch_readers = list(readers)
        self.merge_function = DeduplicateMergeFunction(False)

        key_columns = [f"_KEY_{pk}" for pk in primary_keys]
        key_schema = pa.schema([pa.field(column, pa.string()) for column in key_columns])
        self.user_key_comparator = built_comparator(key_schema)

        def element_comparator(e1_tuple, e2_tuple):
            key1, e1 = e1_tuple
            key2, e2 = e2_tuple

            result = self.user_key_comparator(key1, key2)
            if result != 0:
                return result

            return e1.kv.sequence_number - e2.kv.sequence_number

        from functools import cmp_to_key
        self.element_key = cmp_to_key(element_comparator)

        self.min_heap = []
        self.polled = []

    def read_batch(self) -> Optional[RecordIterator]:
        for reader in self.next_batch_readers:
            while True:
                iterator = reader.read_batch()
                if iterator is None:
                    reader.close()
                    break

                kv = iterator.next()
                if kv is None:
                    iterator.release_batch()
                else:
                    element = Element(kv, iterator, reader)
                    entry = HeapEntry(kv.key, element, self.user_key_comparator)
                    heapq.heappush(self.min_heap, entry)
                    break

        self.next_batch_readers.clear()

        if not self.min_heap:
            return None

        return SortMergeIterator(
            self,
            self.polled,
            self.min_heap,
            self.merge_function,
            self.user_key_comparator,
            self.next_batch_readers
        )

    def close(self):
        for reader in self.next_batch_readers:
            reader.close()

        for entry in self.min_heap:
            entry.element.iterator.release_batch()
            entry.element.reader.close()

        for element in self.polled:
            element.iterator.release_batch()
            element.reader.close()
