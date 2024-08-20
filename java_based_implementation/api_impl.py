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

from java_based_implementation.java_gateway import get_gateway
from java_based_implementation.util.java_utils import to_j_catalog_context
from paimon_python_api import (catalog, table, read_builder, table_scan, split, table_read,
                               write_builder, table_write, commit_message, table_commit)
from pyarrow import (RecordBatch, BufferOutputStream, RecordBatchStreamWriter,
                     RecordBatchStreamReader, BufferReader)
from typing import List


class Catalog(catalog.Catalog):

    def __init__(self, j_catalog):
        self._j_catalog = j_catalog

    @staticmethod
    def create(catalog_context: dict) -> 'Catalog':
        j_catalog_context = to_j_catalog_context(catalog_context)
        gateway = get_gateway()
        j_catalog = gateway.jvm.CatalogFactory.createCatalog(j_catalog_context)
        return Catalog(j_catalog)

    def get_table(self, identifier: str) -> 'Table':
        gateway = get_gateway()
        j_identifier = gateway.jvm.Identifier.fromString(identifier)
        j_table = self._j_catalog.getTable(j_identifier)
        return Table(j_table)


class Table(table.Table):

    def __init__(self, j_table):
        self._j_table = j_table

    def new_read_builder(self) -> 'ReadBuilder':
        j_read_builder = get_gateway().jvm.InvocationUtil.getReadBuilder(self._j_table)
        return ReadBuilder(j_read_builder, self._j_table.rowType())

    def new_batch_write_builder(self) -> 'BatchWriteBuilder':
        j_batch_write_builder = get_gateway().jvm.InvocationUtil.getBatchWriteBuilder(self._j_table)
        return BatchWriteBuilder(j_batch_write_builder, self._j_table.rowType())


class ReadBuilder(read_builder.ReadBuilder):

    def __init__(self, j_read_builder, j_row_type):
        self._j_read_builder = j_read_builder
        self._j_row_type = j_row_type

    def with_projection(self, projection: List[List[int]]) -> 'ReadBuilder':
        self._j_read_builder.withProjection(projection)
        return self

    def with_limit(self, limit: int) -> 'ReadBuilder':
        self._j_read_builder.withLimit(limit)
        return self

    def new_scan(self) -> 'TableScan':
        j_table_scan = self._j_read_builder.newScan()
        return TableScan(j_table_scan)

    def new_read(self) -> 'TableRead':
        j_table_read = self._j_read_builder.newRead()
        return TableRead(j_table_read, self._j_row_type)


class TableScan(table_scan.TableScan):

    def __init__(self, j_table_scan):
        self._j_table_scan = j_table_scan

    def plan(self) -> 'Plan':
        j_plan = self._j_table_scan.plan()
        j_splits = j_plan.splits()
        return Plan(j_splits)


class Plan(table_scan.Plan):

    def __init__(self, j_splits):
        self._j_splits = j_splits

    def splits(self) -> List['Split']:
        return list(map(lambda s: Split(s), self._j_splits))


class Split(split.Split):

    def __init__(self, j_split):
        self._j_split = j_split

    def to_j_split(self):
        return self._j_split


class TableRead(table_read.TableRead):

    def __init__(self, j_table_read, j_row_type):
        self._j_table_read = j_table_read
        self._j_bytes_reader = get_gateway().jvm.InvocationUtil.createBytesReader(
            j_table_read, j_row_type)

    def create_reader(self, split: Split):
        self._j_bytes_reader.setSplit(split.to_j_split())
        return BatchReader(self._j_bytes_reader)

    def close(self):
        self._j_bytes_reader.close()


class BatchReader(table_read.BatchReader):

    def __init__(self, j_bytes_reader):
        self._j_bytes_reader = j_bytes_reader
        self._inited = False
        self._has_next = True
        self._next_arrow_reader()

    def next_batch(self):
        if not self._has_next:
            return None

        try:
            return self._current_arrow_reader.read_next_batch()
        except StopIteration:
            self._current_arrow_reader.close()
            self._next_arrow_reader()
            if not self._has_next:
                return None
            else:
                return self._current_arrow_reader.read_next_batch()

    def _next_arrow_reader(self):
        byte_array = self._j_bytes_reader.next()
        if byte_array is None:
            self._has_next = False
        else:
            self._current_arrow_reader = RecordBatchStreamReader(BufferReader(byte_array))


class BatchWriteBuilder(write_builder.BatchWriteBuilder):

    def __init__(self, j_batch_write_builder, j_row_type):
        self._j_batch_write_builder = j_batch_write_builder
        self._j_row_type = j_row_type

    def with_overwrite(self, static_partition: dict) -> 'BatchWriteBuilder':
        self._j_batch_write_builder.withOverwrite(static_partition)
        return self

    def new_write(self) -> 'BatchTableWrite':
        j_batch_table_write = self._j_batch_write_builder.newWrite()
        return BatchTableWrite(j_batch_table_write, self._j_row_type)

    def new_commit(self) -> 'BatchTableCommit':
        j_batch_table_commit = self._j_batch_write_builder.newCommit()
        return BatchTableCommit(j_batch_table_commit)


class BatchTableWrite(table_write.BatchTableWrite):

    def __init__(self, j_batch_table_write, j_row_type):
        self._j_batch_table_write = j_batch_table_write
        self._j_bytes_writer = get_gateway().jvm.InvocationUtil.createBytesWriter(
            j_batch_table_write, j_row_type)

    def write(self, record_batch: RecordBatch):
        stream = BufferOutputStream()
        with RecordBatchStreamWriter(stream, record_batch.schema) as writer:
            writer.write(record_batch)
            writer.close()
        arrow_bytes = stream.getvalue().to_pybytes()
        self._j_bytes_writer.write(arrow_bytes)

    def prepare_commit(self) -> List['CommitMessage']:
        j_commit_messages = self._j_batch_table_write.prepareCommit()
        return list(map(lambda cm: CommitMessage(cm), j_commit_messages))

    def close(self):
        self._j_batch_table_write.close()
        self._j_bytes_writer.close()


class CommitMessage(commit_message.CommitMessage):

    def __init__(self, j_commit_message):
        self._j_commit_message = j_commit_message

    def to_j_commit_message(self):
        return self._j_commit_message


class BatchTableCommit(table_commit.BatchTableCommit):

    def __init__(self, j_batch_table_commit):
        self._j_batch_table_commit = j_batch_table_commit

    def commit(self, commit_messages: List[CommitMessage]):
        j_commit_messages = list(map(lambda cm: cm.to_j_commit_message(), commit_messages))
        self._j_batch_table_commit.commit(j_commit_messages)

    def close(self):
        self._j_batch_table_commit.close()
