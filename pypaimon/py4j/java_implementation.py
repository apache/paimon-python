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

# pypaimon.api implementation based on Java code & py4j lib

import duckdb
import pandas as pd
import pyarrow as pa
import ray

from duckdb.duckdb import DuckDBPyConnection
from pypaimon.py4j.java_gateway import get_gateway
from pypaimon.py4j.util import java_utils, constants
from pypaimon.api import \
    (catalog, table, read_builder, table_scan, split,
     table_read, write_builder, table_write, commit_message,
     table_commit, Schema, predicate)
from typing import List, Iterator, Optional, Any


class Catalog(catalog.Catalog):

    def __init__(self, j_catalog, catalog_options: dict):
        self._j_catalog = j_catalog
        self._catalog_options = catalog_options

    @staticmethod
    def create(catalog_options: dict) -> 'Catalog':
        j_catalog_context = java_utils.to_j_catalog_context(catalog_options)
        gateway = get_gateway()
        j_catalog = gateway.jvm.CatalogFactory.createCatalog(j_catalog_context)
        return Catalog(j_catalog, catalog_options)

    def get_table(self, identifier: str) -> 'Table':
        j_identifier = java_utils.to_j_identifier(identifier)
        j_table = self._j_catalog.getTable(j_identifier)
        return Table(j_table, self._catalog_options)

    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        if properties is None:
            properties = {}
        self._j_catalog.createDatabase(name, ignore_if_exists, properties)

    def create_table(self, identifier: str, schema: Schema, ignore_if_exists: bool):
        j_identifier = java_utils.to_j_identifier(identifier)
        j_schema = java_utils.to_paimon_schema(schema)
        self._j_catalog.createTable(j_identifier, j_schema, ignore_if_exists)


class Table(table.Table):

    def __init__(self, j_table, catalog_options: dict):
        self._j_table = j_table
        self._catalog_options = catalog_options

    def new_read_builder(self) -> 'ReadBuilder':
        j_read_builder = get_gateway().jvm.InvocationUtil.getReadBuilder(self._j_table)
        return ReadBuilder(j_read_builder, self._j_table.rowType(), self._catalog_options)

    def new_batch_write_builder(self) -> 'BatchWriteBuilder':
        java_utils.check_batch_write(self._j_table)
        j_batch_write_builder = get_gateway().jvm.InvocationUtil.getBatchWriteBuilder(self._j_table)
        return BatchWriteBuilder(j_batch_write_builder)


class ReadBuilder(read_builder.ReadBuilder):

    def __init__(self, j_read_builder, j_row_type, catalog_options: dict):
        self._j_read_builder = j_read_builder
        self._j_row_type = j_row_type
        self._catalog_options = catalog_options

    def with_filter(self, predicate: 'Predicate'):
        self._j_read_builder.withFilter(predicate.to_j_predicate())
        return self

    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        field_names = list(map(lambda field: field.name(), self._j_row_type.getFields()))
        int_projection = list(map(lambda p: field_names.index(p), projection))
        gateway = get_gateway()
        int_projection_arr = gateway.new_array(gateway.jvm.int, len(projection))
        for i in range(len(projection)):
            int_projection_arr[i] = int_projection[i]
        self._j_read_builder.withProjection(int_projection_arr)
        return self

    def with_limit(self, limit: int) -> 'ReadBuilder':
        self._j_read_builder.withLimit(limit)
        return self

    def new_scan(self) -> 'TableScan':
        j_table_scan = self._j_read_builder.newScan()
        return TableScan(j_table_scan)

    def new_read(self) -> 'TableRead':
        j_table_read = self._j_read_builder.newRead().executeFilter()
        return TableRead(j_table_read, self._j_read_builder.readType(), self._catalog_options)

    def new_predicate_builder(self) -> 'PredicateBuilder':
        return PredicateBuilder(self._j_row_type)


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

    def __init__(self, j_table_read, j_read_type, catalog_options):
        self._j_table_read = j_table_read
        self._j_read_type = j_read_type
        self._catalog_options = catalog_options
        self._j_bytes_reader = None
        self._arrow_schema = java_utils.to_arrow_schema(j_read_type)

    def to_arrow(self, splits):
        record_batch_reader = self.to_arrow_batch_reader(splits)
        return pa.Table.from_batches(record_batch_reader, schema=self._arrow_schema)

    def to_arrow_batch_reader(self, splits):
        self._init()
        j_splits = list(map(lambda s: s.to_j_split(), splits))
        self._j_bytes_reader.setSplits(j_splits)
        batch_iterator = self._batch_generator()
        return pa.RecordBatchReader.from_batches(self._arrow_schema, batch_iterator)

    def to_pandas(self, splits: List[Split]) -> pd.DataFrame:
        return self.to_arrow(splits).to_pandas()

    def to_duckdb(
            self,
            splits: List[Split],
            table_name: str,
            connection: Optional[DuckDBPyConnection] = None) -> DuckDBPyConnection:
        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow(splits))
        return con

    def to_ray(self, splits: List[Split]) -> ray.data.dataset.Dataset:
        return ray.data.from_arrow(self.to_arrow(splits))

    def _init(self):
        if self._j_bytes_reader is None:
            # get thread num
            max_workers = self._catalog_options.get(constants.MAX_WORKERS)
            if max_workers is None:
                # default is sequential
                max_workers = 1
            else:
                max_workers = int(max_workers)
            if max_workers <= 0:
                raise ValueError("max_workers must be greater than 0")
            self._j_bytes_reader = get_gateway().jvm.InvocationUtil.createParallelBytesReader(
                self._j_table_read, self._j_read_type, max_workers)

    def _batch_generator(self) -> Iterator[pa.RecordBatch]:
        while True:
            next_bytes = self._j_bytes_reader.next()
            if next_bytes is None:
                break
            else:
                stream_reader = pa.RecordBatchStreamReader(pa.BufferReader(next_bytes))
                yield from stream_reader


class BatchWriteBuilder(write_builder.BatchWriteBuilder):

    def __init__(self, j_batch_write_builder):
        self._j_batch_write_builder = j_batch_write_builder

    def overwrite(self, static_partition: Optional[dict] = None) -> 'BatchWriteBuilder':
        if static_partition is None:
            static_partition = {}
        self._j_batch_write_builder.withOverwrite(static_partition)
        return self

    def new_write(self) -> 'BatchTableWrite':
        j_batch_table_write = self._j_batch_write_builder.newWrite()
        return BatchTableWrite(j_batch_table_write, self._j_batch_write_builder.rowType())

    def new_commit(self) -> 'BatchTableCommit':
        j_batch_table_commit = self._j_batch_write_builder.newCommit()
        return BatchTableCommit(j_batch_table_commit)


class BatchTableWrite(table_write.BatchTableWrite):

    def __init__(self, j_batch_table_write, j_row_type):
        self._j_batch_table_write = j_batch_table_write
        self._j_bytes_writer = get_gateway().jvm.InvocationUtil.createBytesWriter(
            j_batch_table_write, j_row_type)
        self._arrow_schema = java_utils.to_arrow_schema(j_row_type)

    def write_arrow(self, table):
        for record_batch in table.to_reader():
            # TODO: can we use a reusable stream in #_write_arrow_batch ?
            self._write_arrow_batch(record_batch)

    def write_arrow_batch(self, record_batch):
        self._write_arrow_batch(record_batch)

    def write_pandas(self, dataframe: pd.DataFrame):
        record_batch = pa.RecordBatch.from_pandas(dataframe, schema=self._arrow_schema)
        self._write_arrow_batch(record_batch)

    def _write_arrow_batch(self, record_batch):
        stream = pa.BufferOutputStream()
        with pa.RecordBatchStreamWriter(stream, record_batch.schema) as writer:
            writer.write(record_batch)
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


class Predicate(predicate.Predicate):

    def __init__(self, j_predicate):
        self._j_predicate = j_predicate

    def to_j_predicate(self):
        return self._j_predicate


class PredicateBuilder(predicate.PredicateBuilder):

    def __init__(self, j_row_type):
        self._field_names = j_row_type.getFieldNames()
        self._j_row_type = j_row_type
        self._j_predicate_builder = get_gateway().jvm.PredicateBuilder(j_row_type)

    def _build(self, method: str, field: str, literals: Optional[List[Any]] = None):
        error = ValueError(f'The field {field} is not in field list {self._field_names}.')
        try:
            index = self._field_names.index(field)
            if index == -1:
                raise error
        except ValueError:
            raise error

        if literals is None:
            literals = []

        j_predicate = get_gateway().jvm.PredicationUtil.build(
            self._j_row_type,
            self._j_predicate_builder,
            method,
            index,
            literals
        )
        return Predicate(j_predicate)

    def equal(self, field: str, literal: Any) -> Predicate:
        return self._build('equal', field, [literal])

    def not_equal(self, field: str, literal: Any) -> Predicate:
        return self._build('notEqual', field, [literal])

    def less_than(self, field: str, literal: Any) -> Predicate:
        return self._build('lessThan', field, [literal])

    def less_or_equal(self, field: str, literal: Any) -> Predicate:
        return self._build('lessOrEqual', field, [literal])

    def greater_than(self, field: str, literal: Any) -> Predicate:
        return self._build('greaterThan', field, [literal])

    def greater_or_equal(self, field: str, literal: Any) -> Predicate:
        return self._build('greaterOrEqual', field, [literal])

    def is_null(self, field: str) -> Predicate:
        return self._build('isNull', field)

    def is_not_null(self, field: str) -> Predicate:
        return self._build('isNotNull', field)

    def startswith(self, field: str, pattern_literal: Any) -> Predicate:
        return self._build('startsWith', field, [pattern_literal])

    def endswith(self, field: str, pattern_literal: Any) -> Predicate:
        return self._build('endsWith', field, [pattern_literal])

    def contains(self, field: str, pattern_literal: Any) -> Predicate:
        return self._build('contains', field, [pattern_literal])

    def is_in(self, field: str, literals: List[Any]) -> Predicate:
        return self._build('in', field, literals)

    def is_not_in(self, field: str, literals: List[Any]) -> Predicate:
        return self._build('notIn', field, literals)

    def between(self, field: str, included_lower_bound: Any, included_upper_bound: Any) \
            -> Predicate:
        return self._build('between', field, [included_lower_bound, included_upper_bound])

    def and_predicates(self, predicates: List[Predicate]) -> Predicate:
        predicates = list(map(lambda p: p.to_j_predicate(), predicates))
        return Predicate(get_gateway().jvm.PredicationUtil.buildAnd(predicates))

    def or_predicates(self, predicates: List[Predicate]) -> Predicate:
        predicates = list(map(lambda p: p.to_j_predicate(), predicates))
        return Predicate(get_gateway().jvm.PredicationUtil.buildOr(predicates))