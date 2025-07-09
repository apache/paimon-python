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
import os

# pypaimon.api implementation based on Java code & py4j lib

import pandas as pd
import pyarrow as pa

from pypaimon.py4j.java_gateway import get_gateway
from pypaimon.py4j.util import java_utils, constants
from pypaimon.py4j.util.java_utils import serialize_java_object, deserialize_java_object
from pypaimon.api import \
    (catalog, table, read_builder, table_scan, split, row_type,
     table_read, write_builder, table_write, commit_message,
     table_commit, Schema, predicate)
from typing import List, Iterator, Optional, Any, TYPE_CHECKING

from pypaimon.pynative.common.exception import PyNativeNotImplementedError
from pypaimon.pynative.common.predicate import PyNativePredicate
from pypaimon.pynative.common.row.internal_row import InternalRow
from pypaimon.pynative.util.reader_converter import ReaderConverter

if TYPE_CHECKING:
    import ray
    from duckdb.duckdb import DuckDBPyConnection


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
        if self._j_table.primaryKeys().isEmpty():
            primary_keys = None
        else:
            primary_keys = [str(key) for key in self._j_table.primaryKeys()]
        if self._j_table.partitionKeys().isEmpty():
            partition_keys = None
        else:
            partition_keys = [str(key) for key in self._j_table.partitionKeys()]
        return ReadBuilder(j_read_builder, self._j_table.rowType(), self._catalog_options,
                           primary_keys, partition_keys)

    def new_batch_write_builder(self) -> 'BatchWriteBuilder':
        java_utils.check_batch_write(self._j_table)
        j_batch_write_builder = get_gateway().jvm.InvocationUtil.getBatchWriteBuilder(self._j_table)
        return BatchWriteBuilder(j_batch_write_builder)


class ReadBuilder(read_builder.ReadBuilder):

    def __init__(self, j_read_builder, j_row_type, catalog_options: dict, primary_keys: List[str], partition_keys: List[str]):
        self._j_read_builder = j_read_builder
        self._j_row_type = j_row_type
        self._catalog_options = catalog_options
        self._primary_keys = primary_keys
        self._partition_keys = partition_keys
        self._predicate = None
        self._projection = None

    def with_filter(self, predicate: 'Predicate'):
        self._predicate = predicate
        self._j_read_builder.withFilter(predicate.to_j_predicate())
        return self

    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        self._projection = projection
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
        return TableRead(j_table_read, self._j_read_builder.readType(), self._catalog_options,
                         self._predicate, self._projection, self._primary_keys, self._partition_keys)

    def new_predicate_builder(self) -> 'PredicateBuilder':
        return PredicateBuilder(self._j_row_type)

    def read_type(self) -> 'RowType':
        return RowType(self._j_read_builder.readType())


class RowType(row_type.RowType):

    def __init__(self, j_row_type):
        self._j_row_type = j_row_type

    def as_arrow(self) -> "pa.Schema":
        return java_utils.to_arrow_schema(self._j_row_type)


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
        return list(map(lambda s: self._build_single_split(s), self._j_splits))

    def _build_single_split(self, j_split) -> 'Split':
        j_split_bytes = serialize_java_object(j_split)
        row_count = j_split.rowCount()
        files_optional = j_split.convertToRawFiles()
        if not files_optional.isPresent():
            file_size = 0
            file_paths = []
        else:
            files = files_optional.get()
            file_size = sum(file.length() for file in files)
            file_paths = [file.path() for file in files]
        return Split(j_split_bytes, row_count, file_size, file_paths)


class Split(split.Split):

    def __init__(self, j_split_bytes, row_count: int, file_size: int, file_paths: List[str]):
        self._j_split_bytes = j_split_bytes
        self._row_count = row_count
        self._file_size = file_size
        self._file_paths = file_paths

    def to_j_split(self):
        return deserialize_java_object(self._j_split_bytes)

    def row_count(self) -> int:
        return self._row_count

    def file_size(self) -> int:
        return self._file_size

    def file_paths(self) -> List[str]:
        return self._file_paths


class TableRead(table_read.TableRead):

    def __init__(self, j_table_read, j_read_type, catalog_options, predicate, projection,
                 primary_keys: List[str], partition_keys: List[str]):
        self._j_table_read = j_table_read
        self._j_read_type = j_read_type
        self._catalog_options = catalog_options

        self._predicate = predicate
        self._projection = projection
        self._primary_keys = primary_keys
        self._partition_keys = partition_keys

        self._arrow_schema = java_utils.to_arrow_schema(j_read_type)
        self._j_bytes_reader = get_gateway().jvm.InvocationUtil.createParallelBytesReader(
            j_table_read, j_read_type, TableRead._get_max_workers(catalog_options))

    def to_arrow(self, splits: List['Split']) -> pa.Table:
        record_generator = self.to_record_generator(splits)

        # If necessary, set the env constants.IMPLEMENT_MODE to 'py4j' to forcibly use py4j reader
        if os.environ.get(constants.IMPLEMENT_MODE, '') != 'py4j' and record_generator is not None:
            return TableRead._iterator_to_pyarrow_table(record_generator, self._arrow_schema)
        else:
            record_batch_reader = self.to_arrow_batch_reader(splits)
            return pa.Table.from_batches(record_batch_reader, schema=self._arrow_schema)

    def to_arrow_batch_reader(self, splits):
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
            connection: Optional["DuckDBPyConnection"] = None) -> "DuckDBPyConnection":
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow(splits))
        return con

    def to_ray(self, splits: List[Split]) -> "ray.data.dataset.Dataset":
        import ray

        return ray.data.from_arrow(self.to_arrow(splits))

    def to_record_generator(self, splits: List['Split']) -> Optional[Iterator[Any]]:
        """
        Returns a generator for iterating over records in the table.
        If pynative reader is not available, returns None.
        """
        try:
            j_splits = list(s.to_j_split() for s in splits)
            j_reader = get_gateway().jvm.InvocationUtil.createReader(self._j_table_read, j_splits)
            converter = ReaderConverter(self._predicate, self._projection, self._primary_keys, self._partition_keys)
            pynative_reader = converter.convert_java_reader(j_reader)

            def _record_generator():
                try:
                    batch = pynative_reader.read_batch()
                    while batch is not None:
                        record = batch.next()
                        while record is not None:
                            yield record
                            record = batch.next()
                        batch.release_batch()
                        batch = pynative_reader.read_batch()
                finally:
                    pynative_reader.close()

            return _record_generator()

        except PyNativeNotImplementedError as e:
            print(f"Generating pynative reader failed, will use py4j reader instead, "
                  f"error message: {str(e)}")
            return None

    @staticmethod
    def _iterator_to_pyarrow_table(record_generator, arrow_schema):
        """
        Converts a record generator into a pyarrow Table using the provided Arrow schema.
        """
        record_batches = []
        current_batch = []
        batch_size = 1024  # Can be adjusted according to needs for batch size

        for record in record_generator:
            record_dict = {field: record.get_field(i) for i, field in enumerate(arrow_schema.names)}
            current_batch.append(record_dict)
            if len(current_batch) >= batch_size:
                batch = pa.RecordBatch.from_pylist(current_batch, schema=arrow_schema)
                record_batches.append(batch)
                current_batch = []

        if current_batch:
            batch = pa.RecordBatch.from_pylist(current_batch, schema=arrow_schema)
            record_batches.append(batch)

        return pa.Table.from_batches(record_batches, schema=arrow_schema)

    @staticmethod
    def _get_max_workers(catalog_options):
        # default is sequential
        max_workers = int(catalog_options.get(constants.MAX_WORKERS, 1))
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")
        return max_workers

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

    def __init__(self, py_predicate: PyNativePredicate, j_predicate_bytes):
        self.py_predicate = py_predicate
        self._j_predicate_bytes = j_predicate_bytes

    def to_j_predicate(self):
        return deserialize_java_object(self._j_predicate_bytes)

    def test(self, record: InternalRow) -> bool:
        return self.py_predicate.test(record)


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
        return Predicate(PyNativePredicate(method, index, field, literals),
                         serialize_java_object(j_predicate))

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
        j_predicates = list(map(lambda p: p.to_j_predicate(), predicates))
        j_predicate = get_gateway().jvm.PredicationUtil.buildAnd(j_predicates)
        return Predicate(PyNativePredicate('and', None, None, predicates),
                         serialize_java_object(j_predicate))

    def or_predicates(self, predicates: List[Predicate]) -> Predicate:
        j_predicates = list(map(lambda p: p.to_j_predicate(), predicates))
        j_predicate = get_gateway().jvm.PredicationUtil.buildOr(j_predicates)
        return Predicate(PyNativePredicate('or', None, None, predicates),
                         serialize_java_object(j_predicate))
