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
from paimon_python_api import catalog, read_builder, table_scan, split, table_read
from paimon_python_api import table
from pyarrow import RecordBatchReader
from typing import List
from typing_extensions import Self


class Catalog(catalog.Catalog):

    def __init__(self, j_catalog):
        self._j_catalog = j_catalog

    @staticmethod
    def create(catalog_context: dict) -> 'Catalog':
        j_catalog_context = to_j_catalog_context(catalog_context)
        gateway = get_gateway()
        j_catalog = gateway.jvm.CatalogFactory.createCatalog(j_catalog_context)
        return Catalog(j_catalog)

    def get_table(self, identifier: tuple) -> 'Table':
        gateway = get_gateway()
        j_identifier = gateway.jvm.Identifier.fromString(identifier)
        j_table = self._j_catalog.getTable(j_identifier)
        return Table(j_table)


class Table(table.Table):

    def __init__(self, j_table):
        self._j_table = j_table

    def new_read_builder(self) -> 'ReadBuilder':
        j_read_builder = self._j_table.newReadBuilder()
        return ReadBuilder(j_read_builder)


class ReadBuilder(read_builder.ReadBuilder):

    def __init__(self, j_read_builder):
        self._j_read_builder = j_read_builder

    def with_projection(self, projection: List[List[int]]) -> Self:
        self._j_read_builder.withProjection(projection)
        return self

    def with_limit(self, limit: int) -> Self:
        self._j_read_builder.withLimit(limit)
        return self

    def new_scan(self) -> 'TableScan':
        j_table_scan = self._j_read_builder.newScan()
        return TableScan(j_table_scan)

    def new_read(self) -> 'TableRead':
        # TODO
        pass


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

    def create_reader(self, split: Split) -> RecordBatchReader:
        # TODO
        pass