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

import pandas as pd
import pyarrow as pa

from pypaimon.api import Schema
from pypaimon.py4j.tests import PypaimonTestBase


class NativeReaderTest(PypaimonTestBase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string())
        ])
        cls.pk_pa_schema = pa.schema([
            ('f0', pa.int32(), False),
            ('f1', pa.string()),
            ('f2', pa.string())
        ])
        cls._expected_full_data = pd.DataFrame({
            'f0': [1, 2, 3, 4, 5, 6, 7, 8],
            'f1': ['a', 'b', 'c', None, 'e', 'f', 'g', 'h'],
            'f2': ['A', 'B', 'C', 'D', 'E', 'F', 'G', None],
        })
        cls._expected_full_data['f0'] = cls._expected_full_data['f0'].astype('int32')
        cls.expected_full = pa.Table.from_pandas(cls._expected_full_data,
                                                 schema=cls.simple_pa_schema)
        cls._expected_full_data_pk = pd.DataFrame({
            'f0': [1, 2, 3, 4, 6],
            'f1': ['a', 'x', 'y', None, 'z'],
            'f2': ['A', 'X', 'Y', 'D', 'Z'],
        })
        cls._expected_full_data_pk['f0'] = cls._expected_full_data_pk['f0'].astype('int32')
        cls.expected_full_pk = pa.Table.from_pandas(cls._expected_full_data_pk,
                                                    schema=cls.pk_pa_schema)

    def testParquetAppendOnlyReader(self):
        schema = Schema(self.simple_pa_schema)
        self.catalog.create_table('default.test_append_only_parquet', schema, False)
        table = self.catalog.get_table('default.test_append_only_parquet')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder)
        self.assertEqual(actual, self.expected_full)

    def testOrcAppendOnlyReader(self):
        schema = Schema(self.simple_pa_schema, options={'file.format': 'orc'})
        self.catalog.create_table('default.test_append_only_orc', schema, False)
        table = self.catalog.get_table('default.test_append_only_orc')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder)
        self.assertEqual(actual, self.expected_full)

    def testAvroAppendOnlyReader(self):
        schema = Schema(self.simple_pa_schema, options={'file.format': 'avro'})
        self.catalog.create_table('default.test_append_only_avro', schema, False)
        table = self.catalog.get_table('default.test_append_only_avro')
        self._write_test_table(table)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder)
        self.assertEqual(actual, self.expected_full)

    def testAppendOnlyReaderWithFilter(self):
        schema = Schema(self.simple_pa_schema)
        self.catalog.create_table('default.test_append_only_filter', schema, False)
        table = self.catalog.get_table('default.test_append_only_filter')
        self._write_test_table(table)
        predicate_builder = table.new_read_builder().new_predicate_builder()

        p1 = predicate_builder.less_than('f0', 7)
        p2 = predicate_builder.greater_or_equal('f0', 2)
        p3 = predicate_builder.between('f0', 0, 5)  # from now, [2/b, 3/c, 4/d, 5/e] left
        p4 = predicate_builder.is_not_in('f1', ['a', 'b'])  # exclude 2/b
        p5 = predicate_builder.is_in('f2', ['A', 'B', 'D', 'E', 'F', 'G'])  # exclude 3/c
        p6 = predicate_builder.is_not_null('f1')    # exclude 4/d
        g1 = predicate_builder.and_predicates([p1, p2, p3, p4, p5, p6])
        read_builder = table.new_read_builder().with_filter(g1)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected_full.slice(4, 1)  # 5/e
        ])
        self.assertEqual(actual, expected)

        p7 = predicate_builder.startswith('f1', 'a')
        p8 = predicate_builder.endswith('f2', 'C')
        p9 = predicate_builder.contains('f2', 'E')
        p10 = predicate_builder.equal('f1', 'f')
        p11 = predicate_builder.is_null('f2')
        g2 = predicate_builder.or_predicates([p7, p8, p9, p10, p11])
        read_builder = table.new_read_builder().with_filter(g2)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected_full.slice(0, 1),  # 1/a
            self.expected_full.slice(2, 1),  # 3/c
            self.expected_full.slice(4, 1),  # 5/e
            self.expected_full.slice(5, 1),  # 6/f
            self.expected_full.slice(7, 1),  # 8/h
        ])
        self.assertEqual(actual, expected)

        g3 = predicate_builder.and_predicates([g1, g2])
        read_builder = table.new_read_builder().with_filter(g3)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected_full.slice(4, 1),  # 5/e
        ])
        self.assertEqual(actual, expected)

        # Same as java, 'not_equal' will also filter records of 'None' value
        p12 = predicate_builder.not_equal('f1', 'f')
        read_builder = table.new_read_builder().with_filter(p12)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            # not only 6/f, but also 4/d will be filtered
            self.expected_full.slice(0, 1),  # 1/a
            self.expected_full.slice(1, 1),  # 2/b
            self.expected_full.slice(2, 1),  # 3/c
            self.expected_full.slice(4, 1),  # 5/e
            self.expected_full.slice(6, 1),  # 7/g
            self.expected_full.slice(7, 1),  # 8/h
        ])
        self.assertEqual(actual, expected)

    def testAppendOnlyReaderWithProjection(self):
        schema = Schema(self.simple_pa_schema)
        self.catalog.create_table('default.test_append_only_projection', schema, False)
        table = self.catalog.get_table('default.test_append_only_projection')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['f0', 'f2'])
        actual = self._read_test_table(read_builder)
        expected = self.expected_full.select(['f0', 'f2'])
        self.assertEqual(actual, expected)

    def testAppendOnlyReaderWithLimit(self):
        schema = Schema(self.simple_pa_schema, options={'source.split.target-size': '1mb'})
        self.catalog.create_table('default.test_append_only_limit', schema, False)
        table = self.catalog.get_table('default.test_append_only_limit')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_limit(1)
        actual = self._read_test_table(read_builder)
        # only records from 1st commit (1st split) will be read
        expected = pa.concat_tables([
            self.expected_full.slice(0, 1),  # 1/a
            self.expected_full.slice(1, 1),  # 2/b
            self.expected_full.slice(2, 1),  # 3/c
            self.expected_full.slice(3, 1),  # 4/d
        ])
        self.assertEqual(actual, expected)

    # TODO: test cases for avro filter and projection

    def testPkParquetReader(self):
        schema = Schema(self.pk_pa_schema, primary_keys=['f0'], options={
            'bucket': '2'
        })
        self.catalog.create_table('default.test_pk_parquet', schema, False)
        table = self.catalog.get_table('default.test_pk_parquet')
        self._write_test_table(table, for_pk=True)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder)
        self.assertEqual(actual, self.expected_full_pk)

    def testPkParquetReaderWithMinHeap(self):
        schema = Schema(self.pk_pa_schema, primary_keys=['f0'], options={
            'bucket': '1',
            'sort-engine': 'min-heap'
        })
        self.catalog.create_table('default.test_pk_parquet_loser_tree', schema, False)
        table = self.catalog.get_table('default.test_pk_parquet_loser_tree')
        self._write_test_table(table, for_pk=True)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder)
        self.assertEqual(actual, self.expected_full_pk)

    def testPkOrcReader(self):
        schema = Schema(self.pk_pa_schema, primary_keys=['f0'], options={
            'bucket': '1',
            'file.format': 'orc'
        })
        self.catalog.create_table('default.test_pk_orc', schema, False)
        table = self.catalog.get_table('default.test_pk_orc')
        self._write_test_table(table, for_pk=True)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder)
        self.assertEqual(actual, self.expected_full_pk)

    def testPkAvroReader(self):
        schema = Schema(self.pk_pa_schema, primary_keys=['f0'], options={
            'bucket': '1',
            'file.format': 'avro'
        })
        self.catalog.create_table('default.test_pk_avro', schema, False)
        table = self.catalog.get_table('default.test_pk_avro')
        self._write_test_table(table, for_pk=True)

        read_builder = table.new_read_builder()
        actual = self._read_test_table(read_builder)
        self.assertEqual(actual, self.expected_full_pk)

    def testPkReaderWithFilter(self):
        schema = Schema(self.pk_pa_schema, primary_keys=['f0'], options={
            'bucket': '1'
        })
        self.catalog.create_table('default.test_pk_filter', schema, False)
        table = self.catalog.get_table('default.test_pk_filter')
        self._write_test_table(table, for_pk=True)
        predicate_builder = table.new_read_builder().new_predicate_builder()

        p1 = predicate_builder.between('f0', 0, 5)
        p2 = predicate_builder.is_not_in('f1', ['a', 'x'])
        p3 = predicate_builder.is_not_null('f1')
        g1 = predicate_builder.and_predicates([p1, p2, p3])
        p4 = predicate_builder.equal('f2', 'Z')
        g2 = predicate_builder.or_predicates([g1, p4])
        read_builder = table.new_read_builder().with_filter(g2)
        actual = self._read_test_table(read_builder)
        expected = pa.concat_tables([
            self.expected_full_pk.slice(2, 1),  # 3/y
            self.expected_full_pk.slice(4, 1),  # 6/z
        ])
        self.assertEqual(actual, expected)

    def testPkReaderWithProjection(self):
        schema = Schema(self.pk_pa_schema, primary_keys=['f0'], options={
            'bucket': '1'
        })
        self.catalog.create_table('default.test_pk_projection', schema, False)
        table = self.catalog.get_table('default.test_pk_projection')
        self._write_test_table(table, for_pk=True)

        read_builder = table.new_read_builder().with_projection(['f0', 'f2'])
        actual = self._read_test_table(read_builder)
        expected = self.expected_full_pk.select(['f0', 'f2'])
        self.assertEqual(actual, expected)

    def _write_test_table(self, table, for_pk=False):
        write_builder = table.new_batch_write_builder()

        # first write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'f0': [1, 2, 3, 4],
            'f1': ['a', 'b', 'c', None],
            'f2': ['A', 'B', 'C', 'D'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.simple_pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # second write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        if for_pk:
            data2 = {
                'f0': [2, 3, 6],
                'f1': ['x', 'y', 'z'],
                'f2': ['X', 'Y', 'Z'],
            }
        else:
            data2 = {
                'f0': [5, 6, 7, 8],
                'f1': ['e', 'f', 'g', 'h'],
                'f2': ['E', 'F', 'G', None],
            }
        pa_table = pa.Table.from_pydict(data2, schema=self.simple_pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _read_test_table(self, read_builder):
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        self.assertNotEqual(table_read.to_record_generator(splits), None)
        return table_read.to_arrow(splits)
