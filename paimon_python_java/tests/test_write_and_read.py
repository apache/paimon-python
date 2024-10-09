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
import shutil
import tempfile
import unittest
import pandas as pd
import pyarrow as pa
import setup_utils.java_setuputils as setuputils

from paimon_python_api import Schema
from paimon_python_java import Catalog
from paimon_python_java.java_gateway import get_gateway
from paimon_python_java.util import java_utils
from py4j.protocol import Py4JJavaError


class TableWriteReadTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setuputils.setup_java_bridge()
        cls.warehouse = tempfile.mkdtemp()
        cls.simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string())
        ])
        cls.catalog = Catalog.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        setuputils.clean()
        if os.path.exists(cls.warehouse):
            shutil.rmtree(cls.warehouse)

    def testReadEmptyAppendTable(self):
        schema = Schema(self.simple_pa_schema)
        self.catalog.create_table('default.empty_append_table', schema, False)
        table = self.catalog.get_table('default.empty_append_table')

        # read data
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        self.assertTrue(len(splits) == 0)

    def testReadEmptyPkTable(self):
        schema = Schema(self.simple_pa_schema, primary_keys=['f0'], options={'bucket': '1'})
        self.catalog.create_table('default.empty_pk_table', schema, False)

        # use Java API to generate data
        gateway = get_gateway()
        j_catalog_context = java_utils.to_j_catalog_context({'warehouse': self.warehouse})
        j_catalog = gateway.jvm.CatalogFactory.createCatalog(j_catalog_context)
        j_identifier = gateway.jvm.Identifier.fromString('default.empty_pk_table')
        j_table = j_catalog.getTable(j_identifier)
        j_write_builder = gateway.jvm.InvocationUtil.getBatchWriteBuilder(j_table)

        # first commit
        generic_row = gateway.jvm.GenericRow(gateway.jvm.RowKind.INSERT, 2)
        generic_row.setField(0, 1)
        generic_row.setField(1, gateway.jvm.BinaryString.fromString('a'))
        table_write = j_write_builder.newWrite()
        table_write.write(generic_row)
        table_commit = j_write_builder.newCommit()
        table_commit.commit(table_write.prepareCommit())
        table_write.close()
        table_commit.close()

        # second commit
        generic_row = gateway.jvm.GenericRow(gateway.jvm.RowKind.DELETE, 2)
        generic_row.setField(0, 1)
        generic_row.setField(1, gateway.jvm.BinaryString.fromString('a'))
        table_write = j_write_builder.newWrite()
        table_write.write(generic_row)
        table_commit = j_write_builder.newCommit()
        table_commit.commit(table_write.prepareCommit())
        table_write.close()
        table_commit.close()

        # read data
        table = self.catalog.get_table('default.empty_pk_table')
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        data_frames = [
            batch.to_pandas()
            for split in splits
            for batch in table_read.create_reader([split])
        ]
        self.assertEqual(len(data_frames), 0)

    def testWriteReadAppendTable(self):
        schema = Schema(self.simple_pa_schema)
        self.catalog.create_table('default.simple_append_table', schema, False)
        table = self.catalog.get_table('default.simple_append_table')

        # prepare data
        data = {
            'f0': [1, 2, 3],
            'f1': ['a', 'b', 'c'],
        }
        df = pd.DataFrame(data)
        record_batch = pa.RecordBatch.from_pandas(df, schema=self.simple_pa_schema)

        # write and commit data
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write(record_batch)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)

        table_write.close()
        table_commit.close()

        # read data
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        data_frames = [
            batch.to_pandas()
            for split in splits
            for batch in table_read.create_reader([split])
        ]
        result = pd.concat(data_frames)

        # check data (ignore index)
        expected = df
        expected['f0'] = df['f0'].astype('int32')
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True), expected.reset_index(drop=True))

    def testWriteWrongSchema(self):
        schema = Schema(self.simple_pa_schema)
        self.catalog.create_table('default.test_wrong_schema', schema, False)
        table = self.catalog.get_table('default.test_wrong_schema')

        data = {
            'f0': [1, 2, 3],
            'f1': ['a', 'b', 'c'],
        }
        df = pd.DataFrame(data)
        schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string())
        ])
        record_batch = pa.RecordBatch.from_pandas(df, schema=schema)

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()

        with self.assertRaises(Py4JJavaError) as e:
            table_write.write(record_batch)
        self.assertEqual(
            str(e.exception.java_exception),
            '''java.lang.RuntimeException: Input schema isn't consistent with table schema.
\tTable schema is: [f0: Int(32, true), f1: Utf8]
\tInput schema is: [f0: Int(64, true), f1: Utf8]''')

    def testCannotWriteDynamicBucketTable(self):
        schema = Schema(self.simple_pa_schema, primary_keys=['f0'])
        self.catalog.create_table('default.test_dynamic_bucket', schema, False)
        table = self.catalog.get_table('default.test_dynamic_bucket')

        with self.assertRaises(TypeError) as e:
            table.new_batch_write_builder()
        self.assertEqual(
            str(e.exception),
            "Doesn't support writing dynamic bucket or cross partition table.")

    def testParallelRead(self):
        catalog = Catalog.create({'warehouse': self.warehouse, 'max-workers': '2'})
        schema = Schema(self.simple_pa_schema)
        catalog.create_table('default.test_parallel_read', schema, False)
        table = catalog.get_table('default.test_parallel_read')

        # prepare data
        n_times = 4
        expected_data = {
            'f0': [],
            'f1': []
        }
        for i in range(n_times):
            data = {
                'f0': [i],
                'f1': [str(i * 2)]
            }
            expected_data['f0'].append(i)
            expected_data['f1'].append(str(i * 2))

            df = pd.DataFrame(data)
            record_batch = pa.RecordBatch.from_pandas(df, schema=self.simple_pa_schema)

            # write and commit data
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()

            table_write.write(record_batch)
            commit_messages = table_write.prepare_commit()
            table_commit.commit(commit_messages)

        # read data parallely
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        data_frames = [
            batch.to_pandas()
            for batch in table_read.create_reader(splits)
        ]
        result = pd.concat(data_frames)

        expected_df = pd.DataFrame(expected_data)
        expected_df['f0'] = expected_df['f0'].astype('int32')

        # check data (ignore index)
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True), expected_df.reset_index(drop=True))
