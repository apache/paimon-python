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
import tempfile
import unittest
import pandas as pd
import pyarrow as pa

from java_based_implementation.api_impl import Catalog, Table
from java_based_implementation.java_gateway import get_gateway
from java_based_implementation.tests.utils import set_bridge_jar, create_simple_table
from java_based_implementation.util import constants, java_utils
from py4j.protocol import Py4JJavaError


class TableWriteReadTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        classpath = set_bridge_jar()
        os.environ[constants.PYPAIMON_JAVA_CLASSPATH] = classpath
        cls.warehouse = tempfile.mkdtemp()

    def testReadEmptyAppendTable(self):
        create_simple_table(self.warehouse, 'default', 'empty_append_table', False)
        catalog = Catalog.create({'warehouse': self.warehouse})
        table = catalog.get_table('default.empty_append_table')

        # read data
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        self.assertTrue(len(splits) == 0)

    def testReadEmptyPkTable(self):
        create_simple_table(self.warehouse, 'default', 'empty_pk_table', True)
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
        table = Table(j_table)
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        data_frames = [
            batch.to_pandas()
            for split in splits
            for batch in table_read.create_reader(split)
        ]
        self.assertEqual(len(data_frames), 0)

    def testWriteReadAppendTable(self):
        create_simple_table(self.warehouse, 'default', 'simple_append_table', False)

        catalog = Catalog.create({'warehouse': self.warehouse})
        table = catalog.get_table('default.simple_append_table')

        # prepare data
        data = {
            'f0': [1, 2, 3],
            'f1': ['a', 'b', 'c'],
        }
        df = pd.DataFrame(data)
        df['f0'] = df['f0'].astype('int32')
        record_batch = pa.RecordBatch.from_pandas(df)

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
            for batch in table_read.create_reader(split)
        ]
        result = pd.concat(data_frames)

        # check data (ignore index)
        pd.testing.assert_frame_equal(result.reset_index(drop=True), df.reset_index(drop=True))

    def testWriteWrongSchema(self):
        create_simple_table(self.warehouse, 'default', 'test_wrong_schema', False)

        catalog = Catalog.create({'warehouse': self.warehouse})
        table = catalog.get_table('default.test_wrong_schema')

        data = {
            'f0': [1, 2, 3],
            'f1': ['a', 'b', 'c'],
        }
        df = pd.DataFrame(data)
        schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string())
        ])
        record_batch = pa.RecordBatch.from_pandas(df, schema)

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
        create_simple_table(
            self.warehouse,
            'default',
            'test_dynamic_bucket',
            True,
            {'bucket': '-1'}
        )

        catalog = Catalog.create({'warehouse': self.warehouse})
        table = catalog.get_table('default.test_dynamic_bucket')

        with self.assertRaises(TypeError) as e:
            table.new_batch_write_builder()
        self.assertEqual(
            str(e.exception),
            "Doesn't support writing dynamic bucket or cross partition table.")
