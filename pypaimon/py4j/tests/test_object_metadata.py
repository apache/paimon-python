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
import pyarrow as pa

from pypaimon import Schema
from pypaimon.py4j.tests import PypaimonTestBase


class ObjectInfoTest(PypaimonTestBase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string())
        ])

    def test_read_type_metadata(self):
        schema = Schema(self.simple_pa_schema)
        self.catalog.create_table('default.test_read_type_metadata', schema, False)
        table = self.catalog.get_table('default.test_read_type_metadata')

        read_builder = table.new_read_builder()
        read_builder.with_projection(['f1'])
        pa_schema = read_builder.read_type().as_arrow()

        self.assertEqual(len(pa_schema.names), 1)
        self.assertEqual(pa_schema.names[0], 'f1')

    def test_split_metadata(self):
        schema = Schema(self.simple_pa_schema)
        self.catalog.create_table('default.test_split_metadata', schema, False)
        table = self.catalog.get_table('default.test_split_metadata')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = {
            'f0': [1, 2, 3, 4, 5],
            'f1': ['a', 'b', 'c', 'd', 'e'],
        }
        pa_table = pa.Table.from_pydict(data, schema=self.simple_pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        self.assertEqual(len(splits), 1)
        self.assertEqual(len(splits[0].file_paths()), 1)
        self.assertEqual(splits[0].row_count(), 5)
        self.assertTrue(splits[0].file_paths()[0].endswith('.parquet'))
        self.assertEqual(splits[0].file_size(), os.path.getsize(splits[0].file_paths()[0]))
