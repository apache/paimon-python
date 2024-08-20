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

from java_based_implementation.api_impl import Catalog
from java_based_implementation.tests.utils import set_bridge_jar, create_simple_table
from java_based_implementation.util import constants


class TableWriteReadTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        classpath = set_bridge_jar()
        os.environ[constants.PYPAIMON_JAVA_CLASSPATH] = classpath

    def testWriteReadAppendTable(self):
        warehouse = tempfile.mkdtemp()
        create_simple_table(warehouse, 'default', 'simple_append_table', False)

        catalog = Catalog.create({'warehouse': warehouse})
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
        batches = []
        for split in splits:
            batch_reader = table_read.create_reader(split)
            while True:
                batch = batch_reader.next_batch()
                if batch is None:
                    break
                else:
                    batches.append(batch.to_pandas())

        result = pd.concat(batches)

        # check data
        pd.testing.assert_frame_equal(result, df)

        table_read.close()
