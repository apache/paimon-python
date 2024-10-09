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
import random
import shutil
import string
import tempfile
import pyarrow as pa
import unittest


from paimon_python_api import Schema
from paimon_python_java import Catalog
from paimon_python_java.util import java_utils
from setup_utils import java_setuputils


class DataTypesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        java_setuputils.setup_java_bridge()
        cls.warehouse = tempfile.mkdtemp()
        cls.simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string())
        ])
        cls.catalog = Catalog.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        java_setuputils.clean()
        if os.path.exists(cls.warehouse):
            shutil.rmtree(cls.warehouse)

    def test_int(self):
        pa_schema = pa.schema([
            ('_int8', pa.int8()),
            ('_int16', pa.int16()),
            ('_int32', pa.int32()),
            ('_int64', pa.int64())
        ])
        expected_types = ['TINYINT', 'SMALLINT', 'INT', 'BIGINT']
        self._test_impl(pa_schema, expected_types)

    def test_float(self):
        pa_schema = pa.schema([
            ('_float16', pa.float16()),
            ('_float32', pa.float32()),
            ('_float64', pa.float64())
        ])
        expected_types = ['FLOAT', 'FLOAT', 'DOUBLE']
        self._test_impl(pa_schema, expected_types)

    def test_string(self):
        pa_schema = pa.schema([
            ('_string', pa.string()),
            ('_utf8', pa.utf8())
        ])
        expected_types = ['STRING', 'STRING']
        self._test_impl(pa_schema, expected_types)

    def test_bool(self):
        pa_schema = pa.schema([('_bool', pa.bool_())])
        expected_types = ['BOOLEAN']
        self._test_impl(pa_schema, expected_types)

    def test_null(self):
        pa_schema = pa.schema([('_null', pa.null())])
        expected_types = ['STRING']
        self._test_impl(pa_schema, expected_types)

    def test_unsupported_type(self):
        pa_schema = pa.schema([('_array', pa.list_(pa.int32()))])
        schema = Schema(pa_schema)
        with self.assertRaises(ValueError) as e:
            java_utils.to_paimon_schema(schema)
        self.assertEqual(
            str(e.exception), 'Found unsupported data type list<item: int32> for field _array.')

    def _test_impl(self, pa_schema, expected_types):
        scheme = Schema(pa_schema)
        letters = string.ascii_letters
        identifier = 'default.' + ''.join(random.choice(letters) for _ in range(10))
        self.catalog.create_table(identifier, scheme, False)
        table = self.catalog.get_table(identifier)
        field_types = table._j_table.rowType().getFieldTypes()
        actual_types = list(map(lambda t: t.toString(), field_types))
        self.assertListEqual(actual_types, expected_types)
