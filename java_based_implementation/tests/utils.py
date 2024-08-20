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
import subprocess
import tempfile

from java_based_implementation.java_gateway import get_gateway
from java_based_implementation.util.java_utils import to_j_catalog_context


def set_bridge_jar() -> str:
    current_file_path = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file_path)
    parent_dir = os.path.dirname(current_dir)
    java_module = os.path.join(parent_dir, 'paimon-python-java-bridge')
    # build paimon-python-java-bridge
    subprocess.run(
        ["mvn", "clean", "package"],
        cwd=java_module,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    jar_name = 'paimon-python-java-bridge-0.9-SNAPSHOT.jar'
    jar_file = os.path.join(java_module, 'target', jar_name)
    # move to temp dir
    temp_dir = tempfile.mkdtemp()
    shutil.move(jar_file, temp_dir)
    return os.path.join(temp_dir, jar_name)


def create_simple_table(warehouse, database, table_name, has_pk):
    gateway = get_gateway()

    j_catalog_context = to_j_catalog_context({'warehouse': warehouse})
    j_catalog = gateway.jvm.CatalogFactory.createCatalog(j_catalog_context)

    j_schema_builder = (
        gateway.jvm.Schema.newBuilder()
        .column('f0', gateway.jvm.DataTypes.INT())
        .column('f1', gateway.jvm.DataTypes.STRING())
        .option('bucket', '1')
        .option('bucket-key', 'f0')
    )
    if has_pk:
        j_schema_builder.primaryKey(['f0'])
    j_schema = j_schema_builder.build()

    j_catalog.createDatabase(database, True)
    j_identifier = gateway.jvm.Identifier(database, table_name)
    j_catalog.createTable(j_identifier, j_schema, False)
