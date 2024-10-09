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

import pyarrow as pa

from paimon_python_api import Schema
from paimon_python_java.java_gateway import get_gateway


def to_j_catalog_context(catalog_options: dict):
    gateway = get_gateway()
    j_options = gateway.jvm.Options(catalog_options)
    return gateway.jvm.CatalogContext.create(j_options)


def to_j_identifier(identifier: str):
    return get_gateway().jvm.Identifier.fromString(identifier)


def to_paimon_schema(schema: Schema):
    j_schema_builder = get_gateway().jvm.Schema.newBuilder()

    if schema.partition_keys is not None:
        j_schema_builder.partitionKeys(schema.partition_keys)

    if schema.primary_keys is not None:
        j_schema_builder.primaryKey(schema.primary_keys)

    if schema.options is not None:
        j_schema_builder.options(schema.options)

    j_schema_builder.comment(schema.comment)

    for field in schema.pa_schema:
        column_name = field.name
        column_type = _to_j_type(column_name, field.type)
        j_schema_builder.column(column_name, column_type)
    return j_schema_builder.build()


def check_batch_write(j_table):
    gateway = get_gateway()
    bucket_mode = j_table.bucketMode()
    if bucket_mode == gateway.jvm.BucketMode.HASH_DYNAMIC \
            or bucket_mode == gateway.jvm.BucketMode.CROSS_PARTITION:
        raise TypeError("Doesn't support writing dynamic bucket or cross partition table.")


def _to_j_type(name, pa_type):
    jvm = get_gateway().jvm
    # int
    if pa.types.is_int8(pa_type):
        return jvm.DataTypes.TINYINT()
    elif pa.types.is_int16(pa_type):
        return jvm.DataTypes.SMALLINT()
    elif pa.types.is_int32(pa_type):
        return jvm.DataTypes.INT()
    elif pa.types.is_int64(pa_type):
        return jvm.DataTypes.BIGINT()
    # float
    elif pa.types.is_float16(pa_type) or pa.types.is_float32(pa_type):
        return jvm.DataTypes.FLOAT()
    elif pa.types.is_float64(pa_type):
        return jvm.DataTypes.DOUBLE()
    # string
    elif pa.types.is_string(pa_type):
        return jvm.DataTypes.STRING()
    # bool
    elif pa.types.is_boolean(pa_type):
        return jvm.DataTypes.BOOLEAN()
    elif pa.types.is_null(pa_type):
        print(f"WARN: The type of column '{name}' is null, "
              "and it will be converted to string type by default. "
              "Please check if the original type is string. "
              f"If not, please manually specify the type of '{name}'.")
        return jvm.DataTypes.STRING()
    else:
        raise ValueError(f'Found unsupported data type {str(pa_type)} for field {name}.')
