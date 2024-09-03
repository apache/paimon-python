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


def to_j_catalog_context(catalog_context: dict):
    gateway = get_gateway()
    j_options = gateway.jvm.Options(catalog_context)
    return gateway.jvm.CatalogContext.create(j_options)


def check_batch_write(j_table):
    gateway = get_gateway()
    bucket_mode = j_table.bucketMode()
    if bucket_mode == gateway.jvm.BucketMode.HASH_DYNAMIC \
            or bucket_mode == gateway.jvm.BucketMode.CROSS_PARTITION:
        raise TypeError("Doesn't support writing dynamic bucket or cross partition table.")
