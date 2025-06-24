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
from typing import List

from py4j.java_gateway import JavaObject

from pypaimon.py4j.util import constants
from pypaimon.pynative.common.exception import PyNativeNotImplementedError
from pypaimon.pynative.reader.core.record_reader import RecordReader
from pypaimon.pynative.util.reader_convert_func import (
    create_avro_format_reader,
    create_concat_record_reader,
    create_data_file_record_reader,
    create_drop_delete_reader,
    create_filter_reader,
    create_key_value_unwrap_reader,
    create_key_value_wrap_record_reader,
    create_pyarrow_reader_for_orc,
    create_pyarrow_reader_for_parquet,
    create_sort_merge_reader_minhep,
    create_transform_reader, create_sort_merge_reader_loser_tree,
)

reader_mapping = {
    "org.apache.paimon.mergetree.compact.ConcatRecordReader":
        create_concat_record_reader,
    "org.apache.paimon.io.DataFileRecordReader":
        create_data_file_record_reader,
    "org.apache.paimon.reader.RecordReader$2":
        create_filter_reader,
    "org.apache.paimon.format.parquet.ParquetReaderFactory$ParquetReader":
        create_pyarrow_reader_for_parquet,
    "org.apache.paimon.format.orc.OrcReaderFactory$OrcVectorizedReader":
        create_pyarrow_reader_for_orc,
    "org.apache.paimon.format.avro.AvroBulkFormat$AvroReader":
        create_avro_format_reader,
    "org.apache.paimon.table.source.KeyValueTableRead$1":
        create_key_value_unwrap_reader,
    "org.apache.paimon.reader.RecordReader$1":
        create_transform_reader,
    "org.apache.paimon.mergetree.DropDeleteReader":
        create_drop_delete_reader,
    "org.apache.paimon.mergetree.compact.SortMergeReaderWithMinHeap":
        create_sort_merge_reader_minhep,
    "org.apache.paimon.mergetree.compact.SortMergeReaderWithLoserTree":
        create_sort_merge_reader_loser_tree,
    "org.apache.paimon.io.KeyValueDataFileRecordReader":
        create_key_value_wrap_record_reader,
    # Additional mappings can be added here
}


class ReaderConverter:
    """
    # Convert Java RecordReader to Python RecordReader
    """

    def __init__(self, predicate, projection, primary_keys: List[str], partition_keys: List[str]):
        self.reader_mapping = reader_mapping
        self._predicate = predicate
        self._projection = projection
        self._primary_keys = primary_keys
        self._partition_keys = partition_keys or []

    def convert_java_reader(self, java_reader: JavaObject) -> RecordReader:
        java_class_name = java_reader.getClass().getName()
        if java_class_name in reader_mapping:
            if os.environ.get(constants.PYPAIMON4J_TEST_MODE) == "true":
                print("converting Java reader: " + str(java_class_name))
            return reader_mapping[java_class_name](java_reader, self, self._predicate,
                                                   self._projection, self._primary_keys, self._partition_keys)
        else:
            raise PyNativeNotImplementedError(f"Unsupported RecordReader type: {java_class_name}")
