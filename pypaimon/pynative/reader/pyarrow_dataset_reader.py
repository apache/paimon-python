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

from typing import Optional, List

import pyarrow.dataset as ds

from pypaimon import Predicate
from pypaimon.pynative.common.row.internal_row import InternalRow
from pypaimon.pynative.reader.core.columnar_row_iterator import ColumnarRowIterator
from pypaimon.pynative.reader.core.file_record_iterator import FileRecordIterator
from pypaimon.pynative.reader.core.file_record_reader import FileRecordReader
from pypaimon.pynative.util.predicate_converter import convert_predicate


class PyArrowDatasetReader(FileRecordReader[InternalRow]):
    """
    A PyArrowDatasetReader that reads data from a dataset file using PyArrow,
    and filters it based on the provided predicate and projection.
    """

    def __init__(self, format, file_path, batch_size, projection,
                 predicate: Predicate, primary_keys: List[str]):
        if primary_keys is not None:
            if projection is not None:
                key_columns = []
                for pk in primary_keys:
                    key_column = f"_KEY_{pk}"
                    if key_column not in projection:
                        key_columns.append(key_column)
                system_columns = ["_SEQUENCE_NUMBER", "_VALUE_KIND"]
                projection = key_columns + system_columns + projection
            predicate = None

        if predicate is not None:
            predicate = convert_predicate(predicate)

        self._file_path = file_path
        self.dataset = ds.dataset(file_path, format=format)
        self.scanner = self.dataset.scanner(
            columns=projection,
            filter=predicate,
            batch_size=batch_size
        )
        self.batch_iterator = self.scanner.to_batches()

    def read_batch(self) -> Optional[FileRecordIterator[InternalRow]]:
        try:
            record_batch = next(self.batch_iterator, None)
            if record_batch is None:
                return None

            return ColumnarRowIterator(
                self._file_path,
                record_batch
            )
        except Exception as e:
            print(f"Error reading batch: {e}")
            raise

    def close(self):
        pass
