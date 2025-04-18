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


def create_concat_record_reader(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.concat_record_reader import ConcatRecordReader
    reader_class = j_reader.getClass()
    queue_field = reader_class.getDeclaredField("queue")
    queue_field.setAccessible(True)
    j_supplier_queue = queue_field.get(j_reader)
    return ConcatRecordReader(converter, j_supplier_queue)


def create_data_file_record_reader(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.data_file_record_reader import DataFileRecordReader
    reader_class = j_reader.getClass()
    wrapped_reader_field = reader_class.getDeclaredField("reader")
    wrapped_reader_field.setAccessible(True)
    j_wrapped_reader = wrapped_reader_field.get(j_reader)
    wrapped_reader = converter.convert_java_reader(j_wrapped_reader)
    return DataFileRecordReader(wrapped_reader)


def create_filter_reader(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.filter_record_reader import FilterRecordReader
    reader_class = j_reader.getClass()
    wrapped_reader_field = reader_class.getDeclaredField("val$thisReader")
    wrapped_reader_field.setAccessible(True)
    j_wrapped_reader = wrapped_reader_field.get(j_reader)
    wrapped_reader = converter.convert_java_reader(j_wrapped_reader)
    if primary_keys is not None:
        return FilterRecordReader(wrapped_reader, predicate)
    else:
        return wrapped_reader


def create_pyarrow_reader_for_parquet(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.pyarrow_dataset_reader import PyArrowDatasetReader

    reader_class = j_reader.getClass()
    factory_field = reader_class.getDeclaredField("this$0")
    factory_field.setAccessible(True)
    j_factory = factory_field.get(j_reader)
    factory_class = j_factory.getClass()
    batch_size_field = factory_class.getDeclaredField("batchSize")
    batch_size_field.setAccessible(True)
    batch_size = batch_size_field.get(j_factory)

    file_reader_field = reader_class.getDeclaredField("reader")
    file_reader_field.setAccessible(True)
    j_file_reader = file_reader_field.get(j_reader)
    file_reader_class = j_file_reader.getClass()
    input_file_field = file_reader_class.getDeclaredField("file")
    input_file_field.setAccessible(True)
    j_input_file = input_file_field.get(j_file_reader)
    file_path = j_input_file.getPath().toUri().toString()

    return PyArrowDatasetReader('parquet', file_path, batch_size, projection,
                                predicate, primary_keys)


def create_pyarrow_reader_for_orc(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.pyarrow_dataset_reader import PyArrowDatasetReader

    reader_class = j_reader.getClass()
    file_reader_field = reader_class.getDeclaredField("orcReader")
    file_reader_field.setAccessible(True)
    j_file_reader = file_reader_field.get(j_reader)
    file_reader_class = j_file_reader.getClass()
    path_field = file_reader_class.getDeclaredField("path")
    path_field.setAccessible(True)
    j_path = path_field.get(j_file_reader)
    file_path = j_path.toUri().toString()

    # TODO: Temporarily hard-coded to 1024 as we cannot reflectively obtain this value yet
    batch_size = 1024

    return PyArrowDatasetReader('orc', file_path, batch_size, projection, predicate, primary_keys)


def create_avro_format_reader(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.avro_format_reader import AvroFormatReader

    reader_class = j_reader.getClass()
    path_field = reader_class.getDeclaredField("filePath")
    path_field.setAccessible(True)
    j_path = path_field.get(j_reader)
    file_path = j_path.toUri().toString()

    # TODO: Temporarily hard-coded to 1024 as we cannot reflectively obtain this value yet
    batch_size = 1024

    return AvroFormatReader(file_path, batch_size, None)


def create_key_value_unwrap_reader(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.key_value_unwrap_reader import KeyValueUnwrapReader
    reader_class = j_reader.getClass()
    wrapped_reader_field = reader_class.getDeclaredField("val$reader")
    wrapped_reader_field.setAccessible(True)
    j_wrapped_reader = wrapped_reader_field.get(j_reader)
    wrapped_reader = converter.convert_java_reader(j_wrapped_reader)
    return KeyValueUnwrapReader(wrapped_reader)


def create_transform_reader(j_reader, converter, predicate, projection, primary_keys):
    reader_class = j_reader.getClass()
    wrapped_reader_field = reader_class.getDeclaredField("val$thisReader")
    wrapped_reader_field.setAccessible(True)
    j_wrapped_reader = wrapped_reader_field.get(j_reader)
    # TODO: implement projectKey and projectOuter
    return converter.convert_java_reader(j_wrapped_reader)


def create_drop_delete_reader(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.drop_delete_reader import DropDeleteReader
    reader_class = j_reader.getClass()
    wrapped_reader_field = reader_class.getDeclaredField("reader")
    wrapped_reader_field.setAccessible(True)
    j_wrapped_reader = wrapped_reader_field.get(j_reader)
    wrapped_reader = converter.convert_java_reader(j_wrapped_reader)
    return DropDeleteReader(wrapped_reader)


def create_sort_merge_reader_minhep(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.sort_merge_reader import SortMergeReader
    j_reader_class = j_reader.getClass()
    batch_readers_field = j_reader_class.getDeclaredField("nextBatchReaders")
    batch_readers_field.setAccessible(True)
    j_batch_readers = batch_readers_field.get(j_reader)
    readers = []
    for next_reader in j_batch_readers:
        readers.append(converter.convert_java_reader(next_reader))
    return SortMergeReader(readers, primary_keys)


def create_sort_merge_reader_loser_tree(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.sort_merge_reader import SortMergeReader
    j_reader_class = j_reader.getClass()
    loser_tree_field = j_reader_class.getDeclaredField("loserTree")
    loser_tree_field.setAccessible(True)
    j_loser_tree = loser_tree_field.get(j_reader)
    j_loser_tree_class = j_loser_tree.getClass()
    leaves_field = j_loser_tree_class.getDeclaredField("leaves")
    leaves_field.setAccessible(True)
    j_leaves = leaves_field.get(j_loser_tree)
    readers = []
    for j_leaf in j_leaves:
        j_leaf_class = j_leaf.getClass()
        j_leaf_reader_field = j_leaf_class.getDeclaredField("reader")
        j_leaf_reader_field.setAccessible(True)
        j_leaf_reader = j_leaf_reader_field.get(j_leaf)
        readers.append(converter.convert_java_reader(j_leaf_reader))
    return SortMergeReader(readers, primary_keys)


def create_key_value_wrap_record_reader(j_reader, converter, predicate, projection, primary_keys):
    from pypaimon.pynative.reader.key_value_wrap_reader import KeyValueWrapReader
    reader_class = j_reader.getClass()

    wrapped_reader_field = reader_class.getDeclaredField("reader")
    wrapped_reader_field.setAccessible(True)
    j_wrapped_reader = wrapped_reader_field.get(j_reader)
    wrapped_reader = converter.convert_java_reader(j_wrapped_reader)

    level_field = reader_class.getDeclaredField("level")
    level_field.setAccessible(True)
    level = level_field.get(j_reader)

    serializer_field = reader_class.getDeclaredField("serializer")
    serializer_field.setAccessible(True)
    j_serializer = serializer_field.get(j_reader)
    serializer_class = j_serializer.getClass()
    key_arity_field = serializer_class.getDeclaredField("keyArity")
    key_arity_field.setAccessible(True)
    key_arity = key_arity_field.get(j_serializer)

    reused_value_field = serializer_class.getDeclaredField("reusedValue")
    reused_value_field.setAccessible(True)
    j_reused_value = reused_value_field.get(j_serializer)
    offset_row_class = j_reused_value.getClass()
    arity_field = offset_row_class.getDeclaredField("arity")
    arity_field.setAccessible(True)
    value_arity = arity_field.get(j_reused_value)
    return KeyValueWrapReader(wrapped_reader, level, key_arity, value_arity)
