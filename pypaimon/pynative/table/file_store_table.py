from abc import abstractmethod
from pathlib import Path

from pypaimon.api import Table, BatchWriteBuilder, ReadBuilder
from pypaimon.pynative.catalog.catalog_env import CatalogEnvironment
from pypaimon.pynative.common.exception import PyNativeNotImplementedError
from pypaimon.pynative.common.file_io import FileIO
from pypaimon.pynative.common.identifier import TableIdentifier
from pypaimon.pynative.reader.read_builder_impl import ReadBuilderImpl
from pypaimon.pynative.table.bucket_mode import BucketMode
from pypaimon.pynative.table.row_key_extractor import RowKeyExtractor, FixedBucketRowKeyExtractor, UnawareBucketRowKeyExtractor
from pypaimon.pynative.table.schema_manager import SchemaManager
from pypaimon.pynative.table.table_schema import TableSchema
from pypaimon.pynative.writer.batch_write_builder import BatchWriteBuilderImpl
from pypaimon.pynative.writer.file_store_write import FileStoreWrite


class FileStoreTable(Table):
    def __init__(self, file_io: FileIO, table_identifier: TableIdentifier, table_path: Path, table_schema: TableSchema, catalog_env: CatalogEnvironment = None):
        self.file_io = file_io
        self.table_identifier = table_identifier
        self.table_path = table_path
        self.table_schema = table_schema
        self.catalog_env = catalog_env
        self.options = {} if table_schema.options is None else table_schema.options
        self.schema_manager = SchemaManager(file_io, table_path)
        self.primary_keys = table_schema.primary_keys or []

    @abstractmethod
    def bucket_mode(self) -> BucketMode:
        """"""

    @abstractmethod
    def new_write(self, commit_user: str) -> FileStoreWrite:
        """"""

    def new_read_builder(self) -> ReadBuilder:
        return ReadBuilderImpl(self)

    def new_batch_write_builder(self) -> BatchWriteBuilder:
        return BatchWriteBuilderImpl(self)

    def create_row_key_extractor(self) -> RowKeyExtractor:
        bucket_mode = self.bucket_mode()
        if bucket_mode == BucketMode.HASH_FIXED:
            return FixedBucketRowKeyExtractor(self.table_schema)
        elif bucket_mode == BucketMode.BUCKET_UNAWARE:
            return UnawareBucketRowKeyExtractor(self.table_schema)
        elif bucket_mode == BucketMode.HASH_DYNAMIC or bucket_mode == BucketMode.CROSS_PARTITION:
            raise PyNativeNotImplementedError(bucket_mode)
        else:
            raise ValueError(f"Unsupported mode: {bucket_mode}")


class FileStoreTableFactory:
    @staticmethod
    def create(file_io: FileIO, table_identifier: TableIdentifier, table_path: Path, table_schema: TableSchema, catalog_env: CatalogEnvironment = None) -> FileStoreTable:
        from pypaimon.pynative.table.append_only_table import AppendOnlyTable
        from pypaimon.pynative.table.primary_key_table import PrimaryKeyTable

        if table_schema.primary_keys is not None and table_schema.primary_keys:
            return PrimaryKeyTable(file_io, table_identifier, table_path, table_schema, catalog_env)
        else:
            return AppendOnlyTable(file_io, table_identifier, table_path, table_schema, catalog_env)
