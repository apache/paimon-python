from typing import Optional

from pypaimon.api import Database
from pypaimon.pynative.catalog.abstract_catalog import AbstractCatalog
from pypaimon.pynative.catalog.catalog_constant import CatalogConstants
from pypaimon.pynative.catalog.catalog_exception import DatabaseNotExistException, TableNotExistException
from pypaimon.pynative.common.identifier import TableIdentifier
from pypaimon.pynative.table.schema_manager import SchemaManager


class FileSystemCatalog(AbstractCatalog):

    def __init__(self, catalog_options: dict):
        super().__init__(catalog_options)

    @staticmethod
    def identifier() -> str:
        return "filesystem"

    def allow_custom_table_path(self) -> bool:
        return False

    def get_database(self, name: str) -> 'Database':
        if self.file_io.exists(self.get_database_path(name)):
            return Database(name, {})
        else:
            raise DatabaseNotExistException(name)

    def create_database_impl(self, name: str, properties: Optional[dict] = None):
        if properties and CatalogConstants.DB_LOCATION_PROP in properties:
            raise ValueError(f"Cannot specify location for a database when using fileSystem catalog.")
        path = self.get_database_path(name)
        self.file_io.mkdirs(path)

    def create_table_impl(self, table_identifier: TableIdentifier, schema: 'Schema'):
        table_path = self.get_table_location(table_identifier)
        schema_manager = SchemaManager(self.file_io, table_path)
        schema_manager.create_table(schema)

    def get_table_schema(self, table_identifier: TableIdentifier):
        table_path = self.get_table_location(table_identifier)
        table_schema = SchemaManager(self.file_io, table_path).latest()
        if table_schema is None:
            raise TableNotExistException(table_identifier.get_full_name())
        return table_schema

    def lock_factory(self):
        """Lock Factory"""

    def metastore_client_factory(self):
        return None
