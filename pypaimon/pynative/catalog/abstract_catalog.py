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
#################################################################################

from abc import abstractmethod
from pathlib import Path
from typing import Optional

from pypaimon.api import Schema, Table
from pypaimon.api import Catalog
from pypaimon.pynative.common.exception import PyNativeNotImplementedError
from pypaimon.pynative.catalog.catalog_constant import CatalogConstants
from pypaimon.pynative.catalog.catalog_exception import DatabaseNotExistException, DatabaseAlreadyExistException, \
    TableAlreadyExistException, TableNotExistException
from pypaimon.pynative.catalog.catalog_option import CatalogOptions
from pypaimon.pynative.common.file_io import FileIO
from pypaimon.pynative.common.identifier import TableIdentifier
from pypaimon.pynative.table.core_option import CoreOptions


class AbstractCatalog(Catalog):
    def __init__(self, catalog_options: dict):
        if CatalogOptions.WAREHOUSE not in catalog_options:
           raise ValueError(f"Paimon '{CatalogOptions.WAREHOUSE}' path must be set")
        self.warehouse = Path(catalog_options.get(CatalogOptions.WAREHOUSE))
        self.catalog_options = catalog_options
        self.file_io = FileIO(self.warehouse, self.catalog_options)

    @staticmethod
    @abstractmethod
    def identifier() -> str:
        """Catalog Identifier"""

    @abstractmethod
    def allow_custom_table_path(self) -> bool:
        """Allow Custom Table Path"""

    @abstractmethod
    def create_database_impl(self, name: str, properties: Optional[dict] = None):
        """Create DataBase Implementation"""

    @abstractmethod
    def create_table_impl(self, table_identifier: TableIdentifier, schema: 'Schema'):
        """Create Table Implementation"""

    @abstractmethod
    def get_table_schema(self, table_identifier: TableIdentifier):
        """Get Table Schema"""

    @abstractmethod
    def lock_factory(self):
        """Lock Factory"""

    @abstractmethod
    def metastore_client_factory(self):
        """MetaStore Client Factory"""

    def get_table(self, identifier: str) -> Table:
        return self.get_table_impl(TableIdentifier(identifier))

    def get_table_impl(self, table_identifier: TableIdentifier) -> Table:
        from pypaimon.pynative.table.file_store_table import FileStoreTableFactory

        if CoreOptions.SCAN_FALLBACK_BRANCH in self.catalog_options:
            raise PyNativeNotImplementedError(CoreOptions.SCAN_FALLBACK_BRANCH)

        table_path = self.get_table_location(table_identifier)
        table_schema = self.get_table_schema(table_identifier)
        return FileStoreTableFactory.create(self.file_io, table_identifier, table_path, table_schema)

    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        try:
            self.get_database(name)
            if not ignore_if_exists:
                raise DatabaseAlreadyExistException(name)
        except DatabaseNotExistException:
            self.create_database_impl(name, properties)

    def create_table(self, identifier: str, schema: 'Schema', ignore_if_exists: bool):
        if schema.options and schema.options.get(CoreOptions.AUTO_CREATE):
            raise ValueError(f"The value of {CoreOptions.AUTO_CREATE} property should be False.")
        if schema.options and CoreOptions.PATH in schema.options and not self.allow_custom_table_path():
            raise ValueError(f"The current catalog does not support specifying the table path when creating a table.")

        table_identifier = TableIdentifier(identifier)
        self.get_database(table_identifier.get_database_name())
        try:
            self.get_table_impl(table_identifier)
            if not ignore_if_exists:
                raise TableAlreadyExistException(identifier)
        except TableNotExistException:
            if schema.options and CoreOptions.TYPE in schema.options and schema.options.get(CoreOptions.TYPE) != "table":
                raise PyNativeNotImplementedError(f"Table Type {schema.options.get(CoreOptions.TYPE)}")
            return self.create_table_impl(table_identifier, schema)

    def get_database_path(self, name):
        return self.warehouse / f"{name}{CatalogConstants.DB_SUFFIX}"

    def get_table_location(self, table_identifier: TableIdentifier):
        return self.get_database_path(table_identifier.get_database_name()) / table_identifier.get_table_name()
