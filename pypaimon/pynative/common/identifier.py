from pypaimon.pynative.common.exception import PyNativeNotImplementedError
from pypaimon.pynative.catalog.catalog_constant import CatalogConstants


class TableIdentifier:
    def __init__(self, full_name: str):
        self._full_name = full_name
        self._system_table = None
        self._branch = None
        spliter = CatalogConstants.SYSTEM_TABLE_SPLITTER
        prefix = CatalogConstants.SYSTEM_BRANCH_PREFIX

        parts = full_name.split('.')
        if len(parts) == 2:
            self._database = parts[0]
            self._object = parts[1]
        else:
            raise ValueError(f"Cannot get splits from '{full_name}' to get database and object")
    
        splits = self._object.split(spliter)
        if len(splits) == 1:
            self._table = self._object
        elif len(splits) == 2:
            self._table = splits[0]
            if splits[1].startswith(prefix):
                self._branch = splits[1][len(prefix):]
            else:
                self._system_table = splits[1]
        elif len(splits) == 3:
            if not splits[1].startswith(prefix):
                raise ValueError \
                    (f"System table can only contain one '{spliter}' separator, but this is: {self._object}")
            self._table = splits[0]
            self._branch = splits[1][len(prefix):]
            self._system_table = splits[2]
        else:
            raise ValueError(f"Invalid object name: {self._object}")

        if self._system_table is not None:
            raise PyNativeNotImplementedError("SystemTable")
        elif self._branch is not None:
            raise PyNativeNotImplementedError("BranchTable")

    def get_database_name(self):
        return self._database

    def get_table_name(self):
        return self._table

    def get_full_name(self):
        return self._full_name