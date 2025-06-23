from enum import Enum


class CatalogConstants(str, Enum):

    def __str__(self):
        return self.value

    SYSTEM_TABLE_SPLITTER = '$'
    SYSTEM_BRANCH_PREFIX = 'branch-'

    COMMENT_PROP = "comment"
    OWNER_PROP = "owner"

    DEFAULT_DATABASE = "default"
    DB_SUFFIX = ".db"
    DB_LOCATION_PROP = "location"
