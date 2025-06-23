from enum import Enum


class CatalogOptions(str, Enum):

    def __str__(self):
        return self.value

    WAREHOUSE = "warehouse"
    METASTORE = "metastore"
    URI = "uri"
    TABLE_TYPE = "table.type"
    LOCK_ENABLED = "lock.enabled"
    LOCK_TYPE = "lock.type"
    LOCK_CHECK_MAX_SLEEP = "lock-check-max-sleep"
    LOCK_ACQUIRE_TIMEOUT = "lock-acquire-timeout"
    CLIENT_POOL_SIZE = "client-pool-size"
    CACHE_ENABLED = "cache-enabled"
    CACHE_EXPIRATION_INTERVAL = "cache.expiration-interval"
    CACHE_PARTITION_MAX_NUM = "cache.partition.max-num"
    CACHE_MANIFEST_SMALL_FILE_MEMORY = "cache.manifest.small-file-memory"
    CACHE_MANIFEST_SMALL_FILE_THRESHOLD = "cache.manifest.small-file-threshold"
    CACHE_MANIFEST_MAX_MEMORY = "cache.manifest.max-memory"
    CACHE_SNAPSHOT_MAX_NUM_PER_TABLE = "cache.snapshot.max-num-per-table"
    CASE_SENSITIVE = "case-sensitive"
    SYNC_ALL_PROPERTIES = "sync-all-properties"
    FORMAT_TABLE_ENABLED = "format-table.enabled"

