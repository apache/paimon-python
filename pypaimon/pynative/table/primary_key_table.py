from pypaimon.pynative.table.file_store_table import FileStoreTable
from pypaimon.pynative.table.bucket_mode import BucketMode
from pypaimon.pynative.table.core_option import CoreOptions
from pypaimon.pynative.writer.key_value_file_store_write import KeyValueFileStoreWrite


class PrimaryKeyTable(FileStoreTable):

    def bucket_mode(self) -> BucketMode:
        cross_partition_update = self.table_schema.cross_partition_update()
        if cross_partition_update:
            return BucketMode.CROSS_PARTITION
        bucket_num = self.options.get(CoreOptions.BUCKET, -1)
        if bucket_num == -1:
            return BucketMode.HASH_DYNAMIC
        else:
            return BucketMode.HASH_FIXED

    def new_write(self, commit_user: str):
        return KeyValueFileStoreWrite(self, commit_user)