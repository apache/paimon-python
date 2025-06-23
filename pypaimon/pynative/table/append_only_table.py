from pypaimon.pynative.table.bucket_mode import BucketMode
from pypaimon.pynative.table.core_option import CoreOptions
from pypaimon.pynative.table.file_store_table import FileStoreTable
from pypaimon.pynative.writer.append_only_file_store_write import AppendOnlyFileStoreWrite


class AppendOnlyTable(FileStoreTable):

    def bucket_mode(self) -> BucketMode:
        return BucketMode.BUCKET_UNAWARE if self.options.get(CoreOptions.BUCKET, -1) == -1 else BucketMode.HASH_FIXED

    def new_write(self, commit_user: str):
        return AppendOnlyFileStoreWrite(self, commit_user)
