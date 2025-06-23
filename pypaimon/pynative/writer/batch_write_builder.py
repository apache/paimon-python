import uuid

from typing import Optional

from pypaimon.api import BatchTableWrite, BatchWriteBuilder, BatchTableCommit
from pypaimon.pynative.table.core_option import CoreOptions
from pypaimon.pynative.writer.batch_table_commit_impl import BatchTableCommitImpl
from pypaimon.pynative.writer.batch_table_write_impl import BatchTableWriteImpl


class BatchWriteBuilderImpl(BatchWriteBuilder):
    def __init__(self, table: 'FileStoreTable'):
        self.static_partition = None
        self.table = table
        self.commit_user = self._create_commit_user()

    def overwrite(self, static_partition: Optional[dict] = None) -> BatchWriteBuilder:
        self.static_partition = static_partition
        return self

    def new_write(self) -> BatchTableWrite:
        return BatchTableWriteImpl(self.table, self.commit_user)

    def new_commit(self) -> BatchTableCommit:
        commit = BatchTableCommitImpl(self.table, self.commit_user, self.static_partition)
        return commit

    def _create_commit_user(self):
        if CoreOptions.COMMIT_USER_PREFIX in self.table.options:
            return f"{self.table.options.get(CoreOptions.COMMIT_USER_PREFIX)}_{uuid.uuid4()}"
        else:
            return str(uuid.uuid4())
