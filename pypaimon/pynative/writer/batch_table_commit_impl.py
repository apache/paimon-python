import time
import uuid
from typing import List, Optional, Dict, Any

from pypaimon.api import BatchTableCommit, CommitMessage
from pypaimon.pynative.writer.file_store_commit import FileStoreCommit


class BatchTableCommitImpl(BatchTableCommit):
    """Python implementation of BatchTableCommit for batch writing scenarios.
    
    This is a simplified version compared to Java's TableCommitImpl, focusing on
    batch commit functionality without streaming features like:
    - Complex conflict checking
    - Partition expiration
    - Tag auto management
    - Consumer management
    """
    
    def __init__(self, table: 'FileStoreTable', commit_user: str, static_partition: Optional[dict]):
        self.table = table
        self.commit_user = commit_user
        self.overwrite_partition = static_partition
        self.file_store_commit = FileStoreCommit(table, commit_user)
        self.batch_committed = False

    def commit(self, commit_messages: List[CommitMessage]):
        self._check_committed()

        non_empty_messages = [msg for msg in commit_messages if not msg.is_empty()]
        if not non_empty_messages:
            return

        commit_identifier = int(time.time() * 1000)

        try:
            if self.overwrite_partition is not None:
                self.file_store_commit.overwrite(
                    partition=self.overwrite_partition,
                    commit_messages=non_empty_messages,
                    commit_identifier=commit_identifier
                )
            else:
                self.file_store_commit.commit(
                    commit_messages=non_empty_messages,
                    commit_identifier=commit_identifier
                )
        except Exception as e:
            self.file_store_commit.abort(commit_messages)
            raise RuntimeError(f"Failed to commit: {str(e)}") from e

    def abort(self, commit_messages: List[CommitMessage]):
        self.file_store_commit.abort(commit_messages)

    def close(self):
        if hasattr(self, 'file_store_commit'):
            self.file_store_commit.close()
    
    def _check_committed(self):
        if self.batch_committed:
            raise RuntimeError("BatchTableCommit only supports one-time committing.")
        self.batch_committed = True
