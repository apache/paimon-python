import pyarrow as pa
import uuid
from typing import Dict, Tuple, Optional, List
from abc import ABC, abstractmethod

from pypaimon.api import CommitMessage


class CommitMessageImpl(CommitMessage):
    """Python implementation of CommitMessage"""

    def __init__(self, partition: Tuple, bucket: int, new_files: List[str]):
        self._partition = partition
        self._bucket = bucket
        self._new_files = new_files or []

    def partition(self) -> Tuple:
        """Get the partition of this commit message."""
        return self._partition

    def bucket(self) -> int:
        """Get the bucket of this commit message."""
        return self._bucket

    def new_files(self) -> List[str]:
        """Get the list of new files."""
        return self._new_files

    def is_empty(self):
        return not self._new_files