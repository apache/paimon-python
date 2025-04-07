from typing import Optional

from pypaimon.pynative.row.internal_row import InternalRow


class PartitionSettedRow(InternalRow):
    @classmethod
    def from_(cls, partition_info: 'PartitionInfo') -> 'PartitionSettedRow':
        instance = cls()
        instance.partition_info = partition_info
        return instance

    def replace_row(self, row: Optional['InternalRow']) -> Optional['InternalRow']:
        if row is None:
            return None
        return self.partition_info.apply_to_row(row)