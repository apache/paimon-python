from typing import Optional

from pypaimon.pynative.row.internal_row import InternalRow


class ProjectedRow:
    @classmethod
    def from_(cls, index_mapping: list[int]) -> 'ProjectedRow':
        instance = cls()
        instance.index_mapping = index_mapping
        return instance

    def replace_row(self, row: Optional['InternalRow']) -> Optional['InternalRow']:
        if row is None:
            return None
        return row.project(self.index_mapping)