from typing import Optional

from pypaimon.pynative.row.internal_row import InternalRow


class CastedRow:
    @classmethod
    def from_(cls, cast_mapping: list['CastFieldGetter']) -> 'CastedRow':
        instance = cls()
        instance.cast_mapping = cast_mapping
        return instance

    def replace_row(self, row: Optional['InternalRow']) -> Optional['InternalRow']:
        if row is None:
            return None
        return row.cast(self.cast_mapping)
