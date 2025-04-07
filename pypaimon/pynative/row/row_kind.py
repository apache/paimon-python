from enum import Enum


class RowKind(Enum):
    """
    Insertion operation.
    """
    INSERT = 0

    """
    Update operation with the previous content of the updated row.
    
    <p>This kind SHOULD occur together with {@link #UPDATE_AFTER} for modelling an update that
    needs to retract the previous row first. It is useful in cases of a non-idempotent update,
    i.e., an update of a row that is not uniquely identifiable by a key."""
    UPDATE_BEFORE = 1  # -U: Update operation with the previous content of the updated row
    UPDATE_AFTER = 2  # +U: Update operation with new content of the updated row
    DELETE = 3  # -D: Deletion operation

    def is_add(self) -> bool:
        return self in (RowKind.INSERT, RowKind.UPDATE_AFTER)

    def to_string(self) -> str:
        if self == RowKind.INSERT:
            return "+I"
        elif self == RowKind.UPDATE_BEFORE:
            return "-U"
        elif self == RowKind.UPDATE_AFTER:
            return "+U"
        elif self == RowKind.DELETE:
            return "-D"
        else:
            return "??"

    @staticmethod
    def from_string(kind_str: str) -> 'RowKind':
        """从字符串解析行类型"""
        if kind_str == "+I":
            return RowKind.INSERT
        elif kind_str == "-U":
            return RowKind.UPDATE_BEFORE
        elif kind_str == "+U":
            return RowKind.UPDATE_AFTER
        elif kind_str == "-D":
            return RowKind.DELETE
        else:
            raise ValueError(f"Unknown row kind string: {kind_str}")

