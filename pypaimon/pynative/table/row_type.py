from pypaimon.api.row_type import RowType
from pypaimon.pynative.table.data_field import DataField


class RowTypeImpl(RowType):
    def __init__(self, fields: list[DataField]):
        self.fields = fields

    def as_arrow(self) -> "pa.Schema":
        pass
