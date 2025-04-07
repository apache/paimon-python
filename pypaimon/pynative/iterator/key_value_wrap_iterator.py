from typing import Optional

from pypaimon.pynative.row.key_value import KeyValue
from pypaimon.pynative.row.offset_row import OffsetRow
from pypaimon.pynative.row.row_kind import RowKind
from pypaimon.pynative.iterator.file_record_iterator import FileRecordIterator


class KeyValueWrapIterator(FileRecordIterator):

    def __init__(
            self,
            iterator: FileRecordIterator,
            key_arity: int,
            value_arity: int,
            level: int
    ):
        self.iterator = iterator
        self.key_arity = key_arity
        self.value_arity = value_arity
        self.level = level

        # 创建可重用的OffsetRow对象，包含arity信息
        self.reused_key = OffsetRow(None, 0, key_arity)
        self.reused_value = OffsetRow(None, key_arity + 2, value_arity)  # 跳过sequence_number和value_kind

    # ------- 实际数据例子 -------
    # _KEY_student_id: 003
    # _SEQUENCE_NUMBER: 2
    # _VALUE_KIND: 0
    # student_id: 003
    # name: 小黄
    # age: 20

    def next(self) -> Optional[KeyValue]:
        row = self.iterator.next()
        if row is None:
            return None

        # 使用OffsetRow获取字段
        self.reused_key.replace(row)
        self.reused_value.replace(row)

        # 获取sequence_number和value_kind
        sequence_number = row.get_field(self.key_arity)
        value_kind = RowKind(row.get_field(self.key_arity + 1))

        # 创建KeyValue对象
        return KeyValue(
            key=self.reused_key,
            sequence_number=sequence_number,
            value_kind=value_kind,
            value=self.reused_value
        ).set_level(self.level)

    def returned_position(self) -> int:
        return self.iterator.returned_position()

    def file_path(self) -> str:
        return self.iterator.file_path()

    def release_batch(self):
        self.iterator.release_batch()
