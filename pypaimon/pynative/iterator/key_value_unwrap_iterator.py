from typing import Optional, Any

from pypaimon.pynative.iterator.record_iterator import RecordIterator
from pypaimon.pynative.row.key_value import KeyValue
from pypaimon.pynative.row.row_kind import RowKind


class KeyValueUnwrapIterator(RecordIterator):
    """
    一个RecordIterator，它将KeyValue映射为其value部分的InternalRow，
    同时管理RowKind的重置
    """

    def __init__(self, kv_iterator: RecordIterator):
        self.kv_iterator = kv_iterator
        self.key_value: KeyValue = None
        self.pre_value_row_kind: RowKind = None

    def next(self) -> Optional[Any]:
        """获取下一条记录"""
        # 重置上一个KeyValue的RowKind
        if self.key_value is not None:
            self.key_value.value.set_row_kind(self.pre_value_row_kind)

        # 获取下一个KeyValue
        self.key_value = self.kv_iterator.next()
        if self.key_value is None:
            return None

        # 保存原始value的RowKind
        row_data = self.key_value.value
        self.pre_value_row_kind = row_data.get_row_kind()

        # 获取value部分并设置其RowKind为KeyValue的valueKind
        row_data.set_row_kind(self.key_value.value_kind)
        return row_data

    def release_batch(self) -> None:
        """释放批次资源"""
        self.kv_iterator.release_batch()