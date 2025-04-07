from typing import Optional
import pyarrow as pa

from pypaimon.pynative.row.key_value import InternalRow
from pypaimon.pynative.row.columnar_row import ColumnarRow
from pypaimon.pynative.iterator.file_record_iterator import FileRecordIterator


class ColumnarRowIterator(FileRecordIterator[InternalRow]):
    """基于PyArrow RecordBatch的列式存储行迭代器"""

    def __init__(self, file_path: str, record_batch: pa.RecordBatch):
        self.file_path = file_path
        self._record_batch = record_batch
        self._row = ColumnarRow(record_batch)

        self.num_rows = record_batch.num_rows
        self.next_pos = 0
        self.next_file_pos = 0

    def file_path(self) -> str:
        """返回文件路径"""
        return self.file_path

    def release_batch(self):
        del self._record_batch

    def reset(self, next_file_pos: int):
        """重置迭代器状态"""
        self.next_pos = 0
        self.next_file_pos = next_file_pos

    def next(self) -> Optional[InternalRow]:
        """返回下一行数据"""
        if self.next_pos < self.num_rows:
            self._row.set_row_id(self.next_pos)
            self.next_pos += 1
            self.next_file_pos += 1
            return self._row
        return None

    def returned_position(self) -> int:
        """获取最后返回行的文件位置"""
        return self.next_file_pos - 1

    # def mapping(self,
    #             partition_info: Optional[PartitionInfo] = None,
    #             index_mapping: Optional[List[int]] = None) -> 'ColumnarRowIterator':
    #     """
    #     创建带有分区信息和索引映射的新迭代器
    #
    #     Args:
    #         partition_info: 分区信息
    #         index_mapping: 索引映射数组
    #
    #     Returns:
    #         新的ColumnarRowIterator实例或self
    #     """
    #     if partition_info is None and index_mapping is None:
    #         return self
    #
    #     # 获取当前的Schema和数组
    #     current_schema = self._record_batch.schema
    #     current_arrays = self._record_batch.columns
    #
    #     if partition_info is not None:
    #         # 添加分区列
    #         current_arrays, current_schema = self._add_partition_columns(
    #             current_arrays,
    #             current_schema,
    #             partition_info
    #         )
    #
    #     if index_mapping is not None:
    #         # 重新排序和选择列
    #         current_arrays = self._apply_index_mapping(current_arrays, index_mapping)
    #         current_schema = pa.schema([current_schema.field(i) for i in index_mapping])
    #
    #     # 创建新的RecordBatch
    #     new_batch = pa.RecordBatch.from_arrays(
    #         current_arrays,
    #         schema=current_schema
    #     )
    #
    #     # 创建新的迭代器
    #     new_iterator = ColumnarRowIterator(self.file_path, new_batch)
    #     new_iterator.reset(self.next_file_pos)
    #     return new_iterator
    #
    # def _add_partition_columns(self,
    #                            arrays: List[pa.Array],
    #                            schema: pa.Schema,
    #                            partition_info: PartitionInfo) -> tuple[List[pa.Array], pa.Schema]:
    #     """添加分区列"""
    #     new_arrays = list(arrays)
    #     new_fields = list(schema.fields)
    #
    #     for name, value, dtype in zip(
    #             partition_info.field_names,
    #             partition_info.field_values,
    #             partition_info.field_types
    #     ):
    #         # 创建常量数组
    #         const_array = pa.array([value] * self.num_rows, type=dtype)
    #         new_arrays.append(const_array)
    #         new_fields.append(pa.field(name, dtype))
    #
    #     return new_arrays, pa.schema(new_fields)
    #
    # def _apply_index_mapping(self,
    #                          arrays: List[pa.Array],
    #                          index_mapping: List[int]) -> List[pa.Array]:
    #     """应用索引映射"""
    #     return [arrays[i] for i in index_mapping]