# from typing import List, Optional
# from dataclasses import dataclass
#
#
# @dataclass
# class PartitionInfo:
#     """分区信息类，用于处理分区字段的映射和值获取"""
#
#     def __init__(self, mapping: List[int], partition_type: 'RowType', partition_row: 'BinaryRow'):
#         """
#         Args:
#             mapping: 字段映射数组。
#                     - 如果mapping[i] > 0，表示字段在数据行中的位置
#                     - 如果mapping[i] < 0，表示字段在分区中的位置（需要从partition_row获取）
#             partition_type: 分区字段的类型信息
#             partition_row: 包含分区值的二进制行
#         """
#         self.mapping = mapping
#         self.partition_type = partition_type
#         self.partition_row = partition_row
#
#     @classmethod
#     def from_(cls, mapping: List[int], partition_type: 'RowType',
#               partition_row: 'BinaryRow') -> 'PartitionInfo':
#         """创建PartitionInfo实例"""
#         return cls(mapping, partition_type, partition_row)
#
#     def size(self) -> int:
#         """返回总字段数（包括数据字段和分区字段）"""
#         return len(self.mapping)
#
#     def in_partition_row(self, pos: int) -> bool:
#         """判断指定位置的字段是否是分区字段
#
#         Args:
#             pos: 字段位置
#
#         Returns:
#             如果是分区字段返回True，否则返回False
#         """
#         return self.mapping[pos] < 0
#
#     def get_real_index(self, pos: int) -> int:
#         """获取字段的实际索引
#
#         Args:
#             pos: 字段位置
#
#         Returns:
#             - 如果是数据字段，返回在数据行中的位置（mapping[pos] - 1）
#             - 如果是分区字段，返回在分区行中的位置（abs(mapping[pos]) - 1）
#         """
#         mapped = self.mapping[pos]
#         return abs(mapped) - 1
#
#     def get_type(self, pos: int) -> 'DataType':
#         """获取指定位置字段的数据类型
#
#         Args:
#             pos: 字段位置
#
#         Returns:
#             字段的DataType
#         """
#         if not self.in_partition_row(pos):
#             raise ValueError(f"Position {pos} is not in partition row")
#         return self.partition_type.get_type_at(self.get_real_index(pos))
#
#     def get_partition_row(self) -> 'BinaryRow':
#         """获取分区行数据"""
#         return self.partition_row
#
#
# class PartitionUtils:
#     """分区工具类"""
#
#     @staticmethod
#     def construct_partition_mapping(data_schema: 'TableSchema',
#                                     data_fields: List['DataField']) -> tuple:
#         """构造分区映射
#
#         Args:
#             data_schema: 表结构
#             data_fields: 数据字段列表
#
#         Returns:
#             (partition_mapping, fields_without_partition) 元组
#             - partition_mapping: (mapping数组, 分区RowType)或None
#             - fields_without_partition: 不包含分区字段的字段列表
#         """
#         if not data_schema.partition_keys():
#             return None, data_fields
#
#         partition_names = data_schema.partition_keys()
#         fields_without_partition = []
#
#         # 构建映射数组
#         mapping = [0] * (len(data_fields) + 1)
#         p_count = 0
#
#         for i, field in enumerate(data_fields):
#             if field.name() in partition_names:
#                 # 分区字段用负数表示
#                 mapping[i] = -(partition_names.index(field.name()) + 1)
#                 p_count += 1
#             else:
#                 # 非分区字段用正数表示
#                 mapping[i] = (i - p_count) + 1
#                 fields_without_partition.append(field)
#
#         # 如果没有分区字段，返回None
#         if len(fields_without_partition) == len(data_fields):
#             return None, data_fields
#
#         # 创建分区类型
#         partition_type = data_schema.projected_logical_row_type(data_schema.partition_keys())
#
#         return (mapping, partition_type), fields_without_partition
#
#     @staticmethod
#     def create(partition_pair: Optional[tuple], binary_row: 'BinaryRow') -> Optional[PartitionInfo]:
#         """创建PartitionInfo实例
#
#         Args:
#             partition_pair: (mapping数组, 分区RowType)元组或None
#             binary_row: 分区数据行
#
#         Returns:
#             PartitionInfo实例或None
#         """
#         if partition_pair is None:
#             return None
#         mapping, row_type = partition_pair
#         return PartitionInfo(mapping, row_type, binary_row)