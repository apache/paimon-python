from typing import List, Optional
from dataclasses import dataclass


@dataclass
class PartitionInfo:

    def __init__(self, mapping: List[int], partition_type: 'RowType', partition_row: 'BinaryRow'):
        self.mapping = mapping
        self.partition_type = partition_type
        self.partition_row = partition_row

    @classmethod
    def from_(cls, mapping: List[int], partition_type: 'RowType',
              partition_row: 'BinaryRow') -> 'PartitionInfo':
        return cls(mapping, partition_type, partition_row)

    def size(self) -> int:
        return len(self.mapping)

    def in_partition_row(self, pos: int) -> bool:
        return self.mapping[pos] < 0

    def get_real_index(self, pos: int) -> int:
        mapped = self.mapping[pos]
        return abs(mapped) - 1

    def get_type(self, pos: int) -> 'DataType':
        if not self.in_partition_row(pos):
            raise ValueError(f"Position {pos} is not in partition row")
        return self.partition_type.get_type_at(self.get_real_index(pos))

    def get_partition_row(self) -> 'BinaryRow':
        return self.partition_row


class PartitionUtils:

    @staticmethod
    def construct_partition_mapping(data_schema: 'TableSchema',
                                    data_fields: List['DataField']) -> tuple:
        if not data_schema.partition_keys():
            return None, data_fields

        partition_names = data_schema.partition_keys()
        fields_without_partition = []

        mapping = [0] * (len(data_fields) + 1)
        p_count = 0

        for i, field in enumerate(data_fields):
            if field.name() in partition_names:
                mapping[i] = -(partition_names.index(field.name()) + 1)
                p_count += 1
            else:
                mapping[i] = (i - p_count) + 1
                fields_without_partition.append(field)

        if len(fields_without_partition) == len(data_fields):
            return None, data_fields

        partition_type = data_schema.projected_logical_row_type(data_schema.partition_keys())

        return (mapping, partition_type), fields_without_partition

    @staticmethod
    def create(partition_pair: Optional[tuple], binary_row: 'BinaryRow') -> Optional[PartitionInfo]:
        if partition_pair is None:
            return None
        mapping, row_type = partition_pair
        return PartitionInfo(mapping, row_type, binary_row)