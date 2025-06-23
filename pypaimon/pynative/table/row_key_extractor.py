import pyarrow as pa
import pyarrow.compute as pc
from typing import Tuple, List, Any
from abc import ABC, abstractmethod

from pypaimon.pynative.table.table_schema import TableSchema
from pypaimon.pynative.table.core_option import CoreOptions


class RowKeyExtractor(ABC):
    """Base class for extracting partition and bucket information from PyArrow data."""
    
    def __init__(self, table_schema: TableSchema):
        self.table_schema = table_schema
        self.partition_indices = self._get_field_indices(table_schema.partition_keys)
        
    def _get_field_indices(self, field_names: List[str]) -> List[int]:
        """Convert field names to indices for fast access."""
        if not field_names:
            return []
        field_map = {field.name: i for i, field in enumerate(self.table_schema.fields)}
        return [field_map[name] for name in field_names if name in field_map]
    
    def extract_partition_bucket_batch(self, table: pa.Table) -> Tuple[List[Tuple], List[int]]:
        """
        Extract partition and bucket for all rows in vectorized manner.
        Returns (partitions, buckets) where each partition is a tuple.
        """
        # Extract partition values for all rows
        partitions = self._extract_partitions_batch(table)
        
        # Extract buckets for all rows
        buckets = self._extract_buckets_batch(table)
        
        return partitions, buckets
    
    def _extract_partitions_batch(self, table: pa.Table) -> List[Tuple]:
        """Extract partition values for all rows."""
        if not self.partition_indices:
            return [() for _ in range(table.num_rows)]
        
        # Extract partition columns
        partition_columns = [table.column(i) for i in self.partition_indices]
        
        # Build partition tuples for each row
        partitions = []
        for row_idx in range(table.num_rows):
            partition_values = tuple(col[row_idx].as_py() for col in partition_columns)
            partitions.append(partition_values)
        
        return partitions
    
    @abstractmethod
    def _extract_buckets_batch(self, table: pa.Table) -> List[int]:
        """Extract bucket numbers for all rows. Must be implemented by subclasses."""
        pass
    
    def _compute_hash_for_fields(self, table: pa.Table, field_indices: List[int]) -> List[int]:
        """Compute hash values for specified fields across all rows."""
        columns = [table.column(i) for i in field_indices]
        hashes = []
        for row_idx in range(table.num_rows):
            row_values = tuple(col[row_idx].as_py() for col in columns)
            hashes.append(hash(row_values))
        return hashes


class FixedBucketRowKeyExtractor(RowKeyExtractor):
    """Fixed bucket mode extractor with configurable number of buckets."""
    
    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        self.num_buckets = table_schema.options.get(CoreOptions.BUCKET, -1)
        if self.num_buckets <= 0:
            raise ValueError(f"Fixed bucket mode requires bucket > 0, got {self.num_buckets}")
        
        bucket_key_option = table_schema.options.get(CoreOptions.BUCKET_KEY, '')
        if bucket_key_option.strip():
            self.bucket_keys = [k.strip() for k in bucket_key_option.split(',')]
        else:
            self.bucket_keys = [pk for pk in table_schema.primary_keys 
                               if pk not in table_schema.partition_keys]
        
        self.bucket_key_indices = self._get_field_indices(self.bucket_keys)
    
    def _extract_buckets_batch(self, table: pa.Table) -> List[int]:
        """Extract bucket numbers for all rows using bucket keys."""
        hash_values = self._compute_hash_for_fields(table, self.bucket_key_indices)
        return [abs(hash_val) % self.num_buckets for hash_val in hash_values]


class DynamicBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for dynamic bucket mode (bucket = -1)."""
    
    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        bucket_option = table_schema.options.get(CoreOptions.BUCKET, -1)
        
        if bucket_option != -1:
            raise ValueError(f"Dynamic bucket mode requires bucket = -1, got {bucket_option}")
    
    def _extract_buckets_batch(self, table: pa.Table) -> List[int]:
        """Dynamic bucket mode: bucket assignment is handled by the system."""
        # For now, return 0 for all rows - actual implementation would use IndexMaintainer
        return [0] * table.num_rows


class UnawareBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for unaware bucket mode (bucket = -1, no primary keys)."""
    
    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        bucket_option = table_schema.options.get(CoreOptions.BUCKET, -1)
        
        if bucket_option != -1:
            raise ValueError(f"Unaware bucket mode requires bucket = -1, got {bucket_option}")
    
    def _extract_buckets_batch(self, table: pa.Table) -> List[int]:
        return [0] * table.num_rows


def create_row_key_extractor(table_schema: TableSchema) -> RowKeyExtractor:
    """Factory method to create the appropriate RowKeyExtractor based on table configuration."""
    bucket_option = table_schema.options.get(CoreOptions.BUCKET, -1)
    has_primary_keys = bool(table_schema.primary_keys)
    
    if bucket_option > 0:
        # Fixed bucket mode
        return FixedBucketRowKeyExtractor(table_schema)
    elif bucket_option == -1:
        if has_primary_keys:
            # Dynamic bucket mode (for primary key tables)
            return DynamicBucketRowKeyExtractor(table_schema)
        else:
            # Unaware bucket mode (for append-only tables)
            return UnawareBucketRowKeyExtractor(table_schema)
    else:
        raise ValueError(f"Invalid bucket configuration: {bucket_option}")