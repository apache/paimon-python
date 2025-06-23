from typing import List, Optional

from pypaimon.api import ReadBuilder, PredicateBuilder, TableRead, TableScan, Predicate
from pypaimon.api.row_type import RowType
from pypaimon.pynative.reader.table_scan_impl import TableScanImpl
from pypaimon.pynative.reader.table_read_impl import TableReadImpl
from pypaimon.pynative.reader.predicate_builder_impl import PredicateBuilderImpl


class ReadBuilderImpl(ReadBuilder):
    """Implementation of ReadBuilder for native Python reading."""
    
    def __init__(self, table: 'FileStoreTable'):
        self.table = table
        self._predicate: Optional[Predicate] = None
        self._projection: Optional[List[str]] = None
        self._limit: Optional[int] = None
        
    def with_filter(self, predicate: Predicate) -> 'ReadBuilder':
        """Push filters to be applied during reading."""
        self._predicate = predicate
        return self
        
    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        """Push column projection to reduce data transfer."""
        self._projection = projection
        return self
        
    def with_limit(self, limit: int) -> 'ReadBuilder':
        """Push row limit for optimization."""
        self._limit = limit
        return self
        
    def new_scan(self) -> TableScan:
        """Create a TableScan to perform batch planning."""
        return TableScanImpl(
            table=self.table,
            predicate=self._predicate,
            projection=self._projection,
            limit=self._limit
        )
        
    def new_read(self) -> TableRead:
        """Create a TableRead to read splits."""
        return TableReadImpl(
            table=self.table,
            predicate=self._predicate,
            projection=self._projection
        )
        
    def new_predicate_builder(self) -> PredicateBuilder:
        """Create a builder for Predicate using PyNativePredicate."""
        return PredicateBuilderImpl(self.table.table_schema)
        
    def read_type(self) -> RowType:
        """Return the row type after applying projection."""
        if self._projection:
            # Filter schema fields based on projection
            schema_fields = self.table.table_schema.fields
            projected_fields = []
            
            for field_name in self._projection:
                for field in schema_fields:
                    if field.name == field_name:
                        projected_fields.append(field)
                        break
                        
            return RowType(projected_fields)
        else:
            # Return full schema
            return RowType(self.table.table_schema.fields)
