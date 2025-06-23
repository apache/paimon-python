from typing import List, Optional, TYPE_CHECKING
import pandas as pd
import pyarrow as pa

from pypaimon.api import TableRead, Split, Predicate
from pypaimon.pynative.reader.split_impl import SplitImpl
from pypaimon.pynative.reader.pyarrow_dataset_reader import PyArrowDatasetReader
from pypaimon.pynative.reader.row.internal_row import InternalRow

if TYPE_CHECKING:
    import ray
    from duckdb.duckdb import DuckDBPyConnection


class TableReadImpl(TableRead):
    """Implementation of TableRead for native Python reading."""
    
    def __init__(self, table: 'FileStoreTable', predicate: Optional[Predicate] = None,
                 projection: Optional[List[str]] = None):
        self.table = table
        self.predicate = predicate
        self.projection = projection
        self.primary_keys = table.primary_keys if hasattr(table, 'primary_keys') else []
        self.is_primary_key_table = bool(self.primary_keys)
        
    def to_arrow(self, splits: List[Split]) -> pa.Table:
        """Read data from splits and convert to pyarrow.Table format."""
        tables = []
        
        for split in splits:
            if isinstance(split, SplitImpl):
                if self.is_primary_key_table:
                    # Primary Key table: need merge logic
                    table = self._read_primary_key_split(split)
                else:
                    # Append Only table: direct file reading
                    table = self._read_append_only_split(split)
                    
                if table is not None:
                    tables.append(table)
                        
        if not tables:
            # Return empty table with proper schema
            schema = self._get_arrow_schema()
            return pa.Table.from_arrays([], schema=schema)
            
        # Concatenate all tables
        return pa.concat_tables(tables)
        
    def _read_append_only_split(self, split: SplitImpl) -> Optional[pa.Table]:
        """Read an append-only split by directly reading files."""
        file_batches = []
        file_paths = split.get_file_paths_list()
        
        for file_path in file_paths:
            # Determine file format from extension
            if file_path.endswith('.parquet'):
                format_type = 'parquet'
            elif file_path.endswith('.orc'):
                format_type = 'orc'
            else:
                continue  # Skip unsupported formats
                
            # Create reader for this file
            reader = PyArrowDatasetReader(
                format=format_type,
                file_path=file_path,
                batch_size=1024,  # Default batch size
                projection=self.projection,
                predicate=self.predicate,
                primary_keys=[]  # No primary keys for append-only
            )
            
            # Read all batches from this file
            try:
                while True:
                    batch_iterator = reader.read_batch()
                    if batch_iterator is None:
                        break
                        
                    # Convert iterator to arrow batch
                    batch = self._iterator_to_arrow_batch(batch_iterator)
                    if batch is not None:
                        file_batches.append(batch)
                        
            finally:
                reader.close()
                
        if file_batches:
            return pa.Table.from_batches(file_batches)
        return None
        
    def _read_primary_key_split(self, split: SplitImpl) -> Optional[pa.Table]:
        """Read a primary key split using merge logic."""
        from pypaimon.pynative.reader.key_value_wrap_reader import KeyValueWrapReader
        from pypaimon.pynative.reader.key_value_unwrap_reader import KeyValueUnwrapReader
        from pypaimon.pynative.reader.sort_merge_reader import SortMergeReader
        from pypaimon.pynative.reader.drop_delete_reader import DropDeleteReader
        from pypaimon.pynative.reader.data_file_record_reader import DataFileRecordReader
        
        file_paths = split.get_file_paths_list()
        if not file_paths:
            return None
            
        # Create readers for each file
        kv_readers = []
        for file_path in file_paths:
            # Determine file format from extension
            if file_path.endswith('.parquet'):
                format_type = 'parquet'
            elif file_path.endswith('.orc'):
                format_type = 'orc'
            else:
                continue  # Skip unsupported formats
                
            # Create PyArrow reader
            arrow_reader = PyArrowDatasetReader(
                format=format_type,
                file_path=file_path,
                batch_size=1024,
                projection=None,  # Don't project at file level for PK tables
                predicate=self.predicate,
                primary_keys=self.primary_keys
            )
            
            # Wrap with DataFileRecordReader
            data_reader = DataFileRecordReader(arrow_reader)
            
            # Wrap with KeyValueWrapReader to convert InternalRow to KeyValue
            # TODO: Get proper key_arity and value_arity from table schema
            key_arity = len(self.primary_keys)
            value_arity = len(self.table.table_schema.fields) - key_arity - 2  # -2 for seq and rowkind
            kv_reader = KeyValueWrapReader(data_reader, level=0, 
                                         key_arity=key_arity, value_arity=value_arity)
            kv_readers.append(kv_reader)
            
        if not kv_readers:
            return None
            
        # Use SortMergeReader to merge files by primary key
        merge_reader = SortMergeReader(kv_readers, self.primary_keys)
        
        # Drop delete records
        drop_delete_reader = DropDeleteReader(merge_reader)
        
        # Unwrap KeyValue back to InternalRow
        unwrap_reader = KeyValueUnwrapReader(drop_delete_reader)
        
        # Read all data and convert to Arrow
        all_rows = []
        try:
            while True:
                batch_iterator = unwrap_reader.read_batch()
                if batch_iterator is None:
                    break
                    
                while True:
                    row = batch_iterator.next()
                    if row is None:
                        break
                    all_rows.append(row)
                        
                batch_iterator.release_batch()
                
        finally:
            unwrap_reader.close()
            
        if not all_rows:
            return None
            
        # Convert InternalRows to Arrow Table
        return self._internal_rows_to_arrow_table(all_rows)
        
    def to_arrow_batch_reader(self, splits: List[Split]) -> pa.RecordBatchReader:
        """Read data from splits and convert to pyarrow.RecordBatchReader format."""
        # Convert to table first, then to batch reader
        table = self.to_arrow(splits)
        return table.to_reader()
        
    def to_pandas(self, splits: List[Split]) -> pd.DataFrame:
        """Read data from splits and convert to pandas.DataFrame format."""
        arrow_table = self.to_arrow(splits)
        return arrow_table.to_pandas()
        
    def to_duckdb(self, splits: List[Split], table_name: str,
                  connection: Optional["DuckDBPyConnection"] = None) -> "DuckDBPyConnection":
        """Convert splits into an in-memory DuckDB table which can be queried."""
        try:
            import duckdb
        except ImportError:
            raise ImportError("DuckDB is not installed. Please install it with: pip install duckdb")
            
        if connection is None:
            connection = duckdb.connect()
            
        # Convert to Arrow table first
        arrow_table = self.to_arrow(splits)
        
        # Register the table in DuckDB
        connection.register(table_name, arrow_table)
        
        return connection
        
    def to_ray(self, splits: List[Split]) -> "ray.data.dataset.Dataset":
        """Convert splits into a Ray dataset format."""
        try:
            import ray
        except ImportError:
            raise ImportError("Ray is not installed. Please install it with: pip install ray")
            
        # Convert to Arrow table first
        arrow_table = self.to_arrow(splits)
        
        # Create Ray dataset from Arrow table
        return ray.data.from_arrow(arrow_table)
        
    def _iterator_to_arrow_batch(self, iterator) -> Optional[pa.RecordBatch]:
        """Convert a record iterator to an Arrow RecordBatch."""
        try:
            # For ColumnarRowIterator, we can get the underlying RecordBatch
            if hasattr(iterator, 'record_batch'):
                return iterator.record_batch
            else:
                # For other iterators, we need to collect rows and convert
                rows = []
                while iterator.has_next():
                    row = iterator.next()
                    if row is not None:
                        rows.append(row)
                        
                if not rows:
                    return None
                    
                # Convert rows to Arrow batch
                # This is a simplified implementation
                # In practice, you'd need proper schema conversion
                return self._rows_to_arrow_batch(rows)
                
        except Exception as e:
            print(f"Error converting iterator to Arrow batch: {e}")
            return None
            
    def _rows_to_arrow_batch(self, rows: List[InternalRow]) -> Optional[pa.RecordBatch]:
        """Convert internal rows to Arrow RecordBatch."""
        if not rows:
            return None
            
        # This is a simplified implementation
        # In practice, you'd need to properly convert InternalRow objects
        # to Arrow format based on the table schema
        
        # For now, return None to indicate conversion is not yet implemented
        return None
        
    def _internal_rows_to_arrow_table(self, rows: List[InternalRow]) -> Optional[pa.Table]:
        """Convert internal rows to Arrow Table."""
        if not rows:
            return None
            
        # Get table schema
        table_schema = self.table.table_schema
        
        # Build data columns
        data_columns = []
        field_names = []
        
        # Apply projection if specified
        if self.projection:
            projected_fields = []
            for field_name in self.projection:
                for field in table_schema.fields:
                    if field.name == field_name:
                        projected_fields.append(field)
                        break
            fields_to_use = projected_fields
        else:
            fields_to_use = table_schema.fields
            
        for i, field in enumerate(fields_to_use):
            field_name = field.name
            field_names.append(field_name)
            
            # Extract column data from rows
            column_data = []
            for row in rows:
                if hasattr(row, 'get_field'):
                    # Find the field index in the original schema
                    field_index = None
                    for j, orig_field in enumerate(table_schema.fields):
                        if orig_field.name == field_name:
                            field_index = j
                            break
                    
                    if field_index is not None:
                        value = row.get_field(field_index)
                        column_data.append(value)
                    else:
                        column_data.append(None)
                else:
                    column_data.append(None)
                    
            data_columns.append(column_data)
            
        # Convert to Arrow Table
        try:
            # Create Arrow arrays
            arrow_arrays = []
            for i, column_data in enumerate(data_columns):
                field = fields_to_use[i]
                arrow_type = self._convert_field_type_to_arrow(field.type)
                arrow_array = pa.array(column_data, type=arrow_type)
                arrow_arrays.append(arrow_array)
                
            # Create schema
            arrow_fields = []
            for i, field in enumerate(fields_to_use):
                arrow_type = self._convert_field_type_to_arrow(field.type)
                arrow_field = pa.field(field.name, arrow_type)
                arrow_fields.append(arrow_field)
                
            arrow_schema = pa.schema(arrow_fields)
            
            return pa.Table.from_arrays(arrow_arrays, schema=arrow_schema)
            
        except Exception as e:
            print(f"Error converting rows to Arrow table: {e}")
            return None
            
    def _convert_field_type_to_arrow(self, field_type) -> pa.DataType:
        """Convert Paimon field type to Arrow type."""
        # This is a simplified conversion
        # You'll need to implement proper type mapping based on your DataType structure
        
        if hasattr(field_type, 'type_name'):
            type_name = field_type.type_name.upper()
        else:
            type_name = str(field_type).upper()
            
        if 'INT' in type_name:
            return pa.int32()
        elif 'BIGINT' in type_name or 'LONG' in type_name:
            return pa.int64()
        elif 'FLOAT' in type_name:
            return pa.float32()
        elif 'DOUBLE' in type_name:
            return pa.float64()
        elif 'BOOLEAN' in type_name or 'BOOL' in type_name:
            return pa.bool_()
        elif 'STRING' in type_name or 'VARCHAR' in type_name:
            return pa.string()
        elif 'BINARY' in type_name:
            return pa.binary()
        elif 'DATE' in type_name:
            return pa.date32()
        elif 'TIMESTAMP' in type_name:
            return pa.timestamp('us')
        elif 'DECIMAL' in type_name:
            return pa.decimal128(38, 18)  # Default precision/scale
        else:
            # Default to string for unknown types
            return pa.string()
        
    def _get_arrow_schema(self) -> pa.Schema:
        """Get the Arrow schema for the table."""
        # Convert table schema to Arrow schema
        if hasattr(self.table, 'table_schema'):
            table_schema = self.table.table_schema
            
            # Apply projection if specified
            if self.projection:
                projected_fields = []
                for field_name in self.projection:
                    for field in table_schema.fields:
                        if field.name == field_name:
                            projected_fields.append(field)
                            break
                fields_to_use = projected_fields
            else:
                fields_to_use = table_schema.fields
                
            # Convert to Arrow fields
            arrow_fields = []
            for field in fields_to_use:
                arrow_type = self._convert_field_type_to_arrow(field.type)
                arrow_field = pa.field(field.name, arrow_type)
                arrow_fields.append(arrow_field)
                
            return pa.schema(arrow_fields)
            
        # Return a simple schema for now
        return pa.schema([
            ('placeholder', pa.string())
        ]) 