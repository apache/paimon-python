import pyarrow as pa
import uuid
from collections import defaultdict

from typing import List, Optional

from pypaimon.api import BatchTableWrite, CommitMessage
from pypaimon.pynative.table.core_option import CoreOptions


class BatchTableWriteImpl(BatchTableWrite):
    def __init__(self, table: 'FileStoreTable', commit_user: str):
        self.file_store_write = table.new_write(commit_user)
        self.ignore_delete = table.options.get(CoreOptions.IGNORE_DELETE, False)
        self.row_key_extractor = table.create_row_key_extractor()
        self.not_null_field_index = table.table_schema.get_not_null_field_index()
        self.table = table
        self.batch_committed = False

    def write_arrow(self, table: pa.Table):
        self._check_nullability(table)
        
        partitions, buckets = self.row_key_extractor.extract_partition_bucket_batch(table)
        
        partition_bucket_groups = defaultdict(list)
        for i in range(table.num_rows):
            partition_bucket_groups[(partitions[i], buckets[i])].append(i)
        
        for (partition, bucket), row_indices in partition_bucket_groups.items():
            indices_array = pa.array(row_indices, type=pa.int64())
            sub_table = pa.compute.take(table, indices_array)
            self.file_store_write.write(partition, bucket, sub_table)

    def write_arrow_batch(self, record_batch: pa.RecordBatch):
        pass

    def write_pandas(self, dataframe):
        pass

    def prepare_commit(self) -> List[CommitMessage]:
        """准备提交，收集所有文件变更信息"""
        if self.batch_committed:
            raise RuntimeError("BatchTableWrite only supports one-time committing.")
        
        self.batch_committed = True
        return self.file_store_write.prepare_commit()

    def close(self):
        self.file_store_write.close()

    def _check_nullability(self, table: pa.Table):
        """
        Check nullability constraints for non-null fields.
        This mimics the Java checkNullability(InternalRow row) method.
        Optimized to work directly with PyArrow without pandas conversion.
        """
        if not self.not_null_field_index:
            return
            
        # Use PyArrow's compute functions for efficient null checking
        for field_idx in self.not_null_field_index:
            if field_idx < len(table.schema):
                column = table.column(field_idx)
                column_name = table.schema.field(field_idx).name
                
                # Use PyArrow's is_null compute function for efficient null detection
                null_mask = pa.compute.is_null(column)
                has_nulls = pa.compute.any(null_mask).as_py()
                
                if has_nulls:
                    raise RuntimeError(f"Cannot write null to non-null column({column_name})")

