from typing import List, Optional
from pathlib import Path

from pypaimon.api import TableScan, Plan, Split, Predicate
from pypaimon.pynative.reader.split_impl import SplitImpl, PlanImpl
from pypaimon.pynative.table.snapshot_manager import SnapshotManager
from pypaimon.pynative.table.manifest_list_manager import ManifestListWriter
from pypaimon.pynative.table.manifest_manager import ManifestFileWriter


class TableScanImpl(TableScan):
    """Implementation of TableScan for native Python reading."""
    
    def __init__(self, table: 'FileStoreTable', predicate: Optional[Predicate] = None,
                 projection: Optional[List[str]] = None, limit: Optional[int] = None):
        self.table = table
        self.predicate = predicate
        self.projection = projection
        self.limit = limit
        
        # Initialize managers
        self.snapshot_manager = SnapshotManager(table)
        self.manifest_list_writer = ManifestListWriter(table)
        self.manifest_file_writer = ManifestFileWriter(table)
        
    def plan(self) -> Plan:
        """Plan splits by scanning manifest files and creating data splits."""
        try:
            # Get latest snapshot
            latest_snapshot = self.snapshot_manager.get_latest_snapshot()
            if not latest_snapshot:
                # Empty table, return empty plan
                return PlanImpl([])
                
            # Read all manifest files from snapshot
            manifest_files = self._read_all_manifest_files(latest_snapshot)
            
            # Read manifest entries to get data file paths
            data_files = []
            for manifest_file_path in manifest_files:
                try:
                    entries = self.manifest_file_writer.read(manifest_file_path)
                    for entry in entries:
                        if entry.get('kind') == 'ADD':  # Only include added files
                            data_files.append({
                                'file_path': entry['file_path'],
                                'partition': entry['partition'],
                                'bucket': entry['bucket'],
                                'file_size': entry['file_size'],
                                'record_count': entry['record_count'],
                                'level': entry.get('level', 0)  # For Primary Key tables
                            })
                except Exception as e:
                    print(f"Warning: Failed to read manifest file {manifest_file_path}: {e}")
                    continue
                    
            # Apply partition filtering if predicate exists
            if self.predicate:
                data_files = self._filter_by_predicate(data_files)
                
            # Group files into splits based on table type
            if self.table.primary_keys:
                # Primary Key table: group by partition and bucket for merge reading
                splits = self._create_primary_key_splits(data_files)
            else:
                # Append Only table: one file per split
                splits = self._create_append_only_splits(data_files)
                    
            return PlanImpl(splits)
            
        except Exception as e:
            print(f"Error during table scan planning: {e}")
            return PlanImpl([])
            
    def _read_all_manifest_files(self, snapshot: dict) -> List[str]:
        """Read all manifest files from a snapshot."""
        manifest_files = []
        
        # Read from base manifest list
        if snapshot.get('baseManifestList'):
            try:
                base_manifests = self.manifest_list_writer.read(snapshot['baseManifestList'])
                manifest_files.extend(base_manifests)
            except Exception as e:
                print(f"Warning: Failed to read base manifest list: {e}")
                
        # Read from delta manifest list
        if snapshot.get('deltaManifestList'):
            try:
                delta_manifests = self.manifest_list_writer.read(snapshot['deltaManifestList'])
                manifest_files.extend(delta_manifests)
            except Exception as e:
                print(f"Warning: Failed to read delta manifest list: {e}")
                
        return manifest_files
        
    def _create_append_only_splits(self, data_files: List[dict]) -> List['Split']:
        """Create splits for append-only tables (one file per split)."""
        splits = []
        total_rows = 0
        
        for data_file in data_files:
            # Apply limit early if specified
            if self.limit and total_rows >= self.limit:
                break
                
            file_path = data_file['file_path']
            if Path(file_path).exists():
                split = SplitImpl(
                    file_paths=[file_path],
                    partition=data_file['partition'],
                    bucket=data_file['bucket'],
                    row_count=data_file['record_count'],
                    file_size=data_file['file_size']
                )
                splits.append(split)
                total_rows += data_file['record_count']
                
        return splits
        
    def _create_primary_key_splits(self, data_files: List[dict]) -> List['Split']:
        """Create splits for primary key tables (group by partition and bucket)."""
        from collections import defaultdict
        
        # Group files by (partition, bucket)
        partition_bucket_files = defaultdict(list)
        
        for data_file in data_files:
            file_path = data_file['file_path']
            if Path(file_path).exists():
                key = (str(data_file['partition']), data_file['bucket'])
                partition_bucket_files[key].append(data_file)
                
        # Create splits from grouped files
        splits = []
        total_rows = 0
        
        for (partition_str, bucket), files in partition_bucket_files.items():
            # Apply limit early if specified
            if self.limit and total_rows >= self.limit:
                break
                
            # Sort files by level (lower levels first for LSM merge)
            files.sort(key=lambda f: f['level'])
            
            file_paths = [f['file_path'] for f in files]
            total_file_size = sum(f['file_size'] for f in files)
            total_record_count = sum(f['record_count'] for f in files)
            
            # Use partition from first file
            partition = files[0]['partition']
            
            split = SplitImpl(
                file_paths=file_paths,
                partition=partition,
                bucket=bucket,
                row_count=total_record_count,
                file_size=total_file_size
            )
            splits.append(split)
            total_rows += total_record_count
            
        return splits
        
    def _filter_by_predicate(self, data_files: List[dict]) -> List[dict]:
        """Apply predicate filtering to data files."""
        # For now, we do a simple implementation
        # In a full implementation, this would use partition pruning
        # and other optimizations based on the predicate
        
        # TODO: Implement partition pruning based on predicate
        # For now, return all files (filtering will happen during reading)
        return data_files 