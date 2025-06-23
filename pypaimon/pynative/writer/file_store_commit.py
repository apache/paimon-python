import time
from pathlib import Path
from typing import List, Dict, Any, Optional

from pypaimon.api import CommitMessage
from pypaimon.pynative.writer.commit_message_impl import CommitMessageImpl
from pypaimon.pynative.table.manifest_manager import ManifestFileWriter
from pypaimon.pynative.table.manifest_list_manager import ManifestListWriter
from pypaimon.pynative.table.snapshot_manager import SnapshotManager


class FileStoreCommit:
    """Core commit logic for file store operations.
    
    This class handles:
    - Creating manifest files from commit messages
    - Managing manifest list files (base + delta pattern)
    - Creating and writing snapshot files
    - Atomic commit operations
    """
    
    def __init__(self, table: 'FileStoreTable', commit_user: str):
        self.table = table
        self.commit_user = commit_user
        self.ignore_empty_commit_flag = True
        
        # Initialize managers and writers
        self.snapshot_manager = SnapshotManager(table)
        self.manifest_file_writer = ManifestFileWriter(table)
        self.manifest_list_writer = ManifestListWriter(table)
        
        # Configuration from table options
        self.manifest_target_size = self._parse_size(
            table.options.get('manifest.target-file-size', '8MB')
        )
        self.manifest_merge_min_count = int(
            table.options.get('manifest.merge-min-count', '30')
        )

    def commit(self, commit_messages: List[CommitMessage], commit_identifier: int):
        """Commit the given commit messages in normal append mode."""
        if not commit_messages and self.ignore_empty_commit_flag:
            return
            
        # Step 1: Write new manifest files from commit messages
        new_manifest_files = self._write_manifest_files(commit_messages)
        
        if not new_manifest_files:
            return
            
        # Step 2: Get latest snapshot and existing manifest files
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        existing_manifest_files = []
        
        if latest_snapshot:
            existing_manifest_files = self._read_all_manifest_files(latest_snapshot)
            
        # Step 3: Merge manifest files (base + delta pattern)
        base_manifest_list, delta_manifest_list = self._merge_manifest_files(
            existing_manifest_files, new_manifest_files
        )
        
        # Step 4: Create and write new snapshot
        new_snapshot_id = self._generate_snapshot_id()
        snapshot_data = self._create_snapshot(
            snapshot_id=new_snapshot_id,
            base_manifest_list=base_manifest_list,
            delta_manifest_list=delta_manifest_list,
            commit_identifier=commit_identifier,
            commit_kind="APPEND"
        )
        
        # Step 5: Atomic commit
        self._atomic_commit_snapshot(new_snapshot_id, snapshot_data)
        
    def overwrite(self, partition: Optional[Dict[str, str]], 
                  commit_messages: List[CommitMessage], 
                  commit_identifier: int):
        """Commit with overwrite mode."""
        # For Python batch version, overwrite is simplified
        # Just treat as normal commit but with OVERWRITE kind
        if not commit_messages and self.ignore_empty_commit_flag:
            return
            
        new_manifest_files = self._write_manifest_files(commit_messages)
        
        if not new_manifest_files:
            return
            
        # In overwrite mode, we don't merge with existing manifests
        # Create fresh base manifest list
        base_manifest_list = self.manifest_list_writer.write(new_manifest_files)
        delta_manifest_list = None
        
        new_snapshot_id = self._generate_snapshot_id()
        snapshot_data = self._create_snapshot(
            snapshot_id=new_snapshot_id,
            base_manifest_list=base_manifest_list,
            delta_manifest_list=delta_manifest_list,
            commit_identifier=commit_identifier,
            commit_kind="OVERWRITE"
        )
        
        self._atomic_commit_snapshot(new_snapshot_id, snapshot_data)

    def abort(self, commit_messages: List[CommitMessage]):
        """Clean up data files for failed commit."""
        for message in commit_messages:
            for file_path in message.new_files():
                try:
                    file_path_obj = Path(file_path)
                    if file_path_obj.exists():
                        file_path_obj.unlink()
                except Exception as e:
                    # Log but don't fail on cleanup errors
                    print(f"Warning: Failed to clean up file {file_path}: {e}")
        
    def _write_manifest_files(self, commit_messages: List[CommitMessage]) -> List[str]:
        """Write commit messages to manifest files."""
        if not commit_messages:
            return []
            
        manifest_entries = []
        for message in commit_messages:
            if isinstance(message, CommitMessageImpl):
                # Convert CommitMessage to ManifestEntry format
                for file_path in message.new_files():
                    entry = {
                        'kind': 'ADD',
                        'partition': message.partition(),
                        'bucket': message.bucket(),
                        'file_path': file_path,
                        'file_size': self._get_file_size(file_path),
                        'record_count': self._get_record_count(file_path)
                    }
                    manifest_entries.append(entry)
                    
        if not manifest_entries:
            return []
            
        # Write to manifest file
        manifest_file_path = self.manifest_file_writer.write(manifest_entries)
        return [manifest_file_path]
        
    def _read_all_manifest_files(self, snapshot: Dict[str, Any]) -> List[str]:
        """Read all manifest files from a snapshot."""
        manifest_files = []
        
        # Read from base manifest list
        if snapshot.get('baseManifestList'):
            base_manifests = self.manifest_list_writer.read(snapshot['baseManifestList'])
            manifest_files.extend(base_manifests)
            
        # Read from delta manifest list
        if snapshot.get('deltaManifestList'):
            delta_manifests = self.manifest_list_writer.read(snapshot['deltaManifestList'])
            manifest_files.extend(delta_manifests)
            
        return manifest_files
        
    def _merge_manifest_files(self, existing_files: List[str], 
                            new_files: List[str]) -> tuple[Optional[str], Optional[str]]:
        """Merge manifest files using base + delta pattern."""
        all_files = existing_files + new_files
        
        # Simple strategy: if we have too many files, merge them into base
        if len(all_files) >= self.manifest_merge_min_count:
            # Merge all files into new base
            base_manifest_list = self.manifest_list_writer.write(all_files)
            delta_manifest_list = None
        else:
            # Keep existing as base, new files as delta
            if existing_files:
                base_manifest_list = self.manifest_list_writer.write(existing_files)
            else:
                base_manifest_list = None
                
            delta_manifest_list = self.manifest_list_writer.write(new_files)
            
        return base_manifest_list, delta_manifest_list
        
    def _create_snapshot(self, snapshot_id: int, base_manifest_list: Optional[str],
                        delta_manifest_list: Optional[str], commit_identifier: int,
                        commit_kind: str) -> Dict[str, Any]:
        """Create snapshot data structure."""
        return {
            'version': 3,
            'id': snapshot_id,
            'schemaId': 0,  # Simplified for Python version
            'baseManifestList': base_manifest_list,
            'deltaManifestList': delta_manifest_list,
            'changelogManifestList': None,  # Python version doesn't support changelog
            'indexManifest': None,  # Python version doesn't support index
            'commitUser': self.commit_user,
            'commitIdentifier': commit_identifier,
            'commitKind': commit_kind,
            'timeMillis': int(time.time() * 1000),
            'logOffsets': {},
            'totalRecordCount': self._calculate_total_records(),
            'deltaRecordCount': self._calculate_delta_records(),
            'changelogRecordCount': 0,
            'watermark': None,
            'statistics': None
        }
        
    def _atomic_commit_snapshot(self, snapshot_id: int, snapshot_data: Dict[str, Any]):
        """Atomically commit the snapshot."""
        self.snapshot_manager.commit_snapshot(snapshot_id, snapshot_data)
        
    def _generate_snapshot_id(self) -> int:
        """Generate a new snapshot ID."""
        latest_snapshot = self.snapshot_manager.get_latest_snapshot()
        if latest_snapshot:
            return latest_snapshot['id'] + 1
        else:
            return 1
            
    def _parse_size(self, size_str: str) -> int:
        """Parse size string like '8MB' to bytes."""
        size_str = size_str.upper()
        if size_str.endswith('MB'):
            return int(size_str[:-2]) * 1024 * 1024
        elif size_str.endswith('KB'):
            return int(size_str[:-2]) * 1024
        elif size_str.endswith('GB'):
            return int(size_str[:-2]) * 1024 * 1024 * 1024
        else:
            return int(size_str)
            
    def _get_file_size(self, file_path: str) -> int:
        """Get file size in bytes."""
        try:
            return Path(file_path).stat().st_size
        except:
            return 0
            
    def _get_record_count(self, file_path: str) -> int:
        """Get record count from parquet file."""
        # This would need to read parquet metadata
        # Simplified for now
        return 0
        
    def _calculate_total_records(self) -> int:
        """Calculate total records in table."""
        # Simplified for Python version
        return 0
        
    def _calculate_delta_records(self) -> int:
        """Calculate delta records in this commit."""
        # Simplified for Python version
        return 0 