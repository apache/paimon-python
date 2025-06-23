import json
from pathlib import Path
from typing import Dict, Any, Optional

from pypaimon.pynative.common.file_io import FileIO


class SnapshotManager:
    """Manager for snapshot files using unified FileIO."""
    
    def __init__(self, table: 'FileStoreTable', file_io: FileIO):
        self.table = table
        self.table_path = table.table_path
        self.file_io = file_io
        self.snapshot_dir = self.table_path / "snapshot"
        
    def get_latest_snapshot(self) -> Optional[Dict[str, Any]]:
        """Get the latest snapshot, or None if no snapshots exist."""
        try:
            # Read the LATEST file to get the latest snapshot ID
            latest_file = self.snapshot_dir / "LATEST"
            if not self.file_io.exists(latest_file):
                return None
                
            latest_content = self.file_io.read_file_utf8(latest_file)
            latest_snapshot_id = int(latest_content.strip())
                
            # Read the snapshot file
            snapshot_file = self.snapshot_dir / f"snapshot-{latest_snapshot_id}"
            if not self.file_io.exists(snapshot_file):
                return None
                
            snapshot_content = self.file_io.read_file_utf8(snapshot_file)
            snapshot_data = json.loads(snapshot_content)
                
            return snapshot_data
            
        except Exception:
            # If anything goes wrong, assume no snapshot exists
            return None
            
    def commit_snapshot(self, snapshot_id: int, snapshot_data: Dict[str, Any]):
        """Atomically commit a new snapshot."""
        snapshot_file = self.snapshot_dir / f"snapshot-{snapshot_id}"
        latest_file = self.snapshot_dir / "LATEST"
        
        try:
            # Serialize snapshot data to JSON string
            snapshot_json = json.dumps(snapshot_data, indent=2)
            
            # Try atomic write for snapshot file
            snapshot_success = self.file_io.try_to_write_atomic(snapshot_file, snapshot_json)
            if not snapshot_success:
                # Fall back to regular write
                self.file_io.write_file(snapshot_file, snapshot_json, overwrite=True)
            
            # Try atomic write for LATEST file
            latest_success = self.file_io.try_to_write_atomic(latest_file, str(snapshot_id))
            if not latest_success:
                # Fall back to regular write
                self.file_io.write_file(latest_file, str(snapshot_id), overwrite=True)
            
        except Exception as e:
            # Clean up on failure
            self.file_io.delete_quietly(snapshot_file)
            raise RuntimeError(f"Failed to commit snapshot {snapshot_id}: {e}") from e
            
    def commit_snapshot_with_manifest_list(self, snapshot_id: int, manifest_list_path: str, 
                                         additional_metadata: Optional[Dict[str, Any]] = None):
        """Commit a snapshot with manifest list reference."""
        import time
        
        snapshot_data = {
            "version": 3,
            "snapshot_id": snapshot_id,
            "schema_id": 0,
            "base_manifest_list": manifest_list_path,
            "delta_manifest_list": None,
            "changelog_manifest_list": None,
            "commit_user": "python-client",
            "commit_identifier": snapshot_id,
            "commit_kind": "APPEND",
            "time_millis": int(time.time() * 1000),
            "log_offsets": {},
            "total_record_count": 0,
            "delta_record_count": 0,
            "changelog_record_count": 0,
            "watermark": None
        }
        
        # Add any additional metadata
        if additional_metadata:
            snapshot_data.update(additional_metadata)
            
        self.commit_snapshot(snapshot_id, snapshot_data)
            
    def snapshot_exists(self, snapshot_id: int) -> bool:
        """Check if a snapshot exists."""
        snapshot_file = self.snapshot_dir / f"snapshot-{snapshot_id}"
        return self.file_io.exists(snapshot_file)
        
    def get_snapshot(self, snapshot_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific snapshot by ID."""
        snapshot_file = self.snapshot_dir / f"snapshot-{snapshot_id}"
        
        if not self.file_io.exists(snapshot_file):
            return None
            
        try:
            snapshot_content = self.file_io.read_file_utf8(snapshot_file)
            return json.loads(snapshot_content)
        except Exception as e:
            raise RuntimeError(f"Failed to read snapshot {snapshot_id}: {e}") from e
            
    def get_latest_snapshot_id(self) -> Optional[int]:
        """Get the latest snapshot ID, or None if no snapshots exist."""
        try:
            latest_file = self.snapshot_dir / "LATEST"
            if not self.file_io.exists(latest_file):
                return None
                
            latest_content = self.file_io.read_file_utf8(latest_file)
            return int(latest_content.strip())
            
        except Exception:
            return None
            
    def list_snapshots(self) -> list[int]:
        """List all snapshot IDs in ascending order."""
        try:
            snapshot_ids = []
            
            # List all files in snapshot directory
            file_infos = self.file_io.list_status(self.snapshot_dir)
            
            for file_info in file_infos:
                file_name = Path(file_info.path).name
                if file_name.startswith("snapshot-") and file_name != "snapshot-":
                    try:
                        snapshot_id = int(file_name[9:])  # Remove "snapshot-" prefix
                        snapshot_ids.append(snapshot_id)
                    except ValueError:
                        # Skip invalid snapshot file names
                        continue
                        
            return sorted(snapshot_ids)
            
        except Exception:
            return []
            
    def delete_snapshot(self, snapshot_id: int) -> bool:
        """Delete a specific snapshot."""
        snapshot_file = self.snapshot_dir / f"snapshot-{snapshot_id}"
        
        if not self.file_io.exists(snapshot_file):
            return False
            
        return self.file_io.delete(snapshot_file, recursive=False)
        
    def cleanup_expired_snapshots(self, keep_count: int = 10):
        """Clean up old snapshots, keeping only the specified number of recent ones."""
        if keep_count <= 0:
            return
            
        snapshot_ids = self.list_snapshots()
        
        if len(snapshot_ids) <= keep_count:
            return
            
        # Get latest snapshot ID to avoid deleting it
        latest_id = self.get_latest_snapshot_id()
        
        # Calculate how many to delete
        to_delete_count = len(snapshot_ids) - keep_count
        to_delete = snapshot_ids[:to_delete_count]
        
        # Don't delete the latest snapshot
        if latest_id and latest_id in to_delete:
            to_delete.remove(latest_id)
            
        # Delete old snapshots
        for snapshot_id in to_delete:
            try:
                self.delete_snapshot(snapshot_id)
            except Exception as e:
                # Log but don't fail the entire cleanup
                print(f"Warning: Failed to delete snapshot {snapshot_id}: {e}")
                
    def get_snapshot_directory(self) -> Path:
        """Get the snapshot directory path."""
        return self.snapshot_dir 