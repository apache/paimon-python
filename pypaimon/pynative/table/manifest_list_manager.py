import uuid
import fastavro
from pathlib import Path
from typing import List, Dict, Any, Optional
from io import BytesIO

from pypaimon.pynative.common.file_io import FileIO


class ManifestListWriter:
    """Writer for manifest list files in Avro format using unified FileIO."""
    
    # Avro schema for manifest list entries
    MANIFEST_LIST_SCHEMA = {
        "type": "record",
        "name": "manifest_file_meta",
        "namespace": "org.apache.paimon.avro.generated.record",
        "fields": [
            {
                "name": "file_name",
                "type": "string"
            },
            {
                "name": "file_size",
                "type": "long"
            },
            {
                "name": "num_added_files",
                "type": "long",
                "default": 0
            },
            {
                "name": "num_deleted_files",
                "type": "long",
                "default": 0
            },
            {
                "name": "partition_stats",
                "type": ["null", "bytes"],
                "default": None
            },
            {
                "name": "schema_id",
                "type": "long",
                "default": 0
            },
            {
                "name": "creation_time",
                "type": ["null", "long"],
                "default": None
            }
        ]
    }
    
    def __init__(self, table: 'FileStoreTable', file_io: FileIO):
        self.table = table
        self.table_path = table.table_path
        self.file_io = file_io
        
    def write(self, manifest_file_paths: List[str]) -> Optional[str]:
        """Write manifest file paths to a new manifest list file in Avro format.
        
        Args:
            manifest_file_paths: List of paths to manifest files
            
        Returns:
            Path to the created manifest list file, or None if no manifests
        """
        if not manifest_file_paths:
            return None
            
        # Generate unique manifest list file name
        list_id = str(uuid.uuid4())
        list_filename = f"manifest-list-{list_id}.avro"
        list_path = self.table_path / "manifest" / list_filename
        
        try:
            # Create manifest file metadata
            manifest_metas = []
            for manifest_path in manifest_file_paths:
                meta = self._create_manifest_meta(manifest_path)
                manifest_metas.append(meta)
                
            # Convert to Avro records
            avro_records = self._convert_to_avro_records(manifest_metas)
            
            # Serialize to bytes using fastavro
            buffer = BytesIO()
            fastavro.writer(buffer, self.MANIFEST_LIST_SCHEMA, avro_records)
            avro_bytes = buffer.getvalue()
            
            # Write to file system using FileIO
            with self.file_io.new_output_stream(list_path) as output_stream:
                output_stream.write(avro_bytes)
            
            return str(list_path)
            
        except Exception as e:
            # Clean up on failure
            self.file_io.delete_quietly(list_path)
            raise RuntimeError(f"Failed to write manifest list file: {e}") from e
            
    def read(self, manifest_list_path: str) -> List[str]:
        """Read manifest file paths from an Avro manifest list file.
        
        Args:
            manifest_list_path: Path to the manifest list file
            
        Returns:
            List of manifest file paths
        """
        try:
            manifest_paths = []
            
            # Read bytes from file system using FileIO
            with self.file_io.new_input_stream(Path(manifest_list_path)) as input_stream:
                avro_bytes = input_stream.read()
            
            # Deserialize from bytes using fastavro
            buffer = BytesIO(avro_bytes)
            reader = fastavro.reader(buffer)
            
            for record in reader:
                file_name = record['file_name']
                manifest_paths.append(file_name)
                
            return manifest_paths
            
        except Exception as e:
            raise RuntimeError(f"Failed to read manifest list file {manifest_list_path}: {e}") from e
            
    def read_full_metadata(self, manifest_list_path: str) -> List[Dict[str, Any]]:
        """Read full manifest metadata from an Avro manifest list file.
        
        Args:
            manifest_list_path: Path to the manifest list file
            
        Returns:
            List of manifest file metadata dictionaries
        """
        try:
            manifest_metas = []
            
            # Read bytes from file system using FileIO
            with self.file_io.new_input_stream(Path(manifest_list_path)) as input_stream:
                avro_bytes = input_stream.read()
            
            # Deserialize from bytes using fastavro
            buffer = BytesIO(avro_bytes)
            reader = fastavro.reader(buffer)
            
            for record in reader:
                meta = {
                    'file_name': record['file_name'],
                    'file_size': record['file_size'],
                    'num_added_files': record.get('num_added_files', 0),
                    'num_deleted_files': record.get('num_deleted_files', 0),
                    'partition_stats': record.get('partition_stats'),
                    'schema_id': record.get('schema_id', 0),
                    'creation_time': record.get('creation_time')
                }
                manifest_metas.append(meta)
                
            return manifest_metas
            
        except Exception as e:
            raise RuntimeError(f"Failed to read manifest list metadata {manifest_list_path}: {e}") from e
            
    def _create_manifest_meta(self, manifest_path: str) -> Dict[str, Any]:
        """Create metadata for a manifest file."""
        manifest_file_path = Path(manifest_path)
        
        # Get basic file information using FileIO
        file_size = 0
        creation_time = None
        
        try:
            if self.file_io.exists(manifest_file_path):
                file_size = self.file_io.get_file_size(manifest_file_path)
                # Get creation time from file status
                file_status = self.file_io.get_file_status(manifest_file_path)
                if hasattr(file_status, 'mtime') and file_status.mtime:
                    creation_time = int(file_status.mtime.timestamp() * 1000)
        except Exception:
            # If we can't get file info, use defaults
            pass
        
        # Try to read manifest file to get more accurate stats
        num_added_files = 0
        num_deleted_files = 0
        schema_id = 0
        
        try:
            # Import here to avoid circular imports
            from pypaimon.pynative.table.manifest_manager import ManifestFileWriter
            
            # Create a temporary writer to read the manifest file
            temp_writer = ManifestFileWriter(self.table, self.file_io)
            entries = temp_writer.read(str(manifest_path))
            
            for entry in entries:
                if entry.get('kind', 'ADD') == 'ADD':
                    num_added_files += 1
                else:
                    num_deleted_files += 1
                    
                entry_schema_id = entry.get('schema_id', 0)
                schema_id = max(schema_id, entry_schema_id)
                
        except Exception:
            # If we can't read the manifest file, use default values
            pass
        
        return {
            'file_name': str(manifest_path),
            'file_size': file_size,
            'num_added_files': num_added_files,
            'num_deleted_files': num_deleted_files,
            'partition_stats': None,  # TODO: Implement partition stats
            'schema_id': schema_id,
            'creation_time': creation_time
        }
        
    def _convert_to_avro_records(self, manifest_metas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert manifest metadata to Avro records."""
        avro_records = []
        
        for meta in manifest_metas:
            avro_record = {
                'file_name': meta['file_name'],
                'file_size': meta['file_size'],
                'num_added_files': meta.get('num_added_files', 0),
                'num_deleted_files': meta.get('num_deleted_files', 0),
                'partition_stats': meta.get('partition_stats'),
                'schema_id': meta.get('schema_id', 0),
                'creation_time': meta.get('creation_time')
            }
            avro_records.append(avro_record)
            
        return avro_records
        
    def get_schema(self) -> Dict[str, Any]:
        """Get the Avro schema for manifest list entries."""
        return self.MANIFEST_LIST_SCHEMA
        
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate a record against the Avro schema."""
        try:
            # Use fastavro to validate the record
            buffer = BytesIO()
            fastavro.writer(buffer, self.MANIFEST_LIST_SCHEMA, [record])
            return True
        except Exception:
            return False
            
    def exists(self, manifest_list_path: str) -> bool:
        """Check if manifest list file exists."""
        return self.file_io.exists(Path(manifest_list_path))
        
    def get_file_size(self, manifest_list_path: str) -> int:
        """Get the size of manifest list file."""
        return self.file_io.get_file_size(Path(manifest_list_path)) 