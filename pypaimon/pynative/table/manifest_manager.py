import uuid
import fastavro
from pathlib import Path
from typing import List, Dict, Any
from io import BytesIO

from pypaimon.pynative.common.file_io import FileIO


class ManifestFileWriter:
    """Writer for manifest files in Avro format using unified FileIO."""
    
    # Avro schema for manifest entries
    MANIFEST_ENTRY_SCHEMA = {
        "type": "record",
        "name": "manifest_entry",
        "namespace": "org.apache.paimon.avro.generated.record",
        "fields": [
            {
                "name": "kind",
                "type": {
                    "type": "enum",
                    "name": "FileKind",
                    "symbols": ["ADD", "DELETE"]
                },
                "default": "ADD"
            },
            {
                "name": "partition",
                "type": {
                    "type": "record",
                    "name": "BinaryRow",
                    "fields": [
                        {"name": "values", "type": {"type": "array", "items": "bytes"}}
                    ]
                }
            },
            {
                "name": "bucket",
                "type": "int",
                "default": 0
            },
            {
                "name": "file_path",
                "type": "string"
            },
            {
                "name": "file_size",
                "type": "long"
            },
            {
                "name": "record_count",
                "type": "long"
            },
            {
                "name": "creation_time",
                "type": ["null", "long"],
                "default": None
            },
            {
                "name": "min_key",
                "type": ["null", "bytes"],
                "default": None
            },
            {
                "name": "max_key",
                "type": ["null", "bytes"],
                "default": None
            },
            {
                "name": "schema_id",
                "type": "long",
                "default": 0
            }
        ]
    }
    
    def __init__(self, table: 'FileStoreTable', file_io: FileIO):
        self.table = table
        self.table_path = table.table_path
        self.file_io = file_io
        
    def write(self, manifest_entries: List[Dict[str, Any]]) -> str:
        """Write manifest entries to a new manifest file in Avro format.
        
        Args:
            manifest_entries: List of manifest entry dictionaries
            
        Returns:
            Path to the created manifest file
        """
        if not manifest_entries:
            raise ValueError("Cannot write empty manifest entries")
            
        # Generate unique manifest file name
        manifest_id = str(uuid.uuid4())
        manifest_filename = f"manifest-{manifest_id}.avro"
        manifest_path = self.table_path / "manifest" / manifest_filename
        
        try:
            # Convert to Avro format
            avro_records = self._convert_to_avro_records(manifest_entries)
            
            # Serialize to bytes using fastavro
            buffer = BytesIO()
            fastavro.writer(buffer, self.MANIFEST_ENTRY_SCHEMA, avro_records)
            avro_bytes = buffer.getvalue()
            
            # Write to file system using FileIO
            with self.file_io.new_output_stream(manifest_path) as output_stream:
                output_stream.write(avro_bytes)
            
            return str(manifest_path)
            
        except Exception as e:
            # Clean up on failure
            self.file_io.delete_quietly(manifest_path)
            raise RuntimeError(f"Failed to write manifest file: {e}") from e
            
    def _convert_to_avro_records(self, manifest_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert manifest entries to Avro records."""
        avro_records = []
        
        for entry in manifest_entries:
            # Convert partition tuple to BinaryRow format
            partition_data = entry.get('partition', ())
            if isinstance(partition_data, (list, tuple)):
                # Convert partition values to bytes for Avro
                partition_values = []
                for val in partition_data:
                    if val is None:
                        partition_values.append(b'')
                    elif isinstance(val, str):
                        partition_values.append(val.encode('utf-8'))
                    elif isinstance(val, (int, float)):
                        partition_values.append(str(val).encode('utf-8'))
                    else:
                        partition_values.append(str(val).encode('utf-8'))
            else:
                partition_values = []
            
            avro_record = {
                'kind': entry.get('kind', 'ADD'),
                'partition': {
                    'values': partition_values
                },
                'bucket': entry.get('bucket', 0),
                'file_path': entry.get('file_path', ''),
                'file_size': entry.get('file_size', 0),
                'record_count': entry.get('record_count', 0),
                'creation_time': entry.get('creation_time'),
                'min_key': entry.get('min_key'),
                'max_key': entry.get('max_key'),
                'schema_id': entry.get('schema_id', 0)
            }
            
            avro_records.append(avro_record)
            
        return avro_records
        
    def read(self, manifest_file_path: str) -> List[Dict[str, Any]]:
        """Read manifest entries from an Avro manifest file.
        
        Args:
            manifest_file_path: Path to the manifest file
            
        Returns:
            List of manifest entry dictionaries
        """
        try:
            entries = []
            
            # Read bytes from file system using FileIO
            with self.file_io.new_input_stream(Path(manifest_file_path)) as input_stream:
                avro_bytes = input_stream.read()
            
            # Deserialize from bytes using fastavro
            buffer = BytesIO(avro_bytes)
            reader = fastavro.reader(buffer)
            
            for record in reader:
                # Convert Avro record back to manifest entry format
                partition_values = record['partition']['values']
                
                # Convert bytes back to appropriate types (simplified)
                partition_tuple = tuple(
                    val.decode('utf-8') if val else None 
                    for val in partition_values
                )
                
                entry = {
                    'kind': record['kind'],
                    'partition': partition_tuple,
                    'bucket': record['bucket'],
                    'file_path': record['file_path'],
                    'file_size': record['file_size'],
                    'record_count': record['record_count'],
                    'creation_time': record.get('creation_time'),
                    'min_key': record.get('min_key'),
                    'max_key': record.get('max_key'),
                    'schema_id': record.get('schema_id', 0)
                }
                
                entries.append(entry)
                
            return entries
            
        except Exception as e:
            raise RuntimeError(f"Failed to read manifest file {manifest_file_path}: {e}") from e
            
    def get_schema(self) -> Dict[str, Any]:
        """Get the Avro schema for manifest entries."""
        return self.MANIFEST_ENTRY_SCHEMA
        
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate a record against the Avro schema."""
        try:
            # Use fastavro to validate the record
            buffer = BytesIO()
            fastavro.writer(buffer, self.MANIFEST_ENTRY_SCHEMA, [record])
            return True
        except Exception:
            return False
            
    def exists(self, manifest_file_path: str) -> bool:
        """Check if manifest file exists."""
        return self.file_io.exists(Path(manifest_file_path))
        
    def get_file_size(self, manifest_file_path: str) -> int:
        """Get the size of manifest file."""
        return self.file_io.get_file_size(Path(manifest_file_path)) 