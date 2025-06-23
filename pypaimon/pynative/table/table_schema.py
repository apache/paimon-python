import json
from pathlib import Path
from typing import List, Dict, Optional
import time

from pypaimon.api import Schema
from pypaimon.api.row_type import RowType
from pypaimon.pynative.common.file_io import FileIO
from pypaimon.pynative.table import schema_util
from pypaimon.pynative.table.core_option import CoreOptions
from pypaimon.pynative.table.data_field import DataField
from pypaimon.pynative.table.row_type import RowTypeImpl
from pypaimon.pynative.table.schema_util import convert_data_fields_to_pa_schema


class TableSchema:
    PAIMON_07_VERSION = 1
    PAIMON_08_VERSION = 2
    CURRENT_VERSION = 3

    def __init__(self, version: int, id: int, fields: List[DataField], highest_field_id: int,
                 partition_keys: List[str], primary_keys: List[str], options: Dict[str, str],
                 comment: Optional[str] = None, time_millis: Optional[int] = None):
        self.version = version
        self.id = id
        self.fields = fields
        self.highest_field_id = highest_field_id
        self.partition_keys = partition_keys
        self.primary_keys = primary_keys
        self.options = options
        self.comment = comment
        self.time_millis = time_millis if time_millis is not None else int(time.time() * 1000)

    @staticmethod
    def from_path(file_io: FileIO, schema_path: Path):
        try:
            json_str = file_io.read_file_utf8(schema_path)
            return TableSchema.from_json(json_str)
        except FileNotFoundError as e:
            raise RuntimeError(f"Schema file not found: {schema_path}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to read schema from {schema_path}") from e

    @staticmethod
    def from_json(json_str: str):
        try:
            data = json.loads(json_str)
            
            version = data.get("version", TableSchema.PAIMON_07_VERSION)
            
            id = data["id"]
            fields = [DataField.from_dict(field) for field in data["fields"]]
            highest_field_id = data["highestFieldId"]
            partition_keys = data["partitionKeys"]
            primary_keys = data["primaryKeys"]
            options = data["options"]
            comment = data.get("comment")
            time_millis = data.get("timeMillis")
            
            if version <= TableSchema.PAIMON_07_VERSION and CoreOptions.BUCKET not in options:
                options[CoreOptions.BUCKET] = "1"
            if version <= TableSchema.PAIMON_08_VERSION and CoreOptions.FILE_FORMAT not in options:
                options[CoreOptions.FILE_FORMAT] = "orc"
            
            return TableSchema(
                version=version,
                id=id,
                fields=fields,
                highest_field_id=highest_field_id,
                partition_keys=partition_keys,
                primary_keys=primary_keys,
                options=options,
                comment=comment,
                time_millis=time_millis
            )
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON format: {json_str}") from e
        except KeyError as e:
            raise RuntimeError(f"Missing required field in schema JSON: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to parse schema from JSON") from e

    def to_json(self) -> str:
        data = {
            "version": self.version,
            "id": self.id,
            "fields": [field.to_dict() for field in self.fields],
            "highestFieldId": self.highest_field_id,
            "partitionKeys": self.partition_keys,
            "primaryKeys": self.primary_keys,
            "options": self.options,
            "timeMillis": self.time_millis
        }
        if self.comment is not None:
            data["comment"] = self.comment
        return json.dumps(data, indent=2, ensure_ascii=False)

    @staticmethod
    def from_schema(schema: Schema):
        fields = schema_util.convert_pa_schema_to_data_fields(schema.pa_schema)
        partition_keys = schema.partition_keys
        primary_keys = schema.primary_keys
        options = schema.options
        highest_field_id = schema_util.get_highest_field_id(fields)
        return TableSchema(
            version=TableSchema.CURRENT_VERSION,
            id=0,
            fields=fields,
            highest_field_id=highest_field_id,
            partition_keys=partition_keys,
            primary_keys=primary_keys,
            options=options,
            comment=schema.comment
        )

    def to_schema(self) -> Schema:
        """Convert TableSchema to Schema."""
        pa_schema = convert_data_fields_to_pa_schema(self.fields)
        return Schema(
            pa_schema=pa_schema,
            partition_keys=self.partition_keys,
            primary_keys=self.primary_keys,
            options=self.options,
            comment=self.comment
        )

    def logical_row_type(self) -> RowType:
        return RowTypeImpl(self.fields)

    def cross_partition_update(self) -> bool:
        # TODO
        pass