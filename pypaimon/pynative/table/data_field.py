from dataclasses import dataclass
from typing import Optional


class DataType:
    def __init__(self, type_name: str, nullable: bool = True):
        self.type_name = type_name
        self.nullable = nullable

    @classmethod
    def from_string(cls, type_str: str) -> 'DataType':
        parts = type_str.split()
        type_name = parts[0].upper()
        nullable = "NOT NULL" not in type_str.upper()
        return cls(type_name, nullable)

    def __str__(self) -> str:
        result = self.type_name
        if not self.nullable:
            result += " NOT NULL"
        return result

    def __eq__(self, other):
        if not isinstance(other, DataType):
            return False
        return self.type_name == other.type_name and self.nullable == other.nullable


@dataclass
class DataField:
    id: int
    name: str
    type: DataType
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> 'DataField':
        return cls(
            id=data["id"],
            name=data["name"],
            type=DataType.from_string(data["type"]),
            description=data.get("description")
        )

    def to_dict(self) -> dict:
        result = {
            "id": self.id,
            "name": self.name,
            "type": str(self.type)
        }
        if self.description is not None:
            result["description"] = self.description
        return result