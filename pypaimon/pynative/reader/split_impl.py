from typing import List, Iterator, Dict, Any

from pypaimon.api import Split, Plan


class SplitImpl(Split):
    """Implementation of Split for native Python reading."""
    
    def __init__(self, file_paths: List[str], partition: Dict[str, Any], bucket: int,
                 row_count: int, file_size: int):
        self._file_paths = file_paths
        self.partition = partition
        self.bucket = bucket
        self._row_count = row_count
        self._file_size = file_size
        
    def row_count(self) -> int:
        """Return the total row count of the split."""
        return self._row_count
        
    def file_size(self) -> int:
        """Return the total file size of the split."""
        return self._file_size
        
    def file_paths(self) -> Iterator[str]:
        """Return the paths of all raw files in the split."""
        return iter(self._file_paths)
        
    def get_file_paths_list(self) -> List[str]:
        """Return the file paths as a list for convenience."""
        return self._file_paths.copy()


class PlanImpl(Plan):
    """Implementation of Plan for native Python reading."""
    
    def __init__(self, splits: List[Split]):
        self._splits = splits
        
    def splits(self) -> List[Split]:
        """Return the splits."""
        return self._splits 