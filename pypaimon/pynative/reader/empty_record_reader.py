from typing import Optional
from py4j.java_gateway import JavaObject

from pypaimon.pynative.iterator.record_iterator import RecordIterator
from pypaimon.pynative.reader.file_record_reader import FileRecordReader
from pypaimon.pynative.reader.record_reader import RecordReader
from pypaimon.pynative.record_reader_wrapper import convert_java_reader


class EmptyFileRecordReader(FileRecordReader):
    def __init__(self):
        pass

    def read_batch(self) -> Optional[RecordIterator]:
        return None

    def close(self) -> None:
        pass
