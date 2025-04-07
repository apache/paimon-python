from typing import Optional
from py4j.java_gateway import JavaObject

from pypaimon.pynative.iterator.record_iterator import RecordIterator
from pypaimon.pynative.reader.record_reader import RecordReader
from pypaimon.pynative.record_reader_wrapper import convert_java_reader


class ConcatRecordReader(RecordReader):
    """
    Python版本的ConcatRecordReader，直接使用Java的ReaderSupplier队列
    """
    def __init__(self, java_concat_reader: JavaObject):
        """
        Args:
            java_concat_reader: Java ConcatRecordReader实例
        """
        # 获取 Queue 字段
        reader_class = java_concat_reader.getClass()
        queue_field = reader_class.getDeclaredField("queue")
        queue_field.setAccessible(True)
        self.java_queue = queue_field.get(java_concat_reader)
        self.current: Optional[RecordReader] = None

    def read_batch(self) -> Optional[RecordIterator]:
        while True:
            if self.current is not None:
                iterator = self.current.read_batch()
                if iterator is not None:
                    return iterator
                self.current.close()
                self.current = None
            elif not self.java_queue.isEmpty():
                # 直接从Java的ReaderSupplier获取Java RecordReader
                java_supplier = self.java_queue.poll()
                java_reader = java_supplier.get()
                # 将Java RecordReader转换为Python RecordReader
                self.current = convert_java_reader(java_reader)
            else:
                return None

    def close(self) -> None:
        if self.current is not None:
            self.current.close()
