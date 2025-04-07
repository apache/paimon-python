import heapq

from py4j.java_gateway import JavaObject
import pyarrow as pa

from pypaimon.pynative.record_reader_wrapper import convert_java_reader
import heapq
from typing import List, Optional, Any, Callable


from pypaimon.pynative.reader.record_reader import RecordReader
from pypaimon.pynative.iterator.record_iterator import RecordIterator
from pypaimon.pynative.row.key_value import KeyValue
from pypaimon.pynative.row.row_kind import RowKind


class DeduplicateMergeFunction:
    """
    去重合并函数，简单实现，仅保留最新的记录
    """

    def __init__(self, ignore_delete: bool = False):
        """
        初始化去重合并函数

        Args:
            ignore_delete: 是否忽略删除记录
        """
        self.ignore_delete = ignore_delete
        self.latest_kv = None
        self.is_initialized = False
        self.initial_kv = None

    def reset(self) -> None:
        """重置合并函数状态"""
        self.latest_kv = None
        self.is_initialized = False
        self.initial_kv = None

    def add(self, kv: KeyValue) -> None:
        """
        添加一个KeyValue到合并函数中，仅保留最新的记录

        Args:
            kv: 要添加的KeyValue
        """
        # 如果是第一个记录，直接保存
        if self.initial_kv is None:
            self.initial_kv = kv
            return

        # 如果是第二个记录，处理第一个记录
        if not self.is_initialized:
            # 如果不忽略删除记录或第一个记录不是删除记录，则保存
            if not self.ignore_delete or not self.initial_kv.value_kind == RowKind.DELETE:
                self.latest_kv = self.initial_kv
            self.is_initialized = True

        # 如果配置了忽略删除并且是删除记录，则忽略
        if self.ignore_delete and kv.value_kind == RowKind.DELETE:
            return

        # 保存最新记录
        self.latest_kv = kv

    def get_result(self) -> Optional[KeyValue]:
        """
        获取合并结果，如果只有一条记录直接返回，否则返回合并结果

        Returns:
            KeyValue记录，如果没有则返回None
        """
        if not self.is_initialized:
            return self.initial_kv
        return self.latest_kv


def built_comparator(key_schema: pa.Schema) -> Callable[[Any, Any], int]:
    """
    构建键比较器函数

    Args:
        key_schema: 键的PyArrow Schema

    Returns:
        一个比较函数，接受两个键并返回比较结果（-1, 0, 1）
    """

    def comparator(key1, key2) -> int:
        if key1 is None and key2 is None:
            return 0
        if key1 is None:
            return -1
        if key2 is None:
            return 1

        # 遍历schema中的每个字段进行比较
        for i, field in enumerate(key_schema):
            field_type = field.type
            val1 = key1.get_field(i)
            val2 = key2.get_field(i)

            # 处理空值情况
            if val1 is None and val2 is None:
                continue
            if val1 is None:
                return -1
            if val2 is None:
                return 1

            # 根据类型进行比较
            if pa.types.is_integer(field_type) or pa.types.is_floating(field_type) or pa.types.is_boolean(field_type):
                if val1 < val2:
                    return -1
                elif val1 > val2:
                    return 1
            elif pa.types.is_string(field_type) or pa.types.is_binary(field_type):
                if val1 < val2:
                    return -1
                elif val1 > val2:
                    return 1
            elif pa.types.is_timestamp(field_type) or pa.types.is_date(field_type):
                if val1 < val2:
                    return -1
                elif val1 > val2:
                    return 1
            else:
                # 对于复杂类型，可能需要更复杂的处理
                str_val1 = str(val1)
                str_val2 = str(val2)
                if str_val1 < str_val2:
                    return -1
                elif str_val1 > str_val2:
                    return 1

        # 所有字段都相等
        return 0

    return comparator


class Element:
    """记录在最小堆中的元素"""

    def __init__(self, kv, iterator: RecordIterator, reader: RecordReader):
        self.kv = kv
        self.iterator = iterator
        self.reader = reader

    def update(self) -> bool:
        """
        更新元素中的KeyValue

        Returns:
            如果成功更新则返回True，如果已经没有更多记录则返回False
        """
        next_kv = self.iterator.next()
        if next_kv is None:
            return False
        self.kv = next_kv
        return True


# 自定义堆条目类，使得可以在最小堆中正确比较
class HeapEntry:
    def __init__(self, key, element: Element, key_comparator):
        self.key = key
        self.element = element
        self.key_comparator = key_comparator

    def __lt__(self, other):
        # 首先比较键
        result = self.key_comparator(self.key, other.key)
        if result < 0:
            return True
        elif result > 0:
            return False

        # 如果键相同，比较序列号
        return self.element.kv.sequence_number < other.element.kv.sequence_number


class SortMergeIterator(RecordIterator):
    """SortMergeReader的迭代器"""

    def __init__(self, reader, polled: List[Element], min_heap, merge_function,
                 user_key_comparator, next_batch_readers):
        self.reader = reader
        self.polled = polled
        self.min_heap = min_heap
        self.merge_function = merge_function
        self.user_key_comparator = user_key_comparator
        self.next_batch_readers = next_batch_readers
        self.released = False

    def next(self):
        while True:
            has_more = self._next_impl()
            if not has_more:
                return None
            result = self.merge_function.get_result()
            if result is not None:
                return result

    def _next_impl(self):
        """实现下一个元素的获取逻辑"""
        if self.released:
            raise RuntimeError("SortMergeIterator.next调用在release之后")

        if not self.next_batch_readers:
            # 将之前取出的元素放回优先队列
            for element in self.polled:
                if element.update():
                    # 还有数据，添加回优先队列
                    entry = HeapEntry(element.kv.key, element, self.user_key_comparator)
                    heapq.heappush(self.min_heap, entry)
                else:
                    # 当前批次处理完毕，清理并添加到下一批次读取器列表
                    element.iterator.release_batch()
                    self.next_batch_readers.append(element.reader)

            self.polled.clear()

            # 有读取器到达批次末尾，结束当前批次
            if self.next_batch_readers:
                return False

            # 确保最小堆非空
            if not self.min_heap:
                return False

            # 重置合并函数
            self.merge_function.reset()

            # 获取最小堆顶部元素的键
            first_entry = self.min_heap[0]
            key = first_entry.key

            # 取出所有具有相同键的元素
            while self.min_heap and self.user_key_comparator(key, self.min_heap[0].key) == 0:
                entry = heapq.heappop(self.min_heap)
                self.merge_function.add(entry.element.kv)
                self.polled.append(entry.element)

            return True

    def release_batch(self):
        self.released = True


class SortMergeReader:
    def __init__(self, java_reader: JavaObject):
        reader_class = java_reader.getClass()

        batch_readers_field = reader_class.getDeclaredField("nextBatchReaders")
        batch_readers_field.setAccessible(True)
        java_batch_readers = batch_readers_field.get(java_reader)

        self.readers = []
        for next_reader in java_batch_readers:
            self.readers.append(convert_java_reader(next_reader))

        self.next_batch_readers = list(self.readers)

        key_schema = pa.schema([
            pa.field('_KEY_student_id', pa.string())
        ])
        self.user_key_comparator = built_comparator(key_schema)
        self.merge_function = DeduplicateMergeFunction(False)

        # 创建一个方法来比较两个元素
        def element_comparator(e1_tuple, e2_tuple):
            # 元组格式为 (key, element)
            key1, e1 = e1_tuple
            key2, e2 = e2_tuple

            # 首先比较键
            result = self.user_key_comparator(key1, key2)
            if result != 0:
                return result

            # 最后比较序列号
            return e1.kv.sequence_number - e2.kv.sequence_number

        # 使用functools.cmp_to_key将比较函数转换为key函数
        from functools import cmp_to_key
        self.element_key = cmp_to_key(element_comparator)

        # 初始化最小堆和已取出元素列表
        self.min_heap = []
        self.polled = []

    def read_batch(self) -> Optional[RecordIterator]:
        """
        读取下一批数据

        Returns:
            记录迭代器，如果没有更多数据则返回None
        """
        # 从每个读取器中获取数据添加到最小堆
        for reader in self.next_batch_readers:
            while True:
                iterator = reader.read_batch()
                if iterator is None:
                    # 没有更多批次，永久移除此读取器
                    reader.close()
                    break

                kv = iterator.next()
                if kv is None:
                    # 空迭代器，清理并尝试下一批
                    iterator.release_batch()
                else:
                    # 找到下一个kv
                    element = Element(kv, iterator, reader)
                    # 创建堆条目并添加到最小堆
                    entry = HeapEntry(kv.key, element, self.user_key_comparator)
                    heapq.heappush(self.min_heap, entry)
                    break

        # 清空已处理的读取器列表
        self.next_batch_readers.clear()

        if not self.min_heap:
            return None

        return SortMergeIterator(
            self,
            self.polled,
            self.min_heap,
            self.merge_function,
            self.user_key_comparator,
            self.next_batch_readers
        )

    def close(self):
        """关闭读取器并释放资源"""
        # 关闭所有未处理的读取器
        for reader in self.next_batch_readers:
            reader.close()

        # 关闭最小堆中的所有元素
        for entry in self.min_heap:
            entry.element.iterator.release_batch()
            entry.element.reader.close()

        # 关闭已取出元素中的所有元素
        for element in self.polled:
            element.iterator.release_batch()
            element.reader.close()