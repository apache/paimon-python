
import os

from pypaimon import Catalog
from pypaimon.py4j.java_gateway import get_gateway
from pypaimon.py4j.util import constants
from pypaimon.pynative.record_reader_wrapper import convert_java_reader


if __name__ == '__main__':
    os.environ[constants.PYPAIMON4J_TEST_MODE] = 'true'

    this_dir = os.path.abspath(os.path.dirname(__file__))
    project_dir = os.path.dirname(os.path.dirname(this_dir))
    deps = os.path.join(project_dir, "dev/test_deps/*")
    os.environ[constants.PYPAIMON_HADOOP_CLASSPATH] = deps

    java_gateway = get_gateway()

    catalog = Catalog.create({'warehouse': "/tmp/paimon"})
    table = catalog.get_table('school.student_primary_key')
    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    splits = table_scan.plan().splits()
    print(len(splits))

    table_read = read_builder.new_read()
    j_table_read = table_read.j_table_read

    j_reader = j_table_read.createReader(splits[0].to_j_split())

    first_reader = convert_java_reader(j_reader)

    try:
        batch = first_reader.read_batch()
        while batch is not None:
            record = batch.next()
            while record is not None:
                print(record)
                record = batch.next()

            batch.release_batch()
            batch = first_reader.read_batch()
    finally:
        first_reader.close()
