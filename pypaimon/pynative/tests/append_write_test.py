import pyarrow as pa

from pypaimon.api import Schema
from pypaimon.api.catalog_factory import CatalogFactory

if __name__ == '__main__':

    catalog = CatalogFactory.create({
        "warehouse": "/tmp/append_write_test"
    })

    catalog.create_database("test", True)

    catalog.create_table("test.student", Schema(pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string())
        ]), options={}), True)

    table = catalog.get_table("test.student")

    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()

    table_write.write_arrow()

    commit_messages = table_write.prepare_commit()
    table_commit.commit(commit_messages)

    table_write.close()
    table_commit.close()
