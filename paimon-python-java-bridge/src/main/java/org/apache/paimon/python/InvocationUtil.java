/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.python;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Call some methods in Python directly will raise py4j.Py4JException: Method method([]) does not
 * exist. This util is a workaround.
 */
public class InvocationUtil {

    public static BatchWriteBuilder getBatchWriteBuilder(Table table) {
        return table.newBatchWriteBuilder();
    }

    public static ReadBuilder getReadBuilder(Table table) {
        return table.newReadBuilder();
    }

    public static ParallelBytesReader createParallelBytesReader(
            TableRead tableRead, RowType rowType, int threadNum) {
        return new ParallelBytesReader(tableRead, rowType, threadNum);
    }

    public static BytesWriter createBytesWriter(TableWrite tableWrite, RowType rowType) {
        return new BytesWriter(tableWrite, rowType);
    }

    /**
     * To resolve py4j bug: 'py4j.Py4JException: Method createReader([class java.util.ArrayList])
     * does not exist'
     */
    public static RecordReader<InternalRow> createReader(TableRead tableRead, List<Split> splits)
            throws IOException {
        List<ReaderSupplier<InternalRow>> readers = new ArrayList();
        for (Split split : splits) {
            readers.add(() -> tableRead.createReader(split));
        }
        return ConcatRecordReader.create(readers);
    }
}
