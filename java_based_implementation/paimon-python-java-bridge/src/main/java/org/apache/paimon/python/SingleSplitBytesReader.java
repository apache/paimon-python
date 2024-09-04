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

import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

/** Read Arrow bytes from single split. */
public class SingleSplitBytesReader extends AbstractBytesReader {

    private RecordReaderIterator<InternalRow> iterator;
    private final ArrowFormatWriter arrowFormatWriter;

    public SingleSplitBytesReader(TableRead tableRead, RowType rowType) {
        super(tableRead, rowType);
        this.arrowFormatWriter = newWriter();
    }

    public void setSplit(Split split) throws IOException {
        RecordReader<InternalRow> recordReader = tableRead.createReader(split);
        iterator = new RecordReaderIterator<>(recordReader);
        bytesIterator = new RecordBytesIterator(iterator, arrowFormatWriter);
    }

    @Override
    protected VectorSchemaRoot getEmptyVsr() {
        return arrowFormatWriter.getVectorSchemaRoot();
    }

    @Override
    public void closeResources() {
        try {
            iterator.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        arrowFormatWriter.close();
    }
}
