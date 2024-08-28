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

import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Read Arrow bytes from split. */
public class BytesReader {

    private static final int DEFAULT_WRITE_BATCH_SIZE = 2048;

    private final TableRead tableRead;
    private final ArrowFormatWriter arrowFormatWriter;

    private RecordReaderIterator<InternalRow> iterator;
    private InternalRow nextRow;

    public BytesReader(TableRead tableRead, RowType rowType) {
        this.tableRead = tableRead;
        this.arrowFormatWriter = new ArrowFormatWriter(rowType, DEFAULT_WRITE_BATCH_SIZE, true);
    }

    public void setSplit(Split split) throws IOException {
        RecordReader<InternalRow> recordReader = tableRead.createReader(split);
        iterator = new RecordReaderIterator<InternalRow>(recordReader);
        nextRow();
    }

    public byte[] serializeSchema() {
        VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowUtils.serializeToIpc(vsr, out);
        return out.toByteArray();
    }

    @Nullable
    public byte[] next() throws Exception {
        if (nextRow == null) {
            return null;
        }

        int rowCount = 0;
        while (nextRow != null && arrowFormatWriter.write(nextRow)) {
            nextRow();
            rowCount++;
        }

        arrowFormatWriter.flush();
        VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
        vsr.setRowCount(rowCount);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowUtils.serializeToIpc(vsr, out);
        if (nextRow == null) {
            // close resource
            arrowFormatWriter.close();
            iterator.close();
        }
        return out.toByteArray();
    }

    private void nextRow() {
        if (iterator.hasNext()) {
            nextRow = iterator.next();
        } else {
            nextRow = null;
        }
    }
}
