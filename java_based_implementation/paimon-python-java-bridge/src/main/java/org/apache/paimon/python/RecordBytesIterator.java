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
import org.apache.paimon.reader.RecordReaderIterator;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;

/** Cast a {@link RecordReaderIterator} to bytes iterator. */
public class RecordBytesIterator implements Iterator<byte[]> {

    private final RecordReaderIterator<InternalRow> iterator;
    private final ArrowFormatWriter arrowFormatWriter;

    private InternalRow nextRow;

    public RecordBytesIterator(
            RecordReaderIterator<InternalRow> iterator, ArrowFormatWriter arrowFormatWriter) {
        this.iterator = iterator;
        this.arrowFormatWriter = arrowFormatWriter;
        nextRow();
    }

    public boolean hasNext() {
        return nextRow != null;
    }

    public byte[] next() {
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
