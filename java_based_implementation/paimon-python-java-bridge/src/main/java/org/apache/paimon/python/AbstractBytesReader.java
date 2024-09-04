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
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;

/** Base class of bytes reader to read Arrow Bytes from split. */
public abstract class AbstractBytesReader {

    private static final int DEFAULT_WRITE_BATCH_SIZE = 2048;

    private final RowType rowType;
    protected final TableRead tableRead;

    protected Iterator<byte[]> bytesIterator;

    public AbstractBytesReader(TableRead tableRead, RowType rowType) {
        this.rowType = rowType;
        this.tableRead = tableRead;
    }

    public byte[] serializeSchema() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowUtils.serializeToIpc(getEmptyVsr(), out);
        return out.toByteArray();
    }

    protected abstract VectorSchemaRoot getEmptyVsr();

    @Nullable
    public byte[] next() {
        if (bytesIterator.hasNext()) {
            return bytesIterator.next();
        } else {
            closeResources();
            return null;
        }
    }

    protected abstract void closeResources();

    protected ArrowFormatWriter newWriter() {
        return new ArrowFormatWriter(rowType, DEFAULT_WRITE_BATCH_SIZE, true);
    }
}
