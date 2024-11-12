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
import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.stream.Collectors;

/** Write Arrow bytes to Paimon. */
public class BytesWriter {

    private final TableWrite tableWrite;
    private final ArrowBatchReader arrowBatchReader;
    private final BufferAllocator allocator;
    private final List<Field> arrowFields;

    public BytesWriter(TableWrite tableWrite, RowType rowType) {
        this.tableWrite = tableWrite;
        this.arrowBatchReader = new ArrowBatchReader(rowType);
        this.allocator = new RootAllocator();
        arrowFields =
                rowType.getFields().stream()
                        .map(f -> ArrowUtils.toArrowField(f.name(), f.type()))
                        .collect(Collectors.toList());
    }

    public void write(byte[] bytes) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ArrowStreamReader arrowStreamReader = new ArrowStreamReader(bais, allocator);
        VectorSchemaRoot vsr = arrowStreamReader.getVectorSchemaRoot();
        if (!checkTypesIgnoreNullability(arrowFields, vsr.getSchema().getFields())) {
            throw new RuntimeException(
                    String.format(
                            "Input schema isn't consistent with table schema.\n"
                                    + "\tTable schema is: %s\n"
                                    + "\tInput schema is: %s",
                            arrowFields, vsr.getSchema().getFields()));
        }

        while (arrowStreamReader.loadNextBatch()) {
            Iterable<InternalRow> rows = arrowBatchReader.readBatch(vsr);
            for (InternalRow row : rows) {
                tableWrite.write(row);
            }
        }
        arrowStreamReader.close();
    }

    public void close() {
        allocator.close();
    }

    private boolean checkTypesIgnoreNullability(
            List<Field> expectedFields, List<Field> actualFields) {
        if (expectedFields.size() != actualFields.size()) {
            return false;
        }

        for (int i = 0; i < expectedFields.size(); i++) {
            Field expectedField = expectedFields.get(i);
            Field actualField = actualFields.get(i);
            // ArrowType doesn't have nullability (similar to DataTypeRoot)
            if (!actualField.getType().equals(expectedField.getType())
                    || !checkTypesIgnoreNullability(
                            expectedField.getChildren(), actualField.getChildren())) {
                return false;
            }
        }

        return true;
    }
}
