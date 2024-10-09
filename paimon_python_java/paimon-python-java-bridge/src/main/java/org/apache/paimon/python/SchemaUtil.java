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
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayOutputStream;

/** Util to get arrow schema from row type. */
public class SchemaUtil {
    public static byte[] getArrowSchema(RowType rowType) {
        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot emptyRoot = ArrowUtils.createVectorSchemaRoot(rowType, allocator, true);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowUtils.serializeToIpc(emptyRoot, out);
        emptyRoot.close();
        allocator.close();
        return out.toByteArray();
    }
}
