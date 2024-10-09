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
import org.apache.paimon.utils.ThreadPoolUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

/** Parallely read Arrow bytes from multiple splits. */
public class ParallelBytesReader {

    private static final String THREAD_NAME_PREFIX = "PARALLEL_SPLITS_READER";
    private static final int DEFAULT_WRITE_BATCH_SIZE = 1024;

    private final TableRead tableRead;
    private final RowType rowType;
    private final int threadNum;

    private final ConcurrentLinkedQueue<RecordReaderIterator<InternalRow>> iterators;
    private final ConcurrentLinkedQueue<ArrowFormatWriter> arrowFormatWriters;

    private ThreadPoolExecutor executor;
    private Iterator<byte[]> bytesIterator;

    public ParallelBytesReader(TableRead tableRead, RowType rowType, int threadNum) {
        this.tableRead = tableRead;
        this.rowType = rowType;
        this.threadNum = threadNum;
        this.iterators = new ConcurrentLinkedQueue<>();
        this.arrowFormatWriters = new ConcurrentLinkedQueue<>();
    }

    public void setSplits(List<Split> splits) {
        bytesIterator = randomlyExecute(getExecutor(), makeProcessor(), splits);
    }

    @Nullable
    public byte[] next() {
        if (bytesIterator.hasNext()) {
            return bytesIterator.next();
        } else {
            closeResources();
            return null;
        }
    }

    private ThreadPoolExecutor getExecutor() {
        if (executor == null) {
            executor = ThreadPoolUtils.createCachedThreadPool(threadNum, THREAD_NAME_PREFIX);
        }
        return executor;
    }

    private Function<Split, Iterator<byte[]>> makeProcessor() {
        return split -> {
            try {
                RecordReader<InternalRow> recordReader = tableRead.createReader(split);
                RecordReaderIterator<InternalRow> iterator =
                        new RecordReaderIterator<>(recordReader);
                iterators.add(iterator);
                ArrowFormatWriter arrowFormatWriter =
                        new ArrowFormatWriter(rowType, DEFAULT_WRITE_BATCH_SIZE, true);
                arrowFormatWriters.add(arrowFormatWriter);
                return new RecordBytesIterator(iterator, arrowFormatWriter);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private <U, T> Iterator<T> randomlyExecute(
            ExecutorService executor, Function<U, Iterator<T>> processor, Collection<U> input) {
        List<Future<Iterator<T>>> futures = new ArrayList<>(input.size());
        for (U u : input) {
            futures.add(executor.submit(() -> processor.apply(u)));
        }
        return futuresToIterIter(futures);
    }

    private <T> Iterator<T> futuresToIterIter(List<Future<Iterator<T>>> futures) {
        final Queue<Future<Iterator<T>>> queue = new ArrayDeque<>(futures);
        return Iterators.concat(
                new Iterator<Iterator<T>>() {
                    public boolean hasNext() {
                        return !queue.isEmpty();
                    }

                    public Iterator<T> next() {
                        try {
                            return queue.poll().get();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private void closeResources() {
        for (RecordReaderIterator<InternalRow> iterator : iterators) {
            try {
                iterator.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        iterators.clear();

        for (ArrowFormatWriter arrowFormatWriter : arrowFormatWriters) {
            arrowFormatWriter.close();
        }
        arrowFormatWriters.clear();
    }
}
