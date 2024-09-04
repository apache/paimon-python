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

import org.apache.arrow.vector.VectorSchemaRoot;

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
public class ParallelBytesReader extends AbstractBytesReader {

    private static final String THREAD_NAME_PREFIX = "PARALLEL_SPLITS_READER";

    private final ConcurrentLinkedQueue<RecordReaderIterator<InternalRow>> iterators;
    private final ConcurrentLinkedQueue<ArrowFormatWriter> arrowFormatWriters;
    private final int threadNum;

    private ThreadPoolExecutor executor;

    public ParallelBytesReader(TableRead tableRead, RowType rowType, int threadNum) {
        super(tableRead, rowType);
        this.iterators = new ConcurrentLinkedQueue<>();
        this.arrowFormatWriters = new ConcurrentLinkedQueue<>();
        this.threadNum = threadNum;
    }

    public void setSplits(List<Split> splits) {
        bytesIterator = randomlyExecute(getExecutor(), makeProcessor(), splits);
    }

    private ThreadPoolExecutor getExecutor() {
        if (executor == null) {
            executor = ThreadPoolUtils.createCachedThreadPool(threadNum, THREAD_NAME_PREFIX);
        }
        return executor;
    }

    @Override
    protected VectorSchemaRoot getEmptyVsr() {
        ArrowFormatWriter arrowFormatWriter = newWriter();
        VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
        arrowFormatWriter.close();
        return vsr;
    }

    @Override
    public void closeResources() {
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

    private Function<Split, Iterator<byte[]>> makeProcessor() {
        return split -> {
            try {
                RecordReader<InternalRow> recordReader = tableRead.createReader(split);
                RecordReaderIterator<InternalRow> iterator =
                        new RecordReaderIterator<>(recordReader);
                iterators.add(iterator);
                ArrowFormatWriter arrowFormatWriter = newWriter();
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
}
