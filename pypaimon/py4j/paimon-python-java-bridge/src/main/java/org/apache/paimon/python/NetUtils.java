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

import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.net.ServerSocket;

/** Utility for various network related tasks (such as finding free ports). */
public class NetUtils {

    /**
     * Find a non-occupied port.
     *
     * @return A non-occupied port.
     */
    public static Port getAvailablePort() {
        for (int i = 0; i < 50; i++) {
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                int port = serverSocket.getLocalPort();
                if (port != 0) {
                    FileLock fileLock = new FileLock(NetUtils.class.getName() + port);
                    if (fileLock.tryLock()) {
                        return new Port(port, fileLock);
                    } else {
                        fileLock.unlockAndDestroy();
                    }
                }
            } catch (IOException ignored) {
            }
        }

        throw new RuntimeException("Could not find a free permitted port on the machine.");
    }

    /**
     * Port wrapper class which holds a {@link FileLock} until it releases. Used to avoid race
     * condition among multiple threads/processes.
     */
    public static class Port implements AutoCloseable {
        private final int port;
        private final FileLock fileLock;

        public Port(int port, FileLock fileLock) throws IOException {
            Preconditions.checkNotNull(fileLock, "FileLock should not be null");
            Preconditions.checkState(fileLock.isValid(), "FileLock should be locked");
            this.port = port;
            this.fileLock = fileLock;
        }

        public int getPort() {
            return port;
        }

        @Override
        public void close() throws Exception {
            fileLock.unlockAndDestroy();
        }
    }
}
