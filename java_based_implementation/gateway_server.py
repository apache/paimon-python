################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os
import platform
import signal

from subprocess import Popen, PIPE
from java_based_implementation.util.constants import (PYPAIMON_JVM_ARGS, PYPAIMON_JAVA_CLASSPATH,
                                                      PYPAIMON_MAIN_ARGS, PYPAIMON_MAIN_CLASS)


def on_windows():
    return platform.system() == "Windows"


def find_java_executable():
    java_executable = "java.exe" if on_windows() else "java"
    java_home = None

    if java_home is None and "JAVA_HOME" in os.environ:
        java_home = os.environ["JAVA_HOME"]

    if java_home is not None:
        java_executable = os.path.join(java_home, "bin", java_executable)

    return java_executable


def launch_gateway_server_process(env):
    java_executable = find_java_executable()
    # TODO construct Java module log settings
    log_settings = []
    jvm_args = env.get(PYPAIMON_JVM_ARGS, '').split()
    classpath = env.get(PYPAIMON_JAVA_CLASSPATH)
    main_args = env.get(PYPAIMON_MAIN_ARGS, '').split()
    command = [
        java_executable,
        *jvm_args,
        # default jvm args
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=jdk.proxy2/jdk.proxy2=ALL-UNNAMED",
        *log_settings,
        "-cp",
        classpath,
        "-c",
        PYPAIMON_MAIN_CLASS,
        *main_args
    ]

    if not on_windows():
        def preexec_func():
            # ignore ctrl-c / SIGINT
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        preexec_fn = preexec_func
    return Popen(list(filter(lambda c: len(c) != 0, command)),
                 stdin=PIPE, stderr=PIPE, preexec_fn=preexec_fn, env=env)
