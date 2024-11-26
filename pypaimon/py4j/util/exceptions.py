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

import py4j


def install_py4j_hooks():
    """
    Hook the classes such as JavaPackage, etc of Py4j to improve the exception message.
    """
    def wrapped_call(self, *args, **kwargs):
        raise TypeError(
            "Could not found the Java class '%s'. The Java dependencies could be specified via "
            "command line argument '--jarfile' or the config option 'pipeline.jars'" % self._fqn)

    setattr(py4j.java_gateway.JavaPackage, '__call__', wrapped_call)
