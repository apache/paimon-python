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
import urllib.request

from paimon_python_java.util import constants


def setup_hadoop_bundle_jar(hadoop_dir):
    if constants.PYPAIMON_HADOOP_CLASSPATH in os.environ:
        file = os.environ[constants.PYPAIMON_HADOOP_CLASSPATH]
        if os.path.isfile(file):
            return

    url = 'https://repo.maven.apache.org/maven2/org/apache/flink/' \
          'flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar'

    response = urllib.request.urlopen(url)
    if not os.path.exists(hadoop_dir):
        os.mkdir(hadoop_dir)

    jar_path = os.path.join(hadoop_dir, "bundled-hadoop.jar")
    with open(jar_path, 'wb') as file:
        file.write(response.read())

    os.environ[constants.PYPAIMON_HADOOP_CLASSPATH] = jar_path
