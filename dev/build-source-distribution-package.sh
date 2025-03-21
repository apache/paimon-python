#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../"

DEPS_DIR=${PROJECT_ROOT}/deps
rm -rf ${DEPS_DIR}

# prepare bridge jar
BRIDGE_DEPS_DIR=${DEPS_DIR}/jars
mkdir -p ${BRIDGE_DEPS_DIR}
touch ${BRIDGE_DEPS_DIR}/__init__.py

cd ${PROJECT_ROOT}/paimon-python-java-bridge

# get bridge jar version
BRIDGE_JAR_VERSION=$(sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p' pom.xml | head -n 1)

mvn clean install -DskipTests
cp "target/paimon-python-java-bridge-${BRIDGE_JAR_VERSION}.jar" ${BRIDGE_DEPS_DIR}

# prepare hadoop-deps jar
HADOOP_DEPS_DIR=${DEPS_DIR}/hadoop
mkdir -p ${HADOOP_DEPS_DIR}
touch ${HADOOP_DEPS_DIR}/__init__.py

cd ${PROJECT_ROOT}/hadoop-deps

# get hadoop-deps jar version
HADOOP_JAR_VERSION=$(sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p' pom.xml | head -n 1)

mvn clean install -DskipTests
cp "target/hadoop-deps-${HADOOP_JAR_VERSION}.jar" ${HADOOP_DEPS_DIR}

cd ${CURR_DIR}

# build source distribution package

python setup.py sdist

rm -rf ${DEPS_DIR}
cd ${CURR_DIR}
