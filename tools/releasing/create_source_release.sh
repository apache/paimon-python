#!/usr/bin/env bash

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

##
## set build vars
##
OUTPUT_DIR=${OUTPUT_DIR}
GPG_PASSPHRASE=${GPG_PASSPHRASE}

if [ -z "${OUTPUT_DIR}" ]; then
	echo "OUTPUT_DIR was not set"
	exit 1
fi

if [ -z "${GPG_PASSPHRASE}" ]; then
	echo "GPG_PASSPHRASE was not set"
	exit 1
fi

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

###########################

# prepare bridge jar

DEPS_DIR=${PROJECT_ROOT}/deps/jars
rm -rf ${DEPS_DIR}
mkdir -p ${DEPS_DIR}

cd ${PROJECT_ROOT}/paimon-python-java-bridge

# check there is no snapshot dependencies
if grep -q "<version>.*SNAPSHOT</version>" "pom.xml"; then
    echo "paimon-python-java-bridge is snapshot or contains snapshot dependencies"
    exit 1
fi

# get bridge jar version
JAR_VERSION=$(sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p' pom.xml | head -n 1)

mvn clean install -DskipTests
cp "target/paimon-python-java-bridge-${JAR_VERSION}.jar" ${DEPS_DIR}

cd ${CURR_DIR}

# build source release

# get release version
RELEASE_VERSION=$(sed -n 's/^__version__ = "\(.*\)"/\1/p' ${PROJECT_ROOT}/pypaimon/version.py)

# use lint-python.sh script to create a python environment.
dev/lint-python.sh -s basic
source dev/.conda/bin/activate

python setup.py sdist
conda deactivate
PACKAGE_FILE="pypaimon-${RELEASE_VERSION}.tar.gz"
cp "dist/${PACKAGE_FILE}" "${OUTPUT_DIR}/${PACKAGE_FILE}"

cd ${OUTPUT_DIR}

# Sign sha the wheel package
gpg --batch --yes --pinentry-mode loopback --passphrase=$GPG_PASSPHRASE --armor --detach-sign ${PACKAGE_FILE}
$SHASUM ${PACKAGE_FILE} > "${PACKAGE_FILE}.sha512"

rm -rf DEPS_DIR
cd ${CURR_DIR}
