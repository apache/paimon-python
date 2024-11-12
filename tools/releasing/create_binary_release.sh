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
## Required variables
##
RELEASE_VERSION=${RELEASE_VERSION}

if [ -z "${RELEASE_VERSION}" ]; then
	echo "RELEASE_VERSION was not set"
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

RELEASE_DIR=${PROJECT_ROOT}/release/binary
rm -rf ${RELEASE_DIR}
mkdir -p ${RELEASE_DIR}

# use lint-python.sh script to create a python environment.
dev/lint-python.sh -s basic
source dev/.conda/bin/activate

# build
dev/build-wheels.sh

WHEEL_FILE_NAME="paimon_python-${RELEASE_VERSION}-py3-none-any.whl"
cp "dist/${WHEEL_FILE_NAME}" "${RELEASE_DIR}/${WHEEL_FILE_NAME}"

cd ${RELEASE_DIR}

# Sign sha the wheel package
gpg --armor --detach-sig ${WHEEL_FILE_NAME}
$SHASUM ${WHEEL_FILE_NAME} > "${WHEEL_FILE_NAME}.sha512"

cd ${CURR_DIR}
