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
	echo "RELEASE_VERSION is unset"
	exit 1
fi

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="$( cd "$( dirname "${BASE_DIR}/../../../" )" >/dev/null && pwd )"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
    TAR="tar --no-xattrs"
else
    SHASUM="sha512sum"
    TAR="tar"
fi

###########################

RELEASE_DIR=${PROJECT_ROOT}/release/source
CLONE_DIR=${RELEASE_DIR}/paimon-tmp-clone

rm -rf ${RELEASE_DIR}
mkdir -p ${RELEASE_DIR}

# delete the temporary release directory on error
trap 'rm -rf ${RELEASE_DIR}' ERR

echo "Creating source package"

# create a temporary git clone to ensure that we have a pristine source release
git clone ${PROJECT_ROOT} ${CLONE_DIR}

cd ${CLONE_DIR}
JAVA_ROOT="paimon_python_java/paimon-python-java-bridge"
rsync -a \
  --exclude ".DS_Store" --exclude ".asf.yaml" --exclude ".git" \
  --exclude ".github" --exclude ".gitignore" --exclude ".idea" \
  --exclude ".mypy_cache" --exclude ".tox" --exclude "__pycache__" \
  --exclude "build" --exclude "dist" --exclude "*.egg-info" \
  --exclude "dev/.conda" --exclude "dev/.stage.txt" \
  --exclude "dev/download" --exclude "dev/log" --exclude "**/__pycache__" \
  --exclude "${JAVA_ROOT}/dependency-reduced-pom.xml" \
  --exclude "${JAVA_ROOT}/target" \
  . paimon-python-${RELEASE_VERSION}

TAR czf ${RELEASE_DIR}/apache-paimon-python-${RELEASE_VERSION}-src.tgz paimon-python-${RELEASE_VERSION}
gpg --armor --detach-sig ${RELEASE_DIR}/apache-paimon-python-${RELEASE_VERSION}-src.tgz
cd ${RELEASE_DIR}
${SHASUM} apache-paimon-python-${RELEASE_VERSION}-src.tgz > apache-paimon-python-${RELEASE_VERSION}-src.tgz.sha512

rm -rf ${CLONE_DIR}

echo "Done. Source release package and signatures created under ${RELEASE_DIR}/."

cd ${CURR_DIR}
