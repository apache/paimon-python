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
import shutil
import subprocess

from xml.etree import ElementTree

_JAVA_IMPL_MODULE = 'paimon_python_java'
_JAVA_DEPS = 'java_dependencies'
_JAVA_BRIDGE = 'paimon-python-java-bridge'

_PYPAIMON_TOX_TEST = '_PYPAIMON_TOX_TEST'


def get_package_data():
    is_tox_test = os.environ.get(_PYPAIMON_TOX_TEST)
    if is_tox_test and is_tox_test.lower() == "true":
        return ['']

    setup_java_bridge()
    return [os.path.join(_JAVA_DEPS, '*')]


def clean():
    java_deps_dir = os.path.join(_find_java_impl_dir(), _JAVA_DEPS)
    if os.path.exists(java_deps_dir):
        shutil.rmtree(java_deps_dir)


def setup_java_bridge():
    java_impl_dir = _find_java_impl_dir()

    java_deps_dir = os.path.join(java_impl_dir, _JAVA_DEPS)
    if not os.path.exists(java_deps_dir):
        os.mkdir(java_deps_dir)

    java_bridge_dst = os.path.join(java_deps_dir, _JAVA_BRIDGE + '.jar')
    if os.path.exists(java_bridge_dst):
        return

    java_bridge_module = os.path.join(java_impl_dir, _JAVA_BRIDGE)
    subprocess.run(
        ["mvn", "clean", "package"],
        cwd=java_bridge_module,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    shutil.copy(
        os.path.join(java_bridge_module, 'target/{}-{}.jar'
                     .format(_JAVA_BRIDGE, _extract_bridge_version())),
        java_bridge_dst
    )


def _extract_bridge_version():
    pom_path = os.path.join(_find_java_impl_dir(), _JAVA_BRIDGE, 'pom.xml')
    return ElementTree.parse(pom_path).getroot().find(
        'POM:version',
        namespaces={
            'POM': 'http://maven.apache.org/POM/4.0.0'
        }).text


def _find_java_impl_dir():
    this_dir = os.path.abspath(os.path.dirname(__file__))
    paimon_python_dir = os.path.dirname(this_dir)
    return os.path.join(paimon_python_dir, _JAVA_IMPL_MODULE)
