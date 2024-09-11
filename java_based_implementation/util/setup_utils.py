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

import importlib
import os
import shutil
import subprocess

from java_based_implementation.util import constants
from java_based_implementation.util.constants import PYPAIMON_JAVA_CLASSPATH

_JAVA_IMPL_MODULE = 'java_based_implementation'
_JAVA_DEPS = 'java_dependencies'
_JAVA_BRIDGE = 'paimon-python-java-bridge'
# TODO configure version
_JAVA_BRIDGE_VERSION = '0.9-SNAPSHOT'


def get_package_data():
    is_tox_test = os.environ.get(constants.PYPAIMON_TOX_TEST)
    if is_tox_test and is_tox_test.lower() == "true":
        return ['']

    setup_java_bridge()
    return [os.path.join(_JAVA_DEPS, '*')]


def clean():
    java_deps_dir = os.path.join(_find_java_impl_dir(), _JAVA_DEPS)
    if os.path.exists(java_deps_dir):
        shutil.rmtree(java_deps_dir)


def get_classpath(env):
    user_defined = env.get(PYPAIMON_JAVA_CLASSPATH)

    module = importlib.import_module(_JAVA_IMPL_MODULE)
    builtin_java_bridge = os.path.join(*module.__path__, _JAVA_DEPS, _JAVA_BRIDGE + '.jar')

    if user_defined is None:
        return builtin_java_bridge
    else:
        return os.pathsep.join([builtin_java_bridge, user_defined])


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
                     .format(_JAVA_BRIDGE, _JAVA_BRIDGE_VERSION)),
        java_bridge_dst
    )


def _find_java_impl_dir():
    abspath = os.path.abspath(__file__)
    return os.path.dirname(os.path.dirname(abspath))
