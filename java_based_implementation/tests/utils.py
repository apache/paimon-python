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
import tempfile


def set_bridge_jar() -> str:
    java_module = '../paimon-python-java-bridge'
    # build paimon-python-java-bridge
    subprocess.run(
        ["mvn", "clean", "package"],
        cwd=java_module,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    jar_name = 'paimon-python-java-bridge-0.9-SNAPSHOT.jar'
    jar_file = os.path.join(java_module, 'target', jar_name)
    # move to temp dir
    temp_dir = tempfile.mkdtemp()
    shutil.move(jar_file, temp_dir)
    return os.path.join(temp_dir, jar_name)
