![Paimon](https://github.com/apache/paimon/blob/master/docs/static/paimon-simple.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

This repo is for Apache Paimon Python SDK.

# Development Notice

## Checkstyle

[Flake8](https://pypi.org/project/flake8/) is used to enforce some coding guidelines.

1. Install flake8 for your Python interpreter using `pip install flake8`.
2. In PyCharm go to "Settings" → "Tools" → "External Tools".
3. Select the "+" button to add a new external tool.
4. Set "Name" to "flake8".
5. Set "Description" to "Code Style Check".
6. Set "Program" to the path of your Python interpreter, e.g. `/usr/bin/python`.
7. Set "Arguments" to `-m flake8 --config=tox.ini`.
8. Set "Working Directory" to `$ProjectFileDir$`.

You can verify the setup by right-clicking on any file or folder in the flink-python project
and running "External Tools" → "flake8".

# Usage

## Java-Based Implementation

We can use `py4j` to leverage Java code to read Paimon data. This section describes how to use this implementation.

### Set Environment Variables

`py4j` need to access a JVM, so we should set JVM arguments (optional) and Java classpath. A convenient way is using
`os` packages to set environment variables which only affect current process.

```python
import os

os.environ['PYPAIMON_JAVA_CLASSPATH'] = '/path/to/dependent_jars/*'
os.environ['_PYPAIMON_JVM_ARGS'] = 'jvm_arg1 jvm_arg2 ...'
```

NOTE: the package has set paimon core and hadoop dependencies. If you just test in local or run code in hadoop, you doesn't
need to set classpath. If you need other dependencies such as OSS/S3 filesystem jars, or special catalog which isn't implemented 
in paimon core, please download jars and set classpath.

# API Reference
TODO




