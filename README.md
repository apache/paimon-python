![Paimon](https://paimon.apache.org/assets/paimon_blue.svg)

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

### Build paimon-python-java-bridge

```bash
cd java-based-implementation/paimon-python-java-bridge/
mvn clean install -DskipTests
```
The built target is java-based-implementation/paimon-python-java-bridge/target/paimon-python-java-bridge-<version>.jar

### Set Environment Variables

`py4j` need to access a JVM, so we should set JVM arguments (optional) and Java classpath. A convenient way is using
`os` packages to set environment variables which only affect current process.

```python
import os

os.environ['PYPAIMON_JAVA_CLASSPATH'] = '/path/to/paimon-python-java-bridge-<version>.jar'
os.environ['_PYPAIMON_JVM_ARGS'] = 'jvm_arg1 jvm_arg2 ...'
```

# API Reference
TODO




