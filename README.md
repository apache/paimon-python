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

## Check

We provide script to check codes.

```shell
./dev/lint-python.sh    # execute all checks
./dev/lint-python.sh -h # run this to see more usages         
```

# Usage

See Apache Paimon Python API [Doc](https://paimon.apache.org/docs/master/program-api/python-api/).




