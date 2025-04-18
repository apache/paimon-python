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

from functools import reduce

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow.dataset import Expression

from pypaimon import Predicate


def convert_predicate(predicate: Predicate) -> Expression | bool:
    """
    # Convert Paimon's Predicate to PyArrow Dataset's filter
    """
    if not hasattr(predicate, 'py_predicate'):
        raise ValueError("Predicate must have py_predicate attribute")

    py_predicate = predicate.py_predicate

    if py_predicate.method == 'equal':
        return ds.field(py_predicate.field) == py_predicate.literals[0]
    elif py_predicate.method == 'notEqual':
        return ds.field(py_predicate.field) != py_predicate.literals[0]
    elif py_predicate.method == 'lessThan':
        return ds.field(py_predicate.field) < py_predicate.literals[0]
    elif py_predicate.method == 'lessOrEqual':
        return ds.field(py_predicate.field) <= py_predicate.literals[0]
    elif py_predicate.method == 'greaterThan':
        return ds.field(py_predicate.field) > py_predicate.literals[0]
    elif py_predicate.method == 'greaterOrEqual':
        return ds.field(py_predicate.field) >= py_predicate.literals[0]
    elif py_predicate.method == 'isNull':
        return ds.field(py_predicate.field).is_null()
    elif py_predicate.method == 'isNotNull':
        return ds.field(py_predicate.field).is_valid()
    elif py_predicate.method == 'in':
        return ds.field(py_predicate.field).isin(py_predicate.literals)
    elif py_predicate.method == 'notIn':
        return ~ds.field(py_predicate.field).isin(py_predicate.literals)
    elif py_predicate.method == 'startsWith':
        pattern = py_predicate.literals[0]
        return pc.starts_with(ds.field(py_predicate.field).cast(pa.string()), pattern)
    elif py_predicate.method == 'endsWith':
        pattern = py_predicate.literals[0]
        return pc.ends_with(ds.field(py_predicate.field).cast(pa.string()), pattern)
    elif py_predicate.method == 'contains':
        pattern = py_predicate.literals[0]
        return pc.match_substring(ds.field(py_predicate.field).cast(pa.string()), pattern)
    elif py_predicate.method == 'between':
        return (ds.field(py_predicate.field) >= py_predicate.literals[0]) & \
            (ds.field(py_predicate.field) <= py_predicate.literals[1])
    elif py_predicate.method == 'and':
        return reduce(lambda x, y: x & y,
                      [convert_predicate(p) for p in py_predicate.literals])
    elif py_predicate.method == 'or':
        return reduce(lambda x, y: x | y,
                      [convert_predicate(p) for p in py_predicate.literals])
    else:
        raise ValueError(f"Unsupported predicate method: {py_predicate.method}")
