/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.python;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.stream.Collectors;

/** For building Predicate. */
public class PredicationUtil {

    public static Predicate build(
            RowType rowType,
            PredicateBuilder builder,
            String method,
            int index,
            List<Object> literals) {
        literals =
                literals.stream()
                        .map(l -> convertJavaObject(rowType.getTypeAt(index), l))
                        .collect(Collectors.toList());
        switch (method) {
            case "equal":
                return builder.equal(index, literals.get(0));
            case "notEqual":
                return builder.notEqual(index, literals.get(0));
            case "lessThan":
                return builder.lessThan(index, literals.get(0));
            case "lessOrEqual":
                return builder.lessOrEqual(index, literals.get(0));
            case "greaterThan":
                return builder.greaterThan(index, literals.get(0));
            case "greaterOrEqual":
                return builder.greaterOrEqual(index, literals.get(0));
            case "isNull":
                return builder.isNull(index);
            case "isNotNull":
                return builder.isNotNull(index);
            case "startsWith":
                return builder.startsWith(index, literals.get(0));
            case "endsWith":
                return builder.endsWith(index, literals.get(0));
            case "contains":
                return builder.contains(index, literals.get(0));
            case "in":
                return builder.in(index, literals);
            case "notIn":
                return builder.notIn(index, literals);
            case "between":
                return builder.between(index, literals.get(0), literals.get(1));
            default:
                throw new UnsupportedOperationException(
                        "Unknown PredicateBuilder method " + method);
        }
    }

    /** Some type is not convenient to transfer from Python to Java. */
    private static Object convertJavaObject(DataType literalType, Object literal) {
        switch (literalType.getTypeRoot()) {
            case BOOLEAN:
            case DOUBLE:
            case INTEGER:
                return literal;
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString((String) literal);
            case FLOAT:
                return ((Number) literal).floatValue();
            case TINYINT:
                return ((Number) literal).byteValue();
            case SMALLINT:
                return ((Number) literal).shortValue();
            case BIGINT:
                return ((Number) literal).longValue();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate leaf type " + literalType.getTypeRoot().name());
        }
    }

    public static Predicate buildAnd(List<Predicate> predicates) {
        // 'and' is keyword of Python
        return PredicateBuilder.and(predicates);
    }

    public static Predicate buildOr(List<Predicate> predicates) {
        // 'or' is keyword of Python
        return PredicateBuilder.or(predicates);
    }
}
