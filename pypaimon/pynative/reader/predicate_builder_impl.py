from typing import List, Any, Optional

from pypaimon.api import PredicateBuilder, Predicate
from pypaimon.pynative.common.predicate import PyNativePredicate
from pypaimon.pynative.table.table_schema import TableSchema


class PredicateImpl(Predicate):
    """Implementation of Predicate using PyNativePredicate."""
    
    def __init__(self, py_predicate: PyNativePredicate):
        self.py_predicate = py_predicate


class PredicateBuilderImpl(PredicateBuilder):
    """Implementation of PredicateBuilder using PyNativePredicate."""
    
    def __init__(self, table_schema: TableSchema):
        self.table_schema = table_schema
        self.field_names = [field.name for field in table_schema.fields]
        
    def _get_field_index(self, field: str) -> int:
        """Get the index of a field in the schema."""
        try:
            return self.field_names.index(field)
        except ValueError:
            raise ValueError(f'The field {field} is not in field list {self.field_names}.')
            
    def _build_predicate(self, method: str, field: str, literals: Optional[List[Any]] = None) -> Predicate:
        """Build a predicate with the given method, field, and literals."""
        index = self._get_field_index(field)
        py_predicate = PyNativePredicate(
            method=method,
            index=index,
            field=field,
            literals=literals
        )
        return PredicateImpl(py_predicate)
        
    def equal(self, field: str, literal: Any) -> Predicate:
        """Create an equality predicate."""
        return self._build_predicate('equal', field, [literal])
        
    def not_equal(self, field: str, literal: Any) -> Predicate:
        """Create a not-equal predicate."""
        return self._build_predicate('notEqual', field, [literal])
        
    def less_than(self, field: str, literal: Any) -> Predicate:
        """Create a less-than predicate."""
        return self._build_predicate('lessThan', field, [literal])
        
    def less_or_equal(self, field: str, literal: Any) -> Predicate:
        """Create a less-or-equal predicate."""
        return self._build_predicate('lessOrEqual', field, [literal])
        
    def greater_than(self, field: str, literal: Any) -> Predicate:
        """Create a greater-than predicate."""
        return self._build_predicate('greaterThan', field, [literal])
        
    def greater_or_equal(self, field: str, literal: Any) -> Predicate:
        """Create a greater-or-equal predicate."""
        return self._build_predicate('greaterOrEqual', field, [literal])
        
    def is_null(self, field: str) -> Predicate:
        """Create an is-null predicate."""
        return self._build_predicate('isNull', field)
        
    def is_not_null(self, field: str) -> Predicate:
        """Create an is-not-null predicate."""
        return self._build_predicate('isNotNull', field)
        
    def startswith(self, field: str, pattern_literal: Any) -> Predicate:
        """Create a starts-with predicate."""
        return self._build_predicate('startsWith', field, [pattern_literal])
        
    def endswith(self, field: str, pattern_literal: Any) -> Predicate:
        """Create an ends-with predicate."""
        return self._build_predicate('endsWith', field, [pattern_literal])
        
    def contains(self, field: str, pattern_literal: Any) -> Predicate:
        """Create a contains predicate."""
        return self._build_predicate('contains', field, [pattern_literal])
        
    def is_in(self, field: str, literals: List[Any]) -> Predicate:
        """Create an in predicate."""
        return self._build_predicate('in', field, literals)
        
    def is_not_in(self, field: str, literals: List[Any]) -> Predicate:
        """Create a not-in predicate."""
        return self._build_predicate('notIn', field, literals)
        
    def between(self, field: str, included_lower_bound: Any, included_upper_bound: Any) -> Predicate:
        """Create a between predicate."""
        return self._build_predicate('between', field, [included_lower_bound, included_upper_bound])
        
    def and_predicates(self, predicates: List[Predicate]) -> Predicate:
        """Create an AND predicate from multiple predicates."""
        py_predicates = [p.py_predicate for p in predicates]
        py_predicate = PyNativePredicate(
            method='and',
            index=None,
            field=None,
            literals=py_predicates
        )
        return PredicateImpl(py_predicate)
        
    def or_predicates(self, predicates: List[Predicate]) -> Predicate:
        """Create an OR predicate from multiple predicates."""
        py_predicates = [p.py_predicate for p in predicates]
        py_predicate = PyNativePredicate(
            method='or',
            index=None,
            field=None,
            literals=py_predicates
        )
        return PredicateImpl(py_predicate) 