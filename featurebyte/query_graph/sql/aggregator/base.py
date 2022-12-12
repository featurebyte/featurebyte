"""
Base class for aggregation SQL generators
"""
from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod

from sqlglot import expressions


@dataclasses.dataclass
class AggregationResult:
    """
    Representation of aggregation result from an instance of Aggregator
    """

    expr: expressions.Select
    column_names: list[str]
    join_keys: list[str]


class Aggregator(ABC):
    """
    Base class of all aggregators
    """

    @abstractmethod
    def get_required_serving_names(self) -> set[str]:
        """
        Get the set of required serving names

        Returns
        -------
        set[str]
        """

    @abstractmethod
    def get_aggregation_results(self, point_in_time_column: str) -> list[AggregationResult]:
        """
        Construct a query of aggregated results ready to be left joined with request table

        Parameters
        ----------
        point_in_time_column: str
            Name of the point in time column

        Returns
        -------
        list[AggregationResult]
            List of aggregation results
        """

    @abstractmethod
    def get_common_table_expressions(
        self, request_table_name: str
    ) -> list[tuple[str, expressions.Select]]:
        """
        Construct any common table expressions (CTE) required to support the aggregation, typically
        the original request table processed in some ways. This will be used to form the WITH
        section of the feature compute sql.

        Parameters
        ----------
        request_table_name: str
            Name of the request table

        Returns
        -------
        list[tuple[str, expressions.Select]]
            List of common table expressions as tuples. The first element of the tuple is the name
            of the CTE and the second element is the corresponding sql expression.
        """
