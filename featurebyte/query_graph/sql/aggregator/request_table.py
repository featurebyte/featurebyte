"""
Request table processing used by aggregators
"""

from __future__ import annotations

from typing import Tuple

from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.sql.aggregator.base import CommonTable
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.specs import AggregationSpec

AggSpecEntityIDs = Tuple[str, ...]


class RequestTablePlan:
    """
    Responsible for generating a processed request table with distinct serving names
    """

    def __init__(self, is_time_aware: bool):
        self.is_time_aware = is_time_aware
        self.request_table_names: dict[AggSpecEntityIDs, str] = {}
        self.agg_specs: dict[AggSpecEntityIDs, AggregationSpec] = {}

    def add_aggregation_spec(self, agg_spec: AggregationSpec) -> None:
        """
        Update state given an AggregationSpec

        Parameters
        ----------
        agg_spec: AggregationSpec
            Aggregation specification
        """
        key = self.get_key(agg_spec)

        suffix = "_".join(agg_spec.serving_names)
        if self.is_time_aware:
            request_table_name = f"REQUEST_TABLE_POINT_IN_TIME_{suffix}"
        else:
            request_table_name = f"REQUEST_TABLE_{suffix}"

        self.request_table_names[key] = request_table_name
        self.agg_specs[key] = agg_spec

    def get_request_table_name(self, agg_spec: AggregationSpec) -> str:
        """
        Get the processed request table name corresponding to an aggregation spec

        Parameters
        ----------
        agg_spec: AggregationSpec
            Aggregation specification

        Returns
        -------
        str
        """
        key = self.get_key(agg_spec)
        return self.request_table_names[key]

    @staticmethod
    def get_key(agg_spec: AggregationSpec) -> AggSpecEntityIDs:
        """
        Get an internal key used to determine request table sharing

        Parameters
        ----------
        agg_spec: AggregationSpec
            Aggregation specification

        Returns
        -------
        AggSpecEntityIDs
        """
        return tuple(agg_spec.serving_names)

    def construct_request_table_ctes(
        self,
        request_table_name: str,
    ) -> list[CommonTable]:
        """
        Construct SQL statements that build the processed request tables

        Parameters
        ----------
        request_table_name : str
            Name of request table to use

        Returns
        -------
        list[tuple[str, expressions.Select]]
        """
        ctes = []
        for key, table_name in self.request_table_names.items():
            agg_spec = self.agg_specs[key]
            request_table_expr = self.construct_request_table_expr(agg_spec, request_table_name)
            ctes.append(CommonTable(name=table_name, expr=request_table_expr))
        return ctes

    def construct_request_table_expr(
        self,
        agg_spec: AggregationSpec,
        request_table_name: str,
    ) -> expressions.Select:
        """
        Construct a Select statement that forms the processed request table

        Parameters
        ----------
        agg_spec: AggregationSpec
            Aggregation specification
        request_table_name: str
            Name of the original request table that is determined at runtime

        Returns
        -------
        expressions.Select
        """
        selected_columns = []

        if self.is_time_aware:
            selected_columns.append(quoted_identifier(SpecialColumnName.POINT_IN_TIME))

        selected_columns.extend([quoted_identifier(x) for x in agg_spec.serving_names])

        select_distinct_expr = select(*selected_columns).distinct().from_(request_table_name)
        return select_distinct_expr
