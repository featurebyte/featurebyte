"""
Common helpers and data structures for feature SQL generation
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import pandas as pd

from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupbyNode
from featurebyte.query_graph.sql.tiling import get_aggregator

REQUEST_TABLE_NAME = "REQUEST_TABLE"


def construct_cte_sql(cte_statements: list[tuple[str, str]]) -> str:
    """Construct CTEs section of a SQL code

    Parameters
    ----------
    cte_statements : list[tuple[str, str]]
        List of CTE statements

    Returns
    -------
    str
    """
    cte_definitions = []
    for table_name, table_statement in cte_statements:
        cte_definitions.append(f"{table_name} AS ({table_statement})")
    cte_sql = ",\n".join(cte_definitions)
    cte_sql = f"WITH {cte_sql}"
    return cte_sql


def escape_column_name(column_name: str) -> str:
    """Enclose provided column name with quotes

    Parameters
    ----------
    column_name : str
        Column name

    Returns
    -------
    str
    """
    if column_name.startswith('"') and column_name.endswith('"'):
        return column_name
    return f'"{column_name}"'


def escape_column_names(column_names: list[str]) -> list[str]:
    """Enclose provided column names with quotes

    Parameters
    ----------
    column_names : list[str]
        Column names

    Returns
    -------
    list[str]
    """
    return [escape_column_name(x) for x in column_names]


def apply_serving_names_mapping(serving_names: list[str], mapping: dict[str, str]) -> list[str]:
    """Apply user provided mapping to transform the default serving names

    Applicable to the serving_names attribute in TileGenSql and AggregationSpec

    Parameters
    ----------
    serving_names : list[str]
        List of original serving names
    mapping : dict[str, str]
        Mapping from original serving name to new serving name

    Returns
    -------
    list[str]
        Mapped serving names
    """
    updated_serving_names = []
    for serving_name in serving_names:
        updated_serving_names.append(mapping.get(serving_name, serving_name))
    return updated_serving_names


@dataclass
class AggregationSpec:
    """Aggregation specification"""

    # pylint: disable=too-many-instance-attributes

    window: int
    frequency: int
    blind_spot: int
    time_modulo_frequency: int
    tile_table_id: str
    aggregation_id: str
    keys: list[str]
    serving_names: list[str]
    value_by: str | None
    merge_expr: str
    feature_name: str

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column name of the aggregated result
        """
        return f"agg_w{self.window}_{self.aggregation_id}"

    @classmethod
    def from_groupby_query_node(
        cls,
        groupby_node: Node,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> list[AggregationSpec]:
        """Construct an AggregationSpec from a query graph and groupby node

        Parameters
        ----------
        groupby_node : Node
            Query graph node with groupby type
        serving_names_mapping : dict[str, str]
            Mapping from original serving name to new serving name

        Returns
        -------
        list[AggregationSpec]
            List of AggregationSpec
        """
        assert isinstance(groupby_node, GroupbyNode)
        tile_table_id = groupby_node.parameters.tile_id
        aggregation_id = groupby_node.parameters.aggregation_id
        params = groupby_node.parameters.dict()
        assert tile_table_id is not None
        assert aggregation_id is not None

        serving_names = params["serving_names"]
        if serving_names_mapping is not None:
            serving_names = apply_serving_names_mapping(serving_names, serving_names_mapping)

        aggregation_specs = []
        for window, feature_name in zip(params["windows"], params["names"]):
            params = groupby_node.parameters.dict()
            window = int(pd.Timedelta(window).total_seconds())
            agg_spec = cls(
                window=window,
                frequency=params["frequency"],
                time_modulo_frequency=params["time_modulo_frequency"],
                blind_spot=params["blind_spot"],
                tile_table_id=tile_table_id,
                aggregation_id=aggregation_id,
                keys=params["keys"],
                serving_names=serving_names,
                value_by=params["value_by"],
                merge_expr=get_aggregator(params["agg_func"]).merge(aggregation_id),
                feature_name=feature_name,
            )
            aggregation_specs.append(agg_spec)

        return aggregation_specs


@dataclass
class FeatureSpec:
    """Feature specification"""

    feature_name: str
    feature_expr: str


class SQLType(Enum):
    """Type of SQL code corresponding to different operations"""

    BUILD_TILE = "build_tile"
    BUILD_TILE_ON_DEMAND = "build_tile_on_demand"
    EVENT_VIEW_PREVIEW = "event_view_preview"
    GENERATE_FEATURE = "generate_feature"
