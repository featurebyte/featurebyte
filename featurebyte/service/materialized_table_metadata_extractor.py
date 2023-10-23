"""
Materialized table metadata extractor
"""
from typing import Dict, List, Optional, Tuple, cast

from dataclasses import dataclass

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte import SourceTable
from featurebyte.common.utils import dataframe_from_json
from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.models import FeatureStoreModel
from featurebyte.models.materialized_table import ColumnSpecWithEntityId
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.schema.feature_store import FeatureStoreSample
from featurebyte.service.preview import PreviewService
from featurebyte.session.base import BaseSession


@dataclass
class PointInTimeStats:
    """
    Point in time stats
    """

    least_recent: str
    most_recent: str


def _convert_ts_to_str(timestamp_str: str) -> str:
    timestamp_obj = pd.Timestamp(timestamp_str)
    if timestamp_obj.tzinfo is not None:
        timestamp_obj = timestamp_obj.tz_convert("UTC").tz_localize(None)
    return cast(str, timestamp_obj.isoformat())


def get_minimum_iet_sql_expr(
    entity_column_names: List[str], table_details: TableDetails, source_type: SourceType
) -> Expression:
    """
    Get the SQL expression to compute the minimum interval in seconds for each entity.

    Parameters
    ----------
    entity_column_names: List[str]
        List of entity column names
    table_details: TableDetails
        Table details of the materialized table
    source_type: SourceType
        Source type

    Returns
    -------
    str
    """
    adapter = get_sql_adapter(source_type)
    point_in_time_quoted = quoted_identifier(SpecialColumnName.POINT_IN_TIME)
    previous_point_in_time_quoted = quoted_identifier("PREVIOUS_POINT_IN_TIME")

    window_expression = expressions.Window(
        this=expressions.Anonymous(this="LAG", expressions=[point_in_time_quoted]),
        partition_by=[quoted_identifier(col_name) for col_name in entity_column_names],
        order=expressions.Order(expressions=[expressions.Ordered(this=point_in_time_quoted)]),
    )
    aliased_window = expressions.Alias(
        this=window_expression,
        alias=previous_point_in_time_quoted,
    )
    inner_query = expressions.select(aliased_window, point_in_time_quoted).from_(
        get_fully_qualified_table_name(table_details.dict())
    )

    datediff_expr = adapter.datediff_microsecond(
        previous_point_in_time_quoted, point_in_time_quoted
    )
    # Convert microseconds to seconds
    quoted_interval_identifier = quoted_identifier("INTERVAL")
    interval_secs_expr = expressions.Div(this=datediff_expr, expression=make_literal_value(1000000))
    difference_expr = expressions.Alias(this=interval_secs_expr, alias=quoted_interval_identifier)
    iet_expr = expressions.select(difference_expr).from_(inner_query.subquery())
    aliased_min = expressions.Alias(
        this=expressions.Min(this=quoted_interval_identifier),
        alias=quoted_identifier("MIN_INTERVAL"),
    )
    return (
        expressions.select(aliased_min)
        .from_(iet_expr.subquery())
        .where(
            expressions.Not(
                this=expressions.Is(this=quoted_interval_identifier, expression=expressions.Null())
            )
        )
    )


class MaterializedTableMetadataExtractor:
    """
    Helper class to extract metadata from materaliized tables
    """

    def __init__(self, preview_service: PreviewService):
        self.preview_service = preview_service

    async def get_min_interval_secs_between_entities(
        self,
        db_session: BaseSession,
        columns_info: List[ColumnSpecWithEntityId],
        table_details: TableDetails,
    ) -> Optional[float]:
        """
        Get the entity column name to minimum interval mapping.

        Parameters
        ----------
        db_session: BaseSession
            Database session
        columns_info: List[ColumnSpecWithEntityId]
            List of column specs with entity id
        table_details: TableDetails
            Table details of the materialized table

        Returns
        -------
        Optional[float]
            minimum interval in seconds, None if there's only one row
        """
        entity_col_names = [col.name for col in columns_info if col.entity_id is not None]
        # Construct SQL
        sql_expr = get_minimum_iet_sql_expr(entity_col_names, table_details, db_session.source_type)

        # Execute SQL
        sql_string = sql_to_string(sql_expr, db_session.source_type)
        min_interval_df = await db_session.execute_query(sql_string)
        assert min_interval_df is not None
        value = min_interval_df.iloc[0]["MIN_INTERVAL"]
        if value is None or pd.isna(value):
            return None
        return float(value)

    async def get_column_name_to_entity_count(
        self,
        feature_store: FeatureStoreModel,
        table_details: TableDetails,
        columns_info: List[ColumnSpecWithEntityId],
    ) -> Tuple[Dict[str, int], PointInTimeStats]:
        """
        Get the entity column name to unique entity count mapping.

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store document
        table_details: TableDetails
            Table details of the materialized table
        columns_info: List[ColumnSpecWithEntityId]
            List of column specs with entity id

        Returns
        -------
        Tuple[Dict[str, int], PointInTimeStats]
        """
        # Get describe statistics
        source_table = SourceTable(
            feature_store=feature_store,
            tabular_source=TabularSource(
                feature_store_id=feature_store.id,
                table_details=TableDetails(
                    database_name=table_details.database_name,
                    schema_name=table_details.schema_name,
                    table_name=table_details.table_name,
                ),
            ),
            columns_info=columns_info,
        )
        graph, node = source_table.frame.extract_pruned_graph_and_node()
        sample = FeatureStoreSample(
            feature_store_name=feature_store.name,
            graph=graph,
            node_name=node.name,
            stats_names=["unique", "max", "min"],
        )
        describe_stats_json = await self.preview_service.describe(sample, 0, 1234)
        describe_stats_dataframe = dataframe_from_json(describe_stats_json)
        entity_cols = [col for col in columns_info if col.entity_id is not None]
        column_name_to_count = {}
        for col in entity_cols:
            col_name = col.name
            column_name_to_count[
                col_name
            ] = describe_stats_dataframe.loc[  # pylint: disable=no-member
                "unique", col_name
            ]
        least_recent_time_str = describe_stats_dataframe.loc[  # pylint: disable=no-member
            "min", SpecialColumnName.POINT_IN_TIME
        ]
        least_recent_time_str = _convert_ts_to_str(least_recent_time_str)
        most_recent_time_str = describe_stats_dataframe.loc[  # pylint: disable=no-member
            "max", SpecialColumnName.POINT_IN_TIME
        ]
        most_recent_time_str = _convert_ts_to_str(most_recent_time_str)
        return column_name_to_count, PointInTimeStats(
            least_recent=least_recent_time_str, most_recent=most_recent_time_str
        )
