"""
SQL generation for aggregation with time windows
"""
from __future__ import annotations

from typing import Any, Iterable, Tuple

from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte import SourceType
from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.aggregator.base import AggregationResult, Aggregator
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.specs import WindowAggregationSpec
from featurebyte.query_graph.sql.tile_util import calculate_first_and_last_tile_indices

Window = int
Frequency = int
BlindSpot = int
TimeModuloFreq = int
AggSpecEntityIDs = Tuple[str, ...]
TileIndicesIdType = Tuple[Window, Frequency, BlindSpot, TimeModuloFreq, AggSpecEntityIDs]
TileIdType = str
AggregationSpecIdType = Tuple[TileIdType, Window, AggSpecEntityIDs]


class TileBasedRequestTablePlan:
    """
    SQL generation for expanded request tables

    An expanded request table has the same number of rows as the original request table but with new
    columns added: __FB_LAST_TILE_INDEX and __FB_FIRST_TILE_INDEX. This corresponds to the first and
    last (exclusive) tile index when joining with the tile table.

    Since the required tile indices depend on feature job setting and not the specific
    aggregation method or input, an expanded table can be pre-computed (in the SQL as a common
    table) and shared with different features with the same feature job setting.

    Example:

    If the request data is as follows:
    ----------------------
    POINT_IN_TIME  CUST_ID
    ----------------------
    2022-04-01     C1
    2022-04-10     C2
    ----------------------

    Then an expanded request table would be similar to:
    -------------------------------------------------------------------
    POINT_IN_TIME  CUST_ID  __FB_FIRST_TILE_INDEX  __FB_LAST_TILE_INDEX
    -------------------------------------------------------------------
    2022-04-01     C1       1000                   1010
    2022-04-10     C2       1105                   1115
    -------------------------------------------------------------------
    """

    def __init__(self, source_type: SourceType) -> None:
        self.expanded_request_table_names: dict[TileIndicesIdType, str] = {}
        self.adapter = get_sql_adapter(source_type)

    def add_aggregation_spec(self, agg_spec: WindowAggregationSpec) -> None:
        """
        Process a new AggregationSpec

        Depending on the feature job setting of the provided aggregation, a new expanded request
        table may or may not be required.

        Parameters
        ----------
        agg_spec : WindowAggregationSpec
            Aggregation specification
        """
        unique_tile_indices_id = self.get_unique_tile_indices_id(agg_spec)
        if unique_tile_indices_id not in self.expanded_request_table_names:
            output_table_name = (
                f"REQUEST_TABLE"
                f"_W{agg_spec.window}"
                f"_F{agg_spec.frequency}"
                f"_BS{agg_spec.blind_spot}"
                f"_M{agg_spec.time_modulo_frequency}"
                f"_{'_'.join(agg_spec.serving_names)}"
            )
            self.expanded_request_table_names[unique_tile_indices_id] = output_table_name

    def get_expanded_request_table_name(self, agg_spec: WindowAggregationSpec) -> str:
        """
        Get the name of the expanded request table given and AggregationSpec

        Parameters
        ----------
        agg_spec : WindowAggregationSpec
            Aggregation specification

        Returns
        -------
        str
            Expanded request table name
        """
        key = self.get_unique_tile_indices_id(agg_spec)
        return self.expanded_request_table_names[key]

    @staticmethod
    def get_unique_tile_indices_id(agg_spec: WindowAggregationSpec) -> TileIndicesIdType:
        """
        Get a key for an AggregationSpec that controls reuse of expanded request table

        Parameters
        ----------
        agg_spec : WindowAggregationSpec
            Aggregation specification

        Returns
        -------
        tuple
        """
        unique_tile_indices_id = (
            agg_spec.window,
            agg_spec.frequency,
            agg_spec.blind_spot,
            agg_spec.time_modulo_frequency,
            tuple(agg_spec.serving_names),
        )
        return unique_tile_indices_id

    def construct_request_tile_indices_ctes(
        self,
        request_table_name: str,
    ) -> list[tuple[str, expressions.Select]]:
        """
        Construct SQL statements that build the expanded request tables

        Parameters
        ----------
        request_table_name : str
            Name of request table to use

        Returns
        -------
        list[tuple[str, expressions.Select]]
        """
        expanded_request_ctes = []
        for unique_tile_indices_id, table_name in self.expanded_request_table_names.items():
            (
                window_size,
                frequency,
                _,
                time_modulo_frequency,
                serving_names,
            ) = unique_tile_indices_id
            expanded_table_sql = self.construct_expanded_request_table_sql(
                window_size=window_size,
                frequency=frequency,
                time_modulo_frequency=time_modulo_frequency,
                serving_names=list(serving_names),
                request_table_name=request_table_name,
            )
            expanded_request_ctes.append((quoted_identifier(table_name).sql(), expanded_table_sql))
        return expanded_request_ctes

    def construct_expanded_request_table_sql(
        self,
        window_size: int,
        frequency: int,
        time_modulo_frequency: int,
        serving_names: list[str],
        request_table_name: str,
    ) -> expressions.Select:
        """
        Construct SQL for expanded SQLs

        Parameters
        ----------
        window_size : int
            Feature window size
        frequency : int
            Frequency in feature job setting
        time_modulo_frequency : int
            Time modulo frequency in feature job setting
        serving_names : list[str]
            List of serving names corresponding to entities
        request_table_name: str
            Name of request table to use

        Returns
        -------
        str
            SQL code for expanding request table
        """
        # Input request table can have duplicated time points but aggregation should be done only on
        # distinct time points
        quoted_serving_names = [quoted_identifier(x) for x in serving_names]
        select_distinct_expr = (
            select(quoted_identifier(SpecialColumnName.POINT_IN_TIME), *quoted_serving_names)
            .distinct()
            .from_(request_table_name)
        )
        first_tile_index_expr, last_tile_index_expr = calculate_first_and_last_tile_indices(
            adapter=self.adapter,
            point_in_time_expr=quoted_identifier(SpecialColumnName.POINT_IN_TIME),
            window_size=window_size,
            frequency=frequency,
            time_modulo_frequency=time_modulo_frequency,
        )
        expr = select(
            quoted_identifier(SpecialColumnName.POINT_IN_TIME),
            *quoted_serving_names,
            expressions.alias_(last_tile_index_expr, InternalName.LAST_TILE_INDEX),
            expressions.alias_(first_tile_index_expr, InternalName.FIRST_TILE_INDEX),
        ).from_(select_distinct_expr.subquery())
        return expr


class WindowAggregationSpecSet:
    """
    Responsible for keeping track of WindowAggregationSpec that arises from query graph nodes
    """

    def __init__(self) -> None:
        self.aggregation_specs: dict[AggregationSpecIdType, list[WindowAggregationSpec]] = {}
        self.processed_agg_specs: dict[AggregationSpecIdType, set[str]] = {}

    def add_aggregation_spec(self, aggregation_spec: WindowAggregationSpec) -> None:
        """
        Update state given a WindowAggregationSpec

        Some aggregations can be shared by different features, e.g. "transaction_type (7 day
        entropy)" and "transaction_type (7 day most frequent)" can both reuse the aggregated result
        of "transaction (7 day category count by transaction_type)". This information is tracked
        using the aggregation_id attribute of WindowAggregationSpec - the WindowAggregationSpec for
        all of these three features will have the same aggregation_id.

        Parameters
        ----------
        aggregation_spec : WindowAggregationSpec
            Aggregation_specification
        """
        # AggregationSpec is window size specific. Two AggregationSpec with different window sizes
        # require two different groupbys and left joins in the resulting SQL. Include serving_names
        # here because each group of AggregationSpecs will be joined with exactly one expanded
        # request table, and an expanded request table is specific to serving names
        key = (
            aggregation_spec.tile_table_id,
            aggregation_spec.window,
            tuple(aggregation_spec.serving_names),
        )

        # Initialize containers for new tile_table_id and window combination
        if key not in self.aggregation_specs:
            self.aggregation_specs[key] = []
            self.processed_agg_specs[key] = set()

        agg_id = aggregation_spec.aggregation_id
        # Skip if the same AggregationSpec is already seen
        if agg_id in self.processed_agg_specs[key]:
            return

        # Update containers
        self.aggregation_specs[key].append(aggregation_spec)
        self.processed_agg_specs[key].add(agg_id)

    def get_grouped_aggregation_specs(self) -> Iterable[list[WindowAggregationSpec]]:
        """
        Yields groups of WindowAggregationSpec

        Each group of WindowAggregationSpec has the same tile_table_id. Their tile values can be
        aggregated in a single GROUP BY clause.

        Yields
        ------
        list[AggregationSpec]
            Group of AggregationSpec
        """
        for agg_specs in self.aggregation_specs.values():
            yield agg_specs


class WindowAggregator(Aggregator):
    """
    WindowAggregator is responsible for SQL generation for aggregation with time windows

    Parameters
    ----------
    source_type: SourceType
        Source type information
    """

    def __init__(self, source_type: SourceType) -> None:
        self.window_aggregation_spec_set = WindowAggregationSpecSet()
        self.request_table_plan: TileBasedRequestTablePlan = TileBasedRequestTablePlan(
            source_type=source_type
        )
        self.adapter = get_sql_adapter(source_type)

    def update(self, aggregation_spec: WindowAggregationSpec) -> None:
        """
        Update internal state to account for a WindowAggregationSpec

        Parameters
        ----------
        aggregation_spec: WindowAggregationSpec
            Aggregation specification
        """
        self.window_aggregation_spec_set.add_aggregation_spec(aggregation_spec)
        self.request_table_plan.add_aggregation_spec(aggregation_spec)

    def get_required_serving_names(self) -> set[str]:
        """
        Get the set of required serving names

        Returns
        -------
        set[str]
        """
        out = set()
        for agg_specs in self.window_aggregation_spec_set.get_grouped_aggregation_specs():
            for agg_spec in agg_specs:
                out.update(agg_spec.serving_names)
        return out

    def construct_aggregation_sql(
        self,
        expanded_request_table_name: str,
        tile_table_id: str,
        point_in_time_column: str,
        keys: list[str],
        serving_names: list[str],
        value_by: str | None,
        merge_exprs: list[str],
        agg_result_names: list[str],
        num_tiles: int,
    ) -> expressions.Select:
        """
        Construct SQL code for one specific aggregation

        The aggregation consists of inner joining with the tile table on entity id and required tile
        indices and applying the merge expression.

        When value_by is set, the aggregation above produces an intermediate result that look
        similar to below since tiles building takes into account the category:

        --------------------------------------
        POINT_IN_TIME  ENTITY  CATEGORY  VALUE
        --------------------------------------
        2022-01-01     C1      K1        1
        2022-01-01     C1      K2        2
        2022-01-01     C2      K3        3
        2022-01-01     C3      K1        4
        ...
        --------------------------------------

        We can aggregate the above into key-value pairs by aggregating over point-in-time and entity
        and applying functions such as OBJECT_AGG:

        -----------------------------------------
        POINT_IN_TIME  ENTITY  VALUE_AGG
        -----------------------------------------
        2022-01-01     C1      {"K1": 1, "K2": 2}
        2022-01-01     C2      {"K2": 3}
        2022-01-01     C3      {"K1": 4}
        ...
        -----------------------------------------

        Parameters
        ----------
        expanded_request_table_name : str
            Expanded request table name
        tile_table_id: str
            Tile table name
        point_in_time_column : str
            Point in time column name
        keys : list[str]
            List of join key columns
        serving_names : list[str]
            List of serving name columns
        value_by : str | None
            Optional category parameter for the groupby operation
        merge_exprs : list[str]
            SQL expressions that aggregates intermediate values stored in tile table
        agg_result_names : list[str]
            Column names of the aggregated results
        num_tiles : int
            Feature window size in terms of number of tiles (function of frequency)

        Returns
        -------
        expressions.Select
        """
        # pylint: disable=too-many-locals
        last_index_name = InternalName.LAST_TILE_INDEX.value
        range_join_condition = expressions.or_(
            f"FLOOR(REQ.{last_index_name} / {num_tiles}) = FLOOR(TILE.INDEX / {num_tiles})",
            f"FLOOR(REQ.{last_index_name} / {num_tiles}) - 1 = FLOOR(TILE.INDEX / {num_tiles})",
        )
        join_conditions_lst: Any = [range_join_condition]
        for serving_name, key in zip(serving_names, keys):
            join_conditions_lst.append(
                f"REQ.{quoted_identifier(serving_name).sql()} = TILE.{quoted_identifier(key).sql()}"
            )
        join_conditions = expressions.and_(*join_conditions_lst)

        first_index_name = InternalName.FIRST_TILE_INDEX.value
        range_join_where_conditions = [
            f"TILE.INDEX >= REQ.{first_index_name}",
            f"TILE.INDEX < REQ.{last_index_name}",
        ]

        group_by_keys = [f"REQ.{quoted_identifier(point_in_time_column).sql()}"]
        for serving_name in serving_names:
            group_by_keys.append(f"REQ.{quoted_identifier(serving_name).sql()}")

        if value_by is None:
            inner_agg_result_names = agg_result_names
            inner_group_by_keys = group_by_keys
        else:
            inner_agg_result_names = [
                f"inner_{agg_result_name}" for agg_result_name in agg_result_names
            ]
            inner_group_by_keys = group_by_keys + [f"TILE.{quoted_identifier(value_by).sql()}"]

        inner_agg_expr = (
            select(
                *inner_group_by_keys,
                *[
                    f'{merge_expr} AS "{inner_agg_result_name}"'
                    for merge_expr, inner_agg_result_name in zip(
                        merge_exprs, inner_agg_result_names
                    )
                ],
            )
            .from_(f"{quoted_identifier(expanded_request_table_name).sql()} AS REQ")
            .join(
                tile_table_id,
                join_alias="TILE",
                join_type="inner",
                on=join_conditions,
            )
            .where(*range_join_where_conditions)
            .group_by(*inner_group_by_keys)
        )

        if value_by is None:
            agg_expr = inner_agg_expr
        else:
            agg_expr = self.adapter.construct_key_value_aggregation_sql(
                point_in_time_column=point_in_time_column,
                serving_names=serving_names,
                value_by=value_by,
                agg_result_names=agg_result_names,
                inner_agg_result_names=inner_agg_result_names,
                inner_agg_expr=inner_agg_expr,
            )

        return agg_expr

    def get_aggregation_results(self, point_in_time_column: str) -> list[AggregationResult]:

        results = []

        for agg_specs in self.window_aggregation_spec_set.get_grouped_aggregation_specs():

            # All WindowAggregationSpec in agg_specs share common attributes such as tile_table_id,
            # keys, etc. Get the first one to access them.
            agg_spec = agg_specs[0]
            expanded_request_table_name = self.request_table_plan.get_expanded_request_table_name(
                agg_spec
            )
            merge_exprs = [agg_spec.merge_expr for agg_spec in agg_specs]
            agg_result_names = [agg_spec.agg_result_name for agg_spec in agg_specs]

            agg_expr = self.construct_aggregation_sql(
                expanded_request_table_name=expanded_request_table_name,
                tile_table_id=agg_spec.tile_table_id,
                point_in_time_column=point_in_time_column,
                keys=agg_spec.keys,
                serving_names=agg_spec.serving_names,
                value_by=agg_spec.value_by,
                merge_exprs=merge_exprs,
                agg_result_names=agg_result_names,
                num_tiles=agg_spec.window // agg_spec.frequency,
            )
            agg_result = AggregationResult(
                expr=agg_expr,
                column_names=agg_result_names,
                join_keys=[point_in_time_column] + agg_spec.serving_names,
            )
            results.append(agg_result)

        return results

    def get_ctes(self, request_table_name: str) -> list[tuple[str, expressions.Select]]:
        return self.request_table_plan.construct_request_tile_indices_ctes(request_table_name)
