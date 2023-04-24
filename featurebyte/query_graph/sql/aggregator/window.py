"""
SQL generation for aggregation with time windows
"""
from __future__ import annotations

from typing import Any, Iterable, Optional, Tuple, cast

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.enum import InternalName, SourceType, SpecialColumnName
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    LeftJoinableSubquery,
    TileBasedAggregator,
)
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    CteStatements,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from featurebyte.query_graph.sql.tile_util import calculate_first_and_last_tile_indices

Window = Optional[int]
Frequency = int
BlindSpot = int
TimeModuloFreq = int
AggSpecEntityIDs = Tuple[str, ...]
TileIndicesIdType = Tuple[Window, Frequency, BlindSpot, TimeModuloFreq, AggSpecEntityIDs]
TileIdType = str
IsOrderDependent = bool
AggregationSpecIdType = Tuple[TileIdType, Window, AggSpecEntityIDs, IsOrderDependent]

ROW_NUMBER = "__FB_ROW_NUMBER"


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

    def add_aggregation_spec(self, agg_spec: TileBasedAggregationSpec) -> None:
        """
        Process a new AggregationSpec

        Depending on the feature job setting of the provided aggregation, a new expanded request
        table may or may not be required.

        Parameters
        ----------
        agg_spec : TileBasedAggregationSpec
            Aggregation specification
        """
        assert agg_spec.window is not None
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

    def get_expanded_request_table_name(self, agg_spec: TileBasedAggregationSpec) -> str:
        """
        Get the name of the expanded request table given and AggregationSpec

        Parameters
        ----------
        agg_spec : TileBasedAggregationSpec
            Aggregation specification

        Returns
        -------
        str
            Expanded request table name
        """
        key = self.get_unique_tile_indices_id(agg_spec)
        return self.expanded_request_table_names[key]

    @staticmethod
    def get_unique_tile_indices_id(agg_spec: TileBasedAggregationSpec) -> TileIndicesIdType:
        """
        Get a key for an AggregationSpec that controls reuse of expanded request table

        Parameters
        ----------
        agg_spec : TileBasedAggregationSpec
            Aggregation specification

        Returns
        -------
        tuple
        """
        assert agg_spec.window is not None
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
    ) -> CteStatements:
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
            assert window_size is not None
            expanded_table_sql = self.construct_expanded_request_table_sql(
                window_size=window_size,
                frequency=frequency,
                time_modulo_frequency=time_modulo_frequency,
                serving_names=list(serving_names),
                request_table_name=request_table_name,
            )
            expanded_request_ctes.append((quoted_identifier(table_name), expanded_table_sql))
        return cast(CteStatements, expanded_request_ctes)

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


class TileBasedAggregationSpecSet:
    """
    Responsible for keeping track of TileBasedAggregationSpec that arises from query graph nodes
    """

    def __init__(self) -> None:
        self.aggregation_specs: dict[AggregationSpecIdType, list[TileBasedAggregationSpec]] = {}
        self.processed_agg_specs: dict[AggregationSpecIdType, set[str]] = {}

    def add_aggregation_spec(self, aggregation_spec: TileBasedAggregationSpec) -> None:
        """
        Update state given a TileBasedAggregationSpec

        Some aggregations can be shared by different features, e.g. "transaction_type (7 day
        entropy)" and "transaction_type (7 day most frequent)" can both reuse the aggregated result
        of "transaction (7 day category count by transaction_type)". This information is tracked
        using the aggregation_id attribute of WindowAggregationSpec - the WindowAggregationSpec for
        all of these three features will have the same aggregation_id.

        Parameters
        ----------
        aggregation_spec : TileBasedAggregationSpec
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
            aggregation_spec.is_order_dependent,
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

    def get_grouped_aggregation_specs(self) -> Iterable[list[TileBasedAggregationSpec]]:
        """
        Yields groups of TileBasedAggregationSpec

        Each group of TileBasedAggregationSpec has the same tile_table_id. Their tile values can be
        aggregated in a single GROUP BY clause.

        Yields
        ------
        list[AggregationSpec]
            Group of AggregationSpec
        """
        for agg_specs in self.aggregation_specs.values():
            yield agg_specs


class WindowAggregator(TileBasedAggregator):
    """
    WindowAggregator is responsible for SQL generation for aggregation with time windows

    Parameters
    ----------
    source_type: SourceType
        Source type information
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.window_aggregation_spec_set = TileBasedAggregationSpecSet()
        self.request_table_plan: TileBasedRequestTablePlan = TileBasedRequestTablePlan(
            source_type=self.source_type
        )

    def additional_update(self, aggregation_spec: TileBasedAggregationSpec) -> None:
        """
        Update internal state to account for a WindowAggregationSpec

        Parameters
        ----------
        aggregation_spec: TileBasedAggregationSpec
            Aggregation specification
        """
        if self.is_online_serving:
            return
        assert aggregation_spec.window is not None
        self.window_aggregation_spec_set.add_aggregation_spec(aggregation_spec)
        self.request_table_plan.add_aggregation_spec(aggregation_spec)

    @staticmethod
    def _range_join_request_table_and_tile_table(
        expanded_request_table_name: str,
        tile_table_id: str,
        point_in_time_column: str,
        keys: list[str],
        serving_names: list[str],
        value_by: str | None,
        num_tiles: int,
        tile_value_columns: list[str],
    ) -> Select:
        # Join two tables with range join: REQ (processed request table) and TILE (tile table). For
        # each row in the REQ table, we want to join with rows in the TILE table with tile index
        # between REQ.FIRST_TILE_INDEX and REQ.LAST_TILE_INDEX.
        range_join_where_conditions = [
            f"TILE.INDEX >= REQ.{InternalName.FIRST_TILE_INDEX}",
            f"TILE.INDEX < REQ.{InternalName.LAST_TILE_INDEX}",
        ]

        # Required columns from the request table
        selected_from_request_table = [get_qualified_column_identifier(point_in_time_column, "REQ")]
        for serving_name in serving_names:
            selected_from_request_table.append(get_qualified_column_identifier(serving_name, "REQ"))

        # Required columns from the tile table
        selected_from_tile_table = [
            get_qualified_column_identifier("INDEX", "TILE", quote_column=False)
        ] + [
            get_qualified_column_identifier(tile_value_column, "TILE", quote_column=False)
            for tile_value_column in tile_value_columns
        ]
        if value_by is not None:
            selected_from_tile_table.append(get_qualified_column_identifier(value_by, "TILE"))

        # Narrow down matches using these conditions before filtering by range_join_where_conditions
        range_join_conditions = [
            f"FLOOR(REQ.{InternalName.LAST_TILE_INDEX} / {num_tiles}) = FLOOR(TILE.INDEX / {num_tiles})",
            f"FLOOR(REQ.{InternalName.LAST_TILE_INDEX} / {num_tiles}) - 1 = FLOOR(TILE.INDEX / {num_tiles})",
        ]
        req_joined_with_tiles = None
        for range_join_condition in range_join_conditions:
            join_conditions_lst: Any = [range_join_condition]
            for serving_name, key in zip(serving_names, keys):
                join_conditions_lst.append(
                    f"REQ.{quoted_identifier(serving_name).sql()} = TILE.{quoted_identifier(key).sql()}"
                )
            joined_expr = (
                select(
                    *selected_from_request_table,
                    *selected_from_tile_table,
                )
                .from_(f"{quoted_identifier(expanded_request_table_name).sql()} AS REQ")
                .join(
                    tile_table_id,
                    join_alias="TILE",
                    join_type="inner",
                    on=expressions.and_(*join_conditions_lst),
                )
                .where(*range_join_where_conditions)
            )
            # Use UNION ALL with two separate joins to avoid non-exact join condition with OR which
            # has significant performance impact.
            if req_joined_with_tiles is None:
                req_joined_with_tiles = joined_expr
            else:
                req_joined_with_tiles = expressions.Union(  # type: ignore[unreachable]
                    this=req_joined_with_tiles,
                    distinct=False,
                    expression=joined_expr,
                )
        assert req_joined_with_tiles is not None
        return select().from_(req_joined_with_tiles.subquery())

    def construct_aggregation_sql(  # pylint: disable=too-many-arguments
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
        is_order_dependent: bool,
        tile_value_columns: list[str],
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
        is_order_dependent : bool
            Whether the aggregation depends on the ordering of data
        tile_value_columns : list[str]
            List of column names referenced in the tile table

        Returns
        -------
        expressions.Select
        """
        # Join expanded request table with tile table using range join
        req_joined_with_tiles = self._range_join_request_table_and_tile_table(
            expanded_request_table_name=expanded_request_table_name,
            tile_table_id=tile_table_id,
            point_in_time_column=point_in_time_column,
            keys=keys,
            serving_names=serving_names,
            value_by=value_by,
            num_tiles=num_tiles,
            tile_value_columns=tile_value_columns,
        )

        group_by_keys = [quoted_identifier(point_in_time_column)]
        for serving_name in serving_names:
            group_by_keys.append(quoted_identifier(serving_name))

        if value_by is None:
            inner_agg_result_names = agg_result_names
            inner_group_by_keys = group_by_keys
        else:
            inner_agg_result_names = [
                f"inner_{agg_result_name}" for agg_result_name in agg_result_names
            ]
            inner_group_by_keys = group_by_keys + [quoted_identifier(value_by)]

        if is_order_dependent:
            inner_agg_expr = self.merge_tiles_order_dependent(
                req_joined_with_tiles=req_joined_with_tiles,
                inner_group_by_keys=inner_group_by_keys,
                merge_exprs=merge_exprs,
                inner_agg_result_names=inner_agg_result_names,
            )
        else:
            inner_agg_expr = self.merge_tiles_order_independent(
                req_joined_with_tiles=req_joined_with_tiles,
                inner_group_by_keys=inner_group_by_keys,
                merge_exprs=merge_exprs,
                inner_agg_result_names=inner_agg_result_names,
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

    @staticmethod
    def merge_tiles_order_independent(
        req_joined_with_tiles: Select,
        inner_group_by_keys: list[Expression],
        merge_exprs: list[str],
        inner_agg_result_names: list[str],
    ) -> Select:
        """
        Merge tile values to produce feature values for order independent aggregation methods

        The aggregation is done using GROUP BY.

        Parameters
        ----------
        req_joined_with_tiles: Select
            Result of joining expanded request table with tile table
        inner_group_by_keys: list[Expression]
            Keys that the aggregation should use
        merge_exprs: list[str]
            Expressions that merge tile values to produce feature values
        inner_agg_result_names: list[str]
            Names of the aggregation results, should have the same length as merge_exprs

        Returns
        -------
        Select
        """
        inner_agg_expr = req_joined_with_tiles.select(
            *inner_group_by_keys,
            *[
                alias_(merge_expr, inner_agg_result_name, quoted=True)
                for merge_expr, inner_agg_result_name in zip(merge_exprs, inner_agg_result_names)
            ],
        ).group_by(*inner_group_by_keys)
        return inner_agg_expr

    @staticmethod
    def merge_tiles_order_dependent(
        req_joined_with_tiles: Select,
        inner_group_by_keys: list[Expression],
        merge_exprs: list[str],
        inner_agg_result_names: list[str],
    ) -> Select:
        """
        Merge tile values to produce feature values for order dependent aggregation methods

        The aggregation is done using a nested query involving window function to preserve row
        ordering based on event timestamp. This is to support aggregation methods such as latest
        value, sequence, etc.

        Parameters
        ----------
        req_joined_with_tiles: Select
            Result of joining expanded request table with tile table
        inner_group_by_keys: list[Expression]
            Keys that the aggregation should use
        merge_exprs: list[str]
            Expressions that merge tile values to produce feature values
        inner_agg_result_names: list[str]
            Names of the aggregation results, should have the same length as merge_exprs

        Returns
        -------
        Select
        """

        def _make_window_expr(expr: str | Expression) -> Expression:
            order = expressions.Order(
                expressions=[
                    expressions.Ordered(this="INDEX", desc=True),
                ]
            )
            window_expr = expressions.Window(
                this=expr, partition_by=inner_group_by_keys, order=order
            )
            return window_expr

        window_exprs = [
            alias_(
                _make_window_expr(expressions.Anonymous(this="ROW_NUMBER")),
                alias=ROW_NUMBER,
                quoted=True,
            )
        ] + [
            alias_(_make_window_expr(merge_expr), alias=inner_agg_result_name, quoted=True)
            for (merge_expr, inner_agg_result_name) in zip(merge_exprs, inner_agg_result_names)
        ]
        window_based_expr = req_joined_with_tiles.select(
            *inner_group_by_keys,
            *window_exprs,
        )
        filter_condition = expressions.EQ(
            this=quoted_identifier(ROW_NUMBER), expression=make_literal_value(1)
        )
        inner_agg_expr = select("*").from_(window_based_expr.subquery()).where(filter_condition)
        return inner_agg_expr

    def get_window_aggregations(self, point_in_time_column: str) -> list[LeftJoinableSubquery]:
        """
        Get window aggregation queries

        Parameters
        ----------
        point_in_time_column: str
            Point in time column name

        Returns
        -------
        list[LeftJoinableSubquery]
        """
        results = []

        for agg_specs in self.window_aggregation_spec_set.get_grouped_aggregation_specs():
            # All TileBasedAggregationSpec in agg_specs share common attributes such as
            # tile_table_id, keys, etc. Get the first one to access them.
            agg_spec = agg_specs[0]
            is_order_dependent = agg_specs[0].is_order_dependent
            expanded_request_table_name = self.request_table_plan.get_expanded_request_table_name(
                agg_spec
            )
            merge_exprs = [agg_spec.merge_expr for agg_spec in agg_specs]
            agg_result_names = [agg_spec.agg_result_name for agg_spec in agg_specs]
            tile_value_columns_set = set()
            for agg_spec in agg_specs:
                tile_value_columns_set.update(agg_spec.tile_value_columns)

            assert agg_spec.window is not None
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
                is_order_dependent=is_order_dependent,
                tile_value_columns=sorted(tile_value_columns_set),
            )
            agg_result = LeftJoinableSubquery(
                expr=agg_expr,
                column_names=agg_result_names,
                join_keys=[point_in_time_column] + agg_spec.serving_names,
            )
            results.append(agg_result)

        return results

    def update_aggregation_table_expr_offline(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        queries = self.get_window_aggregations(point_in_time_column)

        return self._update_with_left_joins(
            table_expr=table_expr, current_query_index=current_query_index, queries=queries
        )

    def get_common_table_expressions(self, request_table_name: str) -> CteStatements:
        return self.request_table_plan.construct_request_tile_indices_ctes(request_table_name)
