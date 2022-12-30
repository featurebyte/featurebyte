"""
Module with logic related to feature SQL generation
"""
from __future__ import annotations

from typing import Iterable, Optional, Union

from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.aggregator.base import Aggregator
from featurebyte.query_graph.sql.aggregator.item import ItemAggregator
from featurebyte.query_graph.sql.aggregator.latest import LatestAggregator
from featurebyte.query_graph.sql.aggregator.lookup import LookupAggregator
from featurebyte.query_graph.sql.aggregator.window import WindowAggregator
from featurebyte.query_graph.sql.ast.base import TableNode
from featurebyte.query_graph.sql.ast.generic import AliasNode, Project
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType, construct_cte_sql, quoted_identifier
from featurebyte.query_graph.sql.specs import (
    FeatureSpec,
    ItemAggregationSpec,
    LookupSpec,
    TileBasedAggregationSpec,
)


class FeatureExecutionPlan:
    """Responsible for constructing the SQL to compute features by aggregating tiles"""

    AGGREGATION_TABLE_NAME = "_FB_AGGREGATED"

    def __init__(self, source_type: SourceType, is_online_serving: bool) -> None:
        aggregator_kwargs = {"source_type": source_type, "is_online_serving": is_online_serving}
        self.aggregators = {
            "latest": LatestAggregator(**aggregator_kwargs),
            "lookup": LookupAggregator(**aggregator_kwargs),
            "window": WindowAggregator(**aggregator_kwargs),
            "item": ItemAggregator(**aggregator_kwargs),
        }
        self.feature_specs: dict[str, FeatureSpec] = {}
        self.adapter = get_sql_adapter(source_type)
        self.source_type = source_type

    @property
    def required_serving_names(self) -> set[str]:
        """Returns the list of required serving names

        Returns
        -------
        set[str]
        """
        out = set()
        for aggregator in self.iter_aggregators():
            out.update(aggregator.get_required_serving_names())
        return out

    def iter_aggregators(self) -> Iterable[Aggregator]:
        """Iterate over all the aggregators

        Yields
        ------
        BaseAggregator
            Instance of an aggregator
        """
        yield from self.aggregators.values()

    def add_aggregation_spec(
        self,
        aggregation_spec: Union[TileBasedAggregationSpec, ItemAggregationSpec, LookupSpec],
    ) -> None:
        """Add AggregationSpec to be incorporated when generating SQL

        Parameters
        ----------
        aggregation_spec : AggregationSpec
            Aggregation specification
        """
        key = None
        if isinstance(aggregation_spec, TileBasedAggregationSpec):
            if aggregation_spec.window is None:
                key = "latest"
            else:
                key = "window"
        elif isinstance(aggregation_spec, ItemAggregationSpec):
            key = "item"
        elif isinstance(aggregation_spec, LookupSpec):
            key = "lookup"
        assert key is not None
        aggregator = self.aggregators[key]
        aggregator.update(aggregation_spec)  # type: ignore

    def add_feature_spec(self, feature_spec: FeatureSpec) -> None:
        """Add FeatureSpec to be incorporated when generating SQL

        Parameters
        ----------
        feature_spec : FeatureSpec
            Feature specification

        Raises
        ------
        ValueError
            If there are duplicated feature names
        """
        key = feature_spec.feature_name
        if key in self.feature_specs:
            raise ValueError(f"Duplicated feature name: {key}")
        self.feature_specs[key] = feature_spec

    def construct_combined_aggregation_cte(
        self,
        request_table_name: str,
        point_in_time_column: str,
        request_table_columns: Optional[list[str]],
    ) -> tuple[str, expressions.Select]:
        """Construct SQL code for all aggregations

        Parameters
        ----------
        request_table_name : str
            Name of request table to use
        point_in_time_column : str
            Point in time column
        request_table_columns : Optional[list[str]]
            Request table columns

        Returns
        -------
        tuple[str, expressions.Select]
            Tuple of table name and SQL expression
        """
        # Select original columns first
        if request_table_columns:
            current_columns = request_table_columns[:]
            formatted_request_table_columns = [
                f"REQ.{quoted_identifier(col).sql()}" for col in request_table_columns
            ]
        else:
            current_columns = []
            formatted_request_table_columns = []
        table_expr = select(*formatted_request_table_columns).from_(f"{request_table_name} AS REQ")

        # Update table_expr using the aggregators
        agg_table_index = 0
        for aggregator in self.iter_aggregators():
            agg_result = aggregator.update_aggregation_table_expr(
                table_expr=table_expr,
                point_in_time_column=point_in_time_column,
                current_columns=current_columns,
                current_query_index=agg_table_index,
            )
            table_expr = agg_result.updated_table_expr
            agg_table_index = agg_result.updated_index
            current_columns += agg_result.column_names

        return self.AGGREGATION_TABLE_NAME, table_expr

    def construct_post_aggregation_sql(
        self, cte_context: expressions.Select, request_table_columns: Optional[list[str]]
    ) -> expressions.Select:
        """Construct SQL code for post-aggregation that transforms aggregated results to features

        Most of the time aggregated results are the features. However, some features require
        additional transforms (e.g. UDF, arithmetic expressions, fillna, etc) after aggregation.

        Columns in the request table is required so that all columns in the request table can be
        passed through.

        Parameters
        ----------
        cte_context : expressions.Select
            A partial Select statement with CTEs defined
        request_table_columns : Optional[list[str]]
            Columns in the input request table

        Returns
        -------
        str
        """
        qualified_feature_names = []
        for feature_spec in self.feature_specs.values():
            feature_alias = f"{feature_spec.feature_expr} AS {quoted_identifier(feature_spec.feature_name).sql()}"
            qualified_feature_names.append(feature_alias)

        if request_table_columns:
            request_table_column_names = [
                f"AGG.{quoted_identifier(col).sql()}" for col in request_table_columns
            ]
        else:
            request_table_column_names = []

        table_expr = cte_context.select(
            *request_table_column_names, *qualified_feature_names
        ).from_(f"{self.AGGREGATION_TABLE_NAME} AS AGG")
        return table_expr

    def construct_combined_sql(
        self,
        request_table_name: str,
        point_in_time_column: str,
        request_table_columns: list[str],
        prior_cte_statements: Optional[list[tuple[str, expressions.Select]]] = None,
    ) -> expressions.Select:
        """Construct combined SQL that will generate the features

        Parameters
        ----------
        request_table_name : str
            Name of request table to use
        point_in_time_column : str
            Point in time column
        request_table_columns : list[str]
            Request table columns
        prior_cte_statements : Optional[list[tuple[str, str]]]
            Other CTE statements to incorporate to the final SQL (namely the request data SQL and
            on-demand tile SQL)

        Returns
        -------
        str
        """
        cte_statements = []
        if prior_cte_statements is not None:
            assert isinstance(prior_cte_statements, list)
            cte_statements.extend(prior_cte_statements)

        for aggregator in self.iter_aggregators():
            cte_statements.extend(aggregator.get_common_table_expressions(request_table_name))

        cte_statements.append(
            self.construct_combined_aggregation_cte(
                request_table_name,
                point_in_time_column,
                request_table_columns,
            )
        )
        cte_context = construct_cte_sql(cte_statements)

        post_aggregation_sql = self.construct_post_aggregation_sql(
            cte_context, request_table_columns
        )
        return post_aggregation_sql


class FeatureExecutionPlanner:
    """Responsible for constructing a FeatureExecutionPlan given QueryGraphModel and Node

    Parameters
    ----------
    graph: QueryGraphModel
        Query graph
    source_type: SourceType
        Source type information
    is_online_serving: bool
        Whether the generated code is intended for online serving
    serving_names_mapping: dict[str, str] | None
        Mapping from default serving names to new serving names
    """

    def __init__(
        self,
        graph: QueryGraphModel,
        source_type: SourceType,
        is_online_serving: bool,
        serving_names_mapping: dict[str, str] | None = None,
    ):
        self.graph = graph
        self.plan = FeatureExecutionPlan(source_type, is_online_serving)
        self.source_type = source_type
        self.serving_names_mapping = serving_names_mapping
        self.is_online_serving = is_online_serving

    def generate_plan(self, nodes: list[Node]) -> FeatureExecutionPlan:
        """Generate FeatureExecutionPlan for given list of query graph Nodes

        Parameters
        ----------
        nodes : list[Node]
            Query graph nodes

        Returns
        -------
        FeatureExecutionPlan
        """
        for node in nodes:
            self.process_node(node)
        return self.plan

    def process_node(self, node: Node) -> None:
        """Update plan state for a given query graph Node

        Parameters
        ----------
        node : Node
            Query graph node
        """
        groupby_nodes = list(self.graph.iterate_nodes(node, NodeType.GROUPBY))
        item_groupby_nodes = list(self.graph.iterate_nodes(node, NodeType.ITEM_GROUPBY))
        lookup_nodes = list(self.graph.iterate_nodes(node, NodeType.LOOKUP))
        if groupby_nodes:
            # Feature involves window aggregations. In this case, tiling applies. Even if
            # ITEM_GROUPBY nodes are involved, their results would have already been incorporated in
            # tiles, so we only need to handle GROUPBY node type here.
            for groupby_node in groupby_nodes:
                self.parse_and_update_specs_from_groupby(groupby_node)
        elif item_groupby_nodes:
            # Feature involves non-time-aware aggregations
            for item_groupby_node in item_groupby_nodes:
                self.parse_and_update_specs_from_item_groupby(item_groupby_node)
        else:
            for lookup_node in lookup_nodes:
                self.parse_and_update_specs_from_lookup(lookup_node)
        self.update_feature_specs(node)

    def parse_and_update_specs_from_groupby(self, groupby_node: Node) -> None:
        """Update FeatureExecutionPlan with a groupby query node

        Parameters
        ----------
        groupby_node : Node
            Groupby query node
        """
        agg_specs = TileBasedAggregationSpec.from_groupby_query_node(
            groupby_node, serving_names_mapping=self.serving_names_mapping
        )
        for agg_spec in agg_specs:
            self.plan.add_aggregation_spec(agg_spec)

    def parse_and_update_specs_from_item_groupby(self, node: Node) -> None:
        """Update FeatureExecutionPlan with an item groupby query node

        Parameters
        ----------
        node : Node
            Query graph node
        """
        sql_node = SQLOperationGraph(
            self.graph, SQLType.AGGREGATION, source_type=self.source_type
        ).build(node)
        agg_expr = sql_node.sql
        agg_spec = ItemAggregationSpec.from_item_groupby_query_node(
            node, agg_expr, serving_names_mapping=self.serving_names_mapping
        )
        self.plan.add_aggregation_spec(agg_spec)

    def parse_and_update_specs_from_lookup(self, node: Node) -> None:
        """Update FeatureExecutionPlan with a lookup query node

        Parameters
        ----------
        node : Node
            Query graph node
        """
        agg_specs = LookupSpec.from_lookup_query_node(
            node,
            graph=self.graph,
            source_type=self.source_type,
            serving_names_mapping=self.serving_names_mapping,
        )
        for agg_spec in agg_specs:
            self.plan.add_aggregation_spec(agg_spec)

    def update_feature_specs(self, node: Node) -> None:
        """Update FeatureExecutionPlan with a query graph node

        Parameters
        ----------
        node : Node
            Query graph node
        """
        sql_graph = SQLOperationGraph(
            self.graph, SQLType.POST_AGGREGATION, source_type=self.source_type
        )
        sql_node = sql_graph.build(node)

        if isinstance(sql_node, TableNode):
            # sql_node corresponds to a FeatureGroup that results from point-in-time groupby or item
            # groupby (e.g. AggregatedTilesNode, AggregatedItemGroupby nodes)
            for feature_name, feature_expr in sql_node.columns_map.items():
                feature_spec = FeatureSpec(
                    feature_name=feature_name,
                    feature_expr=feature_expr.sql(),
                )
                self.plan.add_feature_spec(feature_spec)
        else:
            if isinstance(sql_node, Project):
                feature_name = sql_node.column_name
            elif isinstance(sql_node, AliasNode):
                feature_name = sql_node.name
            else:
                # Otherwise, there is no way to know about the feature name. Technically speaking
                # this could still be previewed as an "unnamed" feature since the expression is
                # available, but it cannot be published.
                feature_name = "Unnamed"
            feature_expr_str = sql_node.sql.sql()
            feature_spec = FeatureSpec(feature_name=feature_name, feature_expr=feature_expr_str)
            self.plan.add_feature_spec(feature_spec)
