"""
Module with logic related to feature SQL generation
"""

from __future__ import annotations

from typing import Iterable, Optional, Sequence, Set, Type, Union

import sys
from collections import defaultdict

from bson import ObjectId
from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.enum import DBVarType, SourceType
from featurebyte.models.parent_serving import (
    EntityLookupStep,
    EntityRelationshipsContext,
    FeatureNodeRelationshipsInfo,
    ParentServingPreparation,
)
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.entity_lookup_plan import EntityColumn, EntityLookupPlanner
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import FeatureStoreDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.aggregator.asat import AsAtAggregator
from featurebyte.query_graph.sql.aggregator.base import TileBasedAggregator
from featurebyte.query_graph.sql.aggregator.forward import ForwardAggregator
from featurebyte.query_graph.sql.aggregator.forward_asat import ForwardAsAtAggregator
from featurebyte.query_graph.sql.aggregator.item import ItemAggregator
from featurebyte.query_graph.sql.aggregator.latest import LatestAggregator
from featurebyte.query_graph.sql.aggregator.lookup import LookupAggregator
from featurebyte.query_graph.sql.aggregator.lookup_target import LookupTargetAggregator
from featurebyte.query_graph.sql.aggregator.window import WindowAggregator
from featurebyte.query_graph.sql.ast.base import TableNode
from featurebyte.query_graph.sql.ast.generic import AliasNode, Project
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import (
    CteStatement,
    CteStatements,
    SQLType,
    construct_cte_sql,
    quoted_identifier,
)
from featurebyte.query_graph.sql.parent_serving import construct_request_table_with_parent_entities
from featurebyte.query_graph.sql.specifications.aggregate_asat import AggregateAsAtSpec
from featurebyte.query_graph.sql.specifications.forward_aggregate_asat import (
    ForwardAggregateAsAtSpec,
)
from featurebyte.query_graph.sql.specifications.lookup import LookupSpec
from featurebyte.query_graph.sql.specifications.lookup_target import LookupTargetSpec
from featurebyte.query_graph.sql.specs import (
    AggregationSpec,
    AggregationType,
    FeatureSpec,
    ForwardAggregateSpec,
    ItemAggregationSpec,
    NonTileBasedAggregationSpec,
    TileBasedAggregationSpec,
)
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor

AggregatorType = Union[
    LatestAggregator,
    LookupAggregator,
    LookupTargetAggregator,
    WindowAggregator,
    ItemAggregator,
    AsAtAggregator,
    ForwardAggregator,
    ForwardAsAtAggregator,
]
AggregationSpecType = Union[TileBasedAggregationSpec, NonTileBasedAggregationSpec]

sys.setrecursionlimit(10000)


class FeatureExecutionPlan:
    """Responsible for constructing the SQL to compute features by aggregating tiles"""

    AGGREGATION_TABLE_NAME = "_FB_AGGREGATED"

    def __init__(
        self,
        source_type: SourceType,
        is_online_serving: bool,
        parent_serving_preparation: ParentServingPreparation | None = None,
        feature_store_details: Optional[FeatureStoreDetails] = None,
    ) -> None:
        aggregator_kwargs = {"source_type": source_type, "is_online_serving": is_online_serving}
        self.aggregators: dict[str, AggregatorType] = {
            AggregationType.LATEST: LatestAggregator(**aggregator_kwargs),
            AggregationType.LOOKUP: LookupAggregator(**aggregator_kwargs),
            AggregationType.LOOKUP_TARGET: LookupTargetAggregator(**aggregator_kwargs),
            AggregationType.WINDOW: WindowAggregator(**aggregator_kwargs),
            AggregationType.ITEM: ItemAggregator(**aggregator_kwargs),
            AggregationType.AS_AT: AsAtAggregator(**aggregator_kwargs),
            AggregationType.FORWARD: ForwardAggregator(**aggregator_kwargs),
            AggregationType.FORWARD_AS_AT: ForwardAsAtAggregator(**aggregator_kwargs),
        }
        self.feature_specs: dict[str, FeatureSpec] = {}
        self.feature_entity_lookup_steps: dict[str, EntityLookupStep] = {}
        self.adapter = get_sql_adapter(source_type)
        self.source_type = source_type
        self.parent_serving_preparation = parent_serving_preparation
        self.feature_name_dtype_mapping: dict[str, DBVarType] = {}
        self.feature_store_details = feature_store_details

    @property
    def required_entity_ids(self) -> set[ObjectId]:
        """Returns the list of required entity_ids

        Returns
        -------
        set[ObjectId]
        """
        out = set()
        for aggregator in self.iter_aggregators():
            out.update(aggregator.get_required_entity_ids())
        return out

    @property
    def feature_names(self) -> list[str]:
        """Returns the list of feature names

        Returns
        -------
        list[str]
        """
        return list(self.feature_specs.keys())

    @property
    def tile_based_aggregation_result_names(self) -> list[str]:
        """Returns the list of tile based aggregation result names

        Returns
        -------
        list[str]
        """
        out: Set[str] = set()
        for aggregator in self.iter_aggregators():
            if isinstance(aggregator, TileBasedAggregator):
                for info in aggregator.online_join_info.values():
                    out.update(info.get_original_agg_result_names())
        return list(out)

    def iter_aggregators(self) -> Iterable[AggregatorType]:
        """Iterate over all the aggregators

        Yields
        ------
        BaseAggregator
            Instance of an aggregator
        """
        yield from self.aggregators.values()

    def add_aggregation_spec(
        self,
        aggregation_spec: AggregationSpec,
        node_name: str,
    ) -> None:
        """Add AggregationSpec to be incorporated when generating SQL

        Parameters
        ----------
        aggregation_spec : AggregationSpec
            Aggregation specification
        node_name: str
            Feature node name associated with the aggregation specification
        """
        self.update_feature_entity_lookup_steps(aggregation_spec, node_name)
        key = aggregation_spec.aggregation_type
        aggregator = self.aggregators[key]
        aggregator.update(aggregation_spec)  # type: ignore

    def update_feature_entity_lookup_steps(self, agg_spec: AggregationSpec, node_name: str) -> None:
        """
        Check if any entity lookup steps are required for the aggregation spec and if so update its
        serving_names attribute in place. Also add the required lookup steps into the plan.

        Parameters
        ----------
        agg_spec: AggregationSpec
            Aggregation specification
        node_name: str
            Feature node name associated with the aggregation specification
        """
        entity_relationships_context = (
            self.parent_serving_preparation.entity_relationships_context
            if self.parent_serving_preparation is not None
            else None
        )
        if entity_relationships_context is None:
            return

        parent_entity_columns = self._get_parent_entity_columns(
            entity_relationships_context=entity_relationships_context,
            node_info=entity_relationships_context.get_node_info(node_name),
            agg_spec=agg_spec,
        )

        # Add to overall feature entity lookup steps
        for entity_column in parent_entity_columns.values():
            if entity_column.relationship_info_id is not None:
                entity_lookup_step = (
                    entity_relationships_context.entity_lookup_step_creator.get_entity_lookup_step(
                        relationship_info_id=entity_column.relationship_info_id,
                        child_serving_name_override=entity_column.child_serving_name,
                        parent_serving_name_override=entity_column.serving_name,
                    )
                )
                self.add_feature_entity_lookup_step(entity_lookup_step)

        # Get updated serving names in aggregation spec
        assert agg_spec.entity_ids is not None
        new_serving_names = []
        for entity_id, serving_name in zip(agg_spec.entity_ids, agg_spec.serving_names):
            if entity_id in parent_entity_columns:
                new_serving_name = parent_entity_columns[entity_id].serving_name
            else:
                new_serving_name = serving_name
            new_serving_names.append(new_serving_name)
        agg_spec.serving_names = new_serving_names

    @staticmethod
    def _get_parent_entity_columns(
        entity_relationships_context: EntityRelationshipsContext,
        node_info: FeatureNodeRelationshipsInfo,
        agg_spec: AggregationSpec,
    ) -> dict[ObjectId, EntityColumn]:
        # Lookup feature primary entity from feature list primary entity
        fl_lookup_steps = EntityLookupPlanner.generate_lookup_steps(
            available_entity_ids=entity_relationships_context.feature_list_primary_entity_ids,
            required_entity_ids=node_info.primary_entity_ids,
            relationships_info=entity_relationships_context.feature_list_relationships_info,
        )

        # Lookup feature non-primary entity from feature primary entity
        feature_lookup_steps = EntityLookupPlanner.generate_lookup_steps(
            available_entity_ids=node_info.primary_entity_ids,
            required_entity_ids=agg_spec.entity_ids or [],
            relationships_info=node_info.relationships_info,
        )

        # Get new columns that should be generated by parent entity lookup using feature
        # list primary entity as the starting point
        lookup_steps = fl_lookup_steps + feature_lookup_steps
        parent_entity_columns = {}
        for entity_id, serving_name in zip(
            entity_relationships_context.feature_list_primary_entity_ids,
            entity_relationships_context.feature_list_serving_names,
        ):
            fl_primary_entity_column = EntityColumn(
                entity_id=entity_id,
                serving_name=serving_name,
                child_serving_name=None,
                relationship_info_id=None,
            )
            for parent_entity_column in fl_primary_entity_column.get_parent_entity_columns(
                lookup_steps
            ):
                if parent_entity_column.entity_id not in parent_entity_columns:
                    parent_entity_columns[parent_entity_column.entity_id] = parent_entity_column
        return parent_entity_columns

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
        if feature_spec.feature_dtype is not None:
            self.feature_name_dtype_mapping[key] = feature_spec.feature_dtype

    def add_feature_entity_lookup_step(self, entity_lookup_step: EntityLookupStep) -> None:
        """
        Add an EntityLookupStep to the plan. This looks up a required parent entity column based on
        the relationships defined at feature level.

        Parameters
        ----------
        entity_lookup_step: EntityLookupStep
            EntityLookupStep object
        """
        key = entity_lookup_step.parent.serving_name
        if key not in self.feature_entity_lookup_steps:
            self.feature_entity_lookup_steps[key] = entity_lookup_step

    def construct_request_table_with_parent_entities(
        self,
        entity_lookup_steps: list[EntityLookupStep],
        request_table_name: str,
        request_table_columns: list[str],
    ) -> tuple[expressions.Select, list[str]]:
        """
        Construct updated request table with parent entities added

        Parameters
        ----------
        entity_lookup_steps: list[EntityLookupStep]
            All the entity lookup steps that need to be performed
        request_table_name: str
            Name of the request table
        request_table_columns: list[str]
            Columns in the request table

        Returns
        -------
        tuple[expressions.Select, List[str]]
        """
        assert self.parent_serving_preparation is not None
        parent_serving_result = construct_request_table_with_parent_entities(
            request_table_name=request_table_name,
            request_table_columns=request_table_columns,
            join_steps=entity_lookup_steps,
            feature_store_details=self.parent_serving_preparation.feature_store_details,
        )
        return parent_serving_result.table_expr, parent_serving_result.parent_entity_columns

    def construct_combined_aggregation_cte(
        self,
        request_table_name: str,
        point_in_time_column: str,
        request_table_columns: Optional[list[str]],
    ) -> tuple[CteStatement, list[str]]:
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
        tuple[CteStatement, list[str]]
            Tuple of CteExpression and list of column names
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
        agg_result_names = []
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
            agg_result_names += agg_result.column_names

        return (self.AGGREGATION_TABLE_NAME, table_expr), agg_result_names

    def construct_post_aggregation_sql(
        self,
        cte_context: expressions.Select,
        request_table_columns: Optional[list[str]],
        exclude_post_aggregation: bool,
        agg_result_names: list[str],
        exclude_columns: set[str],
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
        exclude_post_aggregation: bool
            When True, exclude post aggregation transforms and select aggregated columns as the
            output columns directly. Intended to be used by online store pre-computation.
        agg_result_names: bool
            Names of the aggregated columns. Used when excluded_post_aggregation is True.
        exclude_columns: set[str]
            Exclude these columns from the output. This is currently used when generating feature
            retrieval sql for online requests where we want to exclude the internally added point in
            time column from the final output.

        Returns
        -------
        str
        """
        columns: list[expressions.Expression | str] = []
        if exclude_post_aggregation:
            for agg_result_name in agg_result_names:
                columns.append(quoted_identifier(agg_result_name))
        else:
            for feature_spec in self.feature_specs.values():
                feature_expr = feature_spec.feature_expr
                feature_dtype = self.feature_name_dtype_mapping.get(feature_spec.feature_name)
                if feature_dtype is not None:
                    feature_expr = self._cast_output_column_by_dtype(feature_expr, feature_dtype)
                feature_alias = expressions.alias_(
                    feature_expr, alias=feature_spec.feature_name, quoted=True
                )
                columns.append(feature_alias)

        if request_table_columns:
            request_table_column_names = [
                f"AGG.{quoted_identifier(col).sql()}"
                for col in request_table_columns
                if col not in exclude_columns
            ]
        else:
            request_table_column_names = []

        table_expr = cte_context.select(*request_table_column_names, *columns).from_(
            f"{self.AGGREGATION_TABLE_NAME} AS AGG"
        )
        return table_expr

    @staticmethod
    def _cast_output_column_by_dtype(
        feature_expr: expressions.Expression, dtype: DBVarType
    ) -> expressions.Expression:
        if dtype == DBVarType.FLOAT:
            return expressions.Cast(
                this=feature_expr,
                to=expressions.DataType.build("DOUBLE"),
            )
        if dtype == DBVarType.INT:
            return expressions.Cast(
                this=feature_expr,
                to=expressions.DataType.build("BIGINT"),
            )
        return feature_expr

    def get_overall_entity_lookup_steps(self) -> list[EntityLookupStep]:
        """
        Get all the entity lookup steps required

        Returns
        -------
        list[EntityLookupStep]
        """
        out = []
        if self.parent_serving_preparation is not None:
            out.extend(self.parent_serving_preparation.join_steps)
        out.extend(self.feature_entity_lookup_steps.values())
        return out

    def construct_combined_sql(
        self,
        request_table_name: str,
        point_in_time_column: str,
        request_table_columns: Optional[list[str]],
        prior_cte_statements: Optional[CteStatements] = None,
        exclude_post_aggregation: bool = False,
        exclude_columns: Optional[set[str]] = None,
    ) -> expressions.Select:
        """Construct combined SQL that will generate the features

        Parameters
        ----------
        request_table_name : str
            Name of request table to use
        point_in_time_column : str
            Point in time column
        request_table_columns : Optional[list[str]]
            Request table columns
        prior_cte_statements : Optional[list[tuple[str, str]]]
            Other CTE statements to incorporate to the final SQL (namely the request data SQL and
            on-demand tile SQL)
        exclude_post_aggregation: bool
            When True, exclude post aggregation transforms and select aggregated columns as the
            output columns directly. Intended to be used by online store pre-computation.
        exclude_columns: Optional[set[str]]
            When provided, exclude these columns from the output

        Returns
        -------
        str
        """
        cte_statements = []
        if prior_cte_statements is not None:
            assert isinstance(prior_cte_statements, list)
            cte_statements.extend(prior_cte_statements)

        if exclude_columns is None:
            exclude_columns = set()

        overall_entity_lookup_steps = self.get_overall_entity_lookup_steps()
        if overall_entity_lookup_steps:
            assert request_table_columns is not None
            (
                updated_request_table_expr,
                new_columns,
            ) = self.construct_request_table_with_parent_entities(
                entity_lookup_steps=overall_entity_lookup_steps,
                request_table_name=request_table_name,
                request_table_columns=request_table_columns,
            )
            request_table_name = "JOINED_PARENTS_" + request_table_name
            cte_statements.append((request_table_name, updated_request_table_expr))
            request_table_columns = request_table_columns + list(new_columns)
            exclude_columns.update(new_columns)

        for aggregator in self.iter_aggregators():
            cte_statements.extend(aggregator.get_common_table_expressions(request_table_name))

        agg_cte, agg_result_names = self.construct_combined_aggregation_cte(
            request_table_name,
            point_in_time_column,
            request_table_columns,
        )
        cte_statements.append(agg_cte)
        cte_context = construct_cte_sql(cte_statements)

        post_aggregation_sql = self.construct_post_aggregation_sql(
            cte_context=cte_context,
            request_table_columns=request_table_columns,
            exclude_post_aggregation=exclude_post_aggregation,
            agg_result_names=agg_result_names,
            exclude_columns=exclude_columns,
        )
        return post_aggregation_sql


class FeatureExecutionPlanner:  # pylint: disable=too-many-instance-attributes
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
        is_online_serving: bool,
        agg_result_name_include_serving_names: bool = True,
        serving_names_mapping: dict[str, str] | None = None,
        source_type: SourceType | None = None,
        parent_serving_preparation: ParentServingPreparation | None = None,
    ):
        if source_type is None:
            source_type = SourceType.SNOWFLAKE
        self.graph, self.node_name_map = GraphFlatteningTransformer(graph=graph).transform()
        self.op_struct_extractor = OperationStructureExtractor(graph=self.graph)
        self.plan = FeatureExecutionPlan(
            source_type,
            is_online_serving,
            parent_serving_preparation=parent_serving_preparation,
        )
        self.source_type = source_type
        self.serving_names_mapping = serving_names_mapping
        self.agg_result_name_include_serving_names = agg_result_name_include_serving_names
        self.is_online_serving = is_online_serving
        self.adapter = get_sql_adapter(source_type)

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
            # map the input node to the node inside the flattened graph (self.graph)
            mapped_node = self.graph.get_node_by_name(self.node_name_map[node.name])
            self.process_node(mapped_node, original_node_name=node.name)
        return self.plan

    def process_node(self, node: Node, original_node_name: str) -> None:
        """Update plan state for a given query graph Node

        Parameters
        ----------
        node : Node
            Query graph node
        original_node_name: str
            Name of the query graph node before flattening. This is used to retrieve the
            corresponding FeatureNodeRelationshipsInfo object.
        """
        aggregation_specs = defaultdict(list)
        for agg_spec in self.get_aggregation_specs(node):
            self.plan.add_aggregation_spec(agg_spec, original_node_name)
            aggregation_specs[agg_spec.node_name].append(agg_spec)
        self.update_feature_specs(node, dict(aggregation_specs))  # type: ignore[arg-type]

    def get_aggregation_specs(  # pylint: disable=too-many-branches
        self, node: Node
    ) -> list[AggregationSpecType]:
        """Get list of aggregation specs for a given query graph node

        Parameters
        ----------
        node : Node
            Query graph node

        Returns
        -------
        AggregationSpec
        """
        groupby_nodes = list(self.graph.iterate_nodes(node, NodeType.GROUPBY))
        # If ITEM_GROUPBY nodes can be reached without going through GROUPBY nodes, they need to be
        # processed separately as simple aggregations (not part of double aggregations).
        if node.type == NodeType.GROUPBY:
            # This should occur only in test. In practice, all feature nodes are alias or project
            # nodes.
            item_groupby_nodes = []
        else:
            item_groupby_nodes = list(
                self.graph.iterate_nodes(
                    node, NodeType.ITEM_GROUPBY, skip_node_type=NodeType.GROUPBY
                )
            )
        lookup_nodes = list(self.graph.iterate_nodes(node, NodeType.LOOKUP))
        lookup_target_nodes = list(self.graph.iterate_nodes(node, NodeType.LOOKUP_TARGET))
        asat_nodes = list(self.graph.iterate_nodes(node, NodeType.AGGREGATE_AS_AT))
        forward_aggregate_nodes = list(self.graph.iterate_nodes(node, NodeType.FORWARD_AGGREGATE))
        forward_asat_nodes = list(self.graph.iterate_nodes(node, NodeType.FORWARD_AGGREGATE_AS_AT))

        out: list[AggregationSpecType] = []
        if groupby_nodes:
            for groupby_node in groupby_nodes:
                out.extend(self.get_specs_from_groupby(groupby_node))

        if item_groupby_nodes:
            # Feature involves non-time-aware aggregations
            for item_groupby_node in item_groupby_nodes:
                out.extend(self.get_non_tiling_specs(ItemAggregationSpec, item_groupby_node))

        if lookup_nodes:
            for lookup_node in lookup_nodes:
                out.extend(self.get_non_tiling_specs(LookupSpec, lookup_node))

        if lookup_target_nodes:
            for lookup_node in lookup_target_nodes:
                out.extend(self.get_non_tiling_specs(LookupTargetSpec, lookup_node))

        if asat_nodes:
            for asat_node in asat_nodes:
                out.extend(self.get_non_tiling_specs(AggregateAsAtSpec, asat_node))

        if forward_aggregate_nodes:
            for forward_aggregate_node in forward_aggregate_nodes:
                out.extend(self.get_non_tiling_specs(ForwardAggregateSpec, forward_aggregate_node))

        if forward_asat_nodes:
            for forward_asat_node in forward_asat_nodes:
                out.extend(self.get_non_tiling_specs(ForwardAggregateAsAtSpec, forward_asat_node))

        return out

    def get_specs_from_groupby(self, groupby_node: Node) -> Sequence[TileBasedAggregationSpec]:
        """Update FeatureExecutionPlan with a groupby query node

        Parameters
        ----------
        groupby_node : Node
            Groupby query node

        Returns
        -------
        list[AggregationSpec]
        """
        return TileBasedAggregationSpec.from_groupby_query_node(
            self.graph,
            groupby_node,
            self.adapter,
            agg_result_name_include_serving_names=self.agg_result_name_include_serving_names,
            serving_names_mapping=self.serving_names_mapping,
        )

    def get_non_tiling_specs(
        self, spec_cls: Type[NonTileBasedAggregationSpec], node: Node
    ) -> Sequence[NonTileBasedAggregationSpec]:
        """
        Update FeatureExecutionPlan with a node that produces NonTileBasedAggregationSpec

        Parameters
        ----------
        node: Node
            Query graph node
        spec_cls: Type[NonTileBasedAggregationSpec]
            Aggregation specification class

        Returns
        -------
        list[AggregationSpec]
        """
        return spec_cls.from_query_graph_node(
            node,
            graph=self.graph,
            source_type=self.source_type,
            serving_names_mapping=self.serving_names_mapping,
            is_online_serving=self.is_online_serving,
            agg_result_name_include_serving_names=self.agg_result_name_include_serving_names,
        )

    def update_feature_specs(
        self, node: Node, aggregation_specs: dict[str, list[AggregationSpec]]
    ) -> None:
        """Update FeatureExecutionPlan with a query graph node

        Parameters
        ----------
        node : Node
            Query graph node
        aggregation_specs: dict[str, list[AggregationSpec]]
            Aggregation specs to use when constructing SQLOperationGraph
        """
        sql_graph = SQLOperationGraph(
            self.graph,
            SQLType.POST_AGGREGATION,
            source_type=self.source_type,
            aggregation_specs=aggregation_specs,
        )
        sql_node = sql_graph.build(node)
        op_struct = self.op_struct_extractor.extract(node=node).operation_structure_map[node.name]
        name_to_dtype = {
            aggregation.name: aggregation.dtype for aggregation in op_struct.aggregations
        }

        if isinstance(sql_node, TableNode):
            # sql_node corresponds to a FeatureGroup that results from point-in-time groupby or item
            # groupby (e.g. AggregatedTilesNode, AggregatedItemGroupby nodes)
            for feature_name, feature_expr in sql_node.columns_map.items():
                feature_spec = FeatureSpec(
                    feature_name=feature_name,
                    feature_expr=feature_expr,
                    feature_dtype=name_to_dtype[feature_name],
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
            feature_spec = FeatureSpec(
                feature_name=feature_name,
                feature_expr=sql_node.sql,
                feature_dtype=name_to_dtype.get(feature_name),
            )
            self.plan.add_feature_spec(feature_spec)
