"""
SQL generation for looking up parent entities
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.enum import SpecialColumnName, TableDataType
from featurebyte.models.entity_lookup_feature_table import (
    get_scd_parameters_from_scd_relation_table,
)
from featurebyte.models.parent_serving import EntityLookupStep
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.generic import EventLookupParameters, SCDLookupParameters
from featurebyte.query_graph.node.schema import FeatureStoreDetails, TableDetails
from featurebyte.query_graph.sql.aggregator.lookup import LookupAggregator
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import (
    SQLType,
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
)
from featurebyte.query_graph.sql.specifications.lookup import LookupSpec
from featurebyte.query_graph.sql.specs import AggregationSource


@dataclass
class ParentEntityLookupResult:
    """
    Result of updating a request table with parent entities

    table_expr: Select
        Expression of the updated request table
    parent_entity_columns: list[str]
        Parent entity column names that were joined
    new_request_table_name: str
        Name of the updated request table
    new_request_table_columns: list[str]
        Column names of the updated request table
    """

    table_expr: Select
    parent_entity_columns: List[str]
    new_request_table_name: str
    new_request_table_columns: List[str]


def construct_request_table_with_parent_entities(
    request_table_name: Optional[str],
    request_table_columns: list[str],
    join_steps: list[EntityLookupStep],
    feature_store_details: FeatureStoreDetails,
    request_table_details: Optional[TableDetails] = None,
) -> ParentEntityLookupResult:
    """
    Construct a query to join parent entities into the request table

    Parameters
    ----------
    request_table_name: str
        Request table name
    request_table_columns: list[str]
        Column names in the request table
    join_steps: list[EntityLookupStep]
        The list of join steps to be applied. Each step joins a parent entity into the request
        table. Subsequent joins can use the newly joined columns as the join key.
    feature_store_details: FeatureStoreDetails
        Information about the feature store
    request_table_details: Optional[TableDetails]
        Location of the request table if it is a table in the warehouse

    Returns
    -------
    ParentEntityLookupResult
    """
    table_expr = select(*[
        get_qualified_column_identifier(col, "REQ") for col in request_table_columns
    ])
    if request_table_name is not None:
        table_expr = table_expr.from_(expressions.alias_(request_table_name, "REQ"))
    else:
        assert request_table_details is not None
        table_expr = table_expr.from_(
            get_fully_qualified_table_name(request_table_details.model_dump(), alias="REQ"),
        )

    current_columns = request_table_columns[:]
    new_columns = []
    for join_step in join_steps:
        table_expr = _apply_join_step(
            table_expr=table_expr,
            join_step=join_step,
            feature_store_details=feature_store_details,
            current_columns=current_columns,
        )
        current_columns.append(join_step.parent.serving_name)
        new_columns.append(join_step.parent.serving_name)

    return ParentEntityLookupResult(
        table_expr=table_expr,
        parent_entity_columns=new_columns,
        new_request_table_name=(
            "JOINED_PARENTS_" + request_table_name if request_table_name else "JOINED_PARENTS"
        ),
        new_request_table_columns=current_columns,
    )


def _apply_join_step(
    table_expr: Select,
    join_step: EntityLookupStep,
    feature_store_details: FeatureStoreDetails,
    current_columns: list[str],
) -> Select:
    # Use a LookupAggregator to join in the parent entity since the all the different types of
    # lookup logic dependent on the data type still apply (SCD lookup, time based event data lookup,
    # etc)
    aggregator = LookupAggregator(source_info=feature_store_details.details.get_source_info())
    spec = _get_lookup_spec_from_join_step(
        join_step=join_step,
        feature_store_details=feature_store_details,
    )
    aggregator.update(spec)
    aggregation_result = aggregator.update_aggregation_table_expr(
        table_expr=table_expr,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        current_columns=current_columns,
        current_query_index=0,
    )

    return aggregation_result.updated_table_expr


def _get_lookup_spec_from_join_step(
    join_step: EntityLookupStep,
    feature_store_details: FeatureStoreDetails,
) -> LookupSpec:
    # Set up data specific parameters
    if join_step.table.type == TableDataType.SCD_TABLE:
        assert isinstance(join_step.table, SCDTableModel)
        scd_base_params = get_scd_parameters_from_scd_relation_table(join_step.table).model_dump(
            by_alias=True
        )
        scd_parameters = SCDLookupParameters(**scd_base_params)
    else:
        scd_parameters = None

    if join_step.table.type == TableDataType.EVENT_TABLE:
        event_parameters = EventLookupParameters(**join_step.table.model_dump())
    else:
        event_parameters = None

    # Get the sql expression for the data
    graph = QueryGraph()
    input_node = graph.add_node(
        node=join_step.table.construct_input_node(feature_store_details=feature_store_details),
        input_nodes=[],
    )
    to_filter_scd_by_current_flag = False
    sql_input_node = SQLOperationGraph(
        query_graph=graph,
        sql_type=SQLType.AGGREGATION,
        source_info=feature_store_details.details.get_source_info(),
        to_filter_scd_by_current_flag=to_filter_scd_by_current_flag,
    ).build(input_node)
    aggregation_source = AggregationSource(
        expr=sql_input_node.sql,
        query_node_name=input_node.name,
        is_scd_filtered_by_current_flag=to_filter_scd_by_current_flag,
    )

    return LookupSpec(
        node_name="dummy",
        input_column_name=join_step.parent.key,
        feature_name=join_step.parent.serving_name,
        entity_column=join_step.child.key,
        serving_names=[join_step.child.serving_name],
        aggregation_source=aggregation_source,
        scd_parameters=scd_parameters,
        event_parameters=event_parameters,
        serving_names_mapping=None,
        entity_ids=[],  # entity_ids doesn't matter in this case, passing empty list for convenience
        is_parent_lookup=True,
        agg_result_name_include_serving_names=True,
    )
