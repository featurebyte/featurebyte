from __future__ import annotations

from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.enum import SourceType, SpecialColumnName, TableDataType
from featurebyte.models.parent_serving import JoinStep
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.generic import EventLookupParameters, SCDLookupParameters
from featurebyte.query_graph.node.schema import FeatureStoreDetails
from featurebyte.query_graph.sql.aggregator.lookup import LookupAggregator
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import (
    SQLType,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.specs import LookupSpec


def construct_request_table_with_parent_entities(
    request_table_name: str,
    request_table_columns: list[str],
    join_steps: list[JoinStep],
    source_type: SourceType,
    feature_store_details: FeatureStoreDetails,
) -> Select:

    table_expr = select(
        *[get_qualified_column_identifier(col, "REQ") for col in request_table_columns]
    ).from_(expressions.alias_(request_table_name, "REQ"))

    current_columns = request_table_columns[:]
    for join_step in join_steps:
        table_expr = apply_join_step(
            table_expr=table_expr,
            join_step=join_step,
            source_type=source_type,
            feature_store_details=feature_store_details,
            current_columns=current_columns,
        )
        current_columns.append(join_step.parent_serving_name)

    return table_expr


def apply_join_step(
    table_expr: Select,
    join_step: JoinStep,
    source_type: SourceType,
    feature_store_details: FeatureStoreDetails,
    current_columns: list[str],
) -> Select:

    aggregator = LookupAggregator(source_type=source_type)
    spec = get_lookup_spec_from_join_step(
        join_step=join_step,
        feature_store_details=feature_store_details,
        source_type=source_type,
    )
    aggregator.update(spec)

    aggregation_result = aggregator.update_aggregation_table_expr(
        table_expr=table_expr,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        current_columns=current_columns,
        current_query_index=0,
    )

    joined_table_expr = select(
        *[get_qualified_column_identifier(col, "REQ") for col in current_columns],
        alias_(
            get_qualified_column_identifier(spec.agg_result_name, "REQ"),
            alias=join_step.parent_serving_name,
            quoted=True,
        ),
    ).from_(aggregation_result.updated_table_expr.subquery(alias="REQ"))

    return joined_table_expr


def get_lookup_spec_from_join_step(
    join_step: JoinStep,
    feature_store_details: FeatureStoreDetails,
    source_type: SourceType,
) -> LookupSpec:

    graph = QueryGraph()
    input_node = graph.add_node(
        node=join_step.data.construct_input_node(feature_store_details=feature_store_details),
        input_nodes=[],
    )

    if join_step.data.type == TableDataType.SCD_DATA:
        scd_parameters = SCDLookupParameters(**join_step.data.dict())
    else:
        scd_parameters = None

    if join_step.data.type == TableDataType.EVENT_DATA:
        event_parameters = EventLookupParameters(**join_step.data.dict())
    else:
        event_parameters = None

    sql_operation_graph = SQLOperationGraph(
        query_graph=graph, sql_type=SQLType.AGGREGATION, source_type=source_type
    )
    sql_input_node = sql_operation_graph.build(input_node)
    source_expr = sql_input_node.sql

    return LookupSpec(
        input_column_name=join_step.parent_key,
        feature_name=join_step.parent_serving_name,
        entity_column=join_step.child_key,
        serving_names=[join_step.child_serving_name],
        source_expr=source_expr,
        scd_parameters=scd_parameters,
        event_parameters=event_parameters,
        serving_names_mapping=None,
        entity_ids=[],
        is_parent_lookup=True,
    )
