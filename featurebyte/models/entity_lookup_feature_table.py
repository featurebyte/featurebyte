"""
Entity lookup feature table construction
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.parent_serving import EntityLookupStep
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import SCDBaseParameters
from featurebyte.query_graph.transform.decompose_point import FeatureJobSettingExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor


@dataclass
class EntityLookupGraphResult:
    """
    Query graph constructed for parent entity lookup
    """

    graph: QueryGraph
    lookup_node: Node
    feature_node_name: str
    feature_dtype: DBVarType
    feature_job_setting: Optional[FeatureJobSettingUnion]


def get_scd_parameters_from_scd_relation_table(relation_table: SCDTableModel) -> SCDBaseParameters:
    """
    Get SCD parameters from SCD relation table

    Parameters
    ----------
    relation_table: SCDTableModel
        SCD table model

    Returns
    -------
    SCDBaseParameters
    """
    effective_timestamp_metadata = (
        DBVarTypeMetadata(timestamp_schema=relation_table.effective_timestamp_schema)
        if relation_table.effective_timestamp_schema
        else None
    )
    end_timestamp_schema = (
        DBVarTypeMetadata(timestamp_schema=relation_table.end_timestamp_schema)
        if relation_table.end_timestamp_schema
        else None
    )
    return SCDBaseParameters(
        effective_timestamp_column=relation_table.effective_timestamp_column,
        natural_key_column=relation_table.natural_key_column,
        current_flag_column=relation_table.current_flag_column,
        end_timestamp_column=relation_table.end_timestamp_column,
        effective_timestamp_metadata=effective_timestamp_metadata,
        end_timestamp_metadata=end_timestamp_schema,
    )


def get_entity_lookup_graph(
    lookup_step: EntityLookupStep,
    feature_store: FeatureStoreModel,
) -> EntityLookupGraphResult:
    """
    Create a query graph that represents the parent entity lookup operation

    Parameters
    ----------
    lookup_step: EntityLookupStep
        Entity lookup information
    feature_store: FeatureStoreModel
        Feature store

    Returns
    -------
    EntityLookupGraphResult
    """
    relation_table = lookup_step.table
    graph = QueryGraph()
    input_node = graph.add_operation_node(
        node=relation_table.construct_input_node(feature_store_details=feature_store),
        input_nodes=[],
    )
    additional_params: Dict[str, Any]
    if relation_table.type == TableDataType.SCD_TABLE:
        assert isinstance(relation_table, SCDTableModel)
        additional_params = {
            "scd_parameters": get_scd_parameters_from_scd_relation_table(relation_table).model_dump(
                by_alias=True
            )
        }
    elif relation_table.type == TableDataType.EVENT_TABLE:
        assert isinstance(relation_table, EventTableModel)
        additional_params = {
            "event_parameters": {
                "event_timestamp_column": relation_table.event_timestamp_column,
            }
        }
    else:
        additional_params = {}
    lookup_node = graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params={
            "input_column_names": [lookup_step.parent.key],
            "feature_names": [lookup_step.parent.serving_name],
            "entity_column": lookup_step.child.key,
            "serving_name": lookup_step.child.serving_name,
            "entity_id": lookup_step.child.entity_id,
            **additional_params,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node],
    )
    feature_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": [lookup_step.parent.serving_name]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[lookup_node],
    )
    op_struct = (
        OperationStructureExtractor(graph=graph)
        .extract(node=feature_node)
        .operation_structure_map[feature_node.name]
    )
    aggregations = op_struct.aggregations
    assert len(aggregations) == 1
    feature_job_setting = FeatureJobSettingExtractor(graph=graph).extract_from_agg_node(
        node=lookup_node
    )
    return EntityLookupGraphResult(
        graph=graph,
        lookup_node=lookup_node,
        feature_node_name=feature_node.name,
        feature_dtype=aggregations[0].dtype,
        feature_job_setting=feature_job_setting,
    )
