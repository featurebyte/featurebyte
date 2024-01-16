"""
Entity lookup feature table construction
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from dataclasses import dataclass

from bson import ObjectId

from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.entity import EntityModel
from featurebyte.models.entity_universe import (
    EntityUniverseModel,
    EntityUniverseParams,
    get_combined_universe,
)
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_list import FeatureCluster, FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.entity_lookup_plan import EntityLookupPlanner
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import SCDBaseParameters


class EntityLookupStep(FeatureByteBaseModel):
    """
    EntityLookupStep class
    """

    id: PydanticObjectId
    child_entity: EntityModel
    parent_entity: EntityModel
    relation_table: ProxyTableModel


@dataclass
class EntityLookupGraphResult:
    """
    Query graph constructed for parent entity lookup
    """

    graph: QueryGraph
    lookup_node: Node
    feature_node_name: str
    feature_dtype: DBVarType


def get_lookup_feature_table_name(relationship_info_id: ObjectId) -> str:
    """
    Get the offline feature table name for parent entity lookup

    Parameters
    ----------
    relationship_info_id: ObjectId
        Id of the relationship info

    Returns
    -------
    str
    """
    return f"fb_entity_lookup_{relationship_info_id}"


def get_entity_lookup_feature_tables(
    feature_table_primary_entity_ids: List[PydanticObjectId],
    feature_lists: List[FeatureListModel],
    feature_store: FeatureStoreModel,
    entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep],
) -> Optional[List[OfflineStoreFeatureTableModel]]:
    """
    Get list of internal offline store feature tables for parent entity lookup purpose

    Parameters
    ----------
    feature_table_primary_entity_ids: OfflineStoreTable
        Primary entity ids of the offline store feature table
    feature_lists: List[FeatureListModel]
        Currently online enabled feature lists
    feature_store: FeatureStoreModel
        Feature store model
    entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep]
        Mapping from relationship info id to EntityLookupStep objects

    Returns
    -------
    Optional[List[OfflineStoreFeatureTableModel]]
    """

    if len(feature_table_primary_entity_ids) == 0:
        # Skip for feature tables without entity
        return None

    feature_table_primary_entity_ids_set = set(feature_table_primary_entity_ids)

    required_lookup_relationships = set()
    for feature_list in feature_lists:
        # Skip if entities are already fulfilled
        if feature_table_primary_entity_ids_set.issubset(feature_list.primary_entity_ids):
            continue
        # Collect required lookup relationships
        entity_lookup_plan = EntityLookupPlanner.generate_plan(
            feature_table_primary_entity_ids, feature_list.relationships_info or []
        )
        lookup_relationships = entity_lookup_plan.get_entity_lookup_steps(
            feature_list.primary_entity_ids
        )
        if lookup_relationships is not None:
            required_lookup_relationships.update(lookup_relationships)
        for serving_entity_ids in feature_list.supported_serving_entity_ids:
            lookup_relationships = entity_lookup_plan.get_entity_lookup_steps(serving_entity_ids)
            if lookup_relationships is not None:
                required_lookup_relationships.update(lookup_relationships)

    out = []

    for lookup_relationship in required_lookup_relationships:
        lookup_step = entity_lookup_steps_mapping[lookup_relationship.id]
        lookup_graph_result = _get_entity_lookup_graph(
            lookup_step=lookup_step,
            feature_store=feature_store,
        )
        feature_cluster = FeatureCluster(
            feature_store_id=feature_store.id,
            graph=lookup_graph_result.graph,
            node_names=[lookup_graph_result.feature_node_name],
        )
        universe_expr = get_combined_universe(
            entity_universe_params=[
                EntityUniverseParams(
                    graph=lookup_graph_result.graph,
                    node=lookup_graph_result.lookup_node,
                    join_steps=None,
                )
            ],
            source_type=feature_store.type,
        )
        entity_universe = EntityUniverseModel(
            query_template=SqlglotExpressionModel.create(universe_expr)
        )
        entity_lookup_feature_table_model = OfflineStoreFeatureTableModel(
            name=get_lookup_feature_table_name(lookup_step.id),
            feature_ids=[],
            primary_entity_ids=[lookup_step.child_entity.id],
            serving_names=[lookup_step.child_entity.serving_names[0]],
            feature_cluster=feature_cluster,
            output_column_names=[lookup_step.parent_entity.serving_names[0]],
            output_dtypes=[lookup_graph_result.feature_dtype],
            entity_universe=entity_universe,
            has_ttl=False,
            feature_job_setting=FeatureJobSetting(
                frequency="1d",
                time_modulo_frequency="0s",
                blind_spot="0s",
            ),
            is_entity_lookup=True,
        )
        out.append(entity_lookup_feature_table_model)
    return out


def _get_entity_lookup_graph(
    lookup_step: EntityLookupStep,
    feature_store: FeatureStoreModel,
) -> EntityLookupGraphResult:
    relation_table = lookup_step.relation_table
    graph = QueryGraph()
    input_node = graph.add_operation_node(
        node=relation_table.construct_input_node(feature_store_details=feature_store),
        input_nodes=[],
    )

    child_column_name = None
    parent_column_name = None
    feature_dtype = None
    for column_info in relation_table.columns_info:
        if column_info.entity_id == lookup_step.child_entity.id:
            child_column_name = column_info.name
        elif column_info.entity_id == lookup_step.parent_entity.id:
            parent_column_name = column_info.name
            feature_dtype = column_info.dtype
    assert child_column_name is not None
    assert parent_column_name is not None
    assert feature_dtype is not None

    additional_params: Dict[str, Any]
    if relation_table.type == TableDataType.SCD_TABLE:
        assert isinstance(relation_table, SCDTableModel)
        additional_params = {
            "scd_parameters": SCDBaseParameters(
                effective_timestamp_column=relation_table.effective_timestamp_column,
                natural_key_column=relation_table.natural_key_column,
                current_flag_column=relation_table.current_flag_column,
                end_timestamp_column=relation_table.end_timestamp_column,
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
        # TODO: handle ITEM_TABLE which also needs event_parameters
        additional_params = {}
    lookup_node = graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params={
            "input_column_names": [parent_column_name],
            "feature_names": [lookup_step.parent_entity.serving_names[0]],
            "entity_column": child_column_name,
            "serving_name": lookup_step.child_entity.serving_names[0],
            "entity_id": lookup_step.child_entity.id,
            **additional_params,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node],
    )
    feature_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": [lookup_step.parent_entity.serving_names[0]]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[lookup_node],
    )
    return EntityLookupGraphResult(
        graph=graph,
        lookup_node=lookup_node,
        feature_node_name=feature_node.name,
        feature_dtype=feature_dtype,
    )
