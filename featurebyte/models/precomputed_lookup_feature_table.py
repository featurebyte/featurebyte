"""
Classes to support precomputed lookup feature tables
"""

import hashlib
import json
from typing import Dict, List, Optional, Tuple, cast

from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.enum import SpecialColumnName, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity_lookup_feature_table import get_entity_lookup_graph
from featurebyte.models.entity_universe import (
    CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER,
    EntityUniverseModel,
    EntityUniverseParams,
    get_combined_universe,
    get_item_relation_table_lookup_universe,
)
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_feature_table import (
    OfflineStoreFeatureTableModel,
    PrecomputedLookupFeatureTableInfo,
    PrecomputedLookupMapping,
)
from featurebyte.models.parent_serving import EntityLookupStep
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.model.entity_lookup_plan import EntityLookupPlanner
from featurebyte.query_graph.model.entity_relationship_info import (
    EntityAncestorDescendantMapper,
    EntityRelationshipInfo,
)
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import construct_cte_sql, quoted_identifier
from featurebyte.query_graph.sql.parent_serving import construct_request_table_with_parent_entities


def get_lookup_steps_unique_identifier(lookup_steps: List[EntityRelationshipInfo]) -> str:
    """
    Get a short unique identifier for a list of lookup steps

    Parameters
    ----------
    lookup_steps: List[EntityRelationshipInfo]
        Relationships used for parent entity lookup

    Returns
    -------
    str
    """
    hasher = hashlib.shake_128()
    for lookup_step in lookup_steps:
        hasher.update(json.dumps(lookup_step.json_dict()).encode("utf-8"))
    return hasher.hexdigest(3)


def get_precomputed_lookup_feature_table_name(
    source_feature_table_name: str,
    serving_names: List[str],
    lookup_steps: List[EntityRelationshipInfo],
) -> str:
    """
    Construct the name of a precomputed lookup feature table

    Parameters
    ----------
    source_feature_table_name: str
        Name of the offline store feature table that this table originates from
    serving_names: List[str]
        Serving names of the table
    lookup_steps: List[EntityRelationshipInfo]
        Relationships used for parent entity lookup

    Returns
    -------
    str
    """
    serving_names_suffix = OfflineStoreFeatureTableModel.get_serving_names_for_table_name(
        serving_names
    )
    unique_identifier = get_lookup_steps_unique_identifier(lookup_steps)
    return f"{source_feature_table_name}_via_{serving_names_suffix}_{unique_identifier}"


def get_precomputed_lookup_feature_table(
    primary_entity_ids: List[PydanticObjectId],
    feature_ids: List[PydanticObjectId],
    feature_list: FeatureListModel,
    full_serving_entity_ids: List[PydanticObjectId],
    feature_table_name: str,
    feature_table_has_ttl: bool,
    entity_id_to_serving_name: Dict[PydanticObjectId, str],
    entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep],
    feature_store_model: FeatureStoreModel,
    feature_table_id: Optional[PydanticObjectId] = None,
) -> Optional[OfflineStoreFeatureTableModel]:
    """
    Construct a precomputed lookup feature table for a given source feature table in order to
    support a specific deployment with a predetermined serving entity ids

    Parameters
    ----------
    primary_entity_ids: List[PydanticObjectId]
        Primary entity ids of the source feature table
    feature_ids: List[PydanticObjectId]
        List of features that references the source feature table
    feature_list: FeatureListModel
        Feature list associated with the deployment
    full_serving_entity_ids: List[PydanticObjectId]
        Serving entity ids of the deployment
    feature_table_name: str
        Name of the source feature table
    feature_table_has_ttl: bool
        Whether the source feature table has ttl
    entity_id_to_serving_name: Dict[PydanticObjectId, str]
        Mapping from entity id to serving name
    entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep]
        Mapping to obtain EntityLookupStep object given EntityRelationshipInfo id
    feature_store_model: FeatureStoreModel
        Feature store
    feature_table_id: PydanticObjectId
        Id of the source feature table

    Returns
    -------
    Optional[OfflineStoreFeatureTableModel]
    """
    if not set(feature_list.feature_ids).intersection(feature_ids):
        return None

    feature_lists_relationships_info = _get_feature_lists_to_relationships_info([feature_list])[
        feature_list.id
    ]
    primary_entity_ids = sorted(primary_entity_ids)

    relationships_mapper = EntityAncestorDescendantMapper.create(
        feature_lists_relationships_info,
    )
    serving_entity_ids = [
        PydanticObjectId(_id)
        for _id in relationships_mapper.keep_related_entity_ids(
            entity_ids_to_filter=full_serving_entity_ids,
            filter_by=primary_entity_ids,
        )
    ]
    lookup_steps, lookup_mapping = _get_lookup_steps_and_mapping(
        primary_entity_ids=primary_entity_ids,
        serving_entity_ids=serving_entity_ids,
        entity_id_to_serving_name=entity_id_to_serving_name,
        relationships_info=feature_lists_relationships_info,
    )
    key = tuple(lookup_steps)
    if not key:
        return None
    serving_names = [entity_id_to_serving_name[entity_id] for entity_id in serving_entity_ids]
    table = OfflineStoreFeatureTableModel(
        name=get_precomputed_lookup_feature_table_name(
            feature_table_name, serving_names, lookup_steps
        ),
        feature_ids=[],
        primary_entity_ids=serving_entity_ids,
        serving_names=serving_names,
        entity_universe=EntityUniverseModel(
            query_template=SqlglotExpressionModel.create(
                get_child_entity_universe_template(
                    lookup_steps=lookup_steps,
                    entity_lookup_steps_mapping=entity_lookup_steps_mapping,
                    feature_store=feature_store_model,
                ),
                source_type=feature_store_model.get_source_info().source_type,
            )
        ).model_dump(by_alias=True),
        precomputed_lookup_feature_table_info=PrecomputedLookupFeatureTableInfo(
            lookup_steps=lookup_steps,
            source_feature_table_id=feature_table_id,
            lookup_mapping=lookup_mapping,
        ),
        has_ttl=feature_table_has_ttl,
        output_column_names=[],
        output_dtypes=[],
        catalog_id=feature_list.catalog_id,
        feature_store_id=feature_store_model.id,
    )
    assert table.precomputed_lookup_feature_table_info is not None
    return table


def _get_lookup_steps_and_mapping(
    primary_entity_ids: List[PydanticObjectId],
    serving_entity_ids: List[PydanticObjectId],
    entity_id_to_serving_name: Dict[PydanticObjectId, str],
    relationships_info: List[EntityRelationshipInfo],
) -> Tuple[List[EntityRelationshipInfo], List[PrecomputedLookupMapping]]:
    """
    Get the entity lookup steps information required to map a serving_entity_ids to
    primary_entity_ids using available relationships

    Parameters
    ----------
    primary_entity_ids: List[PydanticObjectId]
        Primary entity ids of the source feature table
    serving_entity_ids: List[PydanticObjectId]
        Serving entity ids
    entity_id_to_serving_name: Dict[PydanticObjectId, str]
        Mapping from entity id to serving name
    relationships_info: List[EntityRelationshipInfo]
        Relationships info

    Returns
    -------
    Tuple[List[EntityRelationshipInfo], List[PrecomputedLookupMapping]]
    """
    lookup_steps = []
    lookup_mapping = []
    for primary_entity_id in primary_entity_ids:
        current_lookup_steps = EntityLookupPlanner.generate_lookup_steps(
            available_entity_ids=serving_entity_ids,
            required_entity_ids=[primary_entity_id],
            relationships_info=relationships_info,
        )
        for lookup_step in current_lookup_steps:
            if lookup_step not in lookup_steps:
                lookup_steps.append(lookup_step)
                if lookup_step.entity_id in serving_entity_ids:
                    lookup_mapping.append(
                        PrecomputedLookupMapping(
                            lookup_feature_table_serving_name=entity_id_to_serving_name[
                                lookup_step.entity_id
                            ],
                            source_feature_table_serving_name=entity_id_to_serving_name[
                                primary_entity_id
                            ],
                        )
                    )
    return lookup_steps, lookup_mapping


def get_child_entity_universe_template(
    lookup_steps: List[EntityRelationshipInfo],
    entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep],
    feature_store: FeatureStoreModel,
) -> Expression:
    """
    Get the universe template for a precomputed lookup feature table

    Parameters
    ----------
    lookup_steps: List[EntityRelationshipInfo]
        Relationships to apply for parent entity lookup
    entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep]
        Mapping to obtain EntityLookupStep object given EntityRelationshipInfo id
    feature_store: FeatureStoreModel
        Feature store

    Returns
    -------
    Expression
    """
    entity_lookup_steps = [
        entity_lookup_steps_mapping[lookup_step.id] for lookup_step in lookup_steps
    ]
    if entity_lookup_steps[0].table.type != TableDataType.ITEM_TABLE:
        lookup_graph_result = get_entity_lookup_graph(
            lookup_step=entity_lookup_steps[0],
            feature_store=feature_store,
        )
        initial_universe_expr = cast(
            Select,
            get_combined_universe(
                entity_universe_params=[
                    EntityUniverseParams(
                        graph=lookup_graph_result.graph,
                        node=lookup_graph_result.lookup_node,
                        join_steps=None,
                    )
                ],
                source_info=feature_store.get_source_info(),
            ),
        )
    else:
        initial_universe_expr = get_item_relation_table_lookup_universe(
            entity_lookup_steps[0].table,
            adapter=get_sql_adapter(feature_store.get_source_info()),
        )
    request_table_name = "ENTITY_UNIVERSE"
    request_table_columns = [
        SpecialColumnName.POINT_IN_TIME.value,
        entity_lookup_steps[0].child.serving_name,
    ]
    request_expr = expressions.select(
        expressions.alias_(
            CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER,
            alias=SpecialColumnName.POINT_IN_TIME,
            quoted=True,
        ),
        quoted_identifier(entity_lookup_steps[0].child.serving_name),
    ).from_(initial_universe_expr.subquery())
    parent_entity_lookup_result = construct_request_table_with_parent_entities(
        request_table_name=request_table_name,
        request_table_columns=request_table_columns,
        join_steps=entity_lookup_steps,
        feature_store_details=feature_store.get_feature_store_details(),
    )
    final_universe_expr = construct_cte_sql([
        (request_table_name, request_expr),
        (
            parent_entity_lookup_result.new_request_table_name,
            parent_entity_lookup_result.table_expr,
        ),
    ])
    final_universe_expr = final_universe_expr.select(*[
        quoted_identifier(col) for col in parent_entity_lookup_result.new_request_table_columns
    ]).from_(parent_entity_lookup_result.new_request_table_name)
    return final_universe_expr


def _get_feature_lists_to_relationships_info(
    feature_lists: List[FeatureListModel],
) -> Dict[PydanticObjectId, List[EntityRelationshipInfo]]:
    """
    Get a mapping from feature list id to the feature list's available relationships info

    Parameters
    ----------
    feature_lists: List[FeatureListModel]
        Feature lists to process

    Returns
    -------
    Dict[PydanticObjectId, List[EntityRelationshipInfo]]
    """
    feature_lists_relationships_info = {}
    for feature_list in feature_lists:
        combined_lookup_steps: List[EntityRelationshipInfo] = []
        if feature_list.features_entity_lookup_info:
            for info in feature_list.features_entity_lookup_info:
                for step in info.join_steps:
                    if step not in combined_lookup_steps:
                        combined_lookup_steps.extend(info.join_steps)
        feature_lists_relationships_info[feature_list.id] = (
            feature_list.relationships_info or []
        ) + combined_lookup_steps
    return feature_lists_relationships_info
