"""
Models related to serving parent features
"""
from __future__ import annotations

from typing import List

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.query_graph.node.schema import FeatureStoreDetails


class EntityLookupInfo(FeatureByteBaseModel):
    """
    Information about an entity such as keys, serving names, that are relevant in the context of an
    EntityLookupStep

    entity_id: PydanticObjectId
        Entity id
    key: str
        Column name in the table that is tagged as the entity
    serving_name: str
        Serving name of the entity. Alternatively, this can be thought of as the input or output
        column name of the parent entity lookup operation.
    """

    entity_id: PydanticObjectId
    key: str
    serving_name: str


class EntityLookupStep(FeatureByteBaseModel):
    """
    EntityLookupStep contains all information required to perform a join between two related
    entities for the purpose of serving parent features

    id: PydanticObjectId
        Identifier of the EntityRelationshipInfo corresponding to this lookup step
    table: ProxyTableModel
        The table encoding the relationship between the two entities
    parent: EntityLookupInfo
        Information about the parent entity
    child: EntityLookupInfo
        Information about the child entity
    """

    id: PydanticObjectId
    table: ProxyTableModel
    parent: EntityLookupInfo
    child: EntityLookupInfo


class ParentServingPreparation(FeatureByteBaseModel):
    """
    Operations required to serve parent features from children entities

    join_steps: List[JoinStep]
        List of JoinSteps identified based on the provided entities
    feature_store_details: FeatureStoreDetails
        Feature store information
    """

    join_steps: List[EntityLookupStep]
    feature_store_details: FeatureStoreDetails
