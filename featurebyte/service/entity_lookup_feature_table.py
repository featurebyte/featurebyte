"""
EntityLookupFeatureTableService class
"""
from __future__ import annotations

from typing import Dict, List, Optional

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity_lookup_feature_table import (
    EntityLookupStep,
    get_entity_lookup_feature_tables,
)
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.table import TableService


class EntityLookupFeatureTableService:
    """
    EntityLookupFeatureTable service is responsible for constructing offline store feature tables
    for the purpose of parent entity lookup during serving
    """

    def __init__(
        self,
        entity_service: EntityService,
        table_service: TableService,
        feature_service: FeatureService,
        feature_store_service: FeatureStoreService,
    ):
        self.entity_service = entity_service
        self.table_service = table_service
        self.feature_service = feature_service
        self.feature_store_service = feature_store_service

    async def get_entity_lookup_steps(
        self, entity_relationships_info: List[EntityRelationshipInfo]
    ) -> Dict[PydanticObjectId, EntityLookupStep]:
        """
        Get mapping from relationship info id to EntityLookupStep. EntityLookupStep is an augmented
        EntityRelationshipInfo with id fields converted to models (e.g. EntityModel, TableModel)

        Parameters
        ---------
        entity_relationships_info: List[EntityRelationshipInfo]
            List of EntityRelationshipInfo objects

        Returns
        -------
        Dict[PydanticObjectId, EntityLookupStep]
        """
        out = {}
        for relationship_info in entity_relationships_info:
            if relationship_info.id in out:
                continue
            child_entity = await self.entity_service.get_document(relationship_info.entity_id)
            parent_entity = await self.entity_service.get_document(
                relationship_info.related_entity_id
            )
            relation_table = await self.table_service.get_document(
                relationship_info.relation_table_id
            )
            lookup_step = EntityLookupStep(
                id=relationship_info.id,
                child_entity=child_entity,
                parent_entity=parent_entity,
                relation_table=relation_table,
            )
            out[relationship_info.id] = lookup_step
        return out

    async def get_entity_lookup_steps_mapping(
        self, feature_lists: List[FeatureListModel]
    ) -> Dict[PydanticObjectId, EntityLookupStep]:
        """
        Helper function to get mapping from relationship info id to EntityLookupStep across
        all the feature lists

        Parameters
        ----------
        feature_lists: List[FeatureListModel]
            Feature lists

        Returns
        -------
        Dict[PydanticObjectId, EntityLookupStep]
        """
        all_relationships_info = set()
        for feature_list in feature_lists:
            if feature_list.relationships_info is not None:
                for info in feature_list.relationships_info:
                    all_relationships_info.add(info)
        entity_lookup_steps_mapping = await self.get_entity_lookup_steps(
            list(all_relationships_info)
        )
        return entity_lookup_steps_mapping

    async def get_entity_lookup_feature_tables(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
        feature_lists: List[FeatureListModel],
    ) -> Optional[List[OfflineStoreFeatureTableModel]]:
        """
        Get list of internal offline store feature tables for parent entity lookup purpose

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            Feature table model
        feature_lists: List[FeatureListModel]
            Currently online enabled feature lists

        Returns
        -------
        Optional[List[OfflineStoreFeatureTableModel]]
        """

        feature = await self.feature_service.get_document(feature_table_model.feature_ids[0])
        feature_store = await self.feature_store_service.get_document(
            feature.tabular_source.feature_store_id
        )
        entity_lookup_steps_mapping = await self.get_entity_lookup_steps_mapping(feature_lists)
        return get_entity_lookup_feature_tables(
            feature_table_primary_entity_ids=feature_table_model.primary_entity_ids,
            feature_lists=feature_lists,
            feature_store=feature_store,
            entity_lookup_steps_mapping=entity_lookup_steps_mapping,
        )
