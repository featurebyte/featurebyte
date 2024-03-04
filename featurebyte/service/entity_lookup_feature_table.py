"""
EntityLookupFeatureTableService class
"""
from __future__ import annotations

from typing import Dict, List, Optional

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity_lookup_feature_table import get_entity_lookup_feature_tables
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.parent_serving import EntityLookupStep
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.parent_serving import ParentEntityLookupService


class EntityLookupFeatureTableService:
    """
    EntityLookupFeatureTable service is responsible for constructing offline store feature tables
    for the purpose of parent entity lookup during serving
    """

    def __init__(
        self,
        parent_entity_lookup_service: ParentEntityLookupService,
        feature_service: FeatureService,
        feature_store_service: FeatureStoreService,
    ):
        self.parent_entity_lookup_service = parent_entity_lookup_service
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
        for entity_lookup_step in await self.parent_entity_lookup_service.get_entity_lookup_steps(
            entity_relationships_info
        ):
            out[entity_lookup_step.id] = entity_lookup_step
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
            if feature_list.features_entity_lookup_info is not None:
                for entity_lookup_info in feature_list.features_entity_lookup_info:
                    all_relationships_info.update(entity_lookup_info.join_steps)
        entity_lookup_steps_mapping = await self.get_entity_lookup_steps(
            list(all_relationships_info)
        )
        return entity_lookup_steps_mapping

    async def get_entity_lookup_feature_tables(
        self,
        feature_lists: List[FeatureListModel],
        feature_store_model: FeatureStoreModel,
    ) -> Optional[List[OfflineStoreFeatureTableModel]]:
        """
        Get list of internal offline store feature tables for parent entity lookup purpose

        Parameters
        ----------
        feature_lists: List[FeatureListModel]
            Currently online enabled feature lists
        feature_store_model: FeatureStoreModel
            Feature store document

        Returns
        -------
        Optional[List[OfflineStoreFeatureTableModel]]
        """
        entity_lookup_steps_mapping = await self.get_entity_lookup_steps_mapping(feature_lists)
        return get_entity_lookup_feature_tables(
            feature_lists=feature_lists,
            feature_store=feature_store_model,
            entity_lookup_steps_mapping=entity_lookup_steps_mapping,
        )
