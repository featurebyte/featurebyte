"""
DeploymentService class
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from redis import Redis

from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.deployment import DeploymentServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.storage import Storage


class DeploymentService(
    BaseDocumentService[DeploymentModel, DeploymentModel, DeploymentServiceUpdate]
):
    """
    DeploymentService class
    """

    document_class = DeploymentModel
    document_update_class = DeploymentServiceUpdate

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        feature_store_service: FeatureStoreService,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.feature_store_service = feature_store_service

    async def update_store_info(
        self,
        deployment: DeploymentModel,
        feature_list: FeatureListModel,
        features: List[FeatureModel],
        feature_table_map: Dict[str, OfflineStoreFeatureTableModel],
        serving_entity_specs: Optional[List[ColumnSpec]],
    ) -> None:
        """
        Update store info for a feature list

        Parameters
        ----------
        deployment: DeploymentModel
            Deployment model
        feature_list: FeatureListModel
            Feature list model
        features: List[FeatureModel]
            List of features
        feature_table_map: Dict[str, OfflineStoreFeatureTableModel]
            Feature table map
        serving_entity_specs: Optional[List[ColumnSpec]]
            List of serving entity specs
        """
        assert set(feature_list.feature_ids) == set(feature.id for feature in features)

        feature_store = await self.feature_store_service.get_document(
            document_id=features[0].tabular_source.feature_store_id
        )
        deployment.initialize_store_info(
            features=features,
            feature_store=feature_store,
            feature_table_map=feature_table_map,
            serving_entity_specs=serving_entity_specs,
        )
        if deployment.internal_store_info:
            await self.persistent.update_one(
                collection_name=self.collection_name,
                query_filter=await self.construct_get_query_filter(document_id=deployment.id),
                update={"$set": {"store_info": deployment.internal_store_info}},
                user_id=self.user.id,
                disable_audit=self.should_disable_audit,
            )


class AllDeploymentService(DeploymentService):
    """
    AllDeploymentService class
    """

    @property
    def is_catalog_specific(self) -> bool:
        return False
