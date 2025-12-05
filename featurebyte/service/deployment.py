"""
DeploymentService class
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from redis import Redis

from featurebyte.exception import InvalidComputeOptionValueError
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.deployment_sql import DeploymentSqlModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.deployment import DeploymentServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
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
        catalog_service: CatalogService,
        session_manager_service: SessionManagerService,
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
        self.catalog_service = catalog_service
        self.session_manager_service = session_manager_service

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

    async def update_compute_option_value(
        self, document_id: ObjectId, compute_option_value: str
    ) -> DeploymentModel:
        """
        Update compute option for the deployment

        Parameters
        ----------
        document_id: ObjectId
            ID of the deployment document to update
        compute_option_value: str
            Compute option value to be set

        Returns
        -------
        DeploymentModel
            Updated deployment document

        Raises
        -------
        InvalidComputeOptionValueError
            If the provided compute option value is not valid for the deployment's feature store
        """
        document = await self.get_document(document_id=document_id)

        # Test if the compute option value is valid
        catalog = await self.catalog_service.get_document(document.catalog_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=catalog.default_feature_store_ids[0]
        )
        compute_options = await self.session_manager_service.list_feature_store_compute_options(
            feature_store
        )
        if compute_option_value not in {compute_option.value for compute_option in compute_options}:
            raise InvalidComputeOptionValueError(
                f"Invalid compute option value: {compute_option_value}. "
                f"Valid options are: {[option.value for option in compute_options]}"
            )

        update = DeploymentServiceUpdate(
            compute_option_value=compute_option_value,
        )
        updated_doc = await super().update_document(
            document_id=document_id, data=update, document=document
        )
        assert updated_doc
        return updated_doc

    async def delete_document(
        self,
        document_id: ObjectId,
        exception_detail: Optional[str] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> int:
        """
        Delete deployment document and associated DeploymentSqlModel documents

        Parameters
        ----------
        document_id: ObjectId
            ID of the deployment document to delete
        exception_detail: Optional[str]
            Exception detail
        use_raw_query_filter: bool
            Whether to use raw query filter
        **kwargs: Any
            Additional keyword arguments

        Returns
        -------
        int
            Number of documents deleted
        """
        # Delete associated DeploymentSqlModel documents first
        await self.persistent.delete_many(
            collection_name=DeploymentSqlModel.Settings.collection_name,
            query_filter={"deployment_id": document_id},
            user_id=self.user.id,
        )

        # Delete the deployment document
        return await super().delete_document(
            document_id=document_id,
            exception_detail=exception_detail,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        )


class AllDeploymentService(DeploymentService):
    """
    AllDeploymentService class
    """

    @property
    def is_catalog_specific(self) -> bool:
        return False
