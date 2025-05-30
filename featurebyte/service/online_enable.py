"""
OnlineEnableService class
"""

from __future__ import annotations

from typing import List, Optional

from bson import ObjectId

from featurebyte.exception import DataWarehouseConnectionError
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
from featurebyte.persistent import Persistent
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_manager import FeatureManagerService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession


class OnlineEnableService:
    """
    OnlineEnableService class is responsible for maintaining the feature & feature list structure
    of feature online enablement.
    """

    def __init__(
        self,
        persistent: Persistent,
        session_manager_service: SessionManagerService,
        feature_service: FeatureService,
        feature_store_service: FeatureStoreService,
        feature_namespace_service: FeatureNamespaceService,
        feature_list_service: FeatureListService,
        feature_manager_service: FeatureManagerService,
    ):
        self.persistent = persistent
        self.feature_service = feature_service
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service
        self.feature_namespace_service = feature_namespace_service
        self.feature_list_service = feature_list_service
        self.feature_manager_service = feature_manager_service

    async def _update_feature_lists(
        self,
        feature_list_ids: List[PydanticObjectId],
        feature: FeatureModel,
    ) -> None:
        """
        Update online_enabled_feature_ids in feature list

        Parameters
        ----------
        feature_list_ids: List[PydanticObjectId]
            Feature list IDs to be updated
        feature: FeatureModel
            Updated Feature object
        """
        op_key = "$addToSet" if feature.online_enabled else "$pull"
        await self.feature_list_service.update_documents(
            query_filter={"_id": {"$in": feature_list_ids}},
            update={op_key: {"online_enabled_feature_ids": feature.id}},
        )

    async def _update_feature_namespace(
        self,
        feature_namespace_id: ObjectId,
        feature: FeatureModel,
    ) -> None:
        """
        Update online_enabled_feature_ids in feature namespace

        Parameters
        ----------
        feature_namespace_id: ObjectId
            FeatureNamespace ID
        feature: FeatureModel
            Updated Feature object
        """
        op_key = "$addToSet" if feature.online_enabled else "$pull"
        await self.feature_namespace_service.update_documents(
            query_filter={"_id": {"$in": [feature_namespace_id]}},
            update={op_key: {"online_enabled_feature_ids": feature.id}},
        )

    @staticmethod
    async def update_data_warehouse_with_session(
        session: Optional[BaseSession],
        feature_manager_service: FeatureManagerService,
        feature: FeatureModel,
        target_online_enabled: bool,
    ) -> None:
        """
        Update data warehouse registry upon changes to online enable status, such as enabling or
        disabling scheduled tile and feature jobs

        Parameters
        ----------
        session: Optional[BaseSession]
            Session object
        feature_manager_service: FeatureManagerService
            An instance of FeatureManagerService to handle materialization of features and tiles
        feature: FeatureModel
            Feature used to update the data warehouse
        target_online_enabled: bool
            Target online enabled status
        """
        if target_online_enabled:
            assert session is not None
            extended_feature_model = ExtendedFeatureModel(**feature.model_dump(by_alias=True))
            await feature_manager_service.online_enable(session, extended_feature_model)
        else:
            await feature_manager_service.online_disable(session, feature)

    async def update_data_warehouse(
        self, feature: FeatureModel, target_online_enabled: bool
    ) -> None:
        """
        Update data warehouse registry upon changes to online enable status, such as enabling or
        disabling scheduled tile and feature jobs

        Parameters
        ----------
        feature: FeatureModel
            Feature used to update the data warehouse
        target_online_enabled: bool
            Target online enabled status

        Raises
        ------
        DataWarehouseConnectionError
            If data warehouse session cannot be created
        """
        feature_store_model = await self.feature_store_service.get_document(
            document_id=feature.tabular_source.feature_store_id
        )
        try:
            session = await self.session_manager_service.get_feature_store_session(
                feature_store_model
            )
        except DataWarehouseConnectionError as exc:
            if target_online_enabled:
                raise exc
            # This could happen if the data warehouse is defunct and no session can be established.
            # In case of disabling a feature, we still want to be able to proceed and disable the
            # associated periodic tasks.
            session = None

        await self.update_data_warehouse_with_session(
            session=session,
            feature_manager_service=self.feature_manager_service,
            feature=feature,
            target_online_enabled=target_online_enabled,
        )

    async def update_feature(
        self,
        feature_id: ObjectId,
        online_enabled: bool,
    ) -> FeatureModel:
        """
        Update feature online enabled & trigger list of cascading updates

        Parameters
        ----------
        feature_id: ObjectId
            Target feature ID
        online_enabled: bool
            Value to update the feature online_enabled status

        Returns
        -------
        FeatureModel
        """
        document = await self.feature_service.get_document(document_id=feature_id)
        async with self.persistent.start_transaction():
            feature = await self.feature_service.update_document(
                document_id=feature_id,
                data=FeatureServiceUpdate(online_enabled=online_enabled),
                document=document,
                return_document=True,
                populate_remote_attributes=False,
            )
            assert isinstance(feature, FeatureModel)
            await self._update_feature_namespace(
                feature_namespace_id=feature.feature_namespace_id,
                feature=feature,
            )
            await self._update_feature_lists(
                feature_list_ids=feature.feature_list_ids,
                feature=feature,
            )

        return await self.feature_service.get_document(document_id=feature_id)
