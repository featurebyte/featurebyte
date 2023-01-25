"""
OnlineEnableService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.enum import SourceType
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.models.feature import FeatureModel, FeatureNamespaceModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.persistent import Persistent
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.base_service import BaseService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService


class OnlineEnableService(BaseService):
    """
    OnlineEnableService class is responsible for maintaining the feature & feature list structure
    of feature online enablement.
    """

    def __init__(
        self, user: Any, persistent: Persistent, session_manager_service: SessionManagerService
    ):
        super().__init__(user, persistent)
        self.feature_service = FeatureService(user=user, persistent=persistent)
        self.session_manager_service = session_manager_service
        self.feature_store_service = FeatureStoreService(user=user, persistent=persistent)
        self.feature_namespace_service = FeatureNamespaceService(user=user, persistent=persistent)
        self.feature_list_service = FeatureListService(user=user, persistent=persistent)

    @classmethod
    def _extract_online_enabled_feature_ids(
        cls, feature: FeatureModel, document: FeatureListModel | FeatureNamespaceModel
    ) -> list[ObjectId]:
        if feature.online_enabled:
            return cls.include_object_id(document.online_enabled_feature_ids, feature.id)
        return cls.exclude_object_id(document.online_enabled_feature_ids, feature.id)

    async def _update_feature_list(
        self,
        feature_list_id: ObjectId,
        feature: FeatureModel,
        return_document: bool = True,
    ) -> Optional[FeatureListModel]:
        """
        Update online_enabled_feature_ids in feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            Target feature list ID
        feature: FeatureModel
            Updated Feature object
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListModel]
        """
        document = await self.feature_list_service.get_document(document_id=feature_list_id)
        return await self.feature_list_service.update_document(
            document_id=feature_list_id,
            data=FeatureListServiceUpdate(
                online_enabled_feature_ids=self._extract_online_enabled_feature_ids(
                    feature=feature, document=document
                ),
            ),
            document=document,
            return_document=return_document,
        )

    async def _update_feature_namespace(
        self,
        feature_namespace_id: ObjectId,
        feature: FeatureModel,
        document: Optional[FeatureNamespaceModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureNamespaceModel]:
        """
        Update online_enabled_feature_ids in feature namespace

        Parameters
        ----------
        feature_namespace_id: ObjectId
            FeatureNamespace ID
        feature: FeatureModel
            Updated Feature object
        document: Optional[FeatureNamespaceModel]
            Document to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureNamespaceModel]
        """
        document = await self.feature_namespace_service.get_document(
            document_id=feature_namespace_id
        )
        return await self.feature_namespace_service.update_document(
            document_id=feature_namespace_id,
            data=FeatureNamespaceServiceUpdate(
                online_enabled_feature_ids=self._extract_online_enabled_feature_ids(
                    feature=feature, document=document
                ),
            ),
            document=document,
            return_document=return_document,
        )

    async def _update_data_warehouse(self, feature: FeatureModel, get_credential: Any) -> None:
        """
        Update data warehouse registry upon changes to online enable status, such as enabling or
        disabling scheduled tile and feature jobs

        Parameters
        ----------
        feature: FeatureModel
            Updated Feature object
        get_credential: Any
            Get credential handler function
        """
        extended_feature_model = ExtendedFeatureModel(**feature.dict())

        online_feature_spec = OnlineFeatureSpec(feature=extended_feature_model)

        if not online_feature_spec.is_online_store_eligible:
            return

        feature_store_model = await self.feature_store_service.get_document(
            document_id=feature.tabular_source.feature_store_id
        )
        session = await self.session_manager_service.get_feature_store_session(
            feature_store_model, get_credential
        )
        assert feature_store_model.type == SourceType.SNOWFLAKE
        feature_manager = FeatureManagerSnowflake(session)

        if feature.online_enabled:
            await feature_manager.online_enable(online_feature_spec)
        else:
            await feature_manager.online_disable(online_feature_spec)

    async def update_feature(
        self,
        feature_id: ObjectId,
        online_enabled: bool,
        get_credential: Any,
    ) -> FeatureModel:
        """
        Update feature online enabled & trigger list of cascading updates

        Parameters
        ----------
        feature_id: ObjectId
            Target feature ID
        online_enabled: bool
            Value to update the feature online_enabled status
        get_credential: Any
            Get credential handler function

        Returns
        -------
        FeatureModel
        """
        document = await self.feature_service.get_document(document_id=feature_id)
        if document.online_enabled != online_enabled:
            async with self.persistent.start_transaction():
                feature = await self.feature_service.update_document(
                    document_id=feature_id,
                    data=FeatureServiceUpdate(online_enabled=online_enabled),
                    document=document,
                    return_document=True,
                )
                assert isinstance(feature, FeatureModel)
                await self._update_feature_namespace(
                    feature_namespace_id=feature.feature_namespace_id,
                    feature=feature,
                    return_document=False,
                )
                for feature_list_id in feature.feature_list_ids:
                    await self._update_feature_list(
                        feature_list_id=feature_list_id,
                        feature=feature,
                        return_document=False,
                    )
                await self._update_data_warehouse(feature=feature, get_credential=get_credential)
                return await self.feature_service.get_document(document_id=feature_id)
        return document
