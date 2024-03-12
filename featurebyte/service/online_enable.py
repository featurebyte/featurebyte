"""
OnlineEnableService class
"""
from __future__ import annotations

from typing import Optional

from bson.objectid import ObjectId

from featurebyte.exception import CredentialsError
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_namespace import FeatureNamespaceModel
from featurebyte.models.online_store_spec import OnlineFeatureSpec
from featurebyte.persistent import Persistent
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_manager import FeatureManagerService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.mixin import OpsServiceMixin
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession


class OnlineEnableService(OpsServiceMixin):
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
        # pylint: disable=too-many-arguments
        self.persistent = persistent
        self.feature_service = feature_service
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service
        self.feature_namespace_service = feature_namespace_service
        self.feature_list_service = feature_list_service
        self.feature_manager_service = feature_manager_service

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

    @staticmethod
    async def update_data_warehouse_with_session(
        session: Optional[BaseSession],
        feature_manager_service: FeatureManagerService,
        feature: FeatureModel,
        is_recreating_schema: bool = False,
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
            Updated Feature object
        is_recreating_schema: bool
            Whether we are recreating the working schema from scratch. Only set as True when called
            by WorkingSchemaService.
        """
        extended_feature_model = ExtendedFeatureModel(**feature.dict(by_alias=True))
        online_feature_spec = OnlineFeatureSpec(feature=extended_feature_model)

        if feature.online_enabled:
            assert session is not None
            await feature_manager_service.online_enable(
                session, online_feature_spec, is_recreating_schema=is_recreating_schema
            )
        else:
            await feature_manager_service.online_disable(session, online_feature_spec)

    async def update_data_warehouse(
        self, updated_feature: FeatureModel, online_enabled_before_update: bool
    ) -> None:
        """
        Update data warehouse registry upon changes to online enable status, such as enabling or
        disabling scheduled tile and feature jobs

        Parameters
        ----------
        updated_feature: FeatureModel
            Updated Feature
        online_enabled_before_update: bool
            Online enabled status

        Raises
        ------
        CredentialsError
            If data warehouse session cannot be created
        """
        if updated_feature.online_enabled == online_enabled_before_update:
            # updated_feature has the same online_enabled status as the original one
            # no need to update
            return

        feature_store_model = await self.feature_store_service.get_document(
            document_id=updated_feature.tabular_source.feature_store_id
        )
        try:
            session = await self.session_manager_service.get_feature_store_session(
                feature_store_model
            )
        except CredentialsError as exc:
            if updated_feature.online_enabled:
                raise exc
            # This could happen if the data warehouse is defunct and no session can be established.
            # In case of disabling a feature, we still want to be able to proceed and disable the
            # associated periodic tasks.
            session = None

        await self.update_data_warehouse_with_session(
            session=session,
            feature_manager_service=self.feature_manager_service,
            feature=updated_feature,
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

            return await self.feature_service.get_document(document_id=feature_id)
