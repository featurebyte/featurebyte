"""
DeployService class
"""
from __future__ import annotations

from typing import Any, Callable, Optional

from bson.objectid import ObjectId

from featurebyte.exception import DocumentCreationError, DocumentError, DocumentUpdateError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureListStatus,
)
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.persistent import Persistent
from featurebyte.schema.deployment import DeploymentUpdate
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_list_status import FeatureListStatusService
from featurebyte.service.mixin import OpsServiceMixin
from featurebyte.service.online_enable import OnlineEnableService


class DeployService(OpsServiceMixin):
    """
    DeployService class is responsible for maintaining the feature & feature list structure
    of feature list deployment.
    """

    def __init__(
        self,
        persistent: Persistent,
        feature_service: FeatureService,
        online_enable_service: OnlineEnableService,
        feature_list_status_service: FeatureListStatusService,
        deployment_service: DeploymentService,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_list_service: FeatureListService,
    ):
        self.persistent = persistent
        self.feature_service = feature_service
        self.online_enable_service = online_enable_service
        self.feature_list_service = feature_list_service
        self.feature_list_status_service = feature_list_status_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.deployment_service = deployment_service

    @classmethod
    def _extract_deployed_feature_list_ids(
        cls, feature_list: FeatureListModel, document: FeatureListNamespaceModel | FeatureModel
    ) -> list[ObjectId]:
        if feature_list.deployed:
            return cls.include_object_id(document.deployed_feature_list_ids, feature_list.id)
        return cls.exclude_object_id(document.deployed_feature_list_ids, feature_list.id)

    async def _update_feature(
        self,
        feature_id: ObjectId,
        feature_list: FeatureListModel,
        return_document: bool = True,
    ) -> Optional[FeatureModel]:
        """
        Update deployed_feature_list_ids in feature. For each update, trigger online service to update
        online enabled status at feature level.

        Parameters
        ----------
        feature_id: ObjectId
            Target Feature ID
        feature_list: FeatureListModel
            Updated FeatureList object (deployed status)
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureModel]:
        """
        document = await self.feature_service.get_document(document_id=feature_id)
        deployed_feature_list_ids = self._extract_deployed_feature_list_ids(
            feature_list=feature_list, document=document
        )
        online_enabled = len(deployed_feature_list_ids) > 0
        if document.online_enabled != online_enabled:
            document = await self.online_enable_service.update_feature(
                feature_id=feature_id,
                online_enabled=online_enabled,
            )

        return await self.feature_service.update_document(
            document_id=feature_id,
            data=FeatureServiceUpdate(
                deployed_feature_list_ids=deployed_feature_list_ids,
            ),
            document=document,
            return_document=return_document,
        )

    async def _update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        feature_list: FeatureListModel,
        return_document: bool = True,
    ) -> Optional[FeatureListNamespaceModel]:
        """
        Update deployed_feature_list_ids in feature list namespace

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            Target FeatureListNamespace ID
        feature_list: FeatureListModel
            Updated FeatureList object (deployed status)
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListNamespaceModel]
        """
        document = await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )

        # update deployed feature list ids
        deployed_feature_list_ids = self._extract_deployed_feature_list_ids(
            feature_list=feature_list,
            document=document,
        )
        await self.feature_list_namespace_service.update_document(
            document_id=feature_list_namespace_id,
            data=FeatureListNamespaceServiceUpdate(
                deployed_feature_list_ids=deployed_feature_list_ids
            ),
            document=document,
            return_document=False,
        )

        # update feature list status
        feature_list_status = None
        if deployed_feature_list_ids:
            # if there is any deployed feature list, set status to deployed
            feature_list_status = FeatureListStatus.DEPLOYED
        elif document.status == FeatureListStatus.DEPLOYED and not deployed_feature_list_ids:
            # if the deployed status has 0 deployed feature list, set status to public draft
            feature_list_status = FeatureListStatus.PUBLIC_DRAFT

        if feature_list_status:
            await self.feature_list_status_service.update_feature_list_namespace_status(
                feature_list_namespace_id=feature_list_namespace_id,
                target_feature_list_status=feature_list_status,
            )

        if return_document:
            return await self.feature_list_namespace_service.get_document(
                document_id=feature_list_namespace_id
            )
        return None

    async def _validate_deployed_operation(
        self, feature_list: FeatureListModel, deployed: bool
    ) -> None:
        if deployed:
            # if deploying, check feature list status is not deprecated
            feature_list_namespace = await self.feature_list_namespace_service.get_document(
                document_id=feature_list.feature_list_namespace_id
            )
            if feature_list_namespace.status == FeatureListStatus.DEPRECATED:
                raise DocumentUpdateError("Deprecated feature list cannot be deployed.")

            # if enabling deployment, check is there any feature with readiness not equal to production ready
            query_filter = {"_id": {"$in": feature_list.feature_ids}}
            async for feature in self.feature_service.list_documents_as_dict_iterator(
                query_filter=query_filter
            ):
                if FeatureReadiness(feature["readiness"]) != FeatureReadiness.PRODUCTION_READY:
                    raise DocumentUpdateError(
                        "Only FeatureList object of all production ready features can be deployed."
                    )

    async def _revert_changes(
        self,
        feature_list_id: ObjectId,
        deployed: bool,
        feature_online_enabled_map: dict[PydanticObjectId, bool],
        get_credential: Any,
    ) -> None:
        # revert feature list deploy status
        feature_list = await self.feature_list_service.update_document(
            document_id=feature_list_id,
            data=FeatureListServiceUpdate(deployed=deployed),
            return_document=True,
        )

        # revert all online enabled status back before raising exception
        for feature_id, online_enabled in feature_online_enabled_map.items():
            async with self.persistent.start_transaction():
                document = await self.online_enable_service.update_feature(
                    feature_id=feature_id,
                    online_enabled=online_enabled,
                )
            # move update warehouse and backfill tiles to outside of transaction
            await self.online_enable_service.update_data_warehouse(
                updated_feature=document,
                online_enabled_before_update=online_enabled,
                get_credential=get_credential,
            )

        # update feature list namespace again
        assert isinstance(feature_list, FeatureListModel)
        await self._update_feature_list_namespace(
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            feature_list=feature_list,
            return_document=False,
        )

    async def _update_feature_list(
        self,
        feature_list_id: ObjectId,
        get_credential: Any,
        update_progress: Optional[Callable[[int, str], None]] = None,
        return_document: bool = True,
    ) -> Optional[FeatureListModel]:
        """
        Update deployed status in feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            Target feature list ID
        get_credential: Any
            Get credential handler function
        update_progress: Callable[[int, str], None]
            Update progress handler function
        return_document: bool
            Whether to return updated document

        Returns
        -------
        Optional[FeatureListModel]

        Raises
        ------
        Exception
            When there is an unexpected error during feature online_enabled status update
        """
        if update_progress:
            update_progress(0, "Start updating feature list")

        list_deployment_results = await self.deployment_service.list_documents_as_dict(
            query_filter={"feature_list_id": feature_list_id, "enabled": True}
        )
        target_deployed = list_deployment_results["total"] > 0
        document = await self.feature_list_service.get_document(document_id=feature_list_id)

        if document.deployed != target_deployed:
            await self._validate_deployed_operation(document, target_deployed)

            # variables to store feature list's & features' initial state
            original_deployed = document.deployed
            feature_online_enabled_map = {}

            try:
                feature_list = await self.feature_list_service.update_document(
                    document_id=feature_list_id,
                    data=FeatureListServiceUpdate(deployed=target_deployed),
                    document=document,
                    return_document=True,
                )
                assert isinstance(feature_list, FeatureListModel)

                if update_progress:
                    update_progress(20, "Update features")

                # make each feature online enabled first
                for ind, feature_id in enumerate(document.feature_ids):
                    async with self.persistent.start_transaction():
                        feature = await self.feature_service.get_document(document_id=feature_id)
                        feature_online_enabled_map[feature.id] = feature.online_enabled
                        updated_feature = await self._update_feature(
                            feature_id=feature_id,
                            feature_list=feature_list,
                            return_document=True,
                        )

                    if updated_feature:
                        # move update warehouse and backfill tiles to outside of transaction
                        await self.online_enable_service.update_data_warehouse(
                            updated_feature=updated_feature,
                            online_enabled_before_update=feature.online_enabled,
                            get_credential=get_credential,
                        )

                    if update_progress:
                        percent = 20 + int(60 / len(document.feature_ids) * (ind + 1))
                        update_progress(percent, f"Updated {feature.name}")

                if update_progress:
                    update_progress(80, "Update feature list")

                async with self.persistent.start_transaction():
                    await self._update_feature_list_namespace(
                        feature_list_namespace_id=feature_list.feature_list_namespace_id,
                        feature_list=feature_list,
                        return_document=False,
                    )
                    if return_document:
                        return await self.feature_list_service.get_document(
                            document_id=feature_list_id
                        )

                if update_progress:
                    update_progress(100, "Updated feature list")

            except Exception as exc:
                try:
                    await self._revert_changes(
                        feature_list_id=feature_list_id,
                        deployed=original_deployed,
                        feature_online_enabled_map=feature_online_enabled_map,
                        get_credential=get_credential,
                    )
                except Exception as revert_exc:
                    raise revert_exc from exc
                raise exc
        return self.conditional_return(document=document, condition=return_document)

    async def create_deployment(
        self,
        feature_list_id: ObjectId,
        deployment_id: ObjectId,
        deployment_name: Optional[str],
        to_enable_deployment: bool,
        get_credential: Any,
        update_progress: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        """
        Create deployment for the given feature list feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            Feature list ID
        deployment_id: ObjectId
            Deployment ID
        deployment_name: Optional[str]
            Deployment name
        to_enable_deployment: bool
            Whether to enable deployment
        get_credential: Any
            Get credential handler function
        update_progress: Callable[[int, str], None]
            Update progress handler function

        Raises
        ------
        DocumentCreationError, Exception
            When there is an unexpected error during deployment creation
        """
        feature_list = await self.feature_list_service.get_document(document_id=feature_list_id)
        default_deployment_name = (
            f"Deployment with {feature_list.name}_{feature_list.version.to_str()}"
        )
        try:
            await self.deployment_service.create_document(
                data=DeploymentModel(
                    _id=deployment_id,
                    name=deployment_name or default_deployment_name,
                    feature_list_id=feature_list_id,
                    enabled=to_enable_deployment,
                )
            )
            await self._update_feature_list(
                feature_list_id=feature_list_id,
                get_credential=get_credential,
                update_progress=update_progress,
            )
        except Exception as exc:
            try:
                await self.deployment_service.delete_document(document_id=deployment_id)
            except Exception as delete_exc:
                raise DocumentCreationError("Failed to create deployment") from delete_exc
            if isinstance(exc, DocumentError):
                raise exc
            raise DocumentCreationError("Failed to create deployment") from exc

    async def update_deployment(
        self,
        deployment_id: ObjectId,
        enabled: bool,
        get_credential: Any,
        update_progress: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        """
        Update deployment enabled status

        Parameters
        ----------
        deployment_id: ObjectId
            Deployment ID
        enabled: bool
            Enabled status
        get_credential: Any
            Get credential handler function
        update_progress: Callable[[int, str], None]
            Update progress handler function

        Raises
        ------
        DocumentUpdateError, Exception
            When there is an unexpected error during deployment update
        """
        deployment = await self.deployment_service.get_document(document_id=deployment_id)
        original_enabled = deployment.enabled
        if original_enabled != enabled:
            try:
                await self.deployment_service.update_document(
                    document_id=deployment_id,
                    data=DeploymentUpdate(enabled=enabled),
                )
                await self._update_feature_list(
                    feature_list_id=deployment.feature_list_id,
                    get_credential=get_credential,
                    update_progress=update_progress,
                )
            except Exception as exc:
                try:
                    await self.deployment_service.update_document(
                        document_id=deployment_id,
                        data=DeploymentUpdate(enabled=original_enabled),
                    )
                except Exception as revert_exc:
                    raise DocumentUpdateError("Failed to update deployment") from revert_exc
                if isinstance(exc, DocumentError):
                    raise exc
                raise DocumentUpdateError("Failed to update deployment") from exc
