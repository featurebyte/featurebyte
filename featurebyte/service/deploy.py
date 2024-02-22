"""
DeployService class
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Callable, Coroutine, List, Optional

from bson.objectid import ObjectId

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.exception import DocumentCreationError, DocumentError, DocumentUpdateError
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment import DeploymentModel, FeastIntegrationSettings
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel, ServingEntity
from featurebyte.models.feature_list_namespace import FeatureListNamespaceModel, FeatureListStatus
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.persistent import Persistent
from featurebyte.schema.deployment import DeploymentUpdate
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.context import ContextService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity_relationship_extractor import ServingEntityEnumeration
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_list_status import FeatureListStatusService
from featurebyte.service.feature_offline_store_info import OfflineStoreInfoInitializationService
from featurebyte.service.mixin import OpsServiceMixin
from featurebyte.service.offline_store_feature_table_manager import (
    OfflineStoreFeatureTableManagerService,
)
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.use_case import UseCaseService


class DeployService(OpsServiceMixin):
    """
    DeployService class is responsible for maintaining the feature & feature list structure
    of feature list deployment.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(  # pylint: disable=too-many-arguments
        self,
        persistent: Persistent,
        feature_service: FeatureService,
        online_enable_service: OnlineEnableService,
        feature_list_status_service: FeatureListStatusService,
        deployment_service: DeploymentService,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_list_service: FeatureListService,
        offline_store_feature_table_manager_service: OfflineStoreFeatureTableManagerService,
        offline_store_info_initialization_service: OfflineStoreInfoInitializationService,
        feast_registry_service: FeastRegistryService,
        use_case_service: UseCaseService,
        context_service: ContextService,
    ):
        self.persistent = persistent
        self.feature_service = feature_service
        self.online_enable_service = online_enable_service
        self.feature_list_service = feature_list_service
        self.feature_list_status_service = feature_list_status_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.deployment_service = deployment_service
        self.offline_store_feature_table_manager_service = (
            offline_store_feature_table_manager_service
        )
        self.offline_store_info_initialization_service = offline_store_info_initialization_service
        self.feast_registry_service = feast_registry_service
        self.use_case_service = use_case_service
        self.context_service = context_service

    @classmethod
    def _extract_deployed_feature_list_ids(
        cls, feature_list: FeatureListModel, document: FeatureListNamespaceModel | FeatureModel
    ) -> list[ObjectId]:
        if feature_list.deployed:
            return cls.include_object_id(document.deployed_feature_list_ids, feature_list.id)
        return cls.exclude_object_id(document.deployed_feature_list_ids, feature_list.id)

    async def _update_offline_store_info(
        self, feature: FeatureModel, table_name_prefix: str, target_deployed: bool
    ) -> FeatureModel:
        if FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED and target_deployed:
            store_info = (
                await self.offline_store_info_initialization_service.initialize_offline_store_info(
                    feature=feature, table_name_prefix=table_name_prefix
                )
            )
            await self.feature_service.update_offline_store_info(
                document_id=feature.id, store_info=store_info.dict(by_alias=True)
            )
            return await self.feature_service.get_document(document_id=feature.id)
        return feature

    async def _update_feature(
        self,
        feature_id: ObjectId,
        feature_list: FeatureListModel,
    ) -> FeatureModel:
        """
        Update deployed_feature_list_ids in feature. For each update, trigger online service to update
        online enabled status at feature level.

        Parameters
        ----------
        feature_id: ObjectId
            Target Feature ID
        feature_list: FeatureListModel
            Updated FeatureList object (deployed status)

        Returns
        -------
        FeatureModel
        """
        document = await self.feature_service.get_document(document_id=feature_id)
        deployed_feature_list_ids = self._extract_deployed_feature_list_ids(
            feature_list=feature_list, document=document
        )
        online_enabled = len(deployed_feature_list_ids) > 0
        document = await self.online_enable_service.update_feature(
            feature_id=feature_id,
            online_enabled=online_enabled,
        )

        await self.feature_service.update_document(
            document_id=feature_id,
            data=FeatureServiceUpdate(
                deployed_feature_list_ids=deployed_feature_list_ids,
            ),
            document=document,
            return_document=False,
        )
        return await self.feature_service.get_document(document_id=feature_id)

    async def _update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        feature_list: FeatureListModel,
    ) -> None:
        """
        Update deployed_feature_list_ids in feature list namespace

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            Target FeatureListNamespace ID
        feature_list: FeatureListModel
            Updated FeatureList object (deployed status)
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
                query_filter=query_filter, projection={"readiness": 1}
            ):
                if FeatureReadiness(feature["readiness"]) != FeatureReadiness.PRODUCTION_READY:
                    raise DocumentUpdateError(
                        "Only FeatureList object of all production ready features can be deployed."
                    )

    async def _iterate_enabled_deployments_as_dict(
        self, feature_list_id: ObjectId, deployment_id: ObjectId, target_deployed: bool
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Iterate deployments that are enabled, including the one that is going to be enabled in the
        current request

        Parameters
        ----------
        feature_list_id: ObjectId
            Feature list id
        deployment_id: ObjectId
            Deployment id
        target_deployed: bool
            Intended final deployed state of the feature list

        Yields
        ------
        AsyncIterator[Dict[str, Any]]
            List query output
        """
        async for doc in self.deployment_service.list_documents_as_dict_iterator(
            query_filter={"feature_list_id": feature_list_id, "enabled": True}
        ):
            yield doc
        if target_deployed:
            yield await self.deployment_service.get_document_as_dict(deployment_id)

    async def _get_enabled_serving_entity_ids(
        self, feature_list_model: FeatureListModel, deployment_id: ObjectId, target_deployed: bool
    ) -> List[ServingEntity]:
        enabled_serving_entity_ids = []
        context_id_to_model = {}
        use_case_id_to_model = {}

        async for doc in self._iterate_enabled_deployments_as_dict(
            feature_list_model.id, deployment_id, target_deployed
        ):
            context_id = doc["context_id"]

            # Get context from use case if not directly available
            if context_id is None:
                use_case_id = doc["use_case_id"]
                if use_case_id is None:
                    continue
                use_case_model = await self.use_case_service.get_document(use_case_id)
                use_case_id_to_model[use_case_id] = use_case_model
                context_id = use_case_model.context_id

            context_model = await self.context_service.get_document(context_id)
            context_id_to_model[context_id] = context_model

            serving_entity_enumeration = ServingEntityEnumeration.create(
                feature_list_model.relationships_info or []
            )
            for serving_entity_ids in feature_list_model.supported_serving_entity_ids:
                combined_entity_ids = list(serving_entity_ids) + list(
                    context_model.primary_entity_ids
                )
                reduced_entity_ids = serving_entity_enumeration.reduce_entity_ids(
                    combined_entity_ids
                )
                if sorted(reduced_entity_ids) == sorted(context_model.primary_entity_ids):
                    enabled_serving_entity_ids.append(serving_entity_ids)

        return enabled_serving_entity_ids

    async def _revert_changes(
        self,
        feature_list_id: ObjectId,
        deployed: bool,
        feature_online_enabled_map: dict[PydanticObjectId, bool],
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
                document_before_update = await self.feature_service.get_document(
                    document_id=feature_id,
                )
                document = await self.online_enable_service.update_feature(
                    feature_id=feature_id,
                    online_enabled=online_enabled,
                )
            await self.online_enable_service.update_data_warehouse(
                updated_feature=document,
                online_enabled_before_update=document_before_update.online_enabled,
            )

        # update feature list namespace again
        assert isinstance(feature_list, FeatureListModel)
        await self._update_feature_list_namespace(
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            feature_list=feature_list,
        )

    async def _get_feature_list_target_deployed(
        self, feature_list_id: ObjectId, deployment_id: ObjectId, to_enable_deployment: bool
    ) -> bool:
        target_deployed = to_enable_deployment
        if not to_enable_deployment:
            # check whether other deployment are using this feature list
            list_deployment_results = await self.deployment_service.list_documents_as_dict(
                query_filter={
                    "feature_list_id": feature_list_id,
                    "enabled": True,
                    "_id": {"$ne": deployment_id},
                }
            )
            target_deployed = list_deployment_results["total"] > 0
        return target_deployed

    async def _update_feature_list(
        self,
        feature_list_id: ObjectId,
        deployment_id: ObjectId,
        to_enable_deployment: bool,
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> FeatureListModel:
        """
        Update deployed status in feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            Target feature list ID
        deployment_id: ObjectId
            Deployment ID
        to_enable_deployment: bool
            Flag to indicate whether the call is from enable/disable deployment
        update_progress: Callable[[int, str | None], Coroutine[Any, Any, None]]
            Update progress handler function

        Returns
        -------
        Optional[FeatureListModel]

        Raises
        ------
        Exception
            When there is an unexpected error during feature online_enabled status update
        """
        # pylint: disable=too-many-locals
        if update_progress:
            await update_progress(0, "Start updating feature list")

        target_deployed = await self._get_feature_list_target_deployed(
            feature_list_id=feature_list_id,
            deployment_id=deployment_id,
            to_enable_deployment=to_enable_deployment,
        )

        document = await self.feature_list_service.get_document(document_id=feature_list_id)
        enabled_serving_entity_ids = await self._get_enabled_serving_entity_ids(
            feature_list_model=document,
            deployment_id=deployment_id,
            target_deployed=target_deployed,
        )

        if document.deployed != target_deployed:
            await self._validate_deployed_operation(document, target_deployed)

            # variables to store feature list's & features' initial state
            original_deployed = document.deployed
            feature_online_enabled_map = {}

            try:
                feature_list = await self.feature_list_service.update_document(
                    document_id=feature_list_id,
                    data=FeatureListServiceUpdate(
                        deployed=target_deployed,
                        enabled_serving_entity_ids=enabled_serving_entity_ids,
                    ),
                    document=document,
                    return_document=True,
                )
                assert isinstance(feature_list, FeatureListModel)

                feast_registry = await self.feast_registry_service.get_or_create_feast_registry(
                    catalog_id=document.catalog_id,
                    feature_store_id=None,
                )

                if update_progress:
                    await update_progress(5, "Update features")

                # make each feature online enabled first
                feature_models = []
                all_feature_models = []
                is_online_enabling = True
                feature_update_progress = None
                if update_progress:
                    feature_update_progress = get_ranged_progress_callback(update_progress, 5, 80)

                for ind, feature_id in enumerate(document.feature_ids):
                    feature = await self.feature_service.get_document(document_id=feature_id)
                    feature = await self._update_offline_store_info(
                        feature, feast_registry.offline_table_name_prefix, target_deployed
                    )
                    all_feature_models.append(feature)
                    async with self.persistent.start_transaction():
                        feature_online_enabled_map[feature.id] = feature.online_enabled
                        updated_feature = await self._update_feature(
                            feature_id=feature_id,
                            feature_list=feature_list,
                        )
                        if updated_feature.online_enabled != feature.online_enabled:
                            feature_models.append(feature)
                            is_online_enabling = updated_feature.online_enabled

                    # move update warehouse and backfill tiles to outside of transaction
                    await self.online_enable_service.update_data_warehouse(
                        updated_feature=updated_feature,
                        online_enabled_before_update=feature.online_enabled,
                    )

                    if feature_update_progress:
                        await feature_update_progress(
                            int((ind + 1) / len(document.feature_ids) * 100),
                            f"Updated {feature.name}",
                        )

                if feature_update_progress:
                    await feature_update_progress(100, "Updated features")

                if (
                    target_deployed
                    and FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED
                ):
                    await self.feature_list_service.update_store_info(
                        document_id=feature_list_id,
                        features=all_feature_models,
                    )

                async with self.persistent.start_transaction():
                    await self._update_feature_list_namespace(
                        feature_list_namespace_id=feature_list.feature_list_namespace_id,
                        feature_list=feature_list,
                    )

                await self._update_offline_store_feature_tables(
                    feature_models,
                    is_online_enabling,
                    update_progress=get_ranged_progress_callback(update_progress, 80, 100)
                    if update_progress
                    else None,
                )

                if update_progress:
                    await update_progress(100, "Completed updating feature list & features")

            except Exception as exc:
                try:
                    await self._revert_changes(
                        feature_list_id=feature_list_id,
                        deployed=original_deployed,
                        feature_online_enabled_map=feature_online_enabled_map,
                    )
                except Exception as revert_exc:
                    raise revert_exc from exc
                raise exc

        return await self.feature_list_service.get_document(document_id=feature_list_id)

    async def create_deployment(
        self,
        feature_list_id: ObjectId,
        deployment_id: ObjectId,
        deployment_name: Optional[str],
        to_enable_deployment: bool,
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
        use_case_id: Optional[ObjectId] = None,
        context_id: Optional[ObjectId] = None,
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
        update_progress: Callable[[int, str | None], Coroutine[Any, Any, None]]
            Update progress handler function
        use_case_id: ObjectId
            Use Case ID
        context_id: ObjectId
            Context ID

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
                    enabled=False,
                    use_case_id=use_case_id,
                    context_id=context_id,
                )
            )
            await self._update_feature_list(
                feature_list_id=feature_list_id,
                deployment_id=deployment_id,
                to_enable_deployment=to_enable_deployment,
                update_progress=update_progress,
            )
            if to_enable_deployment:
                await self.deployment_service.update_document(
                    document_id=deployment_id, data=DeploymentUpdate(enabled=to_enable_deployment)
                )
        except Exception as exc:
            try:
                await self.deployment_service.delete_document(document_id=deployment_id)
            except Exception as delete_exc:
                raise DocumentCreationError("Failed to create deployment") from delete_exc
            if isinstance(exc, DocumentError):
                raise exc
            raise DocumentCreationError("Failed to create deployment") from exc

    async def _update_offline_store_feature_tables(
        self,
        feature_models: List[FeatureModel],
        is_online_enabling: bool,
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        if FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED:
            if is_online_enabling:
                await self.offline_store_feature_table_manager_service.handle_online_enabled_features(
                    features=feature_models, update_progress=update_progress
                )
            else:
                await self.offline_store_feature_table_manager_service.handle_online_disabled_features(
                    features=feature_models, update_progress=update_progress
                )

    async def update_deployment(
        self,
        deployment_id: ObjectId,
        to_enable_deployment: bool,
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        """
        Update deployment enabled status

        Parameters
        ----------
        deployment_id: ObjectId
            Deployment ID
        to_enable_deployment: bool
            Enabled status
        update_progress: Callable[[int, str | None], Coroutine[Any, Any, None]]
            Update progress handler function

        Raises
        ------
        DocumentUpdateError, Exception
            When there is an unexpected error during deployment update
        """
        deployment = await self.deployment_service.get_document(document_id=deployment_id)
        original_enabled = deployment.enabled
        if original_enabled != to_enable_deployment:
            try:
                await self._update_feature_list(
                    feature_list_id=deployment.feature_list_id,
                    deployment_id=deployment_id,
                    to_enable_deployment=to_enable_deployment,
                    update_progress=update_progress,
                )
                await self.deployment_service.update_document(
                    document_id=deployment_id,
                    data=DeploymentUpdate(enabled=to_enable_deployment),
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
