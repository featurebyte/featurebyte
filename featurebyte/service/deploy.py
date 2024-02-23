"""
DeployService class
"""
from typing import Any, AsyncIterator, Callable, Coroutine, Dict, List, Optional, Sequence, Set

import traceback

from bson import ObjectId

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.feast.model.registry import FeastRegistryModel
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.logging import get_logger
from featurebyte.models.deployment import DeploymentModel, FeastIntegrationSettings
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel, ServingEntity
from featurebyte.models.feature_list_namespace import FeatureListStatus
from featurebyte.models.feature_namespace import FeatureReadiness
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
from featurebyte.service.offline_store_feature_table_manager import (
    OfflineStoreFeatureTableManagerService,
)
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.use_case import UseCaseService
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class DeployFeatureManagementService:
    """DeployFeatureManagementService class is responsible for feature management during deployment."""

    def __init__(
        self,
        feature_service: FeatureService,
        online_enable_service: OnlineEnableService,
        offline_store_info_initialization_service: OfflineStoreInfoInitializationService,
        offline_store_feature_table_manager_service: OfflineStoreFeatureTableManagerService,
    ):
        self.feature_service = feature_service
        self.online_enable_service = online_enable_service
        self.offline_store_info_initialization_service = offline_store_info_initialization_service
        self.offline_store_feature_table_manager_service = (
            offline_store_feature_table_manager_service
        )

    async def validate_online_feature_enablement(self, feature_ids: Sequence[ObjectId]) -> None:
        """
        Validate whether features can be enabled online

        Parameters
        ----------
        feature_ids: Sequence[ObjectId]
            Target feature IDs

        Raises
        ------
        DocumentUpdateError
            If any of the features are not production ready
        """
        query_filter = {"_id": {"$in": feature_ids}}
        async for feature in self.feature_service.list_documents_as_dict_iterator(
            query_filter=query_filter, projection={"readiness": 1}
        ):
            if FeatureReadiness(feature["readiness"]) != FeatureReadiness.PRODUCTION_READY:
                raise DocumentUpdateError(
                    "Only FeatureList object of all production ready features can be deployed."
                )

    async def _update_feature_offline_store_info(
        self, feature: FeatureModel, table_name_prefix: str
    ) -> FeatureModel:
        store_info = (
            await self.offline_store_info_initialization_service.initialize_offline_store_info(
                feature=feature, table_name_prefix=table_name_prefix
            )
        )
        await self.feature_service.update_offline_store_info(
            document_id=feature.id, store_info=store_info.dict(by_alias=True)
        )
        return await self.feature_service.get_document(document_id=feature.id)

    async def _update_feature_online_enabled_status(
        self, feature_id: ObjectId, feature_list_id: ObjectId, feature_list_to_deploy: bool
    ) -> FeatureModel:
        document = await self.feature_service.get_document(document_id=feature_id)
        deployed_feature_list_ids: Set[ObjectId] = set(document.deployed_feature_list_ids)
        if feature_list_to_deploy:
            deployed_feature_list_ids.add(feature_list_id)
        else:
            deployed_feature_list_ids.discard(feature_list_id)

        online_enabled = len(deployed_feature_list_ids) > 0
        document = await self.online_enable_service.update_feature(
            feature_id=feature_id,
            online_enabled=online_enabled,
        )

        await self.feature_service.update_document(
            document_id=feature_id,
            data=FeatureServiceUpdate(deployed_feature_list_ids=sorted(deployed_feature_list_ids)),
            document=document,
            return_document=False,
        )
        return await self.feature_service.get_document(document_id=feature_id)

    async def online_enable_feature(
        self,
        feature: FeatureModel,
        feature_list: FeatureListModel,
        feast_registry: Optional[FeastRegistryModel],
    ) -> FeatureModel:
        """
        Enable feature online

        Parameters
        ----------
        feature: FeatureModel
            Target feature
        feature_list: FeatureListModel
            Target feature list
        feast_registry: Optional[FeastRegistryModel]
            Feast registry of current catalog

        Returns
        -------
        FeatureModel
        """
        # update feature mongo record
        if feast_registry:
            feature = await self._update_feature_offline_store_info(
                feature=feature,
                table_name_prefix=feast_registry.offline_table_name_prefix,
            )

        updated_feature = await self._update_feature_online_enabled_status(
            feature_id=feature.id, feature_list_id=feature_list.id, feature_list_to_deploy=True
        )

        # update data warehouse and backward-fill tiles
        await self.online_enable_service.update_data_warehouse(
            updated_feature=updated_feature,
            online_enabled_before_update=feature.online_enabled,
        )
        return updated_feature

    async def online_disable_feature(
        self,
        feature: FeatureModel,
        feature_list: FeatureListModel,
    ) -> FeatureModel:
        """
        Disable feature online

        Parameters
        ----------
        feature: FeatureModel
            Target feature
        feature_list: FeatureListModel
            Target feature list

        Returns
        -------
        FeatureModel
        """
        # update feature mongo record
        updated_feature = await self._update_feature_online_enabled_status(
            feature_id=feature.id, feature_list_id=feature_list.id, feature_list_to_deploy=False
        )

        # update data warehouse and backward-fill tiles
        await self.online_enable_service.update_data_warehouse(
            updated_feature=updated_feature,
            online_enabled_before_update=feature.online_enabled,
        )
        return updated_feature


class DeployFeatureListManagementService:
    """
    DeployFeatureListManagementService class is responsible for feature list management during deployment.
    """

    def __init__(
        self,
        feature_list_service: FeatureListService,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_list_status_service: FeatureListStatusService,
        deployment_service: DeploymentService,
        use_case_service: UseCaseService,
        context_service: ContextService,
    ):
        self.feature_list_service = feature_list_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_list_status_service = feature_list_status_service
        self.deployment_service = deployment_service
        self.use_case_service = use_case_service
        self.context_service = context_service

    async def get_feature_list(self, feature_list_id: ObjectId) -> FeatureListModel:
        """
        Get feature list

        Parameters
        ----------
        feature_list_id: ObjectId
            Target feature list ID

        Returns
        -------
        FeatureListModel
        """
        return await self.feature_list_service.get_document(document_id=feature_list_id)

    async def validate_feature_list_status(self, feature_list_namespace_id: ObjectId) -> None:
        """
        Validate whether feature list can be deployed

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            Target feature list ID

        Raises
        ------
        DocumentUpdateError
            If feature list status is deprecated
        """
        # if deploying, check feature list status is not deprecated
        feature_list_namespace = await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )
        if feature_list_namespace.status == FeatureListStatus.DEPRECATED:
            raise DocumentUpdateError("Deprecated feature list cannot be deployed.")

    async def _update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        feature_list_id: ObjectId,
        feature_list_to_deploy: bool,
    ) -> None:
        document = await self.feature_list_namespace_service.get_document(
            document_id=feature_list_namespace_id
        )
        deployed_feature_list_ids: Set[ObjectId] = set(document.deployed_feature_list_ids)
        if feature_list_to_deploy:
            deployed_feature_list_ids.add(feature_list_id)
        else:
            deployed_feature_list_ids.discard(feature_list_id)

        await self.feature_list_namespace_service.update_document(
            document_id=feature_list_namespace_id,
            data=FeatureListNamespaceServiceUpdate(
                deployed_feature_list_ids=sorted(deployed_feature_list_ids)
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

    async def _iterate_enabled_deployments_as_dict(
        self, feature_list_id: ObjectId, deployment_id: ObjectId, to_enable_deployment: bool
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Iterate deployments that are enabled, including the one that is going to be enabled in the
        current request

        Parameters
        ----------
        feature_list_id: ObjectId
            Feature list id
        deployment_id: ObjectId
            Deployment id
        to_enable_deployment: bool
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
        if to_enable_deployment:
            yield await self.deployment_service.get_document_as_dict(deployment_id)

    async def _get_enabled_serving_entity_ids(
        self,
        feature_list_model: FeatureListModel,
        deployment_id: ObjectId,
        to_enable_deployment: bool,
    ) -> List[ServingEntity]:
        # List of all possible serving entity ids for the feature list
        supported_serving_entity_ids_set = {
            tuple(serving_entity_ids)
            for serving_entity_ids in feature_list_model.supported_serving_entity_ids
        }

        # List of enabled serving entity ids is a subset of supported_serving_entity_ids and is
        # determined by existing deployments
        enabled_serving_entity_ids = []

        context_id_to_model = {}
        use_case_id_to_model = {}
        async for doc in self._iterate_enabled_deployments_as_dict(
            feature_list_model.id, deployment_id, to_enable_deployment
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
            current_enabled = set()
            for serving_entity_ids in supported_serving_entity_ids_set:
                combined_entity_ids = list(serving_entity_ids) + list(
                    context_model.primary_entity_ids
                )
                reduced_entity_ids = serving_entity_enumeration.reduce_entity_ids(
                    combined_entity_ids
                )
                # Include if serving_entity_ids is the same or a parent of use case primary entity
                if sorted(reduced_entity_ids) == sorted(context_model.primary_entity_ids):
                    enabled_serving_entity_ids.append(serving_entity_ids)
                    current_enabled.add(serving_entity_ids)

            # Don't need to test again serving entity ids that were already enabled
            supported_serving_entity_ids_set.difference_update(current_enabled)

        return [list(serving_entity_ids) for serving_entity_ids in enabled_serving_entity_ids]

    async def deploy_feature_list(
        self, feature_list: FeatureListModel, deployment_id: ObjectId
    ) -> None:
        """
        Deploy feature list

        Parameters
        ----------
        feature_list: FeatureListModel
            Target feature list
        deployment_id: ObjectId
            Deployment ID
        """
        enabled_serving_entity_ids = await self._get_enabled_serving_entity_ids(
            feature_list_model=feature_list,
            deployment_id=deployment_id,
            to_enable_deployment=True,
        )
        await self.feature_list_service.update_document(
            document_id=feature_list.id,
            data=FeatureListServiceUpdate(
                deployed=True,
                enabled_serving_entity_ids=enabled_serving_entity_ids,
            ),
            document=feature_list,
            return_document=False,
        )

        if feature_list.deployed:
            return

        await self._update_feature_list_namespace(
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            feature_list_id=feature_list.id,
            feature_list_to_deploy=True,
        )

    async def undeploy_feature_list(
        self, feature_list: FeatureListModel, deployment_id: ObjectId
    ) -> None:
        """
        Undeploy feature list

        Parameters
        ----------
        feature_list: FeatureListModel
            Target feature list
        deployment_id: ObjectId
            Deployment ID
        """
        enabled_serving_entity_ids = await self._get_enabled_serving_entity_ids(
            feature_list_model=feature_list,
            deployment_id=deployment_id,
            to_enable_deployment=False,
        )
        await self.feature_list_service.update_document(
            document_id=feature_list.id,
            data=FeatureListServiceUpdate(
                deployed=False,
                enabled_serving_entity_ids=enabled_serving_entity_ids,
            ),
            document=feature_list,
            return_document=False,
        )
        await self._update_feature_list_namespace(
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            feature_list_id=feature_list.id,
            feature_list_to_deploy=False,
        )


class FeastIntegrationService:
    """
    FeastIntegrationService class is responsible for orchestrating Feast integration related operations.
    """

    def __init__(
        self,
        feast_registry_service: FeastRegistryService,
        feature_list_service: FeatureListService,
        offline_store_feature_table_manager_service: OfflineStoreFeatureTableManagerService,
    ):
        self.feast_registry_service = feast_registry_service
        self.feature_list_service = feature_list_service
        self.offline_store_feature_table_manager_service = (
            offline_store_feature_table_manager_service
        )

    async def get_or_create_feast_registry(self, catalog_id: ObjectId) -> FeastRegistryModel:
        """
        Get existing or create new Feast registry

        Parameters
        ----------
        catalog_id: ObjectId
            Target catalog ID

        Returns
        -------
        FeastRegistryModel
        """
        feast_registry = await self.feast_registry_service.get_or_create_feast_registry(
            catalog_id=catalog_id,
            feature_store_id=None,
        )
        return feast_registry

    async def get_feast_registry(self) -> Optional[FeastRegistryModel]:
        """
        Get Feast registry

        Returns
        -------
        Optional[FeastRegistryModel]
        """
        feature_registry = await self.feast_registry_service.get_feast_registry_for_catalog()
        return feature_registry

    async def handle_online_enabled_features(
        self,
        features: List[FeatureModel],
        update_progress: Callable[[int, Optional[str]], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Handle online enabled features

        Parameters
        ----------
        features: List[FeatureModel]
            List of features to be enabled online
        update_progress: Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
            Optional progress update callback
        """
        await self.offline_store_feature_table_manager_service.handle_online_enabled_features(
            features=features, update_progress=update_progress
        )

    async def handle_online_disabled_features(
        self,
        features: List[FeatureModel],
        update_progress: Callable[[int, Optional[str]], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Handle online disabled features

        Parameters
        ----------
        features: List[FeatureModel]
            List of features to be disabled online
        update_progress: Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
            Optional progress update callback
        """
        await self.offline_store_feature_table_manager_service.handle_online_disabled_features(
            features=features, update_progress=update_progress
        )

    async def handle_deployed_feature_list(
        self,
        feature_list: FeatureListModel,
        online_enabled_features: List[FeatureModel],
    ) -> None:
        """
        Handle deployed feature list

        Parameters
        ----------
        feature_list: FeatureListModel
            Target feature list
        online_enabled_features: List[FeatureModel]
            List of online enabled features
        """
        await self.feature_list_service.update_store_info(
            document_id=feature_list.id, features=online_enabled_features
        )


class DeployService:
    """DeployService class is responsible for orchestrating deployment."""

    def __init__(
        self,
        deployment_service: DeploymentService,
        deploy_feature_list_management_service: DeployFeatureListManagementService,
        deploy_feature_management_service: DeployFeatureManagementService,
        feast_integration_service: FeastIntegrationService,
        task_progress_updater: TaskProgressUpdater,
    ):
        self.deployment_service = deployment_service
        self.feature_list_management_service = deploy_feature_list_management_service
        self.feature_management_service = deploy_feature_management_service
        self.feast_integration_service = feast_integration_service
        self.task_progress_updater = task_progress_updater

    async def _update_progress(self, percent: int, message: Optional[str] = None) -> None:
        await self.task_progress_updater.update_progress(percent=percent, message=message)

    async def _validate_deployment_enablement(self, feature_list: FeatureListModel) -> None:
        await self.feature_list_management_service.validate_feature_list_status(
            feature_list_namespace_id=feature_list.feature_list_namespace_id
        )
        await self.feature_management_service.validate_online_feature_enablement(
            feature_ids=feature_list.feature_ids
        )

    async def _enable_deployment(
        self,
        deployment: DeploymentModel,
        feature_list: FeatureListModel,
    ) -> None:
        await self._update_progress(percent=1, message="Validating deployment enablement...")
        await self._validate_deployment_enablement(feature_list)

        # create or get feast registry
        feast_registry = None
        if FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED:
            feast_registry = await self.feast_integration_service.get_or_create_feast_registry(
                catalog_id=deployment.catalog_id
            )

        # enable features online
        feature_update_progress = get_ranged_progress_callback(
            self.task_progress_updater.update_progress, 2, 70
        )
        features = []
        features_offline_feature_table_to_update = []
        for feature_id in feature_list.feature_ids:
            feature = await self.feature_management_service.feature_service.get_document(
                document_id=feature_id
            )
            updated_feature = await self.feature_management_service.online_enable_feature(
                feature=feature,
                feature_list=feature_list,
                feast_registry=feast_registry,
            )
            if updated_feature.online_enabled != feature.online_enabled:
                features_offline_feature_table_to_update.append(updated_feature)
            features.append(updated_feature)
            await feature_update_progress(
                int(len(features) / len(feature_list.feature_ids) * 100),
                f"Enabling feature online ({feature.name}) ...",
            )

        # deploy feature list
        await self._update_progress(71, f"Deploying feature list ({feature_list.name}) ...")
        await self.feature_list_management_service.deploy_feature_list(
            feature_list=feature_list, deployment_id=deployment.id
        )

        if feast_registry:
            await self.feast_integration_service.handle_online_enabled_features(
                features=features_offline_feature_table_to_update,
                update_progress=get_ranged_progress_callback(
                    self.task_progress_updater.update_progress, 72, 98
                ),
            )
            await self._update_progress(
                99, f"Updating deployed feature list ({feature_list.name}) ..."
            )
            await self.feast_integration_service.handle_deployed_feature_list(
                feature_list=feature_list, online_enabled_features=features
            )

        # update deployment status
        await self.deployment_service.update_document(
            document_id=deployment.id,
            data=DeploymentUpdate(enabled=True),
            return_document=False,
        )
        await self._update_progress(100, "Deployment enabled successfully!")

    async def _check_feature_list_used_in_other_deployment(
        self, feature_list_id: ObjectId, exclude_deployment_id: ObjectId
    ) -> bool:
        # check whether other deployment are using this feature list
        list_deployment_results = await self.deployment_service.list_documents_as_dict(
            query_filter={
                "feature_list_id": feature_list_id,
                "enabled": True,
                "_id": {"$ne": exclude_deployment_id},
            }
        )
        return bool(list_deployment_results["total"] > 0)

    async def enable_deployment(
        self,
        deployment: DeploymentModel,
        feature_list: FeatureListModel,
    ) -> None:
        """
        Enable deployment

        Parameters
        ----------
        deployment: DeploymentModel
            Target deployment
        feature_list: FeatureListModel
            Feature list of the deployment

        Raises
        ------
        DocumentUpdateError
            When there is an unexpected error during deployment enablement
        """
        try:
            await self._enable_deployment(deployment, feature_list)
        except Exception as exc:
            exc_traceback = traceback.format_exc()
            logger.error(
                f"Failed to enable deployment {deployment.id}: {exc}. {exc_traceback}",
                exc_info=True,
            )
            await self.disable_deployment(deployment, feature_list)
            raise DocumentUpdateError("Failed to enable deployment") from exc

    async def disable_deployment(
        self,
        deployment: DeploymentModel,
        feature_list: FeatureListModel,
    ) -> None:
        """
        Disable deployment

        Parameters
        ----------
        deployment: DeploymentModel
            Target deployment
        feature_list: FeatureListModel
            Feature list of the deployment
        """
        # enable features online
        if not await self._check_feature_list_used_in_other_deployment(
            feature_list_id=feature_list.id, exclude_deployment_id=deployment.id
        ):
            # disable features online
            feature_update_progress = get_ranged_progress_callback(
                self.task_progress_updater.update_progress, 0, 70
            )
            features = []
            features_offline_feature_table_to_update = []
            for feature_id in feature_list.feature_ids:
                feature = await self.feature_management_service.feature_service.get_document(
                    document_id=feature_id
                )
                updated_feature = await self.feature_management_service.online_disable_feature(
                    feature=feature,
                    feature_list=feature_list,
                )
                if feature.online_enabled != updated_feature.online_enabled:
                    features_offline_feature_table_to_update.append(updated_feature)
                features.append(updated_feature)
                await feature_update_progress(
                    int(len(features) / len(feature_list.feature_ids) * 100),
                    f"Disabling features online ({feature.name}) ...",
                )

            # undeploy feature list if not used in other deployment
            await self._update_progress(71, f"Undeploying feature list ({feature_list.name}) ...")
            await self.feature_list_management_service.undeploy_feature_list(
                feature_list=feature_list, deployment_id=deployment.id
            )

            # check if feast integration is enabled for the catalog
            if FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED:
                feast_registry = await self.feast_integration_service.get_feast_registry()
                if feast_registry and features_offline_feature_table_to_update:
                    await self.feast_integration_service.handle_online_disabled_features(
                        features=features_offline_feature_table_to_update,
                        update_progress=get_ranged_progress_callback(
                            self.task_progress_updater.update_progress, 72, 99
                        ),
                    )

        # update deployment status
        await self.deployment_service.update_document(
            document_id=deployment.id,
            data=DeploymentUpdate(enabled=False),
            return_document=False,
        )
        await self._update_progress(100, "Deployment disabled successfully!")

    async def create_deployment(
        self,
        feature_list_id: ObjectId,
        deployment_id: ObjectId,
        deployment_name: Optional[str],
        to_enable_deployment: bool,
        use_case_id: Optional[ObjectId] = None,
        context_id: Optional[ObjectId] = None,
    ) -> None:
        """
        Create deployment for the given feature list

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
        use_case_id: ObjectId
            Use Case ID
        context_id: ObjectId
            Context ID

        Raises
        ------
        DocumentCreationError
            When there is an unexpected error during deployment creation
        """
        feature_list = await self.feature_list_management_service.get_feature_list(
            feature_list_id=feature_list_id
        )
        default_deployment_name = (
            f"Deployment with {feature_list.name}_{feature_list.version.to_str()}"
        )
        deployment = None
        try:
            deployment = await self.deployment_service.create_document(
                data=DeploymentModel(
                    _id=deployment_id,
                    name=deployment_name or default_deployment_name,
                    feature_list_id=feature_list_id,
                    enabled=False,
                    use_case_id=use_case_id,
                    context_id=context_id,
                )
            )
            if to_enable_deployment:
                await self.enable_deployment(deployment, feature_list)
        except Exception as exc:
            if deployment:
                await self.deployment_service.delete_document(document_id=deployment_id)
            raise DocumentCreationError("Failed to create deployment") from exc

    async def update_deployment(
        self,
        deployment_id: ObjectId,
        to_enable_deployment: bool,
    ) -> None:
        """
        Update deployment enabled status

        Parameters
        ----------
        deployment_id: ObjectId
            Deployment ID
        to_enable_deployment: bool
            Enabled status
        """
        deployment = await self.deployment_service.get_document(document_id=deployment_id)
        feature_list = await self.feature_list_management_service.get_feature_list(
            feature_list_id=deployment.feature_list_id
        )
        if to_enable_deployment:
            await self.enable_deployment(deployment, feature_list)
        else:
            await self.disable_deployment(deployment, feature_list)
