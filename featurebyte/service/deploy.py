"""
DeployService class
"""

import time
import traceback
from typing import Any, AsyncIterator, Callable, Coroutine, Dict, List, Optional, Sequence, Set

from bson import ObjectId

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.enum import DBVarType
from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.feast.model.registry import FeastRegistryModel
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment import (
    DeploymentModel,
    FeastIntegrationSettings,
    FeastRegistryInfo,
)
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel, ServingEntity
from featurebyte.models.feature_list_namespace import FeatureListStatus
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.models.system_metrics import DeploymentEnablementMetrics
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.schema.deployment import DeploymentServiceUpdate
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListServiceUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.context import ContextService
from featurebyte.service.deployed_tile_table_manager import DeployedTileTableManagerService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_list_status import FeatureListStatusService
from featurebyte.service.feature_offline_store_info import OfflineStoreInfoInitializationService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.offline_store_feature_table_manager import (
    OfflineStoreFeatureTableManagerService,
)
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.service.table import TableService
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
            document_id=feature.id, store_info=store_info.model_dump(by_alias=True)
        )
        return await self.feature_service.get_document(document_id=feature.id)

    async def update_feature_online_enabled_status(
        self,
        feature_id: ObjectId,
        feature_list_id: ObjectId,
        feature_list_to_deploy: bool,
    ) -> FeatureModel:
        """
        Update feature online enabled status

        Parameters
        ----------
        feature_id: ObjectId
            Target feature ID
        feature_list_id: ObjectId
            Target feature list ID
        feature_list_to_deploy: bool
            Whether to deploy the feature list

        Returns
        -------
        FeatureModel
            Updated feature model
        """
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

    async def update_offline_feature_table_deployment_reference(
        self,
        feature_ids: List[PydanticObjectId],
        deployment_id: ObjectId,
        to_enable: bool,
    ) -> None:
        """
        Update offline feature table deployment reference

        Parameters
        ----------
        feature_ids: List[PydanticObjectId]
            Target feature IDs
        deployment_id: ObjectId
            Target deployment ID
        to_enable: bool
            Whether to enable the feature list
        """
        await self.offline_store_feature_table_manager_service.update_table_deployment_reference(
            feature_ids=feature_ids,
            deployment_id=deployment_id,
            to_enable=to_enable,
        )

    async def online_enable_feature(
        self,
        feature: FeatureModel,
        feast_registry: Optional[FeastRegistryModel],
    ) -> FeatureModel:
        """
        Enable feature online

        Parameters
        ----------
        feature: FeatureModel
            Target feature
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

        if not feature.online_enabled:
            # update data warehouse and backward-fill tiles if the feature is not already online
            await self.online_enable_service.update_data_warehouse(
                feature=feature, target_online_enabled=True
            )

        return feature

    async def online_disable_feature(
        self, feature: FeatureModel, feature_list: FeatureListModel
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
        updated_feature = await self.update_feature_online_enabled_status(
            feature_id=feature.id,
            feature_list_id=feature_list.id,
            feature_list_to_deploy=False,
        )

        if not updated_feature.online_enabled:
            # update data warehouse and backward-fill tiles
            await self.online_enable_service.update_data_warehouse(
                feature=feature, target_online_enabled=False
            )

        return updated_feature


class DeploymentServingEntityService:
    """DeploymentServingEntityService is responsible for determining the serving entity ids for a
    deployment
    """

    def __init__(
        self,
        feature_list_service: FeatureListService,
        entity_service: EntityService,
        table_service: TableService,
        use_case_service: UseCaseService,
        context_service: ContextService,
    ):
        self.feature_list_service = feature_list_service
        self.entity_service = entity_service
        self.table_service = table_service
        self.use_case_service = use_case_service
        self.context_service = context_service

    async def get_serving_entity_specs(
        self, serving_entity_ids: List[PydanticObjectId]
    ) -> List[ColumnSpec]:
        """
        Get serving entity name & dtype for the given serving entity ids

        Parameters
        ----------
        serving_entity_ids: List[PydanticObjectId]
            Serving entity ids

        Returns
        -------
        List[ColumnSpec]
        """
        entity_id_to_entity = {}
        entity_id_to_table_id = {}
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": serving_entity_ids}}
        ):
            entity_id_to_entity[entity.id] = entity
            if entity.primary_table_ids:
                entity_id_to_table_id[entity.id] = entity.primary_table_ids[0]
            elif entity.table_ids:
                entity_id_to_table_id[entity.id] = entity.table_ids[0]

        table_id_to_table = {}
        async for table in self.table_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_id_to_table_id.values())}}
        ):
            table_id_to_table[table.id] = table

        entity_specs = []
        for entity_id in serving_entity_ids:
            serving_name = entity_id_to_entity[entity_id].serving_names[0]
            serving_dtype = DBVarType.VARCHAR
            if entity_id in entity_id_to_table_id:
                table = table_id_to_table[entity_id_to_table_id[entity_id]]
                for column in table.columns_info:
                    if column.entity_id == entity_id:
                        serving_dtype = column.dtype
                        break
            entity_specs.append(ColumnSpec(name=serving_name, dtype=serving_dtype))
        return entity_specs

    async def get_deployment_serving_entity_ids(
        self,
        feature_list_id: ObjectId,
        context_id: Optional[ObjectId],
        use_case_id: Optional[ObjectId],
    ) -> ServingEntity:
        """
        Get the serving entity ids for the deployment

        Parameters
        ----------
        feature_list_id: ObjectId
            Id of the feature list
        context_id: Optional[ObjectId]
            Id of the context associated with the deployment
        use_case_id: Optional[ObjectId]
            Id of the use case associated with the deployment

        Returns
        -------
        ServingEntity
        """
        # Get context from use case if not directly available
        feature_list_primary_entity_ids = (
            await self.feature_list_service.get_document_as_dict(
                feature_list_id, projection={"primary_entity_ids": 1}
            )
        )["primary_entity_ids"]
        if context_id is None:
            if use_case_id is None:
                return feature_list_primary_entity_ids  # type: ignore[no-any-return]
            use_case_model = await self.use_case_service.get_document(use_case_id)
            context_id = use_case_model.context_id
        context_model = await self.context_service.get_document(context_id)
        return context_model.primary_entity_ids


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
        deployment_serving_entity_service: DeploymentServingEntityService,
    ):
        self.feature_list_service = feature_list_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.feature_list_status_service = feature_list_status_service
        self.deployment_service = deployment_service
        self.deployment_serving_entity_service = deployment_serving_entity_service

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

    async def deploy_feature_list(self, feature_list: FeatureListModel) -> FeatureListModel:
        """
        Deploy feature list

        Parameters
        ----------
        feature_list: FeatureListModel
            Target feature list

        Returns
        -------
        FeatureListModel
        """
        updated_feature_list = await self.feature_list_service.update_document(
            document_id=feature_list.id,
            data=FeatureListServiceUpdate(
                deployed=True,
            ),
            document=feature_list,
        )

        await self._update_feature_list_namespace(
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            feature_list_id=feature_list.id,
            feature_list_to_deploy=True,
        )
        assert updated_feature_list is not None
        return updated_feature_list

    async def undeploy_feature_list(self, feature_list: FeatureListModel) -> FeatureListModel:
        """
        Undeploy feature list

        Parameters
        ----------
        feature_list: FeatureListModel
            Target feature list

        Returns
        -------
        FeatureListModel
        """
        updated_feature_list = await self.feature_list_service.update_document(
            document_id=feature_list.id,
            data=FeatureListServiceUpdate(
                deployed=False,
            ),
            document=feature_list,
        )

        await self._update_feature_list_namespace(
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            feature_list_id=feature_list.id,
            feature_list_to_deploy=False,
        )
        assert updated_feature_list is not None
        return updated_feature_list


class FeastIntegrationService:
    """
    FeastIntegrationService class is responsible for orchestrating Feast integration related operations.
    """

    def __init__(
        self,
        feast_registry_service: FeastRegistryService,
        feature_list_service: FeatureListService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        offline_store_feature_table_manager_service: OfflineStoreFeatureTableManagerService,
        deployment_service: DeploymentService,
        deployment_serving_entity_service: DeploymentServingEntityService,
    ):
        self.feast_registry_service = feast_registry_service
        self.feature_list_service = feature_list_service
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.offline_store_feature_table_manager_service = (
            offline_store_feature_table_manager_service
        )
        self.deployment_service = deployment_service
        self.deployment_serving_entity_service = deployment_serving_entity_service

    async def get_or_create_feast_registry(self, deployment: DeploymentModel) -> FeastRegistryModel:
        """
        Get existing or create new Feast registry

        Parameters
        ----------
        deployment: DeploymentModel
            Target deployment

        Returns
        -------
        FeastRegistryModel
        """
        feast_registry = await self.feast_registry_service.get_or_create_feast_registry(deployment)
        await self.deployment_service.update_document(
            document_id=deployment.id,
            data=DeploymentServiceUpdate(
                registry_info=FeastRegistryInfo(
                    registry_id=feast_registry.id,
                    registry_path=feast_registry.registry_path,
                )
            ),
            return_document=False,
        )
        return feast_registry

    async def get_feast_registry(self, deployment: DeploymentModel) -> Optional[FeastRegistryModel]:
        """
        Get Feast registry

        Parameters
        ----------
        deployment: DeploymentModel
            Target deployment

        Returns
        -------
        Optional[FeastRegistryModel]
        """
        feature_registry = await self.feast_registry_service.get_feast_registry(
            deployment=deployment
        )
        return feature_registry

    async def handle_online_enabled_features(
        self,
        features: List[FeatureModel],
        feature_list_to_online_enable: FeatureListModel,
        deployment: DeploymentModel,
        update_progress: Callable[[int, Optional[str]], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Handle online enabled features

        Parameters
        ----------
        features: List[FeatureModel]
            List of features to be enabled online
        feature_list_to_online_enable: FeatureListModel
            Target feature list (online enable)
        deployment: DeploymentModel
            Target deployment
        update_progress: Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
            Optional progress update callback
        """
        await self.offline_store_feature_table_manager_service.handle_online_enabled_features(
            features=features,
            feature_list_to_online_enable=feature_list_to_online_enable,
            deployment=deployment,
            update_progress=update_progress,
        )

    async def handle_online_disabled_features(
        self,
        feature_list_to_online_disable: FeatureListModel,
        deployment: DeploymentModel,
        update_progress: Callable[[int, Optional[str]], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Handle online disabled features

        Parameters
        ----------
        feature_list_to_online_disable: FeatureListModel
            Target feature list not to deploy (online disable)
        deployment: DeploymentModel
            Target deployment
        update_progress: Callable[[int, Optional[str]], Coroutine[Any, Any, None]]
            Optional progress update callback
        """
        await self.offline_store_feature_table_manager_service.handle_online_disabled_features(
            feature_list_to_online_disable=feature_list_to_online_disable,
            deployment=deployment,
            update_progress=update_progress,
        )

    async def handle_deployed_feature_list(
        self,
        deployment: DeploymentModel,
        feature_list: FeatureListModel,
        online_enabled_features: List[FeatureModel],
    ) -> None:
        """
        Handle deployed feature list

        Parameters
        ----------
        deployment: DeploymentModel
            Target deployment
        feature_list: FeatureListModel
            Target feature list
        online_enabled_features: List[FeatureModel]
            List of online enabled features
        """
        serving_entity_specs: Optional[List[ColumnSpec]] = None
        serving_names = set()
        if deployment.serving_entity_ids:
            serving_entity_specs = (
                await self.deployment_serving_entity_service.get_serving_entity_specs(
                    serving_entity_ids=deployment.serving_entity_ids
                )
            )
            serving_names = set(
                entity_serving_spec.name for entity_serving_spec in serving_entity_specs
            )

        feature_table_map = {}
        feat_table_service = self.offline_store_feature_table_service
        async for source_table in feat_table_service.list_source_feature_tables_for_deployment(
            deployment_id=deployment.id
        ):
            if serving_names:
                if set(source_table.serving_names).issubset(serving_names):
                    feature_table_map[source_table.name] = source_table
                else:
                    async for (
                        precomputed_table
                    ) in feat_table_service.list_precomputed_lookup_feature_tables_from_source(
                        source_feature_table_id=source_table.id
                    ):
                        if set(precomputed_table.serving_names).issubset(serving_names):
                            feature_table_map[source_table.name] = precomputed_table
            else:
                feature_table_map[source_table.name] = source_table

        await self.feature_list_service.update_document(
            document_id=feature_list.id,
            data=FeatureListServiceUpdate(feast_enabled=True),
        )
        await self.deployment_service.update_store_info(
            deployment=deployment,
            feature_list=feature_list,
            features=online_enabled_features,
            feature_table_map=feature_table_map,
            serving_entity_specs=serving_entity_specs,
        )


class DeployService:
    """DeployService class is responsible for orchestrating deployment."""

    def __init__(
        self,
        deployment_service: DeploymentService,
        deploy_feature_list_management_service: DeployFeatureListManagementService,
        deploy_feature_management_service: DeployFeatureManagementService,
        deployment_serving_entity_service: DeploymentServingEntityService,
        deployed_tile_table_manager_service: DeployedTileTableManagerService,
        feast_integration_service: FeastIntegrationService,
        task_progress_updater: TaskProgressUpdater,
        system_metrics_service: SystemMetricsService,
    ):
        self.deployment_service = deployment_service
        self.feature_list_management_service = deploy_feature_list_management_service
        self.feature_management_service = deploy_feature_management_service
        self.feast_integration_service = feast_integration_service
        self.task_progress_updater = task_progress_updater
        self.deployment_serving_entity_service = deployment_serving_entity_service
        self.deployed_tile_table_manager_service = deployed_tile_table_manager_service
        self.system_metrics_service = system_metrics_service

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
        _tic = time.time()
        await self._update_progress(percent=1, message="Validating deployment enablement...")
        await self._validate_deployment_enablement(feature_list)

        # create or get feast registry
        feast_registry = None
        if FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED:
            feast_registry = await self.feast_integration_service.get_or_create_feast_registry(
                deployment=deployment
            )
            deployment = await self.deployment_service.get_document(document_id=deployment.id)

        feature_models = []
        for feature_id in feature_list.feature_ids:
            feature = await self.feature_management_service.feature_service.get_document(
                document_id=feature_id
            )
            feature_models.append(feature)

        # register deployed tile table models
        await self.deployed_tile_table_manager_service.handle_online_enabled_features(
            features=[feature for feature in feature_models if not feature.online_enabled],
        )

        # enable features online
        feature_update_progress = get_ranged_progress_callback(
            self.task_progress_updater.update_progress, 2, 70
        )
        features_offline_feature_table_to_update = []
        tile_table_seconds = 0.0
        for i, feature in enumerate(feature_models):
            tic = time.time()
            feature = await self.feature_management_service.online_enable_feature(
                feature=feature,
                feast_registry=feast_registry,
            )
            tile_table_seconds += time.time() - tic
            if not feature.online_enabled:
                features_offline_feature_table_to_update.append(feature)

            await feature_update_progress(
                int(i / len(feature_list.feature_ids) * 100),
                f"Enabling feature online ({feature.name}) ...",
            )

        tic = time.time()
        if feast_registry:
            await self.feast_integration_service.handle_online_enabled_features(
                features=features_offline_feature_table_to_update,
                feature_list_to_online_enable=feature_list,
                deployment=deployment,
                update_progress=get_ranged_progress_callback(
                    self.task_progress_updater.update_progress, 72, 95
                ),
            )
        offline_feature_tables_seconds = time.time() - tic

        features = []
        for feature_id in feature_list.feature_ids:
            # update online enabled flag of the feature(s)
            updated_feature = (
                await self.feature_management_service.update_feature_online_enabled_status(
                    feature_id=feature_id,
                    feature_list_id=feature_list.id,
                    feature_list_to_deploy=True,
                )
            )
            features.append(updated_feature)

        # update offline feature table deployment reference
        tic = time.time()
        await self.feature_management_service.update_offline_feature_table_deployment_reference(
            feature_ids=feature_list.feature_ids,
            deployment_id=deployment.id,
            to_enable=True,
        )
        precompute_lookup_feature_tables_seconds = time.time() - tic

        if feast_registry:
            await self._update_progress(
                96, f"Updating deployed feature list ({feature_list.name}) ..."
            )
            await self.feast_integration_service.handle_deployed_feature_list(
                deployment=deployment, feature_list=feature_list, online_enabled_features=features
            )

        # deploy feature list
        await self._update_progress(97, f"Deploying feature list ({feature_list.name}) ...")
        await self.feature_list_management_service.deploy_feature_list(feature_list=feature_list)

        # update deployment status
        await self.deployment_service.update_document(
            document_id=deployment.id,
            data=DeploymentServiceUpdate(enabled=True),
            return_document=False,
        )
        await self._update_progress(100, "Deployment enabled successfully!")

        # update system metrics
        total_seconds = time.time() - _tic
        await self.system_metrics_service.create_metrics(
            DeploymentEnablementMetrics(
                deployment_id=deployment.id,
                tile_tables_seconds=tile_table_seconds,
                offline_feature_tables_seconds=offline_feature_tables_seconds,
                precompute_lookup_feature_tables_seconds=precompute_lookup_feature_tables_seconds,
                total_seconds=total_seconds,
            )
        )

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
            # Get the possibly updated deployment document
            deployment = await self.deployment_service.get_document(
                document_id=deployment.id, populate_remote_attributes=True
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
        if not await self._check_feature_list_used_in_other_deployment(
            feature_list_id=feature_list.id, exclude_deployment_id=deployment.id
        ):
            # disable features online
            feature_update_progress = get_ranged_progress_callback(
                self.task_progress_updater.update_progress, 0, 70
            )
            features = []
            for feature_id in feature_list.feature_ids:
                feature = await self.feature_management_service.feature_service.get_document(
                    document_id=feature_id
                )
                updated_feature = await self.feature_management_service.online_disable_feature(
                    feature=feature,
                    feature_list=feature_list,
                )
                features.append(updated_feature)
                await feature_update_progress(
                    int(len(features) / len(feature_list.feature_ids) * 100),
                    f"Disabling features online ({feature.name}) ...",
                )

            # update offline feature table deployment reference
            await self.feature_management_service.update_offline_feature_table_deployment_reference(
                feature_ids=feature_list.feature_ids,
                deployment_id=deployment.id,
                to_enable=False,
            )

            # undeploy feature list if not used in other deployment
            await self._update_progress(71, f"Undeploying feature list ({feature_list.name}) ...")
            feature_list = await self.feature_list_management_service.undeploy_feature_list(
                feature_list=feature_list,
            )

            # update deployed tile tables
            await self.deployed_tile_table_manager_service.handle_online_disabled_features()

        # check if feast integration is enabled for the catalog
        if FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED:
            feast_registry = await self.feast_integration_service.get_feast_registry(deployment)
            if feast_registry:
                await self.feast_integration_service.handle_online_disabled_features(
                    feature_list_to_online_disable=feature_list,
                    deployment=deployment,
                    update_progress=get_ranged_progress_callback(
                        self.task_progress_updater.update_progress, 72, 99
                    ),
                )

        # update deployment status
        await self.deployment_service.update_document(
            document_id=deployment.id,
            data=DeploymentServiceUpdate(enabled=False),
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
            serving_entity_ids = (
                await self.deployment_serving_entity_service.get_deployment_serving_entity_ids(
                    feature_list_id=feature_list_id,
                    use_case_id=use_case_id,
                    context_id=context_id,
                )
            )
            deployment = await self.deployment_service.create_document(
                data=DeploymentModel(
                    _id=deployment_id or ObjectId(),
                    name=deployment_name or default_deployment_name,
                    feature_list_id=feature_list_id,
                    feature_list_namespace_id=feature_list.feature_list_namespace_id,
                    enabled=False,
                    use_case_id=use_case_id,
                    context_id=context_id,
                    serving_entity_ids=serving_entity_ids,
                )
            )
            if to_enable_deployment:
                await self.enable_deployment(deployment, feature_list)
        except Exception as exc:
            if deployment:
                await self.deployment_service.delete_document(document_id=deployment.id)
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
