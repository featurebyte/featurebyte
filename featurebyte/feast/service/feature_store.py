"""
This module contains classes for constructing feast repository config
"""

# pylint: disable=no-name-in-module
from typing import Any, List, Optional

import tempfile

from asyncache import cached
from bson import ObjectId
from cachetools import LRUCache
from feast import Entity as FeastEntity
from feast import FeatureStore as BaseFeastFeatureStore
from feast import FeatureView as FeastFeatureView
from feast import RepoConfig
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import FeastConfigBaseModel, RegistryConfig
from feast.repo_contents import RepoContents

from featurebyte.feast.model.feature_store import FeatureStoreDetailsWithFeastConfiguration
from featurebyte.feast.model.online_store import get_feast_online_store_details
from featurebyte.feast.model.registry import FeastRegistryModel
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.feast.utils.registry_construction import (
    DEFAULT_REGISTRY_PROJECT_NAME,
    FeastRegistryBuilder,
)
from featurebyte.logging import get_logger
from featurebyte.models.catalog import CatalogModel
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.online_store import OnlineStoreModel
from featurebyte.service.catalog import CatalogService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.online_store import OnlineStoreService
from featurebyte.utils.credential import MongoBackedCredentialProvider

logger = get_logger(__name__)


class FeastFeatureStore(BaseFeastFeatureStore):
    """
    Feast feature store that also tracks the corresponding online store id in featurebyte
    """

    def __init__(self, online_store_id: Optional[ObjectId], *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.online_store_id = online_store_id


feast_feature_store_cache: Any = LRUCache(maxsize=64)


def create_feast_feature_store(
    project_name: Optional[str],
    registry_proto: RegistryProto,
    feature_store: FeatureStoreModel,
    offline_store_credentials: Any,
    online_store_config: Optional[FeastConfigBaseModel],
    effective_online_store_id: Optional[ObjectId],
) -> FeastFeatureStore:
    """
    Create feast repo config

    Parameters
    ----------
    project_name: Optional[str]
        Project name
    registry_proto: RegistryProto
        Feast registry proto
    feature_store: FeatureStoreModel
        Feature store model
    offline_store_credentials: Any
        Offline store credentials
    online_store_config: Optional[FeastConfigBaseModel]
        Online store config
    effective_online_store_id: Optional[ObjectId]
        Effective online store id

    Returns
    -------
    FeastFeatureStore
        Feast feature store
    """
    project_name = project_name or DEFAULT_REGISTRY_PROJECT_NAME
    with tempfile.NamedTemporaryFile() as temp_file:
        # Use temp file to pass the registry proto to the feature store. Once the
        # feature store is created, the temp file is no longer needed.
        feast_registry_path = temp_file.name
        with open(feast_registry_path, mode="wb", buffering=0) as file_handle:
            file_handle.write(registry_proto.SerializeToString())

        # note that registry_store_type is pointing to FeatureByteRegistryStore, which
        # loads the registry proto from the file & stores it in memory without writing it.
        registry_config = RegistryConfig(
            registry_type="file",
            registry_store_type="featurebyte.feast.registry_store.FeatureByteRegistryStore",
            path=feast_registry_path,
            cache_ttl_seconds=0,
        )
        feature_store_details = FeatureStoreDetailsWithFeastConfiguration(
            **feature_store.get_feature_store_details().dict()
        )
        database_credential = None
        storage_credential = None
        if offline_store_credentials:
            database_credential = offline_store_credentials.database_credential
            storage_credential = offline_store_credentials.storage_credential
        repo_config = RepoConfig(
            project=project_name,
            provider="local",
            registry=registry_config,
            offline_store=feature_store_details.details.get_offline_store_config(
                database_credential=database_credential,
                storage_credential=storage_credential,
            ),
            online_store=online_store_config,
            entity_key_serialization_version=2,
        )
        feast_feature_store = FeastFeatureStore(
            config=repo_config,
            online_store_id=effective_online_store_id,
        )
        return feast_feature_store


def _get_feast_feature_store_cache_key(
    feast_registry: FeastRegistryModel,
    feature_store: FeatureStoreModel,
    credentials: Any,
    online_store_config: Optional[FeastConfigBaseModel],
    effective_online_store_id: ObjectId,
) -> Any:
    _ = credentials, online_store_config
    cache_key = (
        feature_store.id,
        feature_store.updated_at,
        feast_registry.id,
        feast_registry.updated_at,
        feast_registry.registry_path,
        effective_online_store_id,
    )
    return cache_key


class FeastFeatureStoreService:
    """Feast feature store service"""

    def __init__(
        self,
        user: Any,
        feast_registry_service: FeastRegistryService,
        mongo_backed_credential_provider: MongoBackedCredentialProvider,
        feature_store_service: FeatureStoreService,
        catalog_service: CatalogService,
        online_store_service: OnlineStoreService,
        deployment_service: DeploymentService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
    ):
        self.user = user
        self.feast_registry_service = feast_registry_service
        self.credential_provider = mongo_backed_credential_provider
        self.feature_store_service = feature_store_service
        self.catalog_service = catalog_service
        self.online_store_service = online_store_service
        self.deployment_service = deployment_service
        self.offline_store_feature_table_service = offline_store_feature_table_service

    @staticmethod
    @cached(cache=feast_feature_store_cache, key=_get_feast_feature_store_cache_key)
    async def _get_feast_feature_store(
        feast_registry: FeastRegistryModel,
        feature_store: FeatureStoreModel,
        credentials: Any,
        online_store_config: Optional[FeastConfigBaseModel],
        effective_online_store_id: ObjectId,
    ) -> FeastFeatureStore:
        feast_feature_store = create_feast_feature_store(
            project_name=feast_registry.name,
            registry_proto=feast_registry.registry_proto(),
            feature_store=feature_store,
            offline_store_credentials=credentials,
            online_store_config=online_store_config,
            effective_online_store_id=effective_online_store_id,
        )
        return feast_feature_store

    @staticmethod
    async def _get_feast_online_store_config(
        online_store_doc: Optional[OnlineStoreModel],
    ) -> Optional[FeastConfigBaseModel]:
        if online_store_doc is None:
            return None

        online_store_details = online_store_doc.details
        online_store = get_feast_online_store_details(online_store_details)
        return online_store.to_feast_online_store_config()

    async def _get_effective_online_store_doc(
        self, online_store_id: Optional[ObjectId], catalog: CatalogModel
    ) -> Optional[OnlineStoreModel]:
        effective_online_store_id = (
            online_store_id if online_store_id is not None else catalog.online_store_id
        )
        online_store_doc = None
        if effective_online_store_id:
            online_store_doc = await self.online_store_service.get_document(
                effective_online_store_id
            )
        return online_store_doc

    async def get_feast_feature_store(
        self,
        feast_registry: FeastRegistryModel,
        online_store_id: Optional[ObjectId] = None,
    ) -> FeastFeatureStore:
        """
        Create feast repo config

        Parameters
        ----------
        feast_registry: FeastRegistryModel
            Feast registry model
        online_store_id: Optional[ObjectId]
            Online store id to use if specified instead of using the one in the catalog. This is used
            when the returned feature store is about to be used to update a different online store
            (e.g. for initialization purpose)

        Returns
        -------
        FeastFeatureStore
            Feast feature store
        """
        logger.info("Creating feast feature store for registry %s", str(feast_registry.id))
        feature_store = await self.feature_store_service.get_document(
            document_id=feast_registry.feature_store_id
        )
        credentials = await self.credential_provider.get_credential(
            user_id=self.user.id, feature_store_name=feature_store.name
        )

        # Get online store feast config
        catalog = await self.catalog_service.get_document(feast_registry.catalog_id)
        online_store_doc = await self._get_effective_online_store_doc(
            online_store_id=online_store_id, catalog=catalog
        )
        online_store_config = await self._get_feast_online_store_config(
            online_store_doc=online_store_doc
        )
        logger.info("Feast feature store cache size: %d", len(feast_feature_store_cache))
        feast_feature_store = await self._get_feast_feature_store(
            feast_registry=feast_registry,
            feature_store=feature_store,
            credentials=credentials,
            online_store_config=online_store_config,
            effective_online_store_id=online_store_doc.id if online_store_doc else None,
        )
        return feast_feature_store  # type:ignore

    async def get_feast_feature_store_for_deployment(
        self, deployment: DeploymentModel
    ) -> Optional[FeastFeatureStore]:
        """
        Retrieve a FeastFeatureStore object for the current catalog

        Parameters
        ----------
        deployment: DeploymentModel
            Deployment model

        Returns
        -------
        Optional[FeastFeatureStore]
        """
        feast_registry = await self.feast_registry_service.get_feast_registry(deployment=deployment)
        if feast_registry is None:
            return None
        assert isinstance(feast_registry, FeastRegistryModel)
        return await self.get_feast_feature_store(feast_registry=feast_registry)

    @classmethod
    def _combine_feast_stores(
        cls,
        project_name: Optional[str],
        online_store: Optional[OnlineStoreModel],
        feature_table_name: str,
        feast_stores: List[FeastFeatureStore],
    ) -> RegistryProto:
        repo_content = RepoContents(
            data_sources=[],
            entities=[],
            feature_views=[],
            feature_services=[],
            on_demand_feature_views=[],
            stream_feature_views=[],
            request_feature_views=[],
        )

        first_store = feast_stores[0]
        repo_content.data_sources.extend(first_store.list_data_sources())
        repo_content.entities.extend(first_store.list_entities())

        first_fv = next(
            fv for fv in first_store.list_feature_views() if fv.name == feature_table_name
        )
        fv_entities = [
            FeastEntity(
                name=entity.name,
                join_keys=[entity.name],
                value_type=entity.dtype.to_value_type(),
            )
            for entity in first_fv.entity_columns
        ]
        feature_view_params = {
            "name": feature_table_name,
            "entities": fv_entities,
            "ttl": first_fv.ttl,
            "online": True,
            "source": first_fv.batch_source,
        }
        name_to_field_map = {}
        for feast_store in feast_stores:
            for feature in feast_store.get_feature_view(feature_table_name).features:
                if feature.name not in name_to_field_map:
                    name_to_field_map[feature.name] = feature

        feature_view_params["schema"] = list(name_to_field_map.values())
        repo_content.feature_views.append(FeastFeatureView(**feature_view_params))

        registry_proto = FeastRegistryBuilder.create_feast_registry_proto_from_repo_content(
            project_name=project_name or DEFAULT_REGISTRY_PROJECT_NAME,
            online_store=online_store,
            repo_content=repo_content,
        )
        return registry_proto

    async def get_feast_feature_store_for_feature_materialization(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
        online_store_id: Optional[ObjectId],
    ) -> Optional[FeastFeatureStore]:
        """
        Retrieve a FeastFeatureStore object for running feature materialization tasks

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            Offline store feature table model to materialize
        online_store_id: Optional[ObjectId]
            Online store id to use if specified instead of using the one in the catalog. This is used
            when the returned feature store is about to be used to update a different online store
            (e.g. for initialization purpose)

        Returns
        -------
        Optional[FeastFeatureStore]
            Feast feature store used for materialization
        """
        feast_stores = []
        first_registry = None
        for deployment_id in feature_table_model.deployment_ids:
            deployment = await self.deployment_service.get_document(document_id=deployment_id)
            if deployment.registry_info:
                first_registry = await self.feast_registry_service.get_document(
                    document_id=deployment.registry_info.registry_id
                )

            feast_store = await self.get_feast_feature_store_for_deployment(deployment)
            if feast_store is not None:
                feast_stores.append(feast_store)

        if not feast_stores or first_registry is None:
            return None

        feature_store = await self.feature_store_service.get_document(
            document_id=first_registry.feature_store_id
        )
        credentials = await self.credential_provider.get_credential(
            user_id=self.user.id, feature_store_name=feature_store.name
        )
        catalog = await self.catalog_service.get_document(first_registry.catalog_id)
        online_store_doc = await self._get_effective_online_store_doc(
            online_store_id=online_store_id, catalog=catalog
        )
        online_store_config = await self._get_feast_online_store_config(
            online_store_doc=online_store_doc
        )
        if (
            feature_table_model.precomputed_lookup_feature_table_info
            and feature_table_model.precomputed_lookup_feature_table_info.source_feature_table_id
        ):
            feature_table_model = await self.offline_store_feature_table_service.get_document(
                document_id=feature_table_model.precomputed_lookup_feature_table_info.source_feature_table_id,
                populate_remote_attributes=False,
            )

        registry_proto = self._combine_feast_stores(
            project_name=first_registry.name,
            online_store=online_store_doc,
            feature_table_name=feature_table_model.name,
            feast_stores=feast_stores,
        )
        feast_feature_store = create_feast_feature_store(
            project_name=first_registry.name,
            registry_proto=registry_proto,
            feature_store=feature_store,
            offline_store_credentials=credentials,
            online_store_config=online_store_config,
            effective_online_store_id=online_store_doc.id if online_store_doc else None,
        )
        return feast_feature_store
