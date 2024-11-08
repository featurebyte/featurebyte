"""
This module contains classes for constructing feast repository config
"""

import tempfile
from typing import Any, Optional

from asyncache import cached
from bson import ObjectId
from cachetools import LRUCache
from feast import FeatureStore as BaseFeastFeatureStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import FeastConfigBaseModel

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
from featurebyte.service.credential import CredentialService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.online_store import OnlineStoreService

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
        offline_store_config = FeastRegistryBuilder.get_offline_store_config(
            feature_store_model=feature_store,
            offline_store_credentials=offline_store_credentials,
        )
        repo_config = FeastRegistryBuilder.create_repo_config(
            project_name=project_name,
            registry_file_path=feast_registry_path,
            offline_store_config=offline_store_config,
            online_store_config=online_store_config,
            registry_store_type="featurebyte.feast.registry_store.FeatureByteRegistryStore",
        )
        feast_feature_store = FeastFeatureStore(
            config=repo_config,
            online_store_id=effective_online_store_id,
        )
        return feast_feature_store


def _get_feast_feature_store_cache_key(
    feast_registry: FeastRegistryModel,
    feature_store: FeatureStoreModel,
    offline_store_credentials: Any,
    online_store_config: Optional[FeastConfigBaseModel],
    effective_online_store_id: ObjectId,
) -> Any:
    _ = offline_store_credentials, online_store_config
    cache_key = (
        feature_store.id,
        feature_store.updated_at,
        feast_registry.id,
        feast_registry.updated_at,
        feast_registry.registry_path,
        effective_online_store_id,
    )
    return cache_key


@cached(cache=feast_feature_store_cache, key=_get_feast_feature_store_cache_key)
async def _get_feast_feature_store(
    feast_registry: FeastRegistryModel,
    feature_store: FeatureStoreModel,
    offline_store_credentials: Any,
    online_store_config: Optional[FeastConfigBaseModel],
    effective_online_store_id: ObjectId,
) -> FeastFeatureStore:
    feast_feature_store = create_feast_feature_store(
        project_name=feast_registry.name,
        registry_proto=feast_registry.registry_proto(),
        feature_store=feature_store,
        offline_store_credentials=offline_store_credentials,
        online_store_config=online_store_config,
        effective_online_store_id=effective_online_store_id,
    )
    return feast_feature_store


class FeastFeatureStoreService:
    """Feast feature store service"""

    def __init__(
        self,
        user: Any,
        feast_registry_service: FeastRegistryService,
        credential_service: CredentialService,
        feature_store_service: FeatureStoreService,
        catalog_service: CatalogService,
        online_store_service: OnlineStoreService,
        deployment_service: DeploymentService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
    ):
        self.user = user
        self.feast_registry_service = feast_registry_service
        self.credential_service = credential_service
        self.feature_store_service = feature_store_service
        self.catalog_service = catalog_service
        self.online_store_service = online_store_service
        self.deployment_service = deployment_service
        self.offline_store_feature_table_service = offline_store_feature_table_service

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

        Raises
        ------
        ValueError
            if unable to retrieve feature store credentials
        """
        logger.info("Creating feast feature store for registry %s", str(feast_registry.id))
        feature_store = await self.feature_store_service.get_document(
            document_id=feast_registry.feature_store_id
        )
        credentials = await self.credential_service.get_credentials(
            user_id=self.user.id, feature_store=feature_store
        )
        if credentials is None:
            logger.error(
                "Unable to retrieve feature store credentials",
                extra={"user_id": self.user.id, "feature_store": feature_store.name},
            )
            raise ValueError("Unable to retrieve feature store credentials")

        # Get online store feast config
        catalog = await self.catalog_service.get_document(feast_registry.catalog_id)
        online_store_doc = await self._get_effective_online_store_doc(
            online_store_id=online_store_id, catalog=catalog
        )
        online_store_config = FeastRegistryBuilder.get_online_store_config(
            online_store=online_store_doc
        )
        logger.info("Feast feature store cache size: %d", len(feast_feature_store_cache))
        feast_feature_store = await _get_feast_feature_store(
            feast_registry=feast_registry,
            feature_store=feature_store,
            offline_store_credentials=credentials,
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
        offline_store_credentials = await self.credential_service.get_credentials(
            user_id=self.user.id, feature_store=feature_store
        )
        if offline_store_credentials is None:
            logger.error(
                "Unable to retrieve feature store credentials",
                extra={"user_id": self.user.id, "feature_store": feature_store.name},
            )
            return None
        offline_store_config = FeastRegistryBuilder.get_offline_store_config(
            feature_store_model=feature_store, offline_store_credentials=offline_store_credentials
        )
        catalog = await self.catalog_service.get_document(first_registry.catalog_id)
        online_store_doc = await self._get_effective_online_store_doc(
            online_store_id=online_store_id, catalog=catalog
        )
        online_store_config = FeastRegistryBuilder.get_online_store_config(
            online_store=online_store_doc
        )
        registry_proto = (
            FeastRegistryBuilder.create_feast_registry_proto_for_feature_materialization(
                project_name=first_registry.name,
                offline_store_config=offline_store_config,
                online_store=online_store_doc,
                feature_table_name=feature_table_model.name,
                feast_stores=feast_stores,
            )
        )
        feast_feature_store = create_feast_feature_store(
            project_name=first_registry.name,
            registry_proto=registry_proto,
            feature_store=feature_store,
            offline_store_credentials=offline_store_credentials,
            online_store_config=online_store_config,
            effective_online_store_id=online_store_doc.id if online_store_doc else None,
        )
        return feast_feature_store
