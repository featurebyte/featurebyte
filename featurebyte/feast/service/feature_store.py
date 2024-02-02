"""
This module contains classes for constructing feast repository config
"""
from typing import Any, Optional

import tempfile

from bson import ObjectId
from feast import FeatureStore as BaseFeastFeatureStore
from feast import RepoConfig
from feast.repo_config import FeastConfigBaseModel, RegistryConfig

from featurebyte.feast.model.feature_store import FeatureStoreDetailsWithFeastConfiguration
from featurebyte.feast.model.online_store import get_feast_online_store_details
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_store import OnlineStoreService
from featurebyte.utils.credential import MongoBackedCredentialProvider


class FeastFeatureStore(BaseFeastFeatureStore):
    """
    Feast feature store that also tracks the corresponding online store id in featurebyte
    """

    def __init__(self, online_store_id: Optional[ObjectId], *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.online_store_id = online_store_id


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
    ):
        self.user = user
        self.feast_registry_service = feast_registry_service
        self.credential_provider = mongo_backed_credential_provider
        self.feature_store_service = feature_store_service
        self.catalog_service = catalog_service
        self.online_store_service = online_store_service

    async def get_feast_feature_store(
        self,
        feast_registry_id: ObjectId,
        online_store_id: Optional[ObjectId] = None,
    ) -> FeastFeatureStore:
        """
        Create feast repo config

        Parameters
        ----------
        feast_registry_id: ObjectId
            Feast registry id
        online_store_id: Optional[ObjectId]
            Online store id to use if specified instead of using the one in the catalog. This is used
            when the returned feature store is about to be used to update a different online store
            (e.g. for initialization purpose)

        Returns
        -------
        FeastFeatureStore
            Feast feature store
        """
        feast_registry = await self.feast_registry_service.get_document(
            document_id=feast_registry_id
        )
        feature_store = await self.feature_store_service.get_document(
            document_id=feast_registry.feature_store_id
        )
        credentials = await self.credential_provider.get_credential(
            user_id=self.user.id, feature_store_name=feature_store.name
        )

        # Get online store feast config
        catalog = await self.catalog_service.get_document(feast_registry.catalog_id)
        effective_online_store_id = (
            online_store_id if online_store_id is not None else catalog.online_store_id
        )
        online_store = await self._get_feast_online_store_config(
            online_store_id=effective_online_store_id,
        )

        with tempfile.NamedTemporaryFile() as temp_file:
            # Use temp file to pass the registry proto to the feature store. Once the
            # feature store is created, the temp file is no longer needed.
            feast_registry_path = temp_file.name
            with open(feast_registry_path, mode="wb", buffering=0) as file_handle:
                file_handle.write(feast_registry.registry_proto().SerializeToString())

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
            if credentials:
                database_credential = credentials.database_credential
                storage_credential = credentials.storage_credential
            repo_config = RepoConfig(
                project=feast_registry.name,
                provider="local",
                registry=registry_config,
                offline_store=feature_store_details.details.get_offline_store_config(
                    database_credential=database_credential,
                    storage_credential=storage_credential,
                ),
                online_store=online_store,
                entity_key_serialization_version=2,
            )
            feast_feature_store = FeastFeatureStore(
                config=repo_config,
                online_store_id=effective_online_store_id,
            )
            return feast_feature_store

    async def _get_feast_online_store_config(
        self, online_store_id: Optional[ObjectId]
    ) -> Optional[FeastConfigBaseModel]:
        if online_store_id is not None:
            online_store_details = (
                await self.online_store_service.get_document(online_store_id)
            ).details
            online_store = get_feast_online_store_details(
                online_store_details
            ).to_feast_online_store_config()
        else:
            online_store = None

        return online_store

    async def get_feast_feature_store_for_catalog(self) -> Optional[FeastFeatureStore]:
        """
        Retrieve a FeastFeatureStore object for the current catalog

        Returns
        -------
        Optional[FeastFeatureStore]
        """
        feast_registry = await self.feast_registry_service.get_feast_registry_for_catalog()
        if feast_registry is None:
            return None
        return await self.get_feast_feature_store(feast_registry_id=feast_registry.id)
