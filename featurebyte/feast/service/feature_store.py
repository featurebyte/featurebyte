"""
This module contains classes for constructing feast repository config
"""
from typing import Any, cast

import tempfile

from bson import ObjectId
from feast import FeatureStore, RepoConfig
from feast.infra.online_stores.redis import RedisOnlineStoreConfig
from feast.repo_config import RegistryConfig

from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.utils.credential import MongoBackedCredentialProvider


class FeastFeatureStoreService:
    """Feast feature store service"""

    def __init__(
        self,
        user: Any,
        feast_registry_service: FeastRegistryService,
        mongo_backed_credential_provider: MongoBackedCredentialProvider,
        feature_store_service: FeatureStoreService,
    ):
        self.user = user
        self.feast_registry_service = feast_registry_service
        self.credential_provider = mongo_backed_credential_provider
        self.feature_store_service = feature_store_service

    async def get_feast_feature_store(
        self,
        feast_registry_id: ObjectId,
    ) -> FeatureStore:
        """
        Create feast repo config

        Parameters
        ----------
        feast_registry_id: ObjectId
            Feast registry id

        Returns
        -------
        FeatureStore
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
            feature_store_details = feature_store.get_feature_store_details()
            database_credential = None
            if credentials:
                database_credential = credentials.database_credential
            repo_config = RepoConfig(
                project=feast_registry.name,
                provider="local",
                registry=registry_config,
                offline_store=feature_store_details.details.get_offline_store_config(
                    credential=database_credential
                ),
                online_store=RedisOnlineStoreConfig(connection_string="localhost:6379"),
            )
            return cast(FeatureStore, FeatureStore(config=repo_config))
