"""
Utility functions for credential management
"""

from __future__ import annotations

from typing import Any, Optional

from abc import ABC, abstractmethod

from bson.objectid import ObjectId

from featurebyte.models.credential import CredentialModel
from featurebyte.models.feature_store import FeatureStoreModel


class CredentialProvider(ABC):
    """
    CredentialProvider is the base class for users to get credentials.
    """

    @abstractmethod
    async def get_credential(
        self, user_id: ObjectId | None, feature_store_name: str
    ) -> CredentialModel | None:
        """
        Retrieve credentials from some persistent source.

        Parameters
        ----------
        user_id: ObjectId | None
            User ID
        feature_store_name: str
            FeatureStore name

        Returns
        -------
        CredentialModel
            CredentialModel for the feature store
        """


class MongoBackedCredentialProvider(CredentialProvider):
    """
    MongoBackedCredentialProvider will retrieve credentials from a mongo database.
    """

    def __init__(self, persistent: Any):
        self.persistent = persistent

    async def get_credential(
        self, user_id: Optional[ObjectId], feature_store_name: str
    ) -> Optional[CredentialModel]:
        feature_store = await self.persistent.find_one(
            collection_name=FeatureStoreModel.collection_name(),
            query_filter={"name": feature_store_name},
        )
        if not feature_store:
            return None

        document = await self.persistent.find_one(
            collection_name=CredentialModel.collection_name(),
            query_filter={"user_id": user_id, "feature_store_id": feature_store["_id"]},
        )
        if document:
            credential = CredentialModel(**document)
            credential.decrypt_credentials()
            return credential
        return None
