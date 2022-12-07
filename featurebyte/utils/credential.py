"""
Utility functions for credential management
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from bson.objectid import ObjectId

from featurebyte.config import Configurations
from featurebyte.models.credential import Credential


class CredentialProvider(ABC):
    """
    CredentialProvider is the base class for users to get credentials.
    """

    @abstractmethod
    async def get_credential(
        self, user_id: ObjectId | None, feature_store_name: str
    ) -> Credential | None:
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
        Credential
            Credential for the database source
        """


class ConfigCredentialProvider(CredentialProvider):
    """
    ConfigCredentialProvider will retrieve the users credentials from a configuration file.
    """

    async def get_credential(
        self, user_id: ObjectId | None, feature_store_name: str
    ) -> Credential | None:
        _ = user_id
        config = Configurations()
        return config.credentials.get(feature_store_name)


async def get_credential(user_id: ObjectId | None, feature_store_name: str) -> Credential | None:
    """
    Delegate to the ConfigCredentialProvider. We will look to deprecate direct access to this function in the future.

    Parameters
    ----------
    user_id: ObjectId | None
        User ID
    feature_store_name: str
        FeatureStore name

    Returns
    -------
    Credential
        Credential for the database source
    """
    credential_provider = ConfigCredentialProvider()
    return await credential_provider.get_credential(user_id, feature_store_name)
