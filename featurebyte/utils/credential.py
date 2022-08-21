"""
Utility functions for credential management
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.config import Configurations
from featurebyte.models.credential import Credential


async def get_credential(user_id: ObjectId | None, feature_store_name: str) -> Credential | None:
    """
    Retrieve credential from FeatureStoreModel

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
    _ = user_id
    config = Configurations()
    return config.credentials.get(feature_store_name)
