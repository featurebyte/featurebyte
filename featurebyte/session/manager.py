"""
SessionManager class
"""
from __future__ import annotations

from typing import Dict, Optional

import json

from cachetools import TTLCache, cached
from pydantic import BaseModel

from featurebyte.enum import SourceType
from featurebyte.logger import logger
from featurebyte.models.credential import Credential
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.session.sqlite import SQLiteSession

SOURCE_TYPE_SESSION_MAP = {
    SourceType.SQLITE: SQLiteSession,
    SourceType.SNOWFLAKE: SnowflakeSession,
}


@cached(cache=TTLCache(maxsize=1024, ttl=1800))
def get_session(item: str, credential_params: str) -> BaseSession:
    """
    Retrieve or create a new session for the given database source key

    Parameters
    ----------
    item: str
        JSON dumps of feature store type & details
    credential_params: str
        JSON dumps of credential parameters used to initiate a new session

    Returns
    -------
    BaseSession
        Newly created session
    """
    item_dict = json.loads(item)
    logger.debug(f'Create a new session for {item_dict["type"]}')
    credential_params_dict = json.loads(credential_params)
    return SOURCE_TYPE_SESSION_MAP[item_dict["type"]](
        **item_dict["details"], **credential_params_dict
    )


class SessionManager(BaseModel):
    """
    Session manager to manage session of different database sources
    """

    credentials: Dict[FeatureStoreModel, Optional[Credential]]

    def __getitem__(self, item: FeatureStoreModel) -> BaseSession:
        """
        Retrieve or create a new session for the given database source key

        Parameters
        ----------
        item: FeatureStoreModel
            Database source object

        Returns
        -------
        BaseSession
            Session that can be used to connect to the specified database

        Raises
        ------
        ValueError
            When credentials do not contain the specified data source info
        """
        if item in self.credentials:
            credential = self.credentials[item]
            credential_params = credential.credential.dict() if credential else {}
            return get_session(
                item=json.dumps(item.dict(include={"type": True, "details": True}), sort_keys=True),
                credential_params=json.dumps(credential_params, sort_keys=True),
            )
        raise ValueError(
            f'Credentials do not contain info for the feature store (feature_store.type: "{item.type}")!'
        )
