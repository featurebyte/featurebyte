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


class SessionManager(BaseModel):
    """
    Session manager to manage session of different database sources
    """

    credentials: Dict[FeatureStoreModel, Optional[Credential]]

    def __hash__(self) -> int:
        return hash(
            json.dumps(
                {
                    hash(key): value.json() if isinstance(value, BaseModel) else value
                    for key, value in self.credentials.items()
                }
            )
        )

    @cached(cache=TTLCache(maxsize=1024, ttl=1800))
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
            logger.debug(f"Create a new session for {item.type}")
            credential = self.credentials[item]
            credential_params = credential.credential.dict() if credential else {}
            return SOURCE_TYPE_SESSION_MAP[item.type](**item.details.dict(), **credential_params)
        raise ValueError(f'Credentials do not contain info for the database source "{item}"!')
