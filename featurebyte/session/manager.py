"""
SessionManager class
"""
from __future__ import annotations

from typing import Any

import json

from asyncache import cached
from cachetools import TTLCache
from pydantic import BaseModel

from featurebyte.config import Credentials
from featurebyte.enum import SourceType
from featurebyte.logger import logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.model.feature_store import DatabaseDetails
from featurebyte.session.base import BaseSession
from featurebyte.session.databricks import DatabricksSession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.session.sqlite import SQLiteSession

SOURCE_TYPE_SESSION_MAP = {
    SourceType.SQLITE: SQLiteSession,
    SourceType.SNOWFLAKE: SnowflakeSession,
    SourceType.DATABRICKS: DatabricksSession,
}

session_cache: TTLCache[Any, Any] = TTLCache(maxsize=1024, ttl=600)


@cached(cache=session_cache)
async def get_session(item: str, credential_params: str) -> BaseSession:
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
    session = SOURCE_TYPE_SESSION_MAP[item_dict["type"]](  # type: ignore
        **item_dict["details"], **credential_params_dict
    )
    await session.initialize()
    return session


class SessionManager(BaseModel):
    """
    Session manager to manage session of different database sources
    """

    credentials: Credentials

    async def get_session_with_params(
        self, feature_store_name: str, session_type: SourceType, details: DatabaseDetails
    ) -> BaseSession:
        """
        Retrieve or create a new session for the given database source key

        Parameters
        ----------
        feature_store_name: str
            feature store name
        session_type: SourceType
            session type
        details: DatabaseDetails
            database details

        Returns
        -------
        BaseSession
            Session that can be used to connect to the specified database

        Raises
        ------
        ValueError
            When credentials do not contain the specified data source info
        """
        if feature_store_name in self.credentials:
            credential = self.credentials[feature_store_name]
            credential_params = credential.credential.dict() if credential else {}
            json_str = json.dumps(
                {
                    "type": session_type,
                    "details": details.json_dict(),
                },
                sort_keys=True,
            )
            session = await get_session(
                item=json_str,
                credential_params=json.dumps(credential_params, sort_keys=True),
            )
            assert isinstance(session, BaseSession)
            return session
        raise ValueError(
            f'Credentials do not contain info for the feature store "{feature_store_name}"!'
        )

    async def get_session(self, item: FeatureStoreModel) -> BaseSession:
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
        """
        return await self.get_session_with_params(item.name, item.type, item.details)
