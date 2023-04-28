"""
SessionManager class
"""
from __future__ import annotations

from typing import Any, Dict

import json
import time

from asyncache import cached
from cachetools import TTLCache
from pydantic import BaseModel

from featurebyte.enum import SourceType
from featurebyte.logging import get_logger
from featurebyte.models.credential import CredentialModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.session.base import BaseSession
from featurebyte.session.databricks import DatabricksSession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.session.spark import SparkSession
from featurebyte.session.sqlite import SQLiteSession

SOURCE_TYPE_SESSION_MAP = {
    SourceType.SQLITE: SQLiteSession,
    SourceType.SNOWFLAKE: SnowflakeSession,
    SourceType.DATABRICKS: DatabricksSession,
    SourceType.SPARK: SparkSession,
}

session_cache: TTLCache[Any, Any] = TTLCache(maxsize=1024, ttl=600)


logger = get_logger(__name__)


async def get_new_session(item: str, credential_params: str) -> BaseSession:
    """
    Create a new session for the given database source key

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
    tic = time.time()
    item_dict = json.loads(item)
    logger.debug(f'Create a new session for {item_dict["type"]}')
    credential_params_dict = json.loads(credential_params)
    session = SOURCE_TYPE_SESSION_MAP[item_dict["type"]](  # type: ignore
        **item_dict["details"], **credential_params_dict
    )
    await session.initialize()
    logger.debug(f"Session creation time: {time.time() - tic:.3f}s")
    return session


@cached(cache=session_cache)
async def get_session(item: str, credential_params: str) -> BaseSession:
    """
    Retrieve or create a new session for the given database source key. If a new session is created,
    it will be cached.

    Parameters
    ----------
    item: str
        JSON dumps of feature store type & details
    credential_params: str
        JSON dumps of credential parameters used to initiate a new session

    Returns
    -------
    BaseSession
        Retrieved or created session object
    """
    return await get_new_session(item, credential_params)


class SessionManager(BaseModel):
    """
    Session manager to manage session of different database sources
    """

    credentials: Dict[str, CredentialModel]

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
        elif session_type not in SourceType.credential_required_types():
            credential = None
        else:
            raise ValueError(
                f'Credentials do not contain info for the feature store "{feature_store_name}"!'
            )

        credential_params = (
            {
                key: value
                for key, value in credential.json_dict().items()
                if key in ["database_credential", "storage_credential"]
            }
            if credential
            else {}
        )

        json_str = json.dumps(
            {
                "type": session_type,
                "details": details.json_dict(),
            },
            sort_keys=True,
        )
        if SOURCE_TYPE_SESSION_MAP[session_type].is_threadsafe():
            get_session_func = get_session
        else:
            get_session_func = get_new_session
        session = await get_session_func(
            item=json_str,
            credential_params=json.dumps(credential_params, sort_keys=True),
        )
        assert isinstance(session, BaseSession)
        return session

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
