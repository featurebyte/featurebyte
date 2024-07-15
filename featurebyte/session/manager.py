"""
SessionManager class
"""

from __future__ import annotations

from typing import Any, Dict, Hashable

import json
import time
from asyncio.exceptions import TimeoutError as AsyncioTimeoutError

from asyncache import cached
from cachetools import keys
from pydantic import BaseModel, Field

from featurebyte.enum import SourceType
from featurebyte.exception import SessionInitializationTimeOut
from featurebyte.logging import get_logger
from featurebyte.models.credential import CredentialModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.session.base import (
    NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    BaseSession,
    session_cache,
    to_thread,
)
from featurebyte.session.databricks import DatabricksSession
from featurebyte.session.databricks_unity import DatabricksUnitySession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.session.spark import SparkSession
from featurebyte.session.sqlite import SQLiteSession

SOURCE_TYPE_SESSION_MAP = {
    SourceType.SQLITE: SQLiteSession,
    SourceType.SNOWFLAKE: SnowflakeSession,
    SourceType.DATABRICKS: DatabricksSession,
    SourceType.DATABRICKS_UNITY: DatabricksUnitySession,
    SourceType.SPARK: SparkSession,
}

logger = get_logger(__name__)


async def get_new_session(item: str, params: str, timeout: float) -> BaseSession:
    """
    Create a new session for the given database source key

    Parameters
    ----------
    item: str
        JSON dumps of feature store type & details
    params: str
        JSON dumps of parameters used to initiate a new session
    timeout: float
        timeout for session creation

    Returns
    -------
    BaseSession
        Newly created session

    Raises
    ------
    SessionInitializationTimeOut
        If session creation timed out
    """
    tic = time.time()
    item_dict = json.loads(item)
    logger.debug(f'Create a new session for {item_dict["type"]}')
    params_dict = json.loads(params)

    def _create_session() -> BaseSession:
        """
        Create a new session for the given database source key

        Returns
        -------
        BaseSession
        """
        return SOURCE_TYPE_SESSION_MAP[item_dict["type"]](  # type: ignore
            **item_dict["details"], **params_dict
        )

    try:
        session: BaseSession = await to_thread(_create_session, timeout)
        await session.initialize()
        logger.debug(f"Session creation time: {time.time() - tic:.3f}s")
    except AsyncioTimeoutError as exc:
        raise SessionInitializationTimeOut(
            f"Session creation timed out after {time.time() - tic:.3f}s"
        ) from exc
    return session


def _session_hash_key(item: str, params: str, timeout: float) -> tuple[Hashable, ...]:
    """
    Return a cache key for the specified hashable arguments. The signature of this function must match the
    signature of the `get_session` function.

    Parameters
    ----------
    item: str
        JSON dumps of feature store type & details
    params: str
        JSON dumps of parameters used to initiate a new session
    timeout: float
        timeout for session creation

    Returns
    -------
    tuple[Hashable, ...]
    """
    # exclude timeout from hash
    _ = timeout
    return keys.hashkey(item=item, params=params)


@cached(cache=session_cache, key=_session_hash_key)
async def get_session(item: str, params: str, timeout: float) -> BaseSession:
    """
    Retrieve or create a new session for the given database source key. If a new session is created,
    it will be cached.

    Parameters
    ----------
    item: str
        JSON dumps of feature store type & details
    params: str
        JSON dumps of parameters used to initiate a new session
    timeout: float
        timeout for session creation

    Returns
    -------
    BaseSession
        Retrieved or created session object
    """
    session = await get_new_session(item, params, timeout=timeout)
    session.set_cache_key(_session_hash_key(item, params, timeout))
    return session


class SessionManager(BaseModel):
    """
    Session manager to manage session of different database sources
    """

    parameters: Dict[str, Any] = Field(default_factory=dict)
    credentials: Dict[str, CredentialModel]

    async def get_session_with_params(
        self,
        feature_store_name: str,
        session_type: SourceType,
        details: DatabaseDetails,
        timeout: float,
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
        timeout: float
            timeout for session creation

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

        params = {**self.parameters}

        # add credentials to session parameters
        credential_params = (
            {
                key: value
                for key, value in credential.json_dict().items()
                if key in ["database_credential", "storage_credential"]
            }
            if credential
            else {}
        )
        params.update(credential_params)

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
            params=json.dumps(params, sort_keys=True),
            timeout=timeout,
        )
        assert isinstance(session, BaseSession)
        return session

    async def get_session(
        self, item: FeatureStoreModel, timeout: float = NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS
    ) -> BaseSession:
        """
        Retrieve or create a new session for the given database source key

        Parameters
        ----------
        item: FeatureStoreModel
            Database source object
        timeout: float
            timeout for session creation

        Returns
        -------
        BaseSession
            Session that can be used to connect to the specified database
        """
        return await self.get_session_with_params(
            item.name, item.type, item.details, timeout=timeout
        )
