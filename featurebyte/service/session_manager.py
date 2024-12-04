"""
SessionManager service
"""

import json
import time
from asyncio.exceptions import TimeoutError
from typing import Any, Hashable, Optional

from asyncache import cached
from cachetools import keys
from pydantic import ValidationError

from featurebyte import SourceType, get_logger  # type: ignore
from featurebyte.enum import StrEnum
from featurebyte.exception import CredentialsError, SessionInitializationTimeOut
from featurebyte.models.base import User
from featurebyte.models.credential import CredentialModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.service.credential import CredentialService
from featurebyte.session.base import (
    NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    BaseSession,
    session_cache,
    to_thread,
)
from featurebyte.session.bigquery import BigQuerySession
from featurebyte.session.databricks import DatabricksSession
from featurebyte.session.databricks_unity import DatabricksUnitySession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.session.spark import SparkSession
from featurebyte.session.sqlite import SQLiteSession

logger = get_logger(__name__)

source_to_session_map: dict[SourceType, type[BaseSession]] = {
    SourceType.SNOWFLAKE: SnowflakeSession,
    SourceType.SQLITE: SQLiteSession,
    SourceType.SPARK: SparkSession,
    SourceType.BIGQUERY: BigQuerySession,
    SourceType.DATABRICKS: DatabricksSession,
    SourceType.DATABRICKS_UNITY: DatabricksUnitySession,
}


class ValidateStatus(StrEnum):
    """
    Returns the status of validation
    """

    NOT_IN_DWH = "NOT_IN_DWH"
    FEATURE_STORE_ID_MATCHES = "FEATURE_STORE_ID_MATCHES"


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

        Raises
        ------
        ValueError
            If source type is not supported
        """
        source_type = SourceType(item_dict["type"])
        session_type = source_to_session_map.get(source_type)
        if session_type is None:
            raise ValueError(f"Unsupported source type: {source_type}")
        return session_type(**item_dict["details"], **params_dict)

    try:
        logger.warning("Session creation time: START")
        session: BaseSession = await to_thread(_create_session, timeout, None)
        await session.initialize()
        logger.warning(f"Session creation time: {time.time() - tic:.3f}s")
    except TimeoutError as exc:
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


class SessionManagerService:
    """
    SessionManagerService class is responsible for retrieving a session manager.
    """

    def __init__(
        self,
        user: Any,
        credential_service: CredentialService,
    ):
        self.user = user
        self.credential_service = credential_service

    async def get_feature_store_session(
        self,
        feature_store: FeatureStoreModel,
        user_override: Optional[User] = None,
        credentials_override: Optional[CredentialModel] = None,
        timeout: float = NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    ) -> BaseSession:
        """
        Get session for feature store. If provided a user, it will use that user's credentials.
        If provided a credential, it will use that credential.

        Parameters
        ----------
        feature_store: FeatureStoreModel
            ExtendedFeatureStoreModel object
        user_override: Optional[User]
            User object to override
        credentials_override: Optional[CredentialModel]
            Credentials object to override
        timeout: float
            timeout for session creation

        Returns
        -------
        BaseSession
            BaseSession object

        Raises
        ------
        CredentialsError
            When the credentials used to access the feature store is missing or invalid
        """
        credentials = credentials_override
        credentials_user = self.user if user_override is None else user_override

        if not credentials:
            # This could return None for feature store sessions that don't require credentials
            credentials = await self.credential_service.get_credentials(
                user_id=credentials_user.id, feature_store=feature_store
            )

        try:
            session = await self.get_session(
                feature_store, credentials=credentials, timeout=timeout
            )
            return session
        except ValidationError as exc:
            raise CredentialsError(
                f'Credential used to access FeatureStore (name: "{feature_store.name}") is missing or invalid.'
            ) from exc

    @classmethod
    async def get_session(
        cls,
        feature_store: FeatureStoreModel,
        credentials: Optional[CredentialModel] = None,
        timeout: float = NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    ) -> BaseSession:
        """
        Retrieve or create a new session for the given database source key

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Database source object
        credentials: Optional[CredentialModel]
            Credentials to be used to connect to the database
        timeout: float
            timeout for session creation

        Raises
        ------
        ValueError
            If credentials are required for the source type and not provided

        Returns
        -------
        BaseSession
            Session that can be used to connect to the specified database
        """
        if feature_store.type in SourceType.credential_required_types() and credentials is None:
            raise ValueError(
                f"Credentials required for source type {feature_store.type} is missing"
            )

        params = {}
        if credentials:
            creds = credentials.model_dump(by_alias=True)
            if "database_credential" in creds:
                params["database_credential"] = creds["database_credential"]
            if "storage_credential" in creds:
                params["storage_credential"] = creds["storage_credential"]

        cache_key = json.dumps(
            {
                "type": feature_store.type,
                "details": feature_store.details.model_dump(by_alias=True),
            },
            sort_keys=True,
        )

        # Get session from cache or create a new session
        source_type = SourceType(feature_store.type)
        session_type = source_to_session_map.get(source_type)
        if session_type is None:
            raise ValueError(f"Unsupported source type: {source_type}")
        session_fn = get_session if session_type.is_threadsafe() else get_new_session

        session = await session_fn(
            item=cache_key,
            params=json.dumps(params, sort_keys=True),
            timeout=timeout,
        )
        assert isinstance(session, BaseSession)
        return session
