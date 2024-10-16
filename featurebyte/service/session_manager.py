"""
SessionManager service
"""

from typing import Any, Awaitable, Callable, Optional

from bson import ObjectId
from pydantic import ValidationError

from featurebyte.exception import CredentialsError
from featurebyte.models.base import User
from featurebyte.models.credential import CredentialModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.service.credential import CredentialService
from featurebyte.service.session_validator import SessionValidatorService
from featurebyte.session.base import NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS, BaseSession
from featurebyte.session.manager import SessionManager


class SessionManagerService:
    """
    SessionManagerService class is responsible for retrieving a session manager.
    """

    def __init__(
        self,
        user: Any,
        credential_service: CredentialService,
        session_validator_service: SessionValidatorService,
    ):
        self.user = user
        self.credential_service = credential_service
        self.session_validator_service = session_validator_service

    async def get_feature_store_session(
        self,
        feature_store: FeatureStoreModel,
        get_credential: Optional[
            Callable[[ObjectId, str], Awaitable[Optional[CredentialModel]]]
        ] = None,
        user_override: Optional[User] = None,
        timeout: float = NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS,
        skip_validation: bool = False,
    ) -> BaseSession:
        """
        Get session for feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            ExtendedFeatureStoreModel object
        get_credential: Optional[Callable[[ObjectId, str], Awaitable[Optional[CredentialModel]]]]
            Override credential handler function
        user_override: Optional[User]
            User object to override
        timeout: float
            timeout for session creation
        skip_validation: bool
            Skip validation

        Returns
        -------
        BaseSession
            BaseSession object

        Raises
        ------
        CredentialsError
            When the credentials used to access the feature store is missing or invalid
        """
        user_to_use = self.user if user_override is None else user_override
        try:
            if get_credential is not None:
                credential = await get_credential(user_to_use.id, feature_store.name)
            else:
                credential = await self.credential_service.find(
                    user_id=user_to_use.id, feature_store_name=feature_store.name
                )
            if credential is None:
                raise CredentialsError(
                    f'Credential used to access FeatureStore (name: "{feature_store.name}") is missing or invalid.'
                )

            credentials = {feature_store.name: credential}
            session_manager = SessionManager(credentials=credentials)
            session = await session_manager.get_session(feature_store, timeout=timeout)
            if not skip_validation:
                await self.session_validator_service.validate_feature_store_exists(
                    feature_store.details
                )
            return session
        except ValidationError as exc:
            raise CredentialsError(
                f'Credential used to access FeatureStore (name: "{feature_store.name}") is missing or invalid.'
            ) from exc
