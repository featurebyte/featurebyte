"""
SessionManager service
"""
from typing import Any

from bson import ObjectId
from pydantic import ValidationError

from featurebyte.exception import CredentialsError
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.service.session_validator import SessionValidatorService
from featurebyte.session.base import BaseSession
from featurebyte.session.manager import SessionManager
from featurebyte.utils.credential import MongoBackedCredentialProvider


class SessionManagerService:
    """
    SessionManagerService class is responsible for retrieving a session manager.
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        credential_provider: MongoBackedCredentialProvider,
        session_validator_service: SessionValidatorService,
    ):
        self.user = user
        self.persistent = persistent
        self.catalog_id = catalog_id
        self.credential_provider = credential_provider
        self.session_validator_service = session_validator_service

    async def get_feature_store_session(
        self, feature_store: FeatureStoreModel, get_credential: Any
    ) -> BaseSession:
        """
        Get session for feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            ExtendedFeatureStoreModel object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        BaseSession
            BaseSession object

        Raises
        ------
        CredentialsError
            When the credentials used to access the feature store is missing or invalid
        """
        try:
            if get_credential is not None:
                credential = await get_credential(
                    user_id=self.user.id, feature_store_name=feature_store.name
                )
            else:
                credential = await self.credential_provider.get_credential(
                    user_id=self.user.id, feature_store_name=feature_store.name
                )
            credentials = {feature_store.name: credential}
            session_manager = SessionManager(credentials=credentials)
            session = await session_manager.get_session(feature_store)
            await self.session_validator_service.validate_feature_store_exists(
                feature_store.details
            )
            return session
        except ValidationError as exc:
            raise CredentialsError(
                f'Credential used to access FeatureStore (name: "{feature_store.name}") is missing or invalid.'
            ) from exc
