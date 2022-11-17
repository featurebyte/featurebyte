"""
SessionManager service
"""
from typing import Any

from pydantic import ValidationError

from featurebyte.exception import CredentialsError
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.session.base import BaseSession
from featurebyte.session.manager import SessionManager
from featurebyte.utils.credential import get_credential


class SessionManagerService:
    """
    SessionManagerService class is responsible for retrieving a session manager.
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, user: Any, persistent: Persistent):
        self.user = user
        self.persistent = persistent

    async def get_feature_store_session(self, feature_store: FeatureStoreModel) -> BaseSession:
        """
        Get session for feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            ExtendedFeatureStoreModel object

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
            credentials = {
                feature_store.name: await get_credential(
                    user_id=self.user.id, feature_store_name=feature_store.name
                )
            }
            session_manager = SessionManager(credentials=credentials)
            return await session_manager.get_session(feature_store)
        except ValidationError as exc:
            raise CredentialsError(
                f'Credential used to access FeatureStore (name: "{feature_store.name}") is missing or invalid.'
            ) from exc
