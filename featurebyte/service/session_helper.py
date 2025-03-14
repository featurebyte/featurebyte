"""
Session helper
"""

from typing import Optional, Tuple

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.base import User
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


class SessionHelper:
    """Session helper class"""

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service

    async def try_to_get_feature_store_and_session(
        self, feature_store_id: ObjectId
    ) -> Optional[Tuple[FeatureStoreModel, BaseSession]]:
        """
        Attempt to get feature store and session

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id

        Returns
        -------
        Optional[Tuple[FeatureStoreModel, BaseSession]]
            Feature store and session
        """

        try:
            feature_store = await self.feature_store_service.get_document(
                document_id=feature_store_id
            )
            session = await self.session_manager_service.get_feature_store_session(
                feature_store, user_override=User(id=feature_store.user_id)
            )
            return feature_store, session
        except Exception as exc:
            # failed to get session, skip dropping tables
            # this may happens if
            # * the feature store is deleted
            # * the credentials of the feature store is invalid or deleted
            logger.exception(f"Error getting session for feature store ({feature_store_id}): {exc}")
        return None
