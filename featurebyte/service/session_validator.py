"""
SessionValidator service
"""

from typing import Any, Optional

from featurebyte.enum import SourceType, StrEnum
from featurebyte.exception import FeatureStoreSchemaCollisionError, NoFeatureStorePresentError
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.session.base import INTERACTIVE_SESSION_TIMEOUT_SECONDS, BaseSession
from featurebyte.session.manager import SessionManager
from featurebyte.utils.credential import MongoBackedCredentialProvider

logger = get_logger(__name__)


class ValidateStatus(StrEnum):
    """
    Returns the status of validation
    """

    NOT_IN_DWH = "NOT_IN_DWH"
    FEATURE_STORE_ID_MATCHES = "FEATURE_STORE_ID_MATCHES"


class SessionValidatorService:
    """
    SessionValidatorService class is responsible for validating whether a
    session that we're trying to initialize is valid.
    """

    def __init__(
        self,
        user: Any,
        mongo_backed_credential_provider: MongoBackedCredentialProvider,
        feature_store_service: FeatureStoreService,
    ):
        self.user = user
        self.credential_provider = mongo_backed_credential_provider
        self.feature_store_service = feature_store_service

    @classmethod
    async def validate_existing_session(
        cls, session: BaseSession, users_feature_store_id: Optional[PydanticObjectId]
    ) -> ValidateStatus:
        """
        Validates whether the existing session is valid.

        Parameters
        ----------
        session: BaseSession
            current session trying to be initialized
        users_feature_store_id: Optional[PydanticObjectId]
            users feature store ID

        Returns
        -------
        ValidateStatus
            status of the validation

        Raises
        ------
        FeatureStoreSchemaCollisionError
            raised when the users_feature_store_id doesn't match the session trying to be initialized
        """
        working_schema_metadata = await session.get_working_schema_metadata()
        registered_feature_store_id = working_schema_metadata.get("feature_store_id")
        # Check that a feature store ID has been registered, and whether they're the same.
        if not registered_feature_store_id:
            return ValidateStatus.NOT_IN_DWH
        if users_feature_store_id != registered_feature_store_id:
            logger.debug(
                f"Found a registered feature_store_id in the DWH {registered_feature_store_id} "
                f"that collides with the feature_store_id {users_feature_store_id} in the persistent "
                f"layer."
            )
            raise FeatureStoreSchemaCollisionError
        return ValidateStatus.FEATURE_STORE_ID_MATCHES

    async def _get_session(
        self,
        feature_store_name: str,
        session_type: SourceType,
        details: DatabaseDetails,
        get_credential: Any,
    ) -> BaseSession:
        """
        Retrieves a session.

        Parameters
        ----------
        feature_store_name: str
            feature store name
        session_type: SourceType
            session type
        details: DatabaseDetails
            JSON dumps of feature store type &
        get_credential: Any
            credential handler function

        Returns
        -------
        BaseSession
            session for the parameters passed in
        """
        if get_credential is not None:
            credential = await get_credential(
                user_id=self.user.id, feature_store_name=feature_store_name
            )
        else:
            credential = await self.credential_provider.get_credential(
                user_id=self.user.id, feature_store_name=feature_store_name
            )
        session_manager = SessionManager(credentials={feature_store_name: credential})
        return await session_manager.get_session_with_params(
            feature_store_name, session_type, details, timeout=INTERACTIVE_SESSION_TIMEOUT_SECONDS
        )

    async def validate_feature_store_exists(
        self,
        details: DatabaseDetails,
    ) -> None:
        """
        Validate whether a feature store exists.

        Parameters
        ----------
        details: DatabaseDetails
            database details

        Raises
        ------
        NoFeatureStorePresentError
            error thrown when no feature store is present
        """
        users_feature_store_id = await self.get_feature_store_id_from_details(details)
        if users_feature_store_id is None:
            raise NoFeatureStorePresentError

    async def validate_feature_store_id_not_used_in_warehouse(
        self,
        feature_store_name: str,
        session_type: SourceType,
        details: DatabaseDetails,
        users_feature_store_id: Optional[PydanticObjectId],
        get_credential: Any = None,
    ) -> ValidateStatus:
        """
        Validate whether the existing details exist in the persistent layer
        or in the data warehouse.

        Parameters
        ----------
        feature_store_name: str
            feature store name
        session_type: SourceType
            session type
        details: DatabaseDetails
            database details
        get_credential: Any
            credential handler function
        users_feature_store_id: Optional[PydanticObjectId]
            users feature store ID

        Returns
        -------
        ValidateStatus
            The status of the validation
        """
        session = await self._get_session(feature_store_name, session_type, details, get_credential)
        return await self.validate_existing_session(session, users_feature_store_id)

    async def get_feature_store_id_from_details(
        self, details: DatabaseDetails
    ) -> Optional[PydanticObjectId]:
        """
        Retrieves feature store ID based on the details passed in.

        Parameters
        ----------
        details: DatabaseDetails
            database details

        Returns
        -------
        Optional[PydanticObjectId]
            Feature store ID if present. If not, returns None.
        """
        response = await self.feature_store_service.list_documents_as_dict(
            query_filter={"details": details.dict()}
        )

        count = response["total"]
        does_exist = count != 0
        # We expect to see at most one entry. Error if there's more than one.
        assert count < 2
        if does_exist:
            return PydanticObjectId(response["data"][0]["_id"])
        return None
