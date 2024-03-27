"""
User service module
"""

from typing import Optional

from bson import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel, PydanticObjectId


class UserService:
    """
    Basic no-op user service.

    We add a basic API here so that the SaaS version can easily override this with more functionality.
    """

    def get_user_name_for_id(self, user_id: Optional[PydanticObjectId]) -> str:
        """
        We return a default user name here since there isn't really a concept of users in the open source version.

        Parameters
        ----------
        user_id: Optional[PydanticObjectId]
            The user id

        Returns
        -------
        User name
        """
        _ = user_id
        return "default user"

    async def get_document(self, document_id: ObjectId) -> FeatureByteBaseDocumentModel:
        """
        No-op get document method

        Parameters
        ----------
        document_id: ObjectId
            The user id

        Returns
        -------
        Dummy document
        """
        _ = document_id
        return FeatureByteBaseDocumentModel(name=None)
