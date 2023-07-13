"""
User service module
"""
from typing import Optional

from featurebyte.models.base import PydanticObjectId


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
