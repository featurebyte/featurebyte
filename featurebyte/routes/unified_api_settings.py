"""
Static user to be used in API routes
"""
# pylint: disable=too-few-public-methods
from bson.objectid import ObjectId

from featurebyte.storage import MongoStorage


class User:
    """
    Skeleton user class to provide static user for API routes
    """

    # DO NOT CHANGE THIS VALUE
    id = ObjectId("62a6d9d023e7a8f2a0dc041a")


def session_user() -> User:
    """
    Get session user

    Returns
    -------
    User
        Session user
    """
    return User()


storage = MongoStorage("mongodb://localhost:27017")
