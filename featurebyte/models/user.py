"""
User module
"""
from bson import ObjectId

from featurebyte.models.base import PydanticObjectId

# This is the default user id that we use for the open source version.
# We provide a static value here since there isn't a real concept of users in the open source version.
DEFAULT_USER_OBJECT_ID = ObjectId("000000000000000000000001")
DEFAULT_USER_PYDANTIC_OBJECT_ID = PydanticObjectId(DEFAULT_USER_OBJECT_ID)
