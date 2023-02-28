"""
Basic test user module
"""
import pytest
from bson import ObjectId

from featurebyte.models.base import PydanticObjectId


@pytest.fixture(name="user_service")
def user_service_fixture(app_container):
    """
    User service fixture
    """
    return app_container.user_service


def test_get_user_name_for_id(user_service):
    """
    Test get_user_name_for_id
    """
    name = user_service.get_user_name_for_id(PydanticObjectId(ObjectId()))
    assert name == "default user"
