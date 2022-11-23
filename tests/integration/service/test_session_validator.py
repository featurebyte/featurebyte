"""
Session validator integration test class
"""

import pytest

from featurebyte import FeatureStore, SourceType
from featurebyte.app import User
from featurebyte.exception import FeatureStoreSchemaCollisionError
from featurebyte.service.session_validator import SessionValidatorService, ValidateStatus


@pytest.fixture(name="session_validator_service")
def get_session_validator_service_fixture(mongo_persistent):
    """
    Get a real session validator service fixture
    """
    user = User()
    service = SessionValidatorService(user, mongo_persistent)
    return service


@pytest.mark.asyncio
async def test_validate_feature_store_id_not_used_in_warehouse(
    session_validator_service, snowflake_details, get_cred
):
    """
    Test validate feature store ID not used in warehouse
    """
    feature_store_name = "snowflake_featurestore"
    status = await session_validator_service.validate_feature_store_id_not_used_in_warehouse(
        feature_store_name=feature_store_name,
        session_type=SourceType.SNOWFLAKE,
        details=snowflake_details,
        get_credential=get_cred,
        users_feature_store_id=None,
    )
    assert status == ValidateStatus.NOT_IN_DWH

    # Create feature store
    feature_store = FeatureStore.create(
        name="snowflake_featurestore",
        source_type=SourceType.SNOWFLAKE,
        details=snowflake_details,
    )

    # Assert that there's a collision now
    with pytest.raises(FeatureStoreSchemaCollisionError):
        await session_validator_service.validate_feature_store_id_not_used_in_warehouse(
            feature_store_name=feature_store_name,
            session_type=SourceType.SNOWFLAKE,
            details=snowflake_details,
            get_credential=get_cred,
            users_feature_store_id=feature_store.id,
        )
