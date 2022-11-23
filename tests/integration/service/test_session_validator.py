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
    persistent, _ = mongo_persistent
    service = SessionValidatorService(user, persistent)
    return service


@pytest.mark.asyncio
@pytest.fixture(name="session_refresher")
async def get_reset_session_fixture(
    session_manager, snowflake_details, snowflake_featurestore_name, get_cred
):
    """
    Resets the session by dropping the working schema table.

    Note that we don't call this by default since most tests won't have to recreate the metadata table.
    For tests in this file, we intentionally want to assert some behaviour around the validation, and as such we
    will manually drop and recreate the metadata as needed.
    """
    session = await session_manager.get_session_with_params(
        feature_store_name=snowflake_featurestore_name,
        session_type=SourceType.SNOWFLAKE,
        details=snowflake_details,
    )
    await session.execute_query(f"DROP TABLE IF EXISTS METADATA_SCHEMA")
    yield


@pytest.mark.asyncio
async def test_validate_feature_store_id_not_used_in_warehouse(
    session_validator_service,
    snowflake_details,
    get_cred,
    snowflake_featurestore_name,
    session_refresher,
):
    """
    Test validate feature store ID not used in warehouse
    """
    # reset
    await session_validator_service.persistent.delete_one(
        collection_name="feature_store", query_filter={}, user_id=User().id
    )
    _ = session_refresher
    status = await session_validator_service.validate_feature_store_id_not_used_in_warehouse(
        feature_store_name=snowflake_featurestore_name,
        session_type=SourceType.SNOWFLAKE,
        details=snowflake_details,
        get_credential=get_cred,
        users_feature_store_id=None,
    )
    assert status == ValidateStatus.NOT_IN_DWH

    # Create feature store
    feature_store = FeatureStore.create(
        name=snowflake_featurestore_name,
        source_type=SourceType.SNOWFLAKE,
        details=snowflake_details,
    )

    # Assert that there's a collision now
    with pytest.raises(FeatureStoreSchemaCollisionError):
        await session_validator_service.validate_feature_store_id_not_used_in_warehouse(
            feature_store_name=snowflake_featurestore_name,
            session_type=SourceType.SNOWFLAKE,
            details=snowflake_details,
            get_credential=get_cred,
            users_feature_store_id=feature_store.id,
        )
