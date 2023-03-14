"""
Session validator integration test class
"""
from unittest import mock

import pytest
import pytest_asyncio

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


@pytest_asyncio.fixture(name="reset_session")
async def get_reset_session_fixture(session_manager, feature_store_details, featurestore_name):
    """
    Resets the session by dropping the working schema table.

    Note that we don't call this by default since most tests won't have to recreate the metadata table.
    For tests in this file, we intentionally want to assert some behaviour around the validation, and as such we
    will manually drop and recreate the metadata as needed.
    """
    session = await session_manager.get_session_with_params(
        feature_store_name=featurestore_name,
        session_type=SourceType.SNOWFLAKE,
        details=feature_store_details,
    )
    await session.execute_query(f"UPDATE METADATA_SCHEMA SET FEATURE_STORE_ID = NULL")


@pytest.mark.skip(reason="skipping while we rollback the default state")
@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
@mock.patch("featurebyte.app.get_persistent")
async def test_validate_feature_store_id_not_used_in_warehouse(
    mock_persistent,
    session_validator_service,
    feature_store_details,
    get_cred,
    featurestore_name,
    reset_session,
    mongo_persistent,
):
    """
    Test validate feature store ID not used in warehouse
    """
    # reset
    _ = reset_session
    mock_persistent.return_value = mongo_persistent[0]
    status = await session_validator_service.validate_feature_store_id_not_used_in_warehouse(
        feature_store_name=featurestore_name,
        session_type=SourceType.SNOWFLAKE,
        details=feature_store_details,
        get_credential=get_cred,
        users_feature_store_id=None,
    )
    assert status == ValidateStatus.NOT_IN_DWH

    # Create feature store
    feature_store = FeatureStore.create(
        name=featurestore_name,
        source_type=SourceType.SNOWFLAKE,
        details=feature_store_details,
    )

    # Assert that there's a collision now
    with pytest.raises(FeatureStoreSchemaCollisionError):
        await session_validator_service.validate_feature_store_id_not_used_in_warehouse(
            feature_store_name=featurestore_name,
            session_type=SourceType.SNOWFLAKE,
            details=feature_store_details,
            get_credential=get_cred,
            users_feature_store_id=feature_store.id,
        )
