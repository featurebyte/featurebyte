"""
Test FeatureService
"""
from unittest.mock import Mock, patch

import pytest
from bson import ObjectId

from featurebyte import FeatureStore, SourceType
from featurebyte.common.model_util import get_version
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.exception import (
    CredentialsError,
    DocumentConflictError,
    DocumentInconsistencyError,
    DuplicatedRegistryError,
    InvalidFeatureRegistryOperationError,
    MissingFeatureRegistryError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature_store import SQLiteDetails, TableDetails
from featurebyte.schema.feature import FeatureCreate
from featurebyte.service.feature import FeatureService


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_fixture(feature_model_dict):
    """
    Feature model dict fixture
    """
    feature_model_dict["_id"] = str(ObjectId())
    feature_model_dict["tabular_source"] = {
        "feature_store_id": str(ObjectId()),
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": "sf_table",
        },
    }
    return feature_model_dict


@pytest.fixture(name="sqlite_feature_store")
def sqlite_feature_store_fixture(mock_get_persistent):
    """
    Sqlite source fixture
    """
    _ = mock_get_persistent
    return FeatureStore(
        name="sqlite_datasource",
        type="sqlite",
        details=SQLiteDetails(filename="some_filename"),
    )


@pytest.mark.asyncio
@patch("featurebyte.session.base.BaseSession.execute_query")
async def test_insert_feature_registry__non_snowflake_feature_store(
    mock_execute_query, feature_model_dict, get_credential, sqlite_feature_store
):
    """
    Test insert_feature_registry function (when feature store is not snowflake)
    """
    feature_store = ExtendedFeatureStoreModel(
        name="sq_feature_store",
        type=SourceType.SQLITE,
        details=SQLiteDetails(filename="some_filename"),
    )
    feature_model_dict["tabular_source"] = {
        "feature_store_id": feature_store.id,
        "table_details": TableDetails(table_name="some_table"),
    }
    feature_model_dict["version"] = VersionIdentifier(name=get_version())
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=sqlite_feature_store)
    await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
        document=feature, get_credential=get_credential
    )
    assert mock_execute_query.call_count == 0


@pytest.mark.asyncio
@patch("featurebyte.session.base.BaseSession.execute_query")
async def test_insert_feature_registry(
    mock_execute_query,
    feature_model_dict,
    snowflake_connector,
    snowflake_feature_store,
    get_credential,
):
    """
    Test insert_feature_registry
    """
    _ = snowflake_connector
    feature_model_dict["version"] = VersionIdentifier(name=get_version())
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
        document=feature,
        get_credential=get_credential,
    )

    match_count = 0
    expected_partial_query = "INSERT INTO FEATURE_REGISTRY"
    for call_args in mock_execute_query.call_args_list:
        if expected_partial_query in call_args.args[0]:
            match_count += 1
    assert match_count > 0


@pytest.mark.asyncio
@patch("featurebyte.service.feature.FeatureManagerSnowflake")
async def test_insert_feature_registry__duplicated_feature_registry_exception(
    mock_feature_manager,
    feature_model_dict,
    get_credential,
    snowflake_connector,
    snowflake_feature_store,
):
    """
    Test insert_feature_registry with duplicated_registry exception
    """
    _ = snowflake_connector
    mock_feature_manager.return_value.insert_feature_registry.side_effect = DuplicatedRegistryError
    feature_model_dict["version"] = VersionIdentifier(name=get_version())
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    with pytest.raises(DocumentConflictError) as exc:
        await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
            document=feature, get_credential=get_credential
        )
    expected_msg = (
        'Feature (name: "sum_30m") has been registered by other feature '
        "at Snowflake feature store."
    )
    assert expected_msg in str(exc)
    assert not mock_feature_manager.return_value.remove_feature_registry.called


@pytest.mark.asyncio
@patch("featurebyte.service.feature.FeatureManagerSnowflake")
async def test_insert_feature_registry__fail_remove_feature_registry_missing_registry(
    mock_feature_manager,
    feature_model_dict,
    get_credential,
    snowflake_connector,
    snowflake_feature_store,
):
    """
    Test insert_feature_registry with duplicated_registry exception
    """
    _ = snowflake_connector
    mock_feature_manager.return_value.insert_feature_registry.side_effect = ValueError
    mock_feature_manager.return_value.remove_feature_registry.side_effect = (
        MissingFeatureRegistryError("Feature version does not exist")
    )
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    with pytest.raises(MissingFeatureRegistryError) as exc:
        await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
            document=feature, get_credential=get_credential
        )
    expected_msg = "Feature version does not exist"
    assert expected_msg in str(exc)
    assert mock_feature_manager.return_value.remove_feature_registry.called


@pytest.mark.asyncio
@patch("featurebyte.service.feature.FeatureManagerSnowflake")
async def test_insert_feature_registry__fail_remove_feature_registry_invalid_registry(
    mock_feature_manager,
    feature_model_dict,
    get_credential,
    snowflake_connector,
    snowflake_feature_store,
):
    """
    Test insert_feature_registry with duplicated_registry exception
    """
    _ = snowflake_connector
    mock_feature_manager.return_value.insert_feature_registry.side_effect = ValueError
    mock_feature_manager.return_value.remove_feature_registry.side_effect = (
        InvalidFeatureRegistryOperationError("Feature version cannot be deleted")
    )
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    with pytest.raises(InvalidFeatureRegistryOperationError) as exc:
        await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
            document=feature, get_credential=get_credential
        )
    expected_msg = "Feature version cannot be deleted"
    assert expected_msg in str(exc)
    assert mock_feature_manager.return_value.remove_feature_registry.called


@pytest.mark.asyncio
@patch("featurebyte.service.feature.FeatureManagerSnowflake")
async def test_insert_feature_registry__fail_remove_feature_registry_other_exception(
    mock_feature_manager,
    feature_model_dict,
    get_credential,
    snowflake_feature_store,
    snowflake_connector,
):
    """
    Test insert_feature_registry with non duplicated feature registry exception
    """
    _ = snowflake_connector
    mock_feature_manager.return_value.insert_feature_registry.side_effect = ValueError
    mock_feature_manager.return_value.remove_feature_registry.side_effect = ValueError

    feature_model_dict["version"] = VersionIdentifier(name=get_version())
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    with pytest.raises(ValueError):
        await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
            document=feature,
            get_credential=get_credential,
        )
    assert mock_feature_manager.return_value.remove_feature_registry.called


@pytest.mark.asyncio
@patch("featurebyte.service.feature.FeatureManagerSnowflake")
async def test_insert_feature_registry__credentials_error(
    mock_feature_manager,
    feature_model_dict,
    sqlite_feature_store,
    snowflake_feature_store,
    snowflake_connector,
):
    """
    Test insert_feature_registry with credentials exception
    """

    async def fake_get_credential(user_id, feature_store_name):
        _ = user_id, feature_store_name
        return {sqlite_feature_store.name: {}}

    _ = snowflake_connector
    mock_feature_manager.return_value.insert_feature_registry.side_effect = ValueError
    feature_model_dict["version"] = VersionIdentifier(name=get_version())
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    with pytest.raises(CredentialsError):
        await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
            document=feature,
            get_credential=fake_get_credential,
        )


@pytest.mark.asyncio
async def test_update_document__inconsistency_error(feature_service, feature):
    """Test feature creation - document inconsistency error"""
    data_dict = feature.dict(by_alias=True)
    data_dict["_id"] = ObjectId()
    data_dict["name"] = "random_name"
    with pytest.raises(DocumentInconsistencyError) as exc:
        await feature_service.create_document(
            data=FeatureCreate(**data_dict),
        )
    expected_msg = (
        'Feature (name: "random_name") object(s) within the same namespace must have the same "name" value '
        '(namespace: "sum_30m", feature: "random_name").'
    )
    assert expected_msg in str(exc.value)
