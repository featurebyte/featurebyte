"""
Test FeatureService
"""
from unittest.mock import Mock, patch

import pytest
from bson import ObjectId

from featurebyte import FeatureStore, SourceType
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.exception import CredentialsError, DocumentConflictError, DuplicatedRegistryError
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_store import SQLiteDetails, TableDetails
from featurebyte.schema.feature import FeatureServiceUpdate
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.service.feature import FeatureService
from tests.unit.service.test_feature_namespace_service import insert_feature_into_persistent


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
async def test_insert_feature_registry__other_exception(
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
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    with pytest.raises(CredentialsError):
        await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
            document=feature,
            get_credential=fake_get_credential,
        )


async def check_states_readiness_change(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    new_feature_id,
    new_flist,
    new_feature_next_readiness,
    expected_default_feature_id,
    expected_default_readiness,
    expected_feature_list_readiness_distribution,
    expected_default_feature_list_id,
    expected_default_feature_list_readiness_distribution,
):
    """Check states after feature readiness get changed"""
    new_feat = await feature_service.update_document(
        document_id=new_feature_id,
        data=FeatureServiceUpdate(readiness=new_feature_next_readiness),
    )
    assert new_feat.feature_list_ids == [new_flist.id]

    # check feature namespace get updated (new feature become the default one)
    updated_feat_namespace = await feature_namespace_service.get_document(
        document_id=new_feat.feature_namespace_id
    )
    assert updated_feat_namespace.default_feature_id == expected_default_feature_id
    assert updated_feat_namespace.readiness == expected_default_readiness

    # check feature list version get updated (new feature list readiness distribution get updated)
    updated_flist = await feature_list_service.get_document(document_id=new_flist.id)
    assert (
        updated_flist.readiness_distribution.__root__
        == expected_feature_list_readiness_distribution
    )

    # check feature list namespace (new feature list becomes the default one)
    updated_flist_namespace = await feature_list_namespace_service.get_document(
        document_id=new_flist.feature_list_namespace_id,
    )
    assert updated_flist_namespace.default_feature_list_id == expected_default_feature_list_id
    assert (
        updated_flist_namespace.readiness_distribution.__root__
        == expected_default_feature_list_readiness_distribution
    )


@pytest.mark.asyncio
async def test_update_document__auto_default_version_mode(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    feature,
    feature_list,
    user,
):
    """Test update document (auto default version mode)"""
    persistent = feature_service.persistent

    # add a deprecated feature version first
    new_feature_id = await insert_feature_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix="_1",
        readiness=FeatureReadiness.DEPRECATED.value,
    )
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(feature_id=new_feature_id),
    )
    assert len(feat_namespace.feature_ids) == 2
    assert new_feature_id in feat_namespace.feature_ids
    assert feat_namespace.default_version_mode == "AUTO"
    assert feat_namespace.default_feature_id == feature.id
    assert feat_namespace.readiness == "DRAFT"

    # create another feature list version
    new_flist = await feature_list_service.create_document(
        data=FeatureListCreate(
            feature_ids=[new_feature_id],
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            name="sf_feature_list",
            version="V220914",
        )
    )
    flist_namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id,
    )
    assert len(flist_namespace.feature_list_ids) == 2
    assert new_flist.id in flist_namespace.feature_list_ids
    assert new_flist.feature_list_namespace_id == feature_list.feature_list_namespace_id
    assert flist_namespace.default_version_mode == "AUTO"
    assert flist_namespace.default_feature_list_id == feature_list.id
    assert flist_namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]

    # upgrade new feature's readiness level to production
    await check_states_readiness_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_flist=new_flist,
        new_feature_next_readiness=FeatureReadiness.PRODUCTION_READY,
        expected_default_feature_id=new_feature_id,
        expected_default_readiness=FeatureReadiness.PRODUCTION_READY,
        expected_feature_list_readiness_distribution=[
            {"readiness": "PRODUCTION_READY", "count": 1}
        ],
        expected_default_feature_list_id=new_flist.id,
        expected_default_feature_list_readiness_distribution=[
            {"readiness": "PRODUCTION_READY", "count": 1}
        ],
    )

    # downgrade new feature's readiness from production ready to deprecated
    await check_states_readiness_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_flist=new_flist,
        new_feature_next_readiness=FeatureReadiness.DEPRECATED,
        expected_default_feature_id=feature.id,
        expected_default_readiness=FeatureReadiness.DRAFT,
        expected_feature_list_readiness_distribution=[{"readiness": "DEPRECATED", "count": 1}],
        expected_default_feature_list_id=feature_list.id,
        expected_default_feature_list_readiness_distribution=[{"readiness": "DRAFT", "count": 1}],
    )


@pytest.mark.asyncio
async def test_update_document__manual_default_version_mode(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    feature,
    feature_list,
    user,
):
    """Test update document (manual default version mode)"""
    persistent = feature_service.persistent

    # update feature namespace default version mode to manual first
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(default_version_mode="MANUAL"),
    )
    assert feat_namespace.default_version_mode == "MANUAL"
    flist_namespace = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceServiceUpdate(default_version_mode="MANUAL"),
    )
    assert flist_namespace.default_version_mode == "MANUAL"

    # add a production ready feature version first
    new_feature_id = await insert_feature_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix="_1",
        readiness=FeatureReadiness.PRODUCTION_READY.value,
    )
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(feature_id=new_feature_id),
    )
    assert feat_namespace.default_feature_id == feature.id
    assert feat_namespace.readiness == "DRAFT"

    # create another new feature list version
    new_flist = await feature_list_service.create_document(
        data=FeatureListCreate(
            feature_ids=[new_feature_id],
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            name="sf_feature_list",
            version="V220914",
        )
    )
    flist_namespace = await feature_list_namespace_service.get_document(
        document_id=new_flist.feature_list_namespace_id,
    )
    assert flist_namespace.default_feature_list_id == feature_list.id
    assert flist_namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]

    # update feature namespace default version mode to auto first
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(default_version_mode="AUTO"),
    )
    assert feat_namespace.default_feature_id == new_feature_id
    assert feat_namespace.readiness == "PRODUCTION_READY"

    # check feature list namespace not affected
    flist_namespace = await feature_list_namespace_service.get_document(
        document_id=new_flist.feature_list_namespace_id,
    )
    assert flist_namespace.default_feature_list_id == feature_list.id
    assert flist_namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]

    # update feature list namespace default version to auto
    flist_namespace = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceServiceUpdate(default_version_mode="AUTO"),
    )
    assert flist_namespace.default_feature_list_id == new_flist.id
    assert flist_namespace.readiness_distribution.__root__ == [
        {"readiness": "PRODUCTION_READY", "count": 1}
    ]
