"""
Unit tests for OfflineStoreFeatureTableCommentService
"""
from unittest.mock import patch

import pytest
import pytest_asyncio

from featurebyte.service.offline_store_feature_table_comment import ColumnComment, TableComment
from tests.util.helper import deploy_feature, deploy_feature_list


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(enable_feast_integration):
    """
    Enable feast integration for all tests in this module
    """
    _ = enable_feast_integration


@pytest.fixture(autouse=True)
def mock_service_get_version():
    """
    Mock get version
    """
    with patch("featurebyte.service.base_feature_service.get_version", return_value="V231227"):
        yield


@pytest.fixture
def service(app_container):
    """
    OfflineStoreFeatureTableCommentService fixture
    """
    return app_container.offline_store_feature_table_comment_service


@pytest.fixture
def catalog_id(app_container):
    """
    Fixture for catalog_id
    """
    return app_container.catalog_id


@pytest_asyncio.fixture
async def deployed_features(
    app_container,
    float_feature,
    non_time_based_feature,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed features

    Notes:
    - float_feature has primary entity of User
    - non_time_based_feature has primary of Transaction
    - Feature list primary entity is Transaction, since User is a parent of Transaction
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies

    complex_feature = float_feature + non_time_based_feature
    complex_feature.name = "Complex Feature"
    complex_feature.save()
    complex_feature.update_description("This is a complex feature")

    float_feature.save()
    float_feature.update_description("This is a float feature")

    non_time_based_feature.save()

    fixtures = {
        "feature_list": await deploy_feature_list(
            app_container, "my_list", [float_feature.id, non_time_based_feature.id]
        ),
        "float_feature": await deploy_feature(app_container, float_feature),
        "transaction_feature": await deploy_feature(app_container, non_time_based_feature),
        "complex_feature": await deploy_feature(app_container, complex_feature),
    }
    return fixtures


@pytest_asyncio.fixture
async def offline_feature_table(app_container, deployed_features, cust_id_entity):
    """
    Fixture for a regular offline feature table
    """
    _ = deployed_features
    service = app_container.offline_store_feature_table_service
    async for feature_table in service.list_documents_iterator(query_filter={}):
        if (
            feature_table.primary_entity_ids == [cust_id_entity.id]
            and not feature_table.is_entity_lookup
        ):
            return feature_table
    raise AssertionError("Feature table not found")


@pytest_asyncio.fixture
async def offline_feature_table_entity_lookup(app_container, deployed_features, cust_id_entity):
    """
    Fixture for a offline feature table for entity lookup
    """
    _ = deployed_features
    service = app_container.offline_store_feature_table_service
    async for feature_table in service.list_documents_iterator(query_filter={}):
        if feature_table.is_entity_lookup:
            return feature_table
    raise AssertionError("Feature table not found")


@pytest.mark.asyncio
async def test_table_comment__non_entity_lookup(service, catalog_id, offline_feature_table):
    """
    Test comments for a regular feature table
    """
    comment = await service.generate_table_comment(offline_feature_table)
    expected = TableComment(
        table_name=f"fb_entity_cust_id_fjs_1800_300_600_ttl_{catalog_id}",
        comment="This feature table consists of features for primary entity customer (serving name: cust_id). It is updated every 1800 second(s), with a blind spot of 600 second(s) and a time modulo frequency of 300 second(s).",
    )
    assert comment == expected


@pytest.mark.asyncio
async def test_table_comment__entity_lookup(
    service, offline_feature_table_entity_lookup, deployed_features
):
    """
    Test comments for an entity lookup feature table
    """
    feature_list = deployed_features["feature_list"]
    relationship_info_id = feature_list.relationships_info[0].id
    comment = await service.generate_table_comment(offline_feature_table_entity_lookup)
    assert comment == TableComment(
        table_name=f"fb_entity_lookup_{relationship_info_id}",
        comment="This feature table is used to lookup the parent entity of transaction (serving name: transaction_id). It is updated every 86400 second(s), with a blind spot of 0 second(s) and a time modulo frequency of 0 second(s).",
    )


@pytest.mark.asyncio
async def test_column_comment__no_description(service, deployed_features):
    """
    Test feature without description
    """
    comments = await service.generate_column_comments([deployed_features["transaction_feature"]])
    assert comments == []


@pytest.mark.asyncio
async def test_column_comment__with_description(service, catalog_id, deployed_features):
    """
    Test feature with description
    """
    comments = await service.generate_column_comments([deployed_features["float_feature"]])
    assert comments == [
        ColumnComment(
            table_name=f"fb_entity_cust_id_fjs_1800_300_600_ttl_{catalog_id}",
            column_name="sum_1d_V231227",
            comment="This is a float feature",
        )
    ]


@pytest.mark.asyncio
async def test_column_comment__complex_features(service, catalog_id, deployed_features):
    """
    Test descriptions for complex features
    """
    comments = await service.generate_column_comments([deployed_features["complex_feature"]])
    assert comments == [
        ColumnComment(
            table_name=f"fb_entity_transaction_id_fjs_86400_0_0_{catalog_id}",
            column_name="__Complex Feature_V231227__part1",
            comment="This intermediate feature is used to compute the final feature Complex Feature_V231227 (This is a complex feature)",
        ),
        ColumnComment(
            table_name=f"fb_entity_cust_id_fjs_1800_300_600_ttl_{catalog_id}",
            column_name="__Complex Feature_V231227__part0",
            comment="This intermediate feature is used to compute the final feature Complex Feature_V231227 (This is a complex feature)",
        ),
    ]
