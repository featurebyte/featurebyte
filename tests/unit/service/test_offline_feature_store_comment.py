"""
Unit tests for OfflineStoreFeatureTableCommentService
"""

from unittest.mock import Mock, call, patch

import pytest
import pytest_asyncio

from featurebyte.service.offline_store_feature_table_comment import ColumnComment, TableComment
from tests.util.helper import deploy_feature, deploy_feature_ids


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


@pytest_asyncio.fixture()
async def deployed_features(
    app_container,
    float_feature,
    non_time_based_feature,
    feature_without_entity,
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
    feature_without_entity.save()

    fixtures = {
        "feature_list": await deploy_feature_ids(
            app_container, "my_list", [float_feature.id, non_time_based_feature.id]
        ),
        "float_feature": await deploy_feature(app_container, float_feature),
        "transaction_feature": await deploy_feature(app_container, non_time_based_feature),
        "feature_without_entity": await deploy_feature(app_container, feature_without_entity),
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
            and feature_table.entity_lookup_info is None
        ):
            return feature_table
    raise AssertionError("Feature table not found")


@pytest_asyncio.fixture
async def offline_feature_table_no_primary_entity(app_container, deployed_features):
    """
    Fixture for an offline feature table without primary entity
    """
    _ = deployed_features
    service = app_container.offline_store_feature_table_service
    async for feature_table in service.list_documents_iterator(query_filter={}):
        if not feature_table.primary_entity_ids:
            return feature_table
    raise AssertionError("Feature table not found")


@pytest.mark.asyncio
async def test_apply_comments(service, mock_snowflake_session):
    """
    Test apply_comments uses session object correctly
    """
    with patch(
        "featurebyte.service.offline_store_feature_table_comment.SessionManagerService.get_feature_store_session"
    ) as patched:
        patched.return_value = mock_snowflake_session
        await service.apply_comments(
            Mock(name="mock_feature_store_model"),
            [
                TableComment(
                    table_name="tab_1",
                    comment="comment for tab_1",
                ),
                ColumnComment(
                    table_name="tab_2",
                    column_name="col_2",
                    comment="comment for tab_2's col_2",
                ),
            ],
        )
    assert mock_snowflake_session.comment_table.call_args_list == [
        call("tab_1", "comment for tab_1")
    ]
    assert mock_snowflake_session.comment_column.call_args_list == [
        call("tab_2", "col_2", "comment for tab_2's col_2")
    ]


@pytest.mark.asyncio
async def test_apply_comments__unexpected_exception(service, mock_snowflake_session, caplog):
    """
    Test apply_comments uses session object correctly
    """
    with patch(
        "featurebyte.service.offline_store_feature_table_comment.SessionManagerService.get_feature_store_session"
    ) as patched:
        patched.return_value = mock_snowflake_session
        mock_snowflake_session.comment_table.side_effect = Exception("Unexpected exception")
        mock_snowflake_session.comment_column.side_effect = Exception(
            "Another unexpected exception"
        )
        await service.apply_comments(
            Mock(name="mock_feature_store_model"),
            [
                TableComment(
                    table_name="tab_1",
                    comment="comment for tab_1",
                ),
                ColumnComment(
                    table_name="tab_2",
                    column_name="col_2",
                    comment="comment for tab_2's col_2",
                ),
            ],
        )

    assert len(caplog.records) == 2
    table_comment_record = caplog.records[0]
    assert table_comment_record.levelname == "ERROR"
    assert table_comment_record.message == "Failed to add comment: Unexpected exception"
    assert table_comment_record.extra == {"table_name": "tab_1"}

    column_comment_record = caplog.records[1]
    assert column_comment_record.levelname == "ERROR"
    assert column_comment_record.message == "Failed to add comment: Another unexpected exception"
    assert column_comment_record.extra == {"table_name": "tab_2", "column_name": "col_2"}


@pytest.mark.asyncio
async def test_table_comment(
    service,
    deployed_features,
    offline_feature_table,
    offline_feature_table_no_primary_entity,
):
    """
    Test comments for offline feature tables
    """
    _ = deployed_features

    # Regular offline feature tables
    comment = await service.generate_table_comment(offline_feature_table)
    expected = TableComment(
        table_name="cat1_cust_id_30m",
        comment="This feature table consists of features for primary entity customer (serving name: cust_id). It is updated every 1800 second(s), with a blind spot of 600 second(s) and a time modulo frequency of 300 second(s).",
    )
    assert comment == expected

    # Regular feature table without primary entity
    comment = await service.generate_table_comment(offline_feature_table_no_primary_entity)
    expected = TableComment(
        table_name="cat1__no_entity_1d",
        comment="This feature table consists of features without a primary entity. It is updated every 86400 second(s), with a blind spot of 7200 second(s) and a time modulo frequency of 3600 second(s).",
    )
    assert comment == expected


@pytest.mark.asyncio
async def test_column_comment(service, deployed_features):
    """
    Test feature without description
    """
    # No description
    comments = await service.generate_column_comments([deployed_features["transaction_feature"]])
    assert comments == []

    # With description
    comments = await service.generate_column_comments([deployed_features["float_feature"]])
    assert comments == [
        ColumnComment(
            table_name="cat1_cust_id_30m",
            column_name="sum_1d_V231227",
            comment="This is a float feature",
        )
    ]

    # Complex feature
    comments = await service.generate_column_comments([deployed_features["complex_feature"]])
    assert comments == [
        ColumnComment(
            table_name="cat1_transaction_id_1d",
            column_name="__Complex Feature_V231227__part1",
            comment=(
                "This intermediate feature is used to compute the feature Complex Feature (version: V231227). "
                "Description of Complex Feature: This is a complex feature"
            ),
        ),
        ColumnComment(
            table_name="cat1_cust_id_30m",
            column_name="__Complex Feature_V231227__part0",
            comment=(
                "This intermediate feature is used to compute the feature Complex Feature (version: V231227). "
                "Description of Complex Feature: This is a complex feature"
            ),
        ),
    ]


@pytest.mark.asyncio
async def test_offline_feature_table_retrieval(app_container, offline_feature_table):
    """Test offline feature table retrieval"""
    offline_table_service = app_container.offline_store_feature_table_service
    table = await offline_table_service.get_or_create_document(offline_feature_table)
    assert table.id == offline_feature_table.id
    assert table.name == offline_feature_table.name


@pytest.mark.asyncio
async def test_offline_feature_table_no_primary_entity_retrieval(
    app_container, offline_feature_table_no_primary_entity
):
    """Test offline feature table without primary entity retrieval"""
    offline_table_service = app_container.offline_store_feature_table_service
    table = await offline_table_service.get_or_create_document(
        offline_feature_table_no_primary_entity
    )
    assert table.id == offline_feature_table_no_primary_entity.id
    assert table.name == offline_feature_table_no_primary_entity.name
