"""
Common test fixtures used across api test directories
"""

import textwrap
import time
from datetime import datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from bson.objectid import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte import (
    Catalog,
    RequestColumn,
    TargetNamespace,
    TimeInterval,
    TimestampSchema,
    UseCase,
)
from featurebyte.api.base_table import TableColumn
from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.item_table import ItemTable
from featurebyte.api.source_table import SourceTable
from featurebyte.config import Configurations
from featurebyte.models.feature_store import TableStatus
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn


@pytest.fixture()
def expected_snowflake_table_preview_query() -> str:
    """
    Expected preview_sql output
    """
    return textwrap.dedent(
        """
        SELECT
          "col_int" AS "col_int",
          "col_float" AS "col_float",
          "col_char" AS "col_char",
          CAST("col_text" AS VARCHAR) AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
          CAST("created_at" AS VARCHAR) AS "created_at",
          "cust_id" AS "cust_id"
        FROM "sf_database"."sf_schema"."sf_table"
        LIMIT 10
        """
    ).strip()


@pytest.fixture()
def expected_item_table_preview_query() -> str:
    """
    Expected preview_sql output
    """
    return textwrap.dedent(
        """
        SELECT
          "event_id_col" AS "event_id_col",
          CAST("item_id_col" AS VARCHAR) AS "item_id_col",
          CAST("item_type" AS VARCHAR) AS "item_type",
          "item_amount" AS "item_amount",
          CAST("created_at" AS VARCHAR) AS "created_at",
          CAST("event_timestamp" AS VARCHAR) AS "event_timestamp"
        FROM "sf_database"."sf_schema"."items_table"
        LIMIT 10
        """
    ).strip()


@pytest.fixture()
def expected_time_series_table_preview_query() -> str:
    """
    Expected preview_sql output
    """
    return textwrap.dedent(
        """
        SELECT
          "col_int" AS "col_int",
          "col_float" AS "col_float",
          "col_char" AS "col_char",
          CAST("col_text" AS VARCHAR) AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          CAST("date" AS VARCHAR) AS "date",
          CAST("created_at" AS VARCHAR) AS "created_at",
          "store_id" AS "store_id",
          CAST("another_timestamp_col" AS VARCHAR) AS "another_timestamp_col"
        FROM "sf_database"."sf_schema"."time_series_table"
        LIMIT 10
        """
    ).strip()


@pytest.fixture(name="catalog")
def catalog_fixture(snowflake_feature_store):
    """
    Catalog object fixture
    """
    catalog = Catalog.create(name="catalog", feature_store_name=snowflake_feature_store.name)
    return catalog


@pytest.fixture(name="snowflake_database_table")
def snowflake_database_table_fixture(snowflake_data_source):
    """
    SourceTable object fixture (using config object)
    """
    yield snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="sf_table",
    )


@pytest.fixture(name="snowflake_event_table")
def snowflake_event_table_fixture(
    snowflake_database_table,
    mock_get_persistent,
    snowflake_event_table_id,
    catalog,
):
    """
    Snowflake EventTable object fixture (using config object)
    """
    _ = catalog, mock_get_persistent

    yield snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
        _id=snowflake_event_table_id,
    )


@pytest.fixture(name="saved_event_table")
def saved_event_table_fixture(snowflake_event_table):
    """
    Saved event table fixture
    """
    previous_id = snowflake_event_table.id
    assert snowflake_event_table.saved is True
    assert snowflake_event_table.id == previous_id
    assert snowflake_event_table.status == TableStatus.PUBLIC_DRAFT
    assert isinstance(snowflake_event_table.created_at, datetime)
    assert isinstance(snowflake_event_table.tabular_source.feature_store_id, ObjectId)

    # test list event table
    event_table_list = EventTable.list()
    assert_frame_equal(
        event_table_list,
        pd.DataFrame({
            "name": [snowflake_event_table.name],
            "type": [snowflake_event_table.type],
            "status": [snowflake_event_table.status],
            "entities": event_table_list["entities"],
            "created_at": [snowflake_event_table.created_at.isoformat()],
        }),
    )
    yield snowflake_event_table


@pytest.fixture(name="saved_dimension_table")
def saved_dimension_table_fixture(snowflake_dimension_table, catalog):
    """
    Saved dimension table fixture
    """
    _ = catalog
    previous_id = snowflake_dimension_table.id
    assert snowflake_dimension_table.saved is True
    assert snowflake_dimension_table.id == previous_id
    assert snowflake_dimension_table.status == TableStatus.PUBLIC_DRAFT
    assert isinstance(snowflake_dimension_table.created_at, datetime)
    assert isinstance(snowflake_dimension_table.tabular_source.feature_store_id, ObjectId)

    yield snowflake_dimension_table


@pytest.fixture(name="snowflake_scd_table")
def snowflake_scd_table_fixture(
    snowflake_database_table_scd_table,
    snowflake_scd_table_id,
    catalog,
):
    """SCDTable object fixture"""
    _ = catalog

    scd_table = snowflake_database_table_scd_table.create_scd_table(
        name="sf_scd_table",
        natural_key_column="col_text",
        surrogate_key_column="col_int",
        effective_timestamp_column="effective_timestamp",
        end_timestamp_column="end_timestamp",
        current_flag_column="is_active",
        _id=snowflake_scd_table_id,
    )
    assert scd_table.frame.node.parameters.id == scd_table.id
    yield scd_table


@pytest.fixture(name="saved_scd_table")
def saved_scd_table_fixture(snowflake_scd_table, catalog):
    """
    Saved SCD table fixture
    """
    _ = catalog
    previous_id = snowflake_scd_table.id
    assert snowflake_scd_table.saved is True
    assert snowflake_scd_table.id == previous_id
    assert snowflake_scd_table.status == TableStatus.PUBLIC_DRAFT
    assert isinstance(snowflake_scd_table.created_at, datetime)
    assert isinstance(snowflake_scd_table.tabular_source.feature_store_id, ObjectId)

    yield snowflake_scd_table


@pytest.fixture(name="saved_time_series_table")
def saved_time_series_table_fixture(snowflake_time_series_table, catalog):
    """
    Saved Time Series table fixture
    """
    _ = catalog
    previous_id = snowflake_time_series_table.id
    assert snowflake_time_series_table.saved is True
    assert snowflake_time_series_table.id == previous_id
    assert snowflake_time_series_table.status == TableStatus.PUBLIC_DRAFT
    assert isinstance(snowflake_time_series_table.created_at, datetime)
    assert isinstance(snowflake_time_series_table.tabular_source.feature_store_id, ObjectId)
    yield snowflake_time_series_table


@pytest.fixture(name="snowflake_time_series_table_with_tz_offset_column")
def snowflake_time_series_table_fixture(
    snowflake_database_time_series_table,
    catalog,
    cust_id_entity,
    transaction_entity,
    mock_detect_and_update_column_dtypes,
):
    """TimeSeriesTable object fixture"""
    _ = catalog, mock_detect_and_update_column_dtypes
    time_series_table = snowflake_database_time_series_table.create_time_series_table(
        name="sf_time_series_table",
        series_id_column="col_int",
        reference_datetime_column="date",
        reference_datetime_schema=TimestampSchema(
            timezone=TimeZoneColumn(column_name="col_text", type="offset"),
            format_string="YYYY-MM-DD HH24:MI:SS",
        ),
        time_interval=TimeInterval(value=1, unit="DAY"),
        record_creation_timestamp_column="created_at",
        description="test time series table",
        _id=ObjectId("63f9506dd478b941271ed957"),
    )
    time_series_table.store_id.as_entity(cust_id_entity.name)
    time_series_table.col_int.as_entity(transaction_entity.name)
    assert time_series_table.frame.node.parameters.id == time_series_table.id
    yield time_series_table


@pytest.fixture(name="snowflake_scd_table_v2")
def snowflake_scd_table_v2_fixture(snowflake_data_source, catalog):
    """SCDTable object fixture"""
    _ = catalog
    scd_table = snowflake_data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="scd_table_v2",
    ).create_scd_table(
        name="sf_scd_table_v2",
        natural_key_column="col_text",
        surrogate_key_column="col_int",
        effective_timestamp_column="event_timestamp",
        end_timestamp_column="end_timestamp",
        current_flag_column="is_active",
    )
    yield scd_table


@pytest.fixture(name="snowflake_item_table")
def snowflake_item_table_fixture(
    snowflake_database_table_item_table,
    mock_get_persistent,
    snowflake_item_table_id,
    saved_event_table,
    cust_id_entity,
    arbitrary_default_feature_job_setting,
):
    """
    Snowflake ItemTable object fixture (using config object)
    """
    _ = mock_get_persistent
    saved_event_table.update_default_feature_job_setting(arbitrary_default_feature_job_setting)
    saved_event_table["cust_id"].as_entity(cust_id_entity.name)
    item_table = snowflake_database_table_item_table.create_item_table(
        name="sf_item_table",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_table_name=saved_event_table.name,
        _id=snowflake_item_table_id,
    )
    yield item_table


@pytest.fixture(name="event_table_with_cron_feature_job_setting")
def event_table_with_cron_feature_job_setting_fixture(saved_event_table, cust_id_entity):
    """
    Fixture for an EventTable with a CronFeatureJobSetting as the default feature job setting
    """
    saved_event_table.update_default_feature_job_setting(
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 0 * * *",
            reference_timezone="Etc/UTC",
            blind_spot="600s",
        )
    )
    saved_event_table.cust_id.as_entity(cust_id_entity.name)
    yield saved_event_table


@pytest.fixture(name="item_table_with_cron_feature_job_setting")
def item_table_with_cron_feature_job_setting_fixture(
    snowflake_item_table,
    saved_event_table,
):
    """
    Fixture for an ItemTable whose EventTable has a CronFeatureJobSetting as the default feature job
    setting.
    """
    saved_event_table.update_default_feature_job_setting(
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 0 * * *",
            reference_timezone="Etc/UTC",
            blind_spot="600s",
        )
    )
    time.sleep(1)
    yield snowflake_item_table


@pytest.fixture()
def item_entity_id():
    """
    Item entity id fixture
    """
    # Note that these IDs are part of the groupby node parameters, it will affect the node hash calculation.
    # Altering these IDs may cause the SDK code generation to fail (due to the generated code could slightly
    # be different).
    return ObjectId("63f9506dd478b941271ed957")


@pytest.fixture
def item_entity(item_entity_id, catalog):
    """
    Item entity fixture
    """
    _ = catalog
    entity = Entity(name="item", serving_names=["item_id"], _id=item_entity_id)
    entity.save()
    return entity


@pytest.fixture(name="saved_item_table")
def saved_item_table_fixture(snowflake_feature_store, snowflake_item_table, item_entity):
    """
    Saved ItemTable fixture
    """
    _ = snowflake_feature_store

    previous_id = snowflake_item_table.id
    assert snowflake_item_table.saved
    assert snowflake_item_table.id == previous_id
    assert snowflake_item_table.status == TableStatus.PUBLIC_DRAFT
    assert isinstance(snowflake_item_table.created_at, datetime)
    assert isinstance(snowflake_item_table.tabular_source.feature_store_id, ObjectId)

    item_id_col = snowflake_item_table.item_id_col
    assert isinstance(item_id_col, TableColumn)
    snowflake_item_table.item_id_col.as_entity(item_entity.name)
    assert snowflake_item_table.item_id_col.info.entity_id == item_entity.id

    # test list event table
    item_table_list = ItemTable.list()
    assert_frame_equal(
        item_table_list,
        pd.DataFrame({
            "name": [snowflake_item_table.name],
            "type": [snowflake_item_table.type],
            "status": [snowflake_item_table.status],
            "entities": [["item"]],
            "created_at": [snowflake_item_table.created_at.isoformat()],
        }),
    )

    yield snowflake_item_table


@pytest.fixture(name="snowflake_item_view")
def snowflake_item_view_fixture(snowflake_item_table):
    """
    ItemView fixture
    """
    item_view = snowflake_item_table.get_view(event_suffix="_event_table")
    yield item_view


@pytest.fixture(name="snowflake_dimension_view")
def snowflake_dimension_view_fixture(snowflake_dimension_table):
    """
    DimensionView fixture
    """
    dimension_view = snowflake_dimension_table.get_view()
    yield dimension_view


@pytest.fixture(name="snowflake_scd_view")
def snowflake_scd_view_fixture(snowflake_scd_table):
    """
    SCDView fixture
    """
    scd_view = snowflake_scd_table.get_view()
    yield scd_view


@pytest.fixture(name="snowflake_change_view")
def snowflake_change_view(snowflake_scd_table):
    """
    ChangeView fixture
    """
    change_view = snowflake_scd_table.get_change_view("col_int")
    yield change_view


@pytest.fixture(name="snowflake_event_view")
def snowflake_event_view_fixture(
    snowflake_event_table, config, arbitrary_default_feature_job_setting
):
    """
    EventTable object fixture
    """
    _ = config
    snowflake_event_table.update_default_feature_job_setting(
        feature_job_setting=arbitrary_default_feature_job_setting
    )
    event_view = snowflake_event_table.get_view()
    yield event_view


@pytest.fixture(name="snowflake_time_series_view")
def snowflake_time_series_view_fixture(
    snowflake_time_series_table, config, arbitrary_default_cron_feature_job_setting
):
    """
    TimeSeriesTable object fixture
    """
    _ = config
    snowflake_time_series_table.update_default_feature_job_setting(
        feature_job_setting=arbitrary_default_cron_feature_job_setting
    )
    time_series_view = snowflake_time_series_table.get_view()
    yield time_series_view


@pytest.fixture(name="feature_job_logs", scope="session")
def feature_job_logs_fixture():
    """
    Feature job log records
    """
    job_logs = pd.read_csv("tests/fixtures/feature_job_status/job_logs.csv")
    job_logs["CREATED_AT"] = pd.to_datetime(job_logs["CREATED_AT"])
    return job_logs


@pytest.fixture(scope="function")
def mock_post_async_task():
    """Mock post_async_task"""

    def post_async_task(route, payload, **kwargs):
        """Mock post_async_task"""
        client = Configurations().get_client()
        return client.post(url=route, json=payload)

    with patch("featurebyte.api.feature_list.FeatureList.post_async_task") as mock_post_async_task:
        mock_post_async_task.side_effect = post_async_task
        yield mock_post_async_task


@pytest.fixture(scope="function", autouse=True)
def mock_detect_and_update_column_dtypes_fixture(mock_detect_and_update_column_dtypes):
    """Mock detect_and_update_column_dtypes"""
    yield mock_detect_and_update_column_dtypes


@pytest.fixture(name="batch_request_table_from_source")
def batch_request_table_from_source_fixture(
    snowflake_database_table, snowflake_execute_query_for_materialized_table, catalog
):
    """Batch request table from source table fixture"""
    _ = catalog, snowflake_execute_query_for_materialized_table
    return snowflake_database_table.create_batch_request_table(
        "batch_request_table_from_source_table"
    )


@pytest.fixture(name="batch_request_table_from_view")
def batch_request_table_from_view_fixture(
    snowflake_event_view,
    snowflake_execute_query_for_materialized_table,
):
    """Batch request table from view fixture"""
    return snowflake_event_view.create_batch_request_table("batch_request_table_from_event_view")


@pytest.fixture(name="use_case")
def use_case_fixture(catalog, float_target, context):
    """
    UseCase fixture
    """
    _ = catalog
    float_target.save()
    target_namespace = TargetNamespace.get(float_target.name)

    use_case = UseCase(
        name="test_use_case",
        target_id=float_target.id,
        target_namespace_id=target_namespace.id,
        context_id=context.id,
        description="test_use_case description",
    )
    previous_id = use_case.id
    assert use_case.saved is False
    use_case.save()
    assert use_case.saved is True
    assert use_case.id == previous_id
    yield use_case


@pytest.fixture(name="deployment")
def deployment_fixture(float_feature, use_case):
    """Deployment fixture"""
    feature_list = FeatureList([float_feature], name="my_feature_list")
    feature_list.save()

    deployment = feature_list.deploy(make_production_ready=True, use_case_name=use_case.name)
    return deployment


@pytest.fixture(name="ttl_non_ttl_composite_feature")
def ttl_non_ttl_composite_feature_fixture(float_feature, non_time_based_feature):
    """Fixture for a composite feature"""
    ttl_component = 2 * (float_feature + 100)
    non_ttl_component = 3 - (non_time_based_feature + 100)
    feature = ttl_component + non_ttl_component
    feature.name = "feature"
    return feature


@pytest.fixture(name="req_col_day_diff_feature")
def req_col_feature_fixture(latest_event_timestamp_feature):
    """Fixture for a feature that uses a request column"""
    feature = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    feature.name = "req_col_feature"
    return feature


@pytest.fixture(name="mock_source_table")
def mock_source_table_fixture():
    """
    Patches the underlying SourceTable in MaterializedTableMixin
    """
    mock_source_table = Mock(
        name="mock_source_table",
        spec=SourceTable,
        preview=Mock(return_value=pd.DataFrame()),
        sample=Mock(return_value=pd.DataFrame()),
        describe=Mock(return_value=pd.DataFrame()),
    )
    mock_feature_store = Mock(
        name="mock_feature_store",
        get_data_source=Mock(
            return_value=Mock(
                name="mock_data_source",
                get_source_table=Mock(return_value=mock_source_table),
            )
        ),
    )
    with patch(
        "featurebyte.api.materialized_table.FeatureStore.get_by_id", return_value=mock_feature_store
    ):
        yield mock_source_table


@pytest.fixture(name="patch_alive_progress", autouse=True)
def patch_alive_progress_fixture():
    """
    Patch the alive progress method
    """
    patched = {}
    patch_targets = [
        "featurebyte.api.utils",
        "featurebyte.api.feature_group",
        "featurebyte.api.mixin",
    ]
    started_patchers = []
    for module in patch_targets:
        patcher = patch(f"{module}.alive_bar")
        patched[module] = patcher.start()
        started_patchers.append(patcher)
    yield patched
    for patcher in started_patchers:
        patcher.stop()
