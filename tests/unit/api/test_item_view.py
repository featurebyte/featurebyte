"""
Unit test for ItemView class
"""
import textwrap

import pytest

from featurebyte.api.feature import Feature
from featurebyte.api.item_view import ItemView
from featurebyte.core.series import Series
from featurebyte.exception import RecordCreationException, RepeatedColumnNamesError
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import get_node


class TestItemView(BaseViewTestSuite):
    """
    ItemView test suite
    """

    protected_columns = ["event_id_col", "item_id_col", "event_timestamp"]
    view_type = ViewType.ITEM_VIEW
    col = "item_amount"
    factory_method = ItemView.from_item_data
    view_class = ItemView

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.event_id_column == view_under_test.event_id_column
        assert row_subset.item_id_column == view_under_test.item_id_column
        assert row_subset.event_data_id == view_under_test.event_data_id
        assert row_subset.event_view.dict() == view_under_test.event_view.dict()
        assert (
            row_subset.default_feature_job_setting.dict()
            == view_under_test.default_feature_job_setting.dict()
        )

    def get_test_view_column_get_item_series_fixture_override(self, view_under_test):
        return {
            "mask": view_under_test[self.col] > 1000,
            "expected_edges": [
                {"source": "input_1", "target": "join_1"},
                {"source": "input_2", "target": "join_1"},
                {"source": "join_1", "target": "project_1"},
                {"source": "project_1", "target": "gt_1"},
                {"source": "project_1", "target": "filter_1"},
                {"source": "gt_1", "target": "filter_1"},
            ],
        }

    def test_setitem__str_key_series_value(self, view_under_test):
        # Override this test to be a no-op since we have a custom test defined below.
        return


def test_from_item_data__auto_join_columns(
    snowflake_item_data,
    snowflake_event_data_id,
    snowflake_item_data_id,
):
    """
    Test ItemView automatically joins timestamp column and entity columns from related EventData
    """
    view = ItemView.from_item_data(snowflake_item_data, event_suffix="_event_table")
    view_dict = view.dict()

    # Check node is a join node which will make event timestamp and EventData entities available
    node_dict = get_node(view_dict["graph"], view_dict["node_name"])
    assert node_dict == {
        "name": "join_1",
        "type": "join",
        "output_type": "frame",
        "parameters": {
            "left_on": "col_int",
            "right_on": "event_id_col",
            "left_input_columns": ["event_timestamp", "cust_id"],
            "left_output_columns": ["event_timestamp_event_table", "cust_id_event_table"],
            "right_input_columns": [
                "event_id_col",
                "item_id_col",
                "item_type",
                "item_amount",
                "created_at",
                "event_timestamp",
            ],
            "right_output_columns": [
                "event_id_col",
                "item_id_col",
                "item_type",
                "item_amount",
                "created_at",
                "event_timestamp",
            ],
            "join_type": "inner",
            "scd_parameters": None,
        },
    }

    # Check Frame attributes
    assert set(view.tabular_data_ids) == {snowflake_item_data_id, snowflake_event_data_id}
    assert view.columns == [
        "event_id_col",
        "item_id_col",
        "item_type",
        "item_amount",
        "created_at",
        "event_timestamp",
        "event_timestamp_event_table",
        "cust_id_event_table",
    ]
    assert view.dtypes.to_dict() == {
        "event_id_col": "INT",
        "item_id_col": "VARCHAR",
        "item_type": "VARCHAR",
        "item_amount": "FLOAT",
        "created_at": "TIMESTAMP_TZ",
        "event_timestamp": "TIMESTAMP_TZ",
        "event_timestamp_event_table": "TIMESTAMP_TZ",
        "cust_id_event_table": "INT",
    }

    # Check preview SQL
    preview_sql = view.preview_sql()
    expected_sql = textwrap.dedent(
        """
        SELECT
          CAST(L."event_timestamp" AS VARCHAR) AS "event_timestamp_event_table",
          L."cust_id" AS "cust_id_event_table",
          R."event_id_col" AS "event_id_col",
          R."item_id_col" AS "item_id_col",
          R."item_type" AS "item_type",
          R."item_amount" AS "item_amount",
          CAST(R."created_at" AS VARCHAR) AS "created_at",
          CAST(R."event_timestamp" AS VARCHAR) AS "event_timestamp"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "event_timestamp" AS "event_timestamp",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."sf_table"
        ) AS L
        INNER JOIN (
          SELECT
            "event_id_col" AS "event_id_col",
            "item_id_col" AS "item_id_col",
            "item_type" AS "item_type",
            "item_amount" AS "item_amount",
            "created_at" AS "created_at",
            "event_timestamp" AS "event_timestamp"
          FROM "sf_database"."sf_schema"."items_table"
        ) AS R
          ON L."col_int" = R."event_id_col"
        LIMIT 10
        """
    ).strip()
    assert preview_sql == expected_sql


def test_has_event_timestamp_column(snowflake_item_view):
    """
    Test that ItemView inherits the event timestamp column from EventView
    """
    assert snowflake_item_view.timestamp_column == "event_timestamp_event_table"


def test_default_feature_job_setting(snowflake_item_view, snowflake_event_data):
    """
    Test that ItemView inherits the same feature job setting from the EventData
    """
    assert (
        snowflake_item_view.default_feature_job_setting
        == snowflake_event_data.default_feature_job_setting
    )


def test_setitem__str_key_series_value(
    snowflake_item_view,
    snowflake_event_data,
    snowflake_item_data,
):
    """
    Test assigning Series object to ItemView
    """
    double_value = snowflake_item_view["item_amount"] * 2
    assert isinstance(double_value, Series)
    snowflake_item_view["double_value"] = double_value
    assert snowflake_item_view.node.dict(exclude={"name": True}) == {
        "type": NodeType.ASSIGN,
        "parameters": {"name": "double_value", "value": None},
        "output_type": NodeOutputType.FRAME,
    }


def test_join_event_data_attributes__more_columns(
    snowflake_item_view,
    snowflake_event_data_id,
    snowflake_item_data_id,
):
    """
    Test joining more columns from EventData after creating ItemView
    """
    view = snowflake_item_view
    view.join_event_data_attributes(["col_float"])
    view_dict = view.dict()

    # Check node
    node_dict = get_node(view_dict["graph"], view_dict["node_name"])
    assert node_dict == {
        "name": "join_2",
        "type": "join",
        "output_type": "frame",
        "parameters": {
            "left_on": "col_int",
            "right_on": "event_id_col",
            "left_input_columns": ["col_float"],
            "left_output_columns": ["col_float"],
            "right_input_columns": [
                "event_id_col",
                "item_id_col",
                "item_type",
                "item_amount",
                "created_at",
                "event_timestamp",
                "event_timestamp_event_table",
                "cust_id_event_table",
            ],
            "right_output_columns": [
                "event_id_col",
                "item_id_col",
                "item_type",
                "item_amount",
                "created_at",
                "event_timestamp",
                "event_timestamp_event_table",
                "cust_id_event_table",
            ],
            "join_type": "inner",
            "scd_parameters": None,
        },
    }

    # Check Frame attributes
    assert len(view.tabular_data_ids) == 2
    assert set(view.tabular_data_ids) == {snowflake_item_data_id, snowflake_event_data_id}
    assert view.columns == [
        "event_id_col",
        "item_id_col",
        "item_type",
        "item_amount",
        "created_at",
        "event_timestamp",
        "event_timestamp_event_table",
        "cust_id_event_table",
        "col_float",
    ]
    assert view.dtypes.to_dict() == {
        "event_id_col": "INT",
        "item_id_col": "VARCHAR",
        "item_type": "VARCHAR",
        "item_amount": "FLOAT",
        "created_at": "TIMESTAMP_TZ",
        "event_timestamp": "TIMESTAMP_TZ",
        "event_timestamp_event_table": "TIMESTAMP_TZ",
        "cust_id_event_table": "INT",
        "col_float": "FLOAT",
    }

    # Check preview SQL
    preview_sql = snowflake_item_view.preview_sql()
    expected_sql = textwrap.dedent(
        """
        SELECT
          L."col_float" AS "col_float",
          R."event_id_col" AS "event_id_col",
          R."item_id_col" AS "item_id_col",
          R."item_type" AS "item_type",
          R."item_amount" AS "item_amount",
          CAST(R."created_at" AS VARCHAR) AS "created_at",
          CAST(R."event_timestamp" AS VARCHAR) AS "event_timestamp",
          CAST(R."event_timestamp_event_table" AS VARCHAR) AS "event_timestamp_event_table",
          R."cust_id_event_table" AS "cust_id_event_table"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "event_timestamp" AS "event_timestamp",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."sf_table"
        ) AS L
        INNER JOIN (
          SELECT
            L."event_timestamp" AS "event_timestamp_event_table",
            L."cust_id" AS "cust_id_event_table",
            R."event_id_col" AS "event_id_col",
            R."item_id_col" AS "item_id_col",
            R."item_type" AS "item_type",
            R."item_amount" AS "item_amount",
            R."created_at" AS "created_at",
            R."event_timestamp" AS "event_timestamp"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_char" AS "col_char",
              "col_text" AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "event_timestamp" AS "event_timestamp",
              "created_at" AS "created_at",
              "cust_id" AS "cust_id"
            FROM "sf_database"."sf_schema"."sf_table"
          ) AS L
          INNER JOIN (
            SELECT
              "event_id_col" AS "event_id_col",
              "item_id_col" AS "item_id_col",
              "item_type" AS "item_type",
              "item_amount" AS "item_amount",
              "created_at" AS "created_at",
              "event_timestamp" AS "event_timestamp"
            FROM "sf_database"."sf_schema"."items_table"
          ) AS R
            ON L."col_int" = R."event_id_col"
        ) AS R
          ON L."col_int" = R."event_id_col"
        LIMIT 10
        """
    ).strip()
    assert preview_sql == expected_sql


def test_join_event_data_attributes__missing_required_event_suffix(snowflake_item_data):
    """
    Test when event_suffix is required but not provided
    """
    with pytest.raises(RepeatedColumnNamesError) as exc:
        ItemView.from_item_data(snowflake_item_data)
    assert "Duplicate column names ['event_timestamp'] found" in str(exc.value)


def test_join_event_data_attributes__invalid_columns(snowflake_item_view):
    """
    Test join_event_data_attributes with invalid columns
    """
    with pytest.raises(ValueError) as exc:
        snowflake_item_view.join_event_data_attributes(["non_existing_column"])
    assert str(exc.value) == "Column does not exist in EventData: non_existing_column"


def test_item_view__item_data_same_event_id_column_as_event_data(snowflake_item_data_same_event_id):
    """
    Test creating ItemView when ItemData has the same event_id_column as EventData
    """
    # No need to specify event_suffix
    item_view = ItemView.from_item_data(snowflake_item_data_same_event_id)
    assert item_view.timestamp_column == "event_timestamp"

    view_dict = item_view.dict()
    node_dict = get_node(view_dict["graph"], view_dict["node_name"])
    assert node_dict == {
        "name": "join_1",
        "type": "join",
        "output_type": "frame",
        "parameters": {
            "left_on": "col_int",
            "right_on": "col_int",
            "left_input_columns": ["event_timestamp", "cust_id"],
            "left_output_columns": ["event_timestamp", "cust_id"],
            "right_input_columns": ["col_int", "item_id_col", "created_at"],
            "right_output_columns": ["col_int", "item_id_col", "created_at"],
            "join_type": "inner",
            "scd_parameters": None,
        },
    }


def test_item_view_groupby__item_data_column(snowflake_item_view):
    """
    Test aggregating a column from ItemData using an EventData entity is allowed
    """
    feature_job_setting = {
        "blind_spot": "30m",
        "frequency": "1h",
        "time_modulo_frequency": "30m",
    }
    feature = snowflake_item_view.groupby("cust_id_event_table").aggregate_over(
        "item_amount",
        method="sum",
        windows=["24h"],
        feature_names=["item_amount_sum_24h"],
        feature_job_setting=feature_job_setting,
    )["item_amount_sum_24h"]
    feature_dict = feature.dict()
    assert feature_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "join_1"},
        {"source": "input_2", "target": "join_1"},
        {"source": "join_1", "target": "groupby_1"},
        {"source": "groupby_1", "target": "project_1"},
    ]


@pytest.fixture(name="groupby_feature_job_setting")
def get_groupby_feature_job_setting_fixture():
    """
    Fixture for feature job setting
    """
    return {
        "blind_spot": "30m",
        "frequency": "1h",
        "time_modulo_frequency": "30m",
    }


def test_item_view_groupby__event_data_column(snowflake_item_view, groupby_feature_job_setting):
    """
    Test aggregating an EventData column using EventData entity is not allowed
    """
    snowflake_item_view.join_event_data_attributes(["col_float"])
    with pytest.raises(ValueError) as exc:
        _ = snowflake_item_view.groupby("cust_id_event_table").aggregate_over(
            "col_float",
            method="sum",
            windows=["24h"],
            feature_names=["item_amount_sum_24h"],
            feature_job_setting=groupby_feature_job_setting,
        )["item_amount_sum_24h"]
    assert str(exc.value) == (
        "Columns imported from EventData and their derivatives should be aggregated in EventView"
    )


def test_item_view_groupby__event_data_column_derived(
    snowflake_item_view, groupby_feature_job_setting
):
    """
    Test aggregating a column derived from EventData column using EventData entity is not allowed
    """
    snowflake_item_view.join_event_data_attributes(["col_float"])
    snowflake_item_view["col_float_v2"] = (snowflake_item_view["col_float"] + 123) - 45
    snowflake_item_view["col_float_v3"] = (snowflake_item_view["col_float_v2"] * 678) / 90
    with pytest.raises(ValueError) as exc:
        _ = snowflake_item_view.groupby("cust_id_event_table").aggregate_over(
            "col_float_v2",
            method="sum",
            windows=["24h"],
            feature_names=["item_amount_sum_24h"],
            feature_job_setting=groupby_feature_job_setting,
        )["item_amount_sum_24h"]
    assert str(exc.value) == (
        "Columns imported from EventData and their derivatives should be aggregated in EventView"
    )


def test_item_view_groupby__event_data_column_derived_mixed(
    snowflake_item_view, groupby_feature_job_setting
):
    """
    Test aggregating a column derived from both EventData and ItemData is allowed
    """
    snowflake_item_view.join_event_data_attributes(["col_float"])
    snowflake_item_view["new_col"] = (
        snowflake_item_view["col_float"] + snowflake_item_view["item_amount"]
    )
    _ = snowflake_item_view.groupby("cust_id_event_table").aggregate_over(
        "new_col",
        method="sum",
        windows=["24h"],
        feature_names=["feature_name"],
        feature_job_setting=groupby_feature_job_setting,
    )["feature_name"]


def test_item_view_groupby__no_value_column(snowflake_item_view):
    """
    Test count aggregation without value_column using EventData entity is allowed
    """
    feature_job_setting = {
        "blind_spot": "30m",
        "frequency": "1h",
        "time_modulo_frequency": "30m",
    }
    _ = snowflake_item_view.groupby("cust_id_event_table").aggregate_over(
        method="count",
        windows=["24h"],
        feature_names=["feature_name"],
        feature_job_setting=feature_job_setting,
    )["feature_name"]


def test_item_view_groupby__event_id_column(snowflake_item_data, transaction_entity):
    """
    Test aggregating on event id column yields item groupby operation (ItemGroupbyNode)
    """
    snowflake_item_data["event_id_col"].as_entity(transaction_entity.name)
    snowflake_item_view = ItemView.from_item_data(snowflake_item_data, event_suffix="_event_table")
    feature = snowflake_item_view.groupby("event_id_col").aggregate(
        method="count",
        feature_name="order_size",
    )
    assert isinstance(feature, Feature)
    feature_dict = feature.dict()
    assert feature_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "join_1"},
        {"source": "input_2", "target": "join_1"},
        {"source": "join_1", "target": "item_groupby_1"},
        {"source": "item_groupby_1", "target": "project_1"},
        {"source": "project_1", "target": "is_null_1"},
        {"source": "project_1", "target": "conditional_1"},
        {"source": "is_null_1", "target": "conditional_1"},
        {"source": "conditional_1", "target": "alias_1"},
    ]
    assert get_node(feature_dict["graph"], "item_groupby_1")["parameters"] == {
        "keys": ["event_id_col"],
        "parent": None,
        "agg_func": "count",
        "value_by": None,
        "name": "order_size",
        "serving_names": ["transaction_id"],
        "entity_ids": [transaction_entity.id],
    }


def test_validate_join(snowflake_scd_view, snowflake_dimension_view, snowflake_item_view):
    """
    Test validate join
    """
    # No error expected
    snowflake_item_view.validate_join(snowflake_dimension_view)
    snowflake_item_view.validate_join(snowflake_item_view)


def test_validate_simple_aggregate_parameters(snowflake_item_data, transaction_entity):
    """
    Test validate_simple_aggregate_parameters
    """
    snowflake_item_data["event_id_col"].as_entity(transaction_entity.name)
    snowflake_item_data["item_id_col"].as_entity(transaction_entity.name)
    snowflake_item_view = ItemView.from_item_data(snowflake_item_data, event_suffix="_event_table")

    # no error expected as other_col is not from event data
    group_by = snowflake_item_view.groupby("event_id_col")
    snowflake_item_view.validate_simple_aggregate_parameters(group_by, "other_col")

    # error expected as groupby column needs to contain the event ID column
    group_by = snowflake_item_view.groupby("item_id_col")
    with pytest.raises(ValueError) as exc:
        snowflake_item_view.validate_simple_aggregate_parameters(group_by, "other_col")
    assert "GroupBy keys must contain the event ID column" in str(exc)

    # no error expected as groupby keys contains event id col
    group_by = snowflake_item_view.groupby(["item_id_col", "event_id_col"])
    snowflake_item_view.validate_simple_aggregate_parameters(group_by, None)

    # no error expected as no value_column is passed in
    group_by = snowflake_item_view.groupby("event_id_col")
    snowflake_item_view.validate_simple_aggregate_parameters(group_by, None)


def test_validate_aggregate_over_parameters(snowflake_item_data, transaction_entity):
    """
    Test validate_aggregate_over_parameters
    """
    snowflake_item_data["event_id_col"].as_entity(transaction_entity.name)
    snowflake_item_data["item_id_col"].as_entity(transaction_entity.name)
    snowflake_item_view = ItemView.from_item_data(snowflake_item_data, event_suffix="_event_table")

    # no error expected as other_col is not from event data
    group_by = snowflake_item_view.groupby("item_id_col")
    snowflake_item_view.validate_aggregate_over_parameters(group_by, "other_col")

    # error expected as groupby column must not contain the event ID column
    group_by = snowflake_item_view.groupby("event_id_col")
    with pytest.raises(ValueError) as exc:
        snowflake_item_view.validate_aggregate_over_parameters(group_by, "other_col")
    assert "GroupBy keys must NOT contain the event ID column" in str(exc)

    # error expected as groupby keys contains event id col
    group_by = snowflake_item_view.groupby(["item_id_col", "event_id_col"])
    with pytest.raises(ValueError) as exc:
        snowflake_item_view.validate_aggregate_over_parameters(group_by, None)
    assert "GroupBy keys must NOT contain the event ID column" in str(exc)

    # no error expected as no value_column is passed in
    group_by = snowflake_item_view.groupby("item_id_col")
    snowflake_item_view.validate_aggregate_over_parameters(group_by, None)


def test_non_time_feature__create_new_version(saved_item_data, transaction_entity):
    """
    Test aggregating on event id column yields item groupby operation (ItemGroupbyNode)
    """
    saved_item_data["event_id_col"].as_entity(transaction_entity.name)
    snowflake_item_view = ItemView.from_item_data(saved_item_data, event_suffix="_event_table")
    feature = snowflake_item_view.groupby("event_id_col").aggregate(
        method="count",
        feature_name="order_size",
    )
    derived_feature = feature + 123
    derived_feature.name = "feat"
    derived_feature.save()

    with pytest.raises(RecordCreationException) as exc:
        derived_feature.create_new_version(
            feature_job_setting=FeatureJobSetting(
                blind_spot="45m", frequency="30m", time_modulo_frequency="15m"
            )
        )

    expected_msg = "Feature job setting has no effect in feature value derivation."
    assert expected_msg in str(exc.value)
