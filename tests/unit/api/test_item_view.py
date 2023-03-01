"""
Unit test for ItemView class
"""
import textwrap

import pytest

from featurebyte import AggFunc
from featurebyte.api.feature import Feature
from featurebyte.api.item_view import ItemView
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordCreationException, RepeatedColumnNamesError
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.critical_data_info import (
    MissingValueImputation,
    StringValueImputation,
    UnexpectedValueImputation,
    ValueBeyondEndpointImputation,
)
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node.nested import ColumnCleaningOperation, DataCleaningOperation
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import check_sdk_code_generation, get_node


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
            "expected_backward_edges_map": {
                "filter_1": ["project_1", "gt_1"],
                "graph_1": ["input_1"],
                "graph_2": ["input_2", "graph_1"],
                "gt_1": ["project_1"],
                "project_1": ["graph_2"],
            },
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
        "name": "graph_2",
        "output_type": "frame",
        "parameters": {
            "graph": {
                "edges": [
                    {"source": "proxy_input_1", "target": "project_1"},
                    {"source": "proxy_input_2", "target": "join_1"},
                    {"source": "project_1", "target": "join_1"},
                ],
                "nodes": [
                    {
                        "name": "proxy_input_1",
                        "output_type": "frame",
                        "parameters": {"input_order": 0},
                        "type": "proxy_input",
                    },
                    {
                        "name": "proxy_input_2",
                        "output_type": "frame",
                        "parameters": {"input_order": 1},
                        "type": "proxy_input",
                    },
                    {
                        "name": "project_1",
                        "output_type": "frame",
                        "parameters": {
                            "columns": [
                                "event_id_col",
                                "item_id_col",
                                "item_type",
                                "item_amount",
                                "created_at",
                                "event_timestamp",
                            ]
                        },
                        "type": "project",
                    },
                    {
                        "name": "join_1",
                        "output_type": "frame",
                        "parameters": {
                            "join_type": "inner",
                            "left_input_columns": ["event_timestamp", "cust_id"],
                            "left_on": "col_int",
                            "left_output_columns": [
                                "event_timestamp_event_table",
                                "cust_id_event_table",
                            ],
                            "right_input_columns": [
                                "event_id_col",
                                "item_id_col",
                                "item_type",
                                "item_amount",
                                "created_at",
                                "event_timestamp",
                            ],
                            "right_on": "event_id_col",
                            "right_output_columns": [
                                "event_id_col",
                                "item_id_col",
                                "item_type",
                                "item_amount",
                                "created_at",
                                "event_timestamp",
                            ],
                            "scd_parameters": None,
                            "metadata": {
                                "type": "join_event_data_attributes",
                                "columns": ["event_timestamp", "cust_id"],
                                "event_suffix": "_event_table",
                            },
                        },
                        "type": "join",
                    },
                ],
            },
            "output_node_name": "join_1",
            "metadata": {
                "event_suffix": "_event_table",
                "view_mode": "auto",
                "drop_column_names": [],
                "data_id": snowflake_item_data_id,
                "column_cleaning_operations": [],
                "event_drop_column_names": ["created_at"],
                "event_column_cleaning_operations": [],
                "event_data_id": snowflake_event_data_id,
                "event_join_column_names": ["event_timestamp", "cust_id"],
            },
            "type": "item_view",
        },
        "type": "graph",
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
          CAST(L."event_timestamp" AS STRING) AS "event_timestamp_event_table",
          L."cust_id" AS "cust_id_event_table",
          R."event_id_col" AS "event_id_col",
          R."item_id_col" AS "item_id_col",
          R."item_type" AS "item_type",
          R."item_amount" AS "item_amount",
          CAST(R."created_at" AS STRING) AS "created_at",
          CAST(R."event_timestamp" AS STRING) AS "event_timestamp"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "event_timestamp" AS "event_timestamp",
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
    assert snowflake_item_data.record_creation_date_column is None
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
        "name": "join_1",
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
            "metadata": {
                "type": "join_event_data_attributes",
                "columns": ["col_float"],
                "event_suffix": None,
            },
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
          CAST(R."created_at" AS STRING) AS "created_at",
          CAST(R."event_timestamp" AS STRING) AS "event_timestamp",
          CAST(R."event_timestamp_event_table" AS STRING) AS "event_timestamp_event_table",
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


def test_item_view__item_data_same_event_id_column_as_event_data(
    snowflake_item_data_same_event_id, snowflake_event_data
):
    """
    Test creating ItemView when ItemData has the same event_id_column as EventData
    """
    # No need to specify event_suffix
    item_view = ItemView.from_item_data(snowflake_item_data_same_event_id)
    assert item_view.timestamp_column == "event_timestamp"

    view_dict = item_view.dict()
    node_dict = get_node(view_dict["graph"], view_dict["node_name"])
    assert node_dict == {
        "name": "graph_2",
        "type": "graph",
        "output_type": "frame",
        "parameters": {
            "graph": {
                "edges": [
                    {"source": "proxy_input_1", "target": "project_1"},
                    {"source": "proxy_input_2", "target": "join_1"},
                    {"source": "project_1", "target": "join_1"},
                ],
                "nodes": [
                    {
                        "name": "proxy_input_1",
                        "type": "proxy_input",
                        "output_type": "frame",
                        "parameters": {"input_order": 0},
                    },
                    {
                        "name": "proxy_input_2",
                        "type": "proxy_input",
                        "output_type": "frame",
                        "parameters": {"input_order": 1},
                    },
                    {
                        "name": "project_1",
                        "type": "project",
                        "output_type": "frame",
                        "parameters": {"columns": ["col_int", "item_id_col", "created_at"]},
                    },
                    {
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
                            "metadata": {
                                "type": "join_event_data_attributes",
                                "event_suffix": None,
                                "columns": ["event_timestamp", "cust_id"],
                            },
                        },
                    },
                ],
            },
            "output_node_name": "join_1",
            "metadata": {
                "event_suffix": None,
                "view_mode": "auto",
                "drop_column_names": [],
                "column_cleaning_operations": [],
                "data_id": snowflake_item_data_same_event_id.id,
                "event_drop_column_names": ["created_at"],
                "event_column_cleaning_operations": [],
                "event_data_id": snowflake_event_data.id,
                "event_join_column_names": ["event_timestamp", "cust_id"],
            },
            "type": "item_view",
        },
    }


def test_item_view_groupby__item_data_column(
    snowflake_item_view, snowflake_item_data, snowflake_event_data
):
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
        {"source": "input_1", "target": "graph_1"},
        {"source": "input_2", "target": "graph_2"},
        {"source": "graph_1", "target": "graph_2"},
        {"source": "graph_2", "target": "groupby_1"},
        {"source": "groupby_1", "target": "project_1"},
    ]

    # check SDK code generation
    check_sdk_code_generation(
        feature,
        to_use_saved_data=False,
        data_id_to_info={
            snowflake_item_data.id: {
                "name": snowflake_item_data.name,
                "record_creation_date_column": snowflake_item_data.record_creation_date_column,
            },
            snowflake_event_data.id: {
                "name": snowflake_event_data.name,
                "record_creation_date_column": snowflake_event_data.record_creation_date_column,
            },
        },
    )


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
    snowflake_item_view, snowflake_item_data, groupby_feature_job_setting
):
    """
    Test aggregating a column derived from both EventData and ItemData is allowed
    """
    snowflake_item_view.join_event_data_attributes(["col_float"])
    snowflake_item_view["new_col"] = (
        snowflake_item_view["col_float"] + snowflake_item_view["item_amount"]
    )
    feat = snowflake_item_view.groupby("cust_id_event_table").aggregate_over(
        "new_col",
        method="sum",
        windows=["24h"],
        feature_names=["feature_name"],
        feature_job_setting=groupby_feature_job_setting,
    )["feature_name"]

    # check SDK code generation
    check_sdk_code_generation(
        feat,
        to_use_saved_data=False,
        data_id_to_info={
            snowflake_item_data.id: {
                "name": snowflake_item_data.name,
                "record_creation_date_column": snowflake_item_data.record_creation_date_column,
            }
        },
    )


def test_item_view_groupby__no_value_column(snowflake_item_view, snowflake_item_data):
    """
    Test count aggregation without value_column using EventData entity is allowed
    """
    feature_job_setting = {
        "blind_spot": "30m",
        "frequency": "1h",
        "time_modulo_frequency": "30m",
    }
    feature = snowflake_item_view.groupby("cust_id_event_table").aggregate_over(
        method="count",
        windows=["24h"],
        feature_names=["feature_name"],
        feature_job_setting=feature_job_setting,
    )["feature_name"]

    # check SDK code generation
    check_sdk_code_generation(
        feature,
        to_use_saved_data=False,
        data_id_to_info={
            snowflake_item_data.id: {
                "name": snowflake_item_data.name,
                "record_creation_date_column": snowflake_item_data.record_creation_date_column,
            }
        },
    )


def test_item_view_groupby__event_id_column(
    snowflake_item_data, snowflake_event_data, transaction_entity
):
    """
    Test aggregating on event id column yields item groupby operation (ItemGroupbyNode)
    """
    snowflake_item_data["event_id_col"].as_entity(transaction_entity.name)
    snowflake_item_view = ItemView.from_item_data(snowflake_item_data, event_suffix="_event_table")
    feature = snowflake_item_view.groupby("event_id_col").aggregate(
        method=AggFunc.COUNT,
        feature_name="order_size",
    )
    assert isinstance(feature, Feature)
    feature_dict = feature.dict()
    assert feature_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "input_2", "target": "graph_2"},
        {"source": "graph_1", "target": "graph_2"},
        {"source": "graph_2", "target": "item_groupby_1"},
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

    # check SDK code generation
    # since the data is not saved, we need to pass in the columns info
    # otherwise, entity id will be missing and code generation will fail during GroupBy construction
    event_data_columns_info = snowflake_event_data.dict(by_alias=True)["columns_info"]
    item_data_columns_info = snowflake_item_data.dict(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        api_object=feature,
        to_use_saved_data=False,
        data_id_to_info={
            snowflake_event_data.id: {
                "name": snowflake_event_data.name,
                "record_creation_date_column": snowflake_event_data.record_creation_date_column,
                "columns_info": event_data_columns_info,
            },
            snowflake_item_data.id: {
                "name": snowflake_item_data.name,
                "record_creation_date_column": snowflake_item_data.record_creation_date_column,
                "columns_info": item_data_columns_info,
            },
        },
    )


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
    snowflake_item_view.validate_simple_aggregate_parameters(group_by.keys, "other_col")

    # error expected as groupby column needs to contain the event ID column
    group_by = snowflake_item_view.groupby("item_id_col")
    with pytest.raises(ValueError) as exc:
        snowflake_item_view.validate_simple_aggregate_parameters(group_by.keys, "other_col")
    assert "GroupBy keys must contain the event ID column" in str(exc)

    # no error expected as groupby keys contains event id col
    group_by = snowflake_item_view.groupby(["item_id_col", "event_id_col"])
    snowflake_item_view.validate_simple_aggregate_parameters(group_by.keys, None)

    # no error expected as no value_column is passed in
    group_by = snowflake_item_view.groupby("event_id_col")
    snowflake_item_view.validate_simple_aggregate_parameters(group_by.keys, None)


def test_validate_aggregate_over_parameters(snowflake_item_data, transaction_entity):
    """
    Test validate_aggregate_over_parameters
    """
    snowflake_item_data["event_id_col"].as_entity(transaction_entity.name)
    snowflake_item_data["item_id_col"].as_entity(transaction_entity.name)
    snowflake_item_view = ItemView.from_item_data(snowflake_item_data, event_suffix="_event_table")

    # no error expected as other_col is not from event data
    group_by = snowflake_item_view.groupby("item_id_col")
    snowflake_item_view.validate_aggregate_over_parameters(group_by.keys, "other_col")

    # error expected as groupby column must not contain the event ID column
    group_by = snowflake_item_view.groupby("event_id_col")
    with pytest.raises(ValueError) as exc:
        snowflake_item_view.validate_aggregate_over_parameters(group_by.keys, "other_col")
    assert "GroupBy keys must NOT contain the event ID column" in str(exc)

    # error expected as groupby keys contains event id col
    group_by = snowflake_item_view.groupby(["item_id_col", "event_id_col"])
    with pytest.raises(ValueError) as exc:
        snowflake_item_view.validate_aggregate_over_parameters(group_by.keys, None)
    assert "GroupBy keys must NOT contain the event ID column" in str(exc)

    # no error expected as no value_column is passed in
    group_by = snowflake_item_view.groupby("item_id_col")
    snowflake_item_view.validate_aggregate_over_parameters(group_by.keys, None)


@pytest.fixture(name="non_time_based_saved_feature")
def non_time_based_saved_feature_fixture(saved_item_data, transaction_entity):
    """Non-time-based saved feature fixture"""
    saved_item_data["event_id_col"].as_entity(transaction_entity.name)
    snowflake_item_view = ItemView.from_item_data(saved_item_data, event_suffix="_event_table")
    feature = snowflake_item_view.groupby("event_id_col").aggregate(
        method="count",
        feature_name="order_size",
    )
    derived_feature = feature + 123
    derived_feature.name = "feat"
    derived_feature.save()
    return derived_feature


def test_non_time_based_feature__create_new_version(non_time_based_saved_feature):
    """
    Test aggregating on event id column yields item groupby operation (ItemGroupbyNode)
    """
    with pytest.raises(RecordCreationException) as exc:
        non_time_based_saved_feature.create_new_version(
            feature_job_setting=FeatureJobSetting(
                blind_spot="45m", frequency="30m", time_modulo_frequency="15m"
            ),
            data_cleaning_operations=None,
        )

    expected_msg = (
        "Feature job setting does not result a new feature version. "
        "This is because the new feature version is the same as the source feature."
    )
    assert expected_msg in str(exc.value)


def test_non_time_based_feature__create_new_version_with_data_cleaning(
    saved_item_data, non_time_based_saved_feature, update_fixtures
):
    """Test creation of new version with data cleaning operations"""
    # check sdk code generation of source feature
    check_sdk_code_generation(non_time_based_saved_feature, to_use_saved_data=True)

    # create a new version with data cleaning operations
    new_version = non_time_based_saved_feature.create_new_version(
        feature_job_setting=None,
        data_cleaning_operations=[
            DataCleaningOperation(
                data_name="sf_item_data",
                column_cleaning_operations=[
                    # group by column
                    ColumnCleaningOperation(
                        column_name="event_id_col",
                        cleaning_operations=[MissingValueImputation(imputed_value=-999)],
                    ),
                    # unaffected column
                    ColumnCleaningOperation(
                        column_name="item_id_col",
                        cleaning_operations=[MissingValueImputation(imputed_value=-99)],
                    ),
                ],
            ),
            DataCleaningOperation(
                data_name="sf_event_data",
                column_cleaning_operations=[
                    # event ID column
                    ColumnCleaningOperation(
                        column_name="col_int",
                        cleaning_operations=[MissingValueImputation(imputed_value=-99)],
                    ),
                    # unaffected column
                    ColumnCleaningOperation(
                        column_name="col_float",
                        cleaning_operations=[MissingValueImputation(imputed_value=0.0)],
                    ),
                ],
            ),
        ],
    )

    # check sdk code generation of newly created feature
    # Note: check_final_hash=False because the hash of the generated SDK code is different from original pruned graph.
    # Inside the item view graph, below is the list of nodes in the nested graph:
    # {"proxy_input_1": ["project_1"], "project_1": ["graph_1"], "graph_1": ["join_1"], "proxy_input_2": ["join_1"]}
    # proxy_input_1: item data node
    # proxy_input_2: event view graph node
    # graph_1: cleaning operation graph node for item data
    # The cleaning operation graph node's output node is a join node. Since all the columns used in the join node
    # will be kept during graph pruning, we can't prune the cleaning operation graph node.
    # However, we can prune the item view graph node's metadata, which affects the generated SDK code.
    # This causes the difference in the generated SDK code has vs the original pruned graph as
    # the graph constructed by the SDK code will not contain unused cleaning operations.
    check_sdk_code_generation(
        new_version,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/feature_non_time_based_with_data_cleaning_operations.py",
        update_fixtures=update_fixtures,
        data_id=saved_item_data.id,
        event_data_id=saved_item_data.event_data_id,
        check_final_hash=False,
    )


def test_as_feature__from_view_column(saved_item_data, item_entity):
    """
    Test calling as_feature() from ItemView column
    """
    view = ItemView.from_item_data(saved_item_data, event_suffix="_event_data")
    feature = view["item_amount"].as_feature("ItemAmountFeature")
    assert feature.name == "ItemAmountFeature"
    assert feature.dtype == DBVarType.FLOAT

    feature_dict = feature.dict()
    graph_dict = feature_dict["graph"]
    feature_node_dict = get_node(graph_dict, feature_dict["node_name"])
    lookup_node_dict = get_node(graph_dict, "lookup_1")
    assert feature_node_dict == {
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["ItemAmountFeature"]},
    }
    assert lookup_node_dict == {
        "name": "lookup_1",
        "type": "lookup",
        "output_type": "frame",
        "parameters": {
            "input_column_names": ["item_amount"],
            "feature_names": ["ItemAmountFeature"],
            "entity_column": "item_id_col",
            "serving_name": "item_id",
            "entity_id": item_entity.id,
            "scd_parameters": None,
            "event_parameters": {"event_timestamp_column": "event_timestamp_event_data"},
        },
    }

    # check SDK code generation
    check_sdk_code_generation(
        feature,
        to_use_saved_data=False,
        data_id_to_info={
            saved_item_data.id: {
                "name": saved_item_data.name,
                "record_creation_date_column": saved_item_data.record_creation_date_column,
            }
        },
    )


def test_sdk_code_generation(saved_item_data, saved_event_data, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    item_view = ItemView.from_item_data(saved_item_data, event_suffix="_event_data")
    check_sdk_code_generation(
        item_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/item_view.py",
        update_fixtures=update_fixtures,
        data_id=saved_item_data.id,
        event_data_id=saved_event_data.id,
    )

    # add some cleaning operations during view construction
    item_view = ItemView.from_item_data(
        saved_item_data,
        event_suffix="_event_data",
        view_mode="manual",
        column_cleaning_operations=[
            ColumnCleaningOperation(
                column_name="item_amount",
                cleaning_operations=[
                    ValueBeyondEndpointImputation(type="less_than", end_point=0, imputed_value=0),
                    StringValueImputation(imputed_value=0),
                ],
            )
        ],
        event_column_cleaning_operations=[
            ColumnCleaningOperation(
                column_name="col_int",
                cleaning_operations=[
                    MissingValueImputation(imputed_value=0),
                ],
            ),
            ColumnCleaningOperation(
                column_name="col_char",
                cleaning_operations=[
                    UnexpectedValueImputation(
                        expected_values=["a", "b", "c", "unknown"], imputed_value="unknown"
                    ),
                ],
            ),
        ],
    )
    check_sdk_code_generation(
        item_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/item_view_with_column_clean_ops.py",
        update_fixtures=update_fixtures,
        data_id=saved_item_data.id,
    )
