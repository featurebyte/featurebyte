"""
Unit test for ItemView class
"""
import textwrap
from unittest.mock import Mock, patch

import pytest

from featurebyte.api.item_view import ItemView
from featurebyte.core.series import Series
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
    use_data_under_test_in_lineage = True
    view_class = ItemView

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.event_id_column == view_under_test.event_id_column
        assert row_subset.item_id_column == view_under_test.item_id_column
        assert row_subset.event_data_id == view_under_test.event_data_id
        assert row_subset.event_view.dict() == view_under_test.event_view.dict()

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
    snowflake_item_data, snowflake_event_data_id, snowflake_item_data_id
):
    """
    Test ItemView automatically joins timestamp column and entity columns from related EventData
    """
    view = ItemView.from_item_data(snowflake_item_data)
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
            "left_input_columns": ["event_timestamp"],
            "left_output_columns": ["event_timestamp"],
            "right_input_columns": [
                "event_id_col",
                "item_id_col",
                "item_type",
                "item_amount",
                "created_at",
            ],
            "right_output_columns": [
                "event_id_col",
                "item_id_col",
                "item_type",
                "item_amount",
                "created_at",
            ],
            "join_type": "left",
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
    ]
    assert view.dtypes.to_dict() == {
        "event_id_col": "INT",
        "item_id_col": "VARCHAR",
        "item_type": "VARCHAR",
        "item_amount": "FLOAT",
        "created_at": "TIMESTAMP",
        "event_timestamp": "TIMESTAMP",
    }
    assert view_dict["row_index_lineage"] == ("input_2", "join_1")
    assert view_dict["column_lineage_map"] == {
        "event_id_col": ("input_2", "join_1"),
        "item_id_col": ("input_2", "join_1"),
        "item_type": ("input_2", "join_1"),
        "item_amount": ("input_2", "join_1"),
        "created_at": ("input_2", "join_1"),
        "event_timestamp": ("input_1", "join_1"),
    }

    # Check preview SQL
    preview_sql = view.preview_sql()
    expected_sql = textwrap.dedent(
        """
        SELECT
          L."event_timestamp" AS "event_timestamp",
          R."event_id_col" AS "event_id_col",
          R."item_id_col" AS "item_id_col",
          R."item_type" AS "item_type",
          R."item_amount" AS "item_amount",
          R."created_at" AS "created_at"
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
        LEFT JOIN (
            SELECT
              "event_id_col" AS "event_id_col",
              "item_id_col" AS "item_id_col",
              "item_type" AS "item_type",
              "item_amount" AS "item_amount",
              "created_at" AS "created_at"
            FROM "sf_database"."sf_schema"."items_table"
        ) AS R
          ON L."col_int" = R."event_id_col"
        LIMIT 10
        """
    ).strip()
    assert preview_sql == expected_sql


def test_from_item_data__event_data_without_event_id_column(snowflake_item_data):
    """
    Test attempting to create ItemView using an old EventData without event_id_column

    Can probably be removed once DEV-556 is resolved
    """
    with patch("featurebyte.api.item_view.EventView") as patched_event_view_cls:
        patched_event_view_cls.from_event_data.return_value = Mock(event_id_column=None)
        with pytest.raises(ValueError) as exc:
            ItemView.from_item_data(snowflake_item_data)
        assert str(exc.value) == "EventData without event_id_column is not supported"


def test_has_event_timestamp_column(snowflake_item_view):
    """
    Test that ItemView inherits the event timestamp column from EventView
    """
    assert snowflake_item_view.timestamp_column == "event_timestamp"


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
    expected_lineage_item_data_columns = (
        snowflake_item_data.node.name,
        snowflake_item_view.node.name,
    )
    expected_lineage_event_data_columns = (
        snowflake_event_data.node.name,
        snowflake_item_view.node.name,
    )
    double_value = snowflake_item_view["item_amount"] * 2
    assert isinstance(double_value, Series)
    snowflake_item_view["double_value"] = double_value
    assert snowflake_item_view.node.dict(exclude={"name": True}) == {
        "type": NodeType.ASSIGN,
        "parameters": {"name": "double_value", "value": None},
        "output_type": NodeOutputType.FRAME,
    }
    assert snowflake_item_view.column_lineage_map == {
        "event_timestamp": expected_lineage_event_data_columns,
        "event_id_col": expected_lineage_item_data_columns,
        "item_id_col": expected_lineage_item_data_columns,
        "item_type": expected_lineage_item_data_columns,
        "item_amount": expected_lineage_item_data_columns,
        "created_at": expected_lineage_item_data_columns,
        "double_value": (snowflake_item_view.node.name,),
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
            ],
            "right_output_columns": [
                "event_id_col",
                "item_id_col",
                "item_type",
                "item_amount",
                "created_at",
                "event_timestamp",
            ],
            "join_type": "left",
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
        "col_float",
    ]
    assert view.dtypes.to_dict() == {
        "event_id_col": "INT",
        "item_id_col": "VARCHAR",
        "item_type": "VARCHAR",
        "item_amount": "FLOAT",
        "created_at": "TIMESTAMP",
        "event_timestamp": "TIMESTAMP",
        "col_float": "FLOAT",
    }
    assert view_dict["row_index_lineage"] == ("input_2", "join_1", "join_2")
    assert view_dict["column_lineage_map"] == {
        "event_id_col": ("input_2", "join_1", "join_2"),
        "item_id_col": ("input_2", "join_1", "join_2"),
        "item_type": ("input_2", "join_1", "join_2"),
        "item_amount": ("input_2", "join_1", "join_2"),
        "created_at": ("input_2", "join_1", "join_2"),
        "event_timestamp": ("input_1", "join_1", "join_2"),
        "col_float": ("input_1", "join_2"),
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
        LEFT JOIN (
            SELECT
              L."event_timestamp" AS "event_timestamp",
              R."event_id_col" AS "event_id_col",
              R."item_id_col" AS "item_id_col",
              R."item_type" AS "item_type",
              R."item_amount" AS "item_amount",
              R."created_at" AS "created_at"
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
            LEFT JOIN (
                SELECT
                  "event_id_col" AS "event_id_col",
                  "item_id_col" AS "item_id_col",
                  "item_type" AS "item_type",
                  "item_amount" AS "item_amount",
                  "created_at" AS "created_at"
                FROM "sf_database"."sf_schema"."items_table"
            ) AS R
              ON L."col_int" = R."event_id_col"
        ) AS R
          ON L."col_int" = R."event_id_col"
        LIMIT 10
        """
    ).strip()
    assert preview_sql == expected_sql


def test_join_event_data_attributes__invalid_columns(snowflake_item_view):
    """
    Test join_event_data_attributes with invalid columns
    """
    with pytest.raises(ValueError) as exc:
        snowflake_item_view.join_event_data_attributes(["non_existing_column"])
    assert str(exc.value) == "Column does not exist in EventData: non_existing_column"
