"""
Unit tests for SlowlyChangingView class
"""
import pytest

from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.exception import JoinViewMismatchError
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType


class TestSlowlyChangingView(BaseViewTestSuite):
    """
    SlowlyChangingView test suite
    """

    protected_columns = ["col_int", "col_text", "event_timestamp", "col_char"]
    view_type = ViewType.SLOWLY_CHANGING_VIEW
    col = "cust_id"
    factory_method = SlowlyChangingView.from_slowly_changing_data
    view_class = SlowlyChangingView
    bool_col = "col_boolean"

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.natural_key_column == view_under_test.natural_key_column
        assert row_subset.surrogate_key_column == view_under_test.surrogate_key_column
        assert row_subset.effective_timestamp_column == view_under_test.effective_timestamp_column
        assert row_subset.end_timestamp_column == view_under_test.end_timestamp_column
        assert row_subset.current_flag == view_under_test.current_flag


def test_validate_join(snowflake_scd_view, snowflake_dimension_view):
    """
    Test validate join
    """
    with pytest.raises(JoinViewMismatchError):
        snowflake_scd_view.validate_join(snowflake_scd_view)

    # assert that joining with a dimension view has no issues
    snowflake_scd_view.validate_join(snowflake_dimension_view)


def test_get_join_column(snowflake_scd_view):
    """
    Test get join column
    """
    column = snowflake_scd_view.get_join_column()
    # col_text is the natural_key column name used when creating this view fixture
    assert column == "col_text"


def test_event_view_join_scd_view(snowflake_event_view, snowflake_scd_view):
    """
    Test additional join parameters are added for SCDView
    """
    snowflake_event_view.join(snowflake_scd_view)
    assert snowflake_event_view.node.parameters.dict() == {
        "left_on": "col_text",
        "right_on": "col_text",
        "left_input_columns": [
            "col_int",
            "col_float",
            "col_char",
            "col_text",
            "col_binary",
            "col_boolean",
            "event_timestamp",
            "created_at",
            "cust_id",
        ],
        "left_output_columns": [
            "col_int",
            "col_float",
            "col_char",
            "col_text",
            "col_binary",
            "col_boolean",
            "event_timestamp",
            "created_at",
            "cust_id",
        ],
        "right_input_columns": [
            "col_int",
            "col_float",
            "col_char",
            "col_text",
            "col_binary",
            "col_boolean",
            "event_timestamp",
            "created_at",
            "cust_id",
        ],
        "right_output_columns": [
            "col_int",
            "col_float",
            "col_char",
            "col_text",
            "col_binary",
            "col_boolean",
            "event_timestamp",
            "created_at",
            "cust_id",
        ],
        "join_type": "left",
        "scd_parameters": {
            "left_timestamp_column": "event_timestamp",
            "right_timestamp_column": "event_timestamp",
            "current_flag": "col_char",
            "end_timestamp_column": "event_timestamp",
        },
    }
