"""
Unit tests for SlowlyChangingView class
"""
from featurebyte.api.scd_view import SlowlyChangingView
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
