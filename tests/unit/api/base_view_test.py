"""
Base View test suite
"""

import pytest

from featurebyte.enum import StrEnum


class ViewType(StrEnum):
    """
    View API object types
    """

    ITEM_VIEW = "item_view"
    EVENT_VIEW = "event_view"
    DIMENSION_VIEW = "dimension_view"


class BaseViewTestSuite:
    """
    BaseViewTestSuite contains common view tests
    """

    protected_columns = []
    view_type: ViewType = ""
    col = ""
    factory_method = None

    @pytest.fixture(name="view_under_test")
    def get_view_under_test_fixture(
        self, snowflake_event_view, snowflake_item_view, snowflake_dimension_view
    ):
        if self.view_type == ViewType.ITEM_VIEW:
            return snowflake_item_view
        if self.view_type == ViewType.EVENT_VIEW:
            return snowflake_event_view
        if self.view_type == ViewType.DIMENSION_VIEW:
            return snowflake_dimension_view
        pytest.fail(
            f"Invalid view type `{self.view_type}` found. Please use (or map) a valid ViewType."
        )

    @pytest.fixture(name="data_under_test")
    def get_data_under_test_fixture(
        self, snowflake_item_data, snowflake_event_data, snowflake_dimension_data
    ):
        if self.view_type == ViewType.ITEM_VIEW:
            return snowflake_item_data
        if self.view_type == ViewType.EVENT_VIEW:
            return snowflake_event_data
        if self.view_type == ViewType.DIMENSION_VIEW:
            return snowflake_dimension_data
        pytest.fail(
            f"Invalid view type `{self.view_type}` found. Please use (or map) a valid ViewType."
        )

    def test_setitem__override_protected_column(self, view_under_test):
        """
        Test attempting to change a view's protected columns
        """
        for column in self.protected_columns:
            assert column in self.protected_columns
            with pytest.raises(ValueError) as exc:
                view_under_test[column] = 1
            expected_msg = f"Column '{column}' cannot be modified!"
            assert expected_msg in str(exc.value)

    def test_unary_op_params(self, view_under_test):
        """
        Test unary operation inherits tabular_data_ids
        """
        column = view_under_test[self.col]
        output = column.isnull()
        assert output.tabular_data_ids == column.tabular_data_ids

    def test_from_data__invalid_input(self):
        """
        Test from_item_data
        """
        with pytest.raises(TypeError) as exc:
            self.factory_method("hello")
        exception_message = str(exc.value)
        assert "type of argument" in exception_message
        assert "got str instead" in exception_message
