"""
Unit tests for DimensionView class
"""
import pytest

from featurebyte.api.dimension_view import DimensionView
from featurebyte.exception import JoinViewMismatchError
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType


class TestDimensionView(BaseViewTestSuite):
    """
    DimensionView test suite
    """

    protected_columns = ["col_int"]
    view_type = ViewType.DIMENSION_VIEW
    col = "cust_id"
    factory_method = DimensionView.from_dimension_data
    view_class = DimensionView
    bool_col = "col_boolean"

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.dimension_data_id_column == view_under_test.dimension_data_id_column


def test_validate_join(snowflake_dimension_view, snowflake_scd_view):
    with pytest.raises(JoinViewMismatchError):
        snowflake_dimension_view.validate_join(snowflake_scd_view)

    # assert that joining with a dimension view has no issues
    snowflake_dimension_view.validate_join(snowflake_dimension_view)


def test_get_join_column(snowflake_dimension_view):
    column = snowflake_dimension_view.get_join_column()
    # col_int is the dimension_data_id_key column name used when creating this view fixture
    assert column == "col_int"
