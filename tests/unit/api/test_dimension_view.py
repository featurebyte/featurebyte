"""
Unit tests for DimensionView class
"""
from featurebyte.api.dimension_view import DimensionView
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType


class TestDimensionView(BaseViewTestSuite):
    """
    DimensionView test suite
    """

    protected_columns = ["col_int"]
    view_type = ViewType.DIMENSION_VIEW
    col = "cust_id"
    factory_method = DimensionView.from_dimension_data
