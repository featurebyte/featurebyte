"""
Unit test for ForecastView class
"""

import pytest

from featurebyte.api.forecast_view import ForecastView
from featurebyte.exception import JoinViewMismatchError
from featurebyte.query_graph.node.cleaning_operation import (
    DisguisedValueImputation,
    MissingValueImputation,
)
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import check_sdk_code_generation


class TestForecastView(BaseViewTestSuite):
    """
    ForecastView test suite
    """

    protected_columns = ["col_int", "effective_timestamp", "forecast_timestamp"]
    view_type = ViewType.FORECAST_VIEW
    col = "col_float"
    view_class = ForecastView
    bool_col = "col_boolean"
    expected_view_with_raw_accessor_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("forecast_timestamp" AS VARCHAR) AS "forecast_timestamp",
      "effective_timestamp" AS "effective_timestamp",
      (
        "col_float" + 1
      ) AS "new_col"
    FROM "sf_database"."sf_schema"."forecast_table"
    LIMIT 10
    """

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.natural_key_column == view_under_test.natural_key_column


def test_from_forecast_table(snowflake_forecast_table, mock_api_object_cache):
    """
    Test ForecastView creation from ForecastTable
    """
    _ = mock_api_object_cache
    forecast_view = snowflake_forecast_table.get_view()
    expected_view_columns_info = [
        col
        for col in snowflake_forecast_table.columns_info
        if col.name != snowflake_forecast_table.record_creation_timestamp_column
    ]
    assert forecast_view.tabular_source == snowflake_forecast_table.tabular_source
    assert forecast_view.row_index_lineage == snowflake_forecast_table.frame.row_index_lineage
    assert forecast_view.columns_info == expected_view_columns_info
    assert forecast_view.natural_key_column == snowflake_forecast_table.natural_key_column
    assert (
        forecast_view.effective_timestamp_column
        == snowflake_forecast_table.effective_timestamp_column
    )
    assert (
        forecast_view.forecast_timestamp_column
        == snowflake_forecast_table.forecast_timestamp_column
    )
    assert forecast_view.timestamp_column == snowflake_forecast_table.timestamp_column


def test_getitem__list_of_str_contains_protected_column(
    snowflake_forecast_table, snowflake_forecast_view
):
    """
    Test retrieving subset of the forecast view columns
    """
    # select a non-protected column; protected columns should be auto-included
    forecast_view_subset = snowflake_forecast_view[["col_char"]]
    assert isinstance(forecast_view_subset, ForecastView)
    assert set(forecast_view_subset.column_var_type_map) == {
        "col_int",
        "effective_timestamp",
        "forecast_timestamp",
        "col_char",
    }
    assert forecast_view_subset.row_index_lineage == snowflake_forecast_view.row_index_lineage

    # select a non-protected column with the protected columns explicitly
    forecast_view_subset2 = snowflake_forecast_view[
        ["col_char", "col_int", "effective_timestamp", "forecast_timestamp"]
    ]
    assert isinstance(forecast_view_subset2, ForecastView)
    assert set(forecast_view_subset2.column_var_type_map) == {
        "col_int",
        "effective_timestamp",
        "forecast_timestamp",
        "col_char",
    }

    # both subsets should point to the same node
    assert forecast_view_subset.node == forecast_view_subset2.node


def test_validate_join(
    snowflake_event_view,
    snowflake_item_view,
    snowflake_dimension_view,
    snowflake_scd_view,
    snowflake_forecast_view,
):
    """
    Test that ForecastView cannot be used as the left-hand side of a join with any view type.
    """
    for other_view in [
        snowflake_event_view,
        snowflake_item_view,
        snowflake_dimension_view,
        snowflake_scd_view,
        snowflake_forecast_view,
    ]:
        with pytest.raises(
            JoinViewMismatchError,
            match="ForecastView cannot be used as the left-hand side of a join",
        ):
            snowflake_forecast_view.validate_join(other_view)


def test_sdk_code_generation(saved_forecast_table, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    forecast_view = saved_forecast_table.get_view()
    check_sdk_code_generation(
        forecast_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/forecast_view.py",
        update_fixtures=update_fixtures,
        table_id=saved_forecast_table.id,
    )

    # add some cleaning operations to the table before view construction
    saved_forecast_table.col_int.update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=-1),
        ]
    )
    saved_forecast_table.col_float.update_critical_data_info(
        cleaning_operations=[
            DisguisedValueImputation(disguised_values=[-99], imputed_value=-1),
        ]
    )

    forecast_view = saved_forecast_table.get_view()
    check_sdk_code_generation(
        forecast_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/forecast_view_with_column_clean_ops.py",
        update_fixtures=update_fixtures,
        table_id=saved_forecast_table.id,
    )
