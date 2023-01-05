"""
Base View test suite
"""
import textwrap

import pytest

from featurebyte.api.data import DataColumn, DataObject
from featurebyte.enum import StrEnum
from featurebyte.query_graph.model.critical_data_info import MissingValueImputation


class DataType(StrEnum):
    """
    Data API object types
    """

    ITEM_DATA = "ItemData"
    EVENT_DATA = "EventData"
    DIMENSION_DATA = "DimensionData"
    SCD_DATA = "SlowlyChangingData"


class BaseDataTestSuite:
    """
    BaseViewTestSuite contains common view tests
    """

    data_type: DataType = ""
    expected_columns = None
    col = ""
    expected_data_sql = ""
    expected_raw_data_sql = ""

    @pytest.fixture(name="data_under_test")
    def get_data_under_test_fixture(
        self,
        snowflake_item_data,
        snowflake_event_data,
        snowflake_dimension_data,
        snowflake_scd_data,
    ):
        """
        Retrieves fixture for data under test.
        """
        data_map = {
            DataType.ITEM_DATA: snowflake_item_data,
            DataType.EVENT_DATA: snowflake_event_data,
            DataType.DIMENSION_DATA: snowflake_dimension_data,
            DataType.SCD_DATA: snowflake_scd_data,
        }
        if self.data_type not in data_map:
            pytest.fail(
                f"Invalid view type `{self.data_type}` found. Please use (or map) a valid DataType."
            )
        return data_map[self.data_type]

    @pytest.fixture(name="imputed_data_under_test")
    def imputed_data_under_test_fixture(self, data_under_test):
        """ """
        data_under_test[self.col].update_critical_data_info(
            cleaning_operations=[MissingValueImputation(imputed_value=0)]
        )
        return data_under_test

    def test_data_column__not_exists(self, data_under_test):
        """
        Test non-exist column retrieval
        """
        with pytest.raises(KeyError) as exc:
            _ = data_under_test["non_exist_column"]
        assert 'Column "non_exist_column" does not exist!' in str(exc.value)

        with pytest.raises(AttributeError) as exc:
            _ = data_under_test.non_exist_column
        assert f"'{self.data_type}' object has no attribute 'non_exist_column'" in str(exc.value)

        # check __getattr__ is working properly
        assert isinstance(data_under_test[self.col], DataColumn)

        # when accessing the `columns` attribute, make sure we retrieve it properly
        assert set(data_under_test.columns) == self.expected_columns

    def test_imputed_data_and_raw_data_preview_sql(self, imputed_data_under_test):
        """
        Test update critical data info on a specific column
        """
        assert (
            imputed_data_under_test.preview_sql() == textwrap.dedent(self.expected_data_sql).strip()
        )
        assert (
            imputed_data_under_test.raw.preview_sql()
            == textwrap.dedent(self.expected_raw_data_sql).strip()
        )

    def test_raw_table_data(self, imputed_data_under_test):
        """
        Test raw table data constructed by using raw data accessor
        """
        raw_data = imputed_data_under_test.raw
        assert isinstance(raw_data, DataObject)
        assert isinstance(raw_data[self.col], DataColumn)
