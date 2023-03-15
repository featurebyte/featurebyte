"""
Base View test suite
"""
import textwrap

import pytest

from featurebyte.api.base_table import TableColumn
from featurebyte.enum import StrEnum
from featurebyte.query_graph.node.cleaning_operation import MissingValueImputation


class DataType(StrEnum):
    """
    Data API object types
    """

    ITEM_DATA = "ItemData"
    EVENT_DATA = "EventData"
    DIMENSION_DATA = "DimensionData"
    SCD_DATA = "SlowlyChangingData"


class BaseTableTestSuite:
    """
    BaseTableTestSuite contains common tests for api table objects.
    """

    data_type: DataType = ""
    expected_columns = None
    col = ""
    expected_data_sql = ""
    expected_data_column_sql = ""
    expected_clean_data_sql = ""
    expected_attr_name_value_pairs = []

    @pytest.fixture(autouse=True)
    def immediately_expired_api_object_cache(self, mock_api_object_cache):
        yield

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
        """
        Retrieves fixture for data under test
        """
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
        assert (
            f"'{data_under_test.__class__.__name__}' object has no attribute 'non_exist_column'"
            in str(exc.value)
        )

        # check __getattr__ is working properly
        assert isinstance(data_under_test[self.col], TableColumn)

        # when accessing the `columns` attribute, make sure we retrieve it properly
        assert set(data_under_test.columns) == self.expected_columns

    def test_data_preview_sql(self, imputed_data_under_test):
        """
        Test preview data (make sure imputed data show only raw data sql)
        """
        data_sql = imputed_data_under_test.preview_sql()
        clean_data_sql = imputed_data_under_test.preview_clean_data_sql()
        assert data_sql == textwrap.dedent(self.expected_data_sql).strip()
        assert clean_data_sql == textwrap.dedent(self.expected_clean_data_sql).strip()

    def test_data_column_preview_sql(self, data_under_test):
        """
        Test preview data column
        """
        data_column_sql = data_under_test[self.col].preview_sql()
        assert data_column_sql == textwrap.dedent(self.expected_data_column_sql).strip()
