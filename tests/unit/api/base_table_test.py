"""
Base View test suite
"""
import textwrap

import pandas as pd
import pytest

from featurebyte.api.base_table import TableColumn
from featurebyte.enum import StrEnum
from featurebyte.models.feature_store import TableStatus
from featurebyte.query_graph.node.cleaning_operation import (
    ColumnCleaningOperation,
    MissingValueImputation,
)


class DataType(StrEnum):
    """
    Data API object types
    """

    ITEM_DATA = "ItemTable"
    EVENT_DATA = "EventTable"
    DIMENSION_DATA = "DimensionTable"
    SCD_DATA = "SCDTable"


class BaseTableTestSuite:
    """
    BaseTableTestSuite contains common tests for api table objects.
    """

    data_type: DataType = ""
    expected_columns = None
    col = ""
    expected_table_sql = ""
    expected_table_column_sql = ""
    expected_clean_table_sql = ""
    expected_clean_table_column_sql = ""
    expected_attr_name_value_pairs = []
    expected_timestamp_column = ""

    @pytest.fixture(autouse=True)
    def immediately_expired_api_object_cache(self, mock_api_object_cache):
        yield

    @pytest.fixture(name="table_under_test")
    def get_table_under_test_fixture(
        self,
        snowflake_item_table,
        snowflake_event_table,
        snowflake_dimension_table,
        snowflake_scd_table,
    ):
        """
        Retrieves fixture for table under test.
        """
        data_map = {
            DataType.ITEM_DATA: snowflake_item_table,
            DataType.EVENT_DATA: snowflake_event_table,
            DataType.DIMENSION_DATA: snowflake_dimension_table,
            DataType.SCD_DATA: snowflake_scd_table,
        }
        if self.data_type not in data_map:
            pytest.fail(
                f"Invalid view type `{self.data_type}` found. Please use (or map) a valid DataType."
            )
        return data_map[self.data_type]

    @pytest.fixture(name="imputed_table_under_test")
    def imputed_table_under_test_fixture(self, table_under_test):
        """
        Retrieves fixture for table under test
        """
        # check column_cleaning_operations is empty
        assert table_under_test.column_cleaning_operations == []

        table_under_test[self.col].update_critical_data_info(
            cleaning_operations=[MissingValueImputation(imputed_value=0)]
        )
        return table_under_test

    def test_table_properties(self, table_under_test):
        """Test table properties"""
        assert table_under_test.timestamp_column == self.expected_timestamp_column
        assert table_under_test.frame.timestamp_column == self.expected_timestamp_column

    def test_table_column__not_exists(self, table_under_test):
        """
        Test non-exist column retrieval
        """
        with pytest.raises(KeyError) as exc:
            _ = table_under_test["non_exist_column"]
        assert 'Column "non_exist_column" does not exist!' in str(exc.value)

        with pytest.raises(AttributeError) as exc:
            _ = table_under_test.non_exist_column
        assert (
            f"'{table_under_test.__class__.__name__}' object has no attribute 'non_exist_column'"
            in str(exc.value)
        )

        # check __getattr__ is working properly
        assert isinstance(table_under_test[self.col], TableColumn)

        # when accessing the `columns` attribute, make sure we retrieve it properly
        assert set(table_under_test.columns) == self.expected_columns

    def test_table_preview_sql(self, imputed_table_under_test, catalog):
        """
        Test preview table (make sure imputed table show only raw table sql)
        """
        table_sql = imputed_table_under_test.preview_sql()
        clean_table_sql = imputed_table_under_test.preview_sql(after_cleaning=True)
        assert table_sql == textwrap.dedent(self.expected_table_sql).strip()
        assert clean_table_sql == textwrap.dedent(self.expected_clean_table_sql).strip()

        # check table properties
        assert imputed_table_under_test.column_cleaning_operations == [
            ColumnCleaningOperation(
                column_name=self.col,
                cleaning_operations=[MissingValueImputation(imputed_value=0)],
            )
        ]
        assert imputed_table_under_test.catalog_id == catalog.id

    def test_table_column_preview_sql(self, table_under_test):
        """
        Test preview table column
        """
        table_column_sql = table_under_test[self.col].preview_sql()
        assert table_column_sql == textwrap.dedent(self.expected_table_column_sql).strip()

    def test_table_column_preview_clean_table_sql(self, imputed_table_under_test):
        """
        Test preview table column
        """
        clean_table_column_sql = imputed_table_under_test[self.col].preview_sql(after_cleaning=True)
        assert (
            clean_table_column_sql == textwrap.dedent(self.expected_clean_table_column_sql).strip()
        )

    def test_update_status(self, table_under_test):
        """
        Test update status
        """
        assert table_under_test.status == TableStatus.PUBLIC_DRAFT
        table_under_test.update_status(TableStatus.PUBLISHED)
        assert table_under_test.status == TableStatus.PUBLISHED

    def test_table_sample_payload(self, table_under_test):
        """
        Test table sample payload
        """
        if self.expected_timestamp_column:
            sample_payload = table_under_test.frame._get_sample_payload(
                from_timestamp="2020-01-01",
                to_timestamp="2020-01-02",
            )
            assert sample_payload.timestamp_column == self.expected_timestamp_column
            assert sample_payload.from_timestamp == pd.Timestamp("2020-01-01")
            assert sample_payload.to_timestamp == pd.Timestamp("2020-01-02")

        else:
            with pytest.raises(ValueError) as exc:
                table_under_test.frame._get_sample_payload(
                    from_timestamp="2020-01-01",
                    to_timestamp="2020-01-02",
                )
            assert "timestamp_column must be specified." in str(exc.value)

    def test_table_column_cleaning_operations(self, imputed_table_under_test):
        """Test table column cleaning operations property"""
        # test column cleaning operations
        table_col = imputed_table_under_test[self.col]
        assert table_col.cleaning_operations == [MissingValueImputation(imputed_value=0)]

        # test column without cleaning operations
        other_col = next(col for col in imputed_table_under_test.columns if col != self.col)
        table_col = imputed_table_under_test[other_col]
        assert table_col.cleaning_operations == []
