"""
Base materialize table test class
"""

import os.path
import tempfile
from abc import abstractmethod
from typing import Any, Dict, Type, TypeVar

import pandas as pd
import pytest

from featurebyte.api.api_object import ApiObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import CAMEL_CASE_TO_SNAKE_CASE_PATTERN

BaseFeatureOrTargetTableT = TypeVar("BaseFeatureOrTargetTableT", bound=ApiObject)


class BaseMaterializedTableApiTest:
    """
    Base materialized table api tests
    """

    table_type: Type[BaseFeatureOrTargetTableT]

    @abstractmethod
    def assert_info_dict(self, info_dict: Dict[str, Any]) -> None:
        """
        Assert info dict that is returned from table.info().

        Parameters
        ----------
        info_dict: Dict[str, Any]
            info_dict that is returned
        """

    @property
    def table_type_name(self) -> str:
        """
        Table type name

        Returns
        -------
        str
        """
        return self.table_type.__name__

    @pytest.fixture(name="table_under_test")
    def get_table_under_test_fixture(self, request):
        table_type_to_fixture_name_map = {
            "TargetTable": "target_table",
            "HistoricalFeatureTable": "historical_feature_table",
            "ObservationTable": "observation_table_from_source",
            "BatchRequestTable": "batch_request_table_from_source",
            "BatchFeatureTable": "batch_feature_table",
        }
        if self.table_type_name not in table_type_to_fixture_name_map:
            pytest.fail(
                f"Invalid type `{self.table_type_name}` found. Please use a valid ApiObject type."
            )
        table_type_fixture_name = table_type_to_fixture_name_map[self.table_type_name]
        return request.getfixturevalue(table_type_fixture_name)

    def test_get(self, table_under_test):
        """
        Test retrieving an table object by name
        """
        retrieved_table = self.table_type.get(table_under_test.name)
        assert retrieved_table == table_under_test

    def assert_list_df(self, df: pd.DataFrame) -> None:
        """
        Assert list dataframe is expected.
        """
        assert df.columns.tolist() == [
            "id",
            "name",
            "feature_store_name",
            "observation_table_name",
            "shape",
            "created_at",
        ]
        expected_name = CAMEL_CASE_TO_SNAKE_CASE_PATTERN.sub(r"_\1", self.table_type_name).lower()
        assert df["name"].tolist() == [f"my_{expected_name}"]
        assert df["feature_store_name"].tolist() == ["sf_featurestore"]
        assert df["observation_table_name"].tolist() == ["observation_table_from_source_table"]
        assert df["shape"].tolist() == [[500, 3]]

    def test_list(self, table_under_test):
        """
        Test listing table objects
        """
        _ = table_under_test
        df = self.table_type.list()
        self.assert_list_df(df)

    def test_delete(self, table_under_test):
        """
        Test delete method
        """
        # check table can be retrieved before deletion
        _ = self.table_type.get(table_under_test.name)

        table_under_test.delete()

        # check the deleted batch feature table is not found anymore
        with pytest.raises(RecordRetrievalException) as exc:
            self.table_type.get(table_under_test.name)

        expected_msg = (
            f'{self.table_type_name} (name: "{table_under_test.name}") not found. '
            f"Please save the {self.table_type_name} object first."
        )
        assert expected_msg in str(exc.value)

    def test_info(self, table_under_test):
        """
        Test get table info
        """
        info_dict = table_under_test.info()
        self.assert_info_dict(info_dict)

    def test_update_description(self, table_under_test):
        """Test update description"""
        assert table_under_test.description is None
        table_under_test.update_description("new description")
        assert table_under_test.description == "new description"
        assert table_under_test.info()["description"] == "new description"
        table_under_test.update_description(None)
        assert table_under_test.description is None
        assert table_under_test.info()["description"] is None

    def test_download(self, table_under_test):
        """
        Test download method
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "output.parquet")
            path = table_under_test.download(file_path)
            assert path.exists()

            # expect download to fail if file already exists
            with pytest.raises(FileExistsError) as exc:
                table_under_test.download(path)
            assert f"{path} already exists" in str(exc.value)

            # expect download to succeed if file already exists and overwrite is True
            table_under_test.download(path, overwrite=True)
