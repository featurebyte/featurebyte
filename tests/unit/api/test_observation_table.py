"""
Unit tests for ObservationTable class
"""
from typing import Any, Dict

from unittest.mock import Mock, call, patch

import pytest

from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.source_table import SourceTable
from tests.unit.api.base_materialize_table_test import BaseMaterializedTableApiTest


class TestObservationTable(BaseMaterializedTableApiTest[ObservationTable]):
    """
    Test observation table
    """

    table_type = ObservationTable

    def assert_info_dict(self, info_dict: Dict[str, Any]) -> None:
        assert info_dict["table_details"]["table_name"].startswith("OBSERVATION_TABLE_")
        assert info_dict == {
            "name": "observation_table_from_source_table",
            "type": "source_table",
            "feature_store_name": "sf_featurestore",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": info_dict["table_details"]["table_name"],
            },
            "created_at": info_dict["created_at"],
            "updated_at": None,
            "description": None,
        }

    @pytest.mark.skip(reason="use other test due to testing of more fixtures")
    def test_list(self, table_under_test):
        ...


@pytest.mark.usefixtures("observation_table_from_source", "observation_table_from_view")
def test_list(catalog):
    """
    Test listing ObservationTable objects
    """
    _ = catalog
    df = ObservationTable.list()
    assert df.columns.tolist() == [
        "id",
        "name",
        "type",
        "shape",
        "feature_store_name",
        "created_at",
    ]
    assert df["name"].tolist() == [
        "observation_table_from_event_view",
        "observation_table_from_source_table",
    ]
    assert (df["feature_store_name"] == "sf_featurestore").all()
    assert df["type"].tolist() == ["view", "source_table"]
    assert (df["shape"] == (100, 2)).all()


def test_shape(observation_table_from_source):
    """
    Test shape method
    """
    assert observation_table_from_source.shape() == (100, 2)


@patch(
    "featurebyte.service.feature_store_warehouse.FeatureStoreWarehouseService.check_table_exists"
)
def test_data_source(mock_check_table_exists, observation_table_from_source):
    """
    Test the underlying SourceTable is constructed properly
    """
    _ = mock_check_table_exists
    observation_table = observation_table_from_source
    source_table = observation_table._source_table
    assert source_table.feature_store.id == observation_table.location.feature_store_id
    assert source_table.tabular_source.table_details == observation_table.location.table_details


@pytest.fixture(name="mock_source_table")
def mock_source_table_fixture():
    """
    Patches the underlying SourceTable in MaterializedTableMixin
    """
    mock_source_table = Mock(name="mock_source_table", spec=SourceTable)
    mock_feature_store = Mock(
        name="mock_feature_store",
        get_data_source=Mock(
            return_value=Mock(
                name="mock_data_source",
                get_source_table=Mock(return_value=mock_source_table),
            )
        ),
    )
    with patch(
        "featurebyte.api.materialized_table.FeatureStore.get_by_id", return_value=mock_feature_store
    ):
        yield mock_source_table


def test_preview(observation_table_from_source, mock_source_table):
    """
    Test preview() calls the underlying SourceTable's preview() method
    """
    result = observation_table_from_source.preview(limit=123)
    assert mock_source_table.preview.call_args == call(limit=123)
    assert result is mock_source_table.preview.return_value


def test_sample(observation_table_from_source, mock_source_table):
    """
    Test sample() calls the underlying SourceTable's sample() method
    """
    result = observation_table_from_source.sample(size=123, seed=456)
    assert mock_source_table.sample.call_args == call(size=123, seed=456)
    assert result is mock_source_table.sample.return_value


def test_describe(observation_table_from_source, mock_source_table):
    """
    Test describe() calls the underlying SourceTable's describe() method
    """
    result = observation_table_from_source.describe(size=123, seed=456)
    assert mock_source_table.describe.call_args == call(size=123, seed=456)
    assert result is mock_source_table.describe.return_value
