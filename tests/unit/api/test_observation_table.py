"""
Unit tests for ObservationTable class
"""
from unittest.mock import Mock, call, patch

import pytest

from featurebyte import RecordRetrievalException
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.source_table import SourceTable


def test_get(observation_table_from_source):
    """
    Test retrieving an ObservationTable object by name
    """
    observation_table = ObservationTable.get(observation_table_from_source.name)
    assert observation_table == observation_table_from_source


@pytest.mark.usefixtures("observation_table_from_source", "observation_table_from_view")
def test_list():
    """
    Test listing ObservationTable objects
    """
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


def test_delete(observation_table_from_view):
    """
    Test delete method
    """
    # check table can be retrieved before deletion
    _ = ObservationTable.get(observation_table_from_view.name)

    observation_table_from_view.delete()

    # check the deleted batch feature table is not found anymore
    with pytest.raises(RecordRetrievalException) as exc:
        ObservationTable.get(observation_table_from_view.name)

    expected_msg = (
        f'ObservationTable (name: "{observation_table_from_view.name}") not found. '
        f"Please save the ObservationTable object first."
    )
    assert expected_msg in str(exc.value)


def test_info(observation_table_from_source):
    """Test get observation table info"""
    info_dict = observation_table_from_source.info()
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
    }


def test_data_source(observation_table_from_source):
    """
    Test the underlying SourceTable is constructed properly
    """
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
