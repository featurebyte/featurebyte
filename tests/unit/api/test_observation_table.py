"""
Unit tests for ObservationTable class
"""
import pytest

from featurebyte import RecordRetrievalException
from featurebyte.api.observation_table import ObservationTable


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
        "feature_store_name",
        "created_at",
    ]
    assert df["name"].tolist() == [
        "observation_table_from_event_view",
        "observation_table_from_source_table",
    ]
    assert (df["feature_store_name"] == "sf_featurestore").all()
    assert df["type"].tolist() == ["view", "source_table"]


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
