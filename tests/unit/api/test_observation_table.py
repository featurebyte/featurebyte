"""
Unit tests for ObservationTable class
"""
import pytest

from featurebyte.api.observation_table import ObservationTable


def test_get(observation_table_from_source):
    """
    Test retrieving an ObservationTable object by name
    """
    observation_table = ObservationTable.get(observation_table_from_source.name)
    assert observation_table.name == observation_table_from_source.name


@pytest.mark.usefixtures("observation_table_from_source", "observation_table_from_view")
def test_list():
    """
    Test listing ObservationTable objects
    """
    df = ObservationTable.list()
    assert df.columns.tolist() == [
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
