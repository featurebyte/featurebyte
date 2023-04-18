"""
Unit tests for HistoricalFeatureTable class
"""
import pytest

from featurebyte.api.historical_feature_table import HistoricalFeatureTable


def test_get(historical_feature_table):
    """
    Test retrieving an HistoricalFeatureTable object by name
    """
    retrieved_historical_feature_table = HistoricalFeatureTable.get(historical_feature_table.name)
    assert retrieved_historical_feature_table.name == historical_feature_table.name


@pytest.mark.usefixtures("historical_feature_table")
def test_list():
    """
    Test listing HistoricalFeatureTable objects
    """
    df = HistoricalFeatureTable.list()
    assert df.columns.tolist() == [
        "name",
        "feature_store_name",
        "observation_table_name",
        "created_at",
    ]
    assert df["name"].tolist() == ["my_historical_feature_table"]
    assert df["feature_store_name"].tolist() == ["sf_featurestore"]
    assert df["observation_table_name"].tolist() == ["observation_table_from_source_table"]
