"""
Unit tests for ModelingTable class
"""
import pytest

from featurebyte.api.modeling_table import HistoricalFeatureTable


def test_get(modeling_table):
    """
    Test retrieving an ModelingTable object by name
    """
    retrieved_modeling_table = HistoricalFeatureTable.get(modeling_table.name)
    assert retrieved_modeling_table.name == modeling_table.name


@pytest.mark.usefixtures("modeling_table")
def test_list():
    """
    Test listing ModelingTable objects
    """
    df = HistoricalFeatureTable.list()
    assert df.columns.tolist() == [
        "name",
        "feature_store_name",
        "observation_table_name",
        "created_at",
    ]
    assert df["name"].tolist() == ["my_modeling_table"]
    assert df["feature_store_name"].tolist() == ["sf_featurestore"]
    assert df["observation_table_name"].tolist() == ["observation_table_from_source_table"]
