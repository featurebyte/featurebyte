"""
Unit tests for ModelingTable class
"""
import pytest

from featurebyte.api.modeling_table import ModelingTable


def test_get(modeling_table):
    """
    Test retrieving an ModelingTable object by name
    """
    retrieved_modeling_table = ModelingTable.get(modeling_table.name)
    assert retrieved_modeling_table.name == modeling_table.name


@pytest.mark.usefixtures("modeling_table")
def test_list():
    """
    Test listing ModelingTable objects
    """
    df = ModelingTable.list()
    assert df.columns.tolist() == [
        "id",
        "created_at",
        "name",
        "feature_store_name",
    ]
    assert df["name"].tolist() == ["my_modeling_table"]
    assert df["feature_store_name"].tolist() == ["sf_featurestore"]
