"""
Unit tests for HistoricalFeatureTable class
"""
import pytest

from featurebyte.api.historical_feature_table import HistoricalFeatureTable
from featurebyte.exception import RecordRetrievalException


def test_get(historical_feature_table):
    """
    Test retrieving an HistoricalFeatureTable object by name
    """
    retrieved_historical_feature_table = HistoricalFeatureTable.get(historical_feature_table.name)
    assert retrieved_historical_feature_table == historical_feature_table


@pytest.mark.usefixtures("historical_feature_table")
def test_list():
    """
    Test listing HistoricalFeatureTable objects
    """
    df = HistoricalFeatureTable.list()
    assert df.columns.tolist() == [
        "id",
        "name",
        "feature_store_name",
        "observation_table_name",
        "shape",
        "created_at",
    ]
    assert df["name"].tolist() == ["my_historical_feature_table"]
    assert df["feature_store_name"].tolist() == ["sf_featurestore"]
    assert df["observation_table_name"].tolist() == ["observation_table_from_source_table"]
    assert (df["shape"] == (500, 1)).all()


def test_delete(historical_feature_table):
    """
    Test delete method
    """
    # check table can be retrieved before deletion
    _ = HistoricalFeatureTable.get(historical_feature_table.name)

    historical_feature_table.delete()

    # check the deleted batch feature table is not found anymore
    with pytest.raises(RecordRetrievalException) as exc:
        HistoricalFeatureTable.get(historical_feature_table.name)

    expected_msg = (
        f'HistoricalFeatureTable (name: "{historical_feature_table.name}") not found. '
        f"Please save the HistoricalFeatureTable object first."
    )
    assert expected_msg in str(exc.value)


def test_info(historical_feature_table):
    """Test get historical feature table info"""
    info_dict = historical_feature_table.info()
    assert info_dict["table_details"]["table_name"].startswith("HISTORICAL_FEATURE_TABLE_")
    assert isinstance(info_dict["feature_list_version"], str)
    assert info_dict == {
        "name": "my_historical_feature_table",
        "feature_list_name": "feature_list_for_historical_feature_table",
        "feature_list_version": info_dict["feature_list_version"],
        "observation_table_name": "observation_table_from_source_table",
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": info_dict["table_details"]["table_name"],
        },
        "created_at": info_dict["created_at"],
        "updated_at": None,
    }
