"""
Unit tests for TargetTable class
"""
import pytest

from featurebyte.api.target_table import TargetTable
from featurebyte.exception import RecordRetrievalException


def test_get(target_table):
    """
    Test retrieving an TargetTable object by name
    """
    retrieved_target_table = TargetTable.get(target_table.name)
    assert retrieved_target_table == target_table


@pytest.mark.usefixtures("target_table")
def test_list():
    """
    Test listing TargetTable objects
    """
    df = TargetTable.list()
    assert df.columns.tolist() == [
        "id",
        "name",
        "feature_store_name",
        "observation_table_name",
        "shape",
        "created_at",
    ]
    assert df["name"].tolist() == ["my_target_table"]
    assert df["feature_store_name"].tolist() == ["sf_featurestore"]
    assert df["observation_table_name"].tolist() == ["observation_table_from_source_table"]
    assert (df["shape"] == (500, 1)).all()


def test_delete(target_table):
    """
    Test delete method
    """
    # check table can be retrieved before deletion
    _ = TargetTable.get(target_table.name)

    target_table.delete()

    # check the deleted batch feature table is not found anymore
    with pytest.raises(RecordRetrievalException) as exc:
        TargetTable.get(target_table.name)

    expected_msg = (
        f'TargetTable (name: "{target_table.name}") not found. '
        f"Please save the TargetTable object first."
    )
    assert expected_msg in str(exc.value)


def test_info(target_table):
    """Test get historical feature table info"""
    info_dict = target_table.info()
    assert info_dict["table_details"]["table_name"].startswith("TARGET_TABLE_")
    assert info_dict == {
        "name": "my_target_table",
        "target_name": "float_target",
        "observation_table_name": "observation_table_from_source_table",
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": info_dict["table_details"]["table_name"],
        },
        "created_at": info_dict["created_at"],
        "updated_at": None,
    }
