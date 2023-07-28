"""
Unit tests for StaticSourceTable class
"""
from unittest.mock import Mock, call, patch

import pytest

from featurebyte import RecordRetrievalException
from featurebyte.api.source_table import SourceTable
from featurebyte.api.static_source_table import StaticSourceTable


def test_get(static_source_table_from_source):
    """
    Test retrieving an StaticSourceTable object by name
    """
    static_source_table = StaticSourceTable.get(static_source_table_from_source.name)
    assert static_source_table == static_source_table_from_source


@pytest.mark.usefixtures("static_source_table_from_source", "static_source_table_from_view")
def test_list(catalog):
    """
    Test listing StaticSourceTable objects
    """
    _ = catalog
    df = StaticSourceTable.list()
    assert df.columns.tolist() == [
        "id",
        "name",
        "type",
        "shape",
        "feature_store_name",
        "created_at",
    ]
    assert df["name"].tolist() == [
        "static_source_table_from_event_view",
        "static_source_table_from_source_table",
    ]
    assert (df["feature_store_name"] == "sf_featurestore").all()
    assert df["type"].tolist() == ["view", "source_table"]
    assert df["shape"].tolist() == [[100, 2]] * 2


def test_delete(static_source_table_from_view):
    """
    Test delete method
    """
    # check table can be retrieved before deletion
    _ = StaticSourceTable.get(static_source_table_from_view.name)

    static_source_table_from_view.delete()

    # check the deleted batch feature table is not found anymore
    with pytest.raises(RecordRetrievalException) as exc:
        StaticSourceTable.get(static_source_table_from_view.name)

    expected_msg = (
        f'StaticSourceTable (name: "{static_source_table_from_view.name}") not found. '
        f"Please save the StaticSourceTable object first."
    )
    assert expected_msg in str(exc.value)


def test_info(static_source_table_from_source):
    """Test get static source table info"""
    info_dict = static_source_table_from_source.info()
    assert info_dict["table_details"]["table_name"].startswith("STATIC_SOURCE_TABLE_")
    assert info_dict == {
        "name": "static_source_table_from_source_table",
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


def test_shape(static_source_table_from_source):
    """
    Test shape method
    """
    assert static_source_table_from_source.shape() == (100, 2)


@patch(
    "featurebyte.service.feature_store_warehouse.FeatureStoreWarehouseService.check_table_exists"
)
def test_data_source(mock_check_table_exists, static_source_table_from_source):
    """
    Test the underlying SourceTable is constructed properly
    """
    _ = mock_check_table_exists
    static_source_table = static_source_table_from_source
    source_table = static_source_table._source_table
    assert source_table.feature_store.id == static_source_table.location.feature_store_id
    assert source_table.tabular_source.table_details == static_source_table.location.table_details


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


def test_preview(static_source_table_from_source, mock_source_table):
    """
    Test preview() calls the underlying SourceTable's preview() method
    """
    result = static_source_table_from_source.preview(limit=123)
    assert mock_source_table.preview.call_args == call(limit=123)
    assert result is mock_source_table.preview.return_value


def test_sample(static_source_table_from_source, mock_source_table):
    """
    Test sample() calls the underlying SourceTable's sample() method
    """
    result = static_source_table_from_source.sample(size=123, seed=456)
    assert mock_source_table.sample.call_args == call(size=123, seed=456)
    assert result is mock_source_table.sample.return_value


def test_describe(static_source_table_from_source, mock_source_table):
    """
    Test describe() calls the underlying SourceTable's describe() method
    """
    result = static_source_table_from_source.describe(size=123, seed=456)
    assert mock_source_table.describe.call_args == call(size=123, seed=456)
    assert result is mock_source_table.describe.return_value


def test_update_description(static_source_table_from_source):
    """Test update description"""
    assert static_source_table_from_source.description is None
    static_source_table_from_source.update_description("new description")
    assert static_source_table_from_source.description == "new description"
    assert static_source_table_from_source.info()["description"] == "new description"
    static_source_table_from_source.update_description(None)
    assert static_source_table_from_source.description is None
    assert static_source_table_from_source.info()["description"] is None
