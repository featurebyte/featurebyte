"""
Unit tests for ObservationTable class
"""
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.api.observation_table import ObservationTable
from featurebyte.models.observation_table import ObservationTableModel


@pytest.fixture(name="observation_table_dict")
def observation_table_dict_fixture(snowflake_event_table):
    feature_store_id = ObjectId()
    return ObservationTableModel(
        **{
            "name": "my_observation_table",
            "location": {
                "feature_store_id": feature_store_id,
                "table_details": {
                    "database_name": "fb_database",
                    "schema_name": "fb_schema",
                    "table_name": "fb_materialized_table",
                },
            },
            "observation_input": {
                "source": snowflake_event_table.tabular_source.dict(),
                "type": "source_table",
            },
        }
    ).dict()


@pytest_asyncio.fixture(name="saved_observation_table_document")
async def saved_observation_table_document_fixture(mock_get_persistent, observation_table_dict):
    """
    Saved observation table fixture
    """
    persistent = mock_get_persistent()
    await persistent.insert_one(
        collection_name=ObservationTableModel.collection_name(),
        document=observation_table_dict,
        user_id=None,
    )


@pytest_asyncio.fixture(name="multiple_saved_observation_table_documents")
async def multiple_saved_observation_table_documents_fixture(
    mock_get_persistent,
    observation_table_dict,
):
    """
    Saved observation tables
    """
    persistent = mock_get_persistent()
    for i in range(3):
        observation_table_dict["_id"] = ObjectId()
        observation_table_dict["name"] = f"my_observation_table_{i}"
        await persistent.insert_one(
            collection_name=ObservationTableModel.collection_name(),
            document=observation_table_dict,
            user_id=None,
        )


@pytest.mark.usefixtures("saved_observation_table_document")
def test_get(observation_table_dict):
    """
    Test retrieving an ObservationTable object by name
    """
    observation_table = ObservationTable.get(observation_table_dict["name"])
    assert observation_table.name == observation_table_dict["name"]


@pytest.mark.usefixtures("multiple_saved_observation_table_documents")
def test_list():
    """
    Test listing ObservationTable objects
    """
    df = ObservationTable.list()
    assert df.columns.tolist() == [
        "id",
        "created_at",
        "name",
        "database_name",
        "schema_name",
        "table_name",
    ]
