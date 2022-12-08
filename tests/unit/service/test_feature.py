"""
Test FeatureService
"""
import json
import os
from unittest.mock import patch

import pytest
from bson import ObjectId

from featurebyte import FeatureStore
from featurebyte.exception import DocumentInconsistencyError, DocumentNotFoundError
from featurebyte.models.feature_store import SQLiteDetails
from featurebyte.schema.feature import FeatureCreate


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_fixture(test_dir):
    """
    Feature model dict fixture
    """
    feature_model_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(feature_model_path) as file_handle:
        return json.load(file_handle)


@pytest.fixture(name="sqlite_feature_store")
def sqlite_feature_store_fixture(mock_get_persistent):
    """
    Sqlite source fixture
    """
    _ = mock_get_persistent
    return FeatureStore(
        name="sqlite_datasource",
        type="sqlite",
        details=SQLiteDetails(filename="some_filename"),
    )


@pytest.fixture(name="mock_execute_query", autouse=True)
def execute_query_fixture():
    """
    Execute query fixture
    """
    with patch("featurebyte.session.base.BaseSession.execute_query") as mock_execute_query:
        yield mock_execute_query


@pytest.mark.asyncio
async def test_update_document__inconsistency_error(feature_service, feature):
    """Test feature creation - document inconsistency error"""
    data_dict = feature.dict(by_alias=True)
    data_dict["_id"] = ObjectId()
    data_dict["name"] = "random_name"
    with pytest.raises(DocumentInconsistencyError) as exc:
        await feature_service.create_document(
            data=FeatureCreate(**data_dict),
        )
    expected_msg = (
        'Feature (name: "random_name") object(s) within the same namespace must have the same "name" value '
        '(namespace: "sum_30m", feature: "random_name").'
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_get_document_by_name_and_version(feature_service, feature):
    """Test feature service - get_document_by_name_and_version"""
    doc = await feature_service.get_document_by_name_and_version(
        name=feature.name, version=feature.version
    )
    assert doc == feature

    with pytest.raises(DocumentNotFoundError) as exc:
        await feature_service.get_document_by_name_and_version(
            name="random_name", version=feature.version
        )
    expected_msg = (
        f'Feature (name: "random_name", version: "{feature.version.to_str()}") not found. '
        f"Please save the Feature object first."
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_feature_document_contains_raw_graph(feature_service, feature):
    """Test raw graph is stored"""
    expected_groupby_node = {
        "name": "groupby_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "sum",
            "aggregation_id": "sum_397b7898e867241e3238cced04423af283a862da",
            "blind_spot": 600,
            "entity_ids": [ObjectId("637449b96d6e838b025328e2")],
            "frequency": 1800,
            "keys": ["cust_id"],
            "names": ["sum_30m"],
            "parent": "col_float",
            "serving_names": ["cust_id"],
            "tile_id": "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "time_modulo_frequency": 300,
            "timestamp": "event_timestamp",
            "value_by": None,
            "windows": ["30m"],
        },
        "type": "groupby",
    }
    # note: raw graph's node parameters is not pruned
    expected_raw_groupby_params = expected_groupby_node["parameters"].copy()
    expected_raw_groupby_params["names"] = ["sum_30m", "sum_2h", "sum_1d"]
    expected_raw_groupby_params["windows"] = ["30m", "2h", "1d"]
    expected_raw_groupby_params["aggregation_id"] = "sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512"
    expected_raw_groupby_node = {**expected_groupby_node, "parameters": expected_raw_groupby_params}
    async for doc in feature_service.list_documents_iterator(query_filter={"_id": feature.id}):
        assert doc["graph"]["nodes"][1] == expected_groupby_node
        assert doc["raw_graph"]["nodes"][1] == expected_raw_groupby_node
