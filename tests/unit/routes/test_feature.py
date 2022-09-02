"""
Tests for Feature route
"""
from datetime import datetime
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId

from featurebyte.api.feature_store import FeatureStore
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.exception import DocumentConflictError, DuplicatedRegistryError
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.feature_store import SourceType, SQLiteDetails, TableDetails
from featurebyte.service.feature import FeatureService
from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureApi(BaseApiTestSuite):
    """
    TestFeatureApi class
    """

    class_name = "Feature"
    base_route = "/feature"
    payload = BaseApiTestSuite.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
    namespace_payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_namespace.json"
    )
    object_id = str(ObjectId())
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'Feature (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `Feature.get_by_id(id="{payload["_id"]}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            f'Feature (name: "sum_30m", version: "{payload["version"]}") already exists. '
            f'Get the existing object by `Feature.get_by_id(id="{payload["_id"]}")`.',
        ),
        (
            {
                **payload,
                "_id": str(ObjectId()),
                "version": f'{payload["version"]}_1',
                "feature_namespace_id": object_id,
            },
            'FeatureNamespace (name: "sum_30m") already exists. '
            'Please rename object (name: "sum_30m") to something else.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "event_data_ids": []},
            [
                {
                    "loc": ["body", "event_data_ids"],
                    "msg": "ensure this value has at least 1 items",
                    "type": "value_error.list.min_items",
                    "ctx": {"limit_value": 1},
                }
            ],
        ),
        (
            {**payload, "_id": object_id, "name": "random_name", "event_data_ids": [object_id]},
            f'EventData (id: "{object_id}") not found. ' f"Please save the EventData object first.",
        ),
        (
            {**payload, "_id": object_id, "name": "random_name"},
            'Feature (name: "random_name") has an inconsistent feature_namespace_id (name: "sum_30m").',
        ),
    ]

    @pytest.fixture(autouse=True)
    def mock_insert_feature_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch("featurebyte.service.feature.FeatureService._insert_feature_registry") as mock:
            yield mock

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_data", "event_data"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["feature_namespace_id"] = str(ObjectId())
            tabular_source = payload["tabular_source"]
            payload["tabular_source"] = {
                "feature_store_id": tabular_source["feature_store_id"],
                "table_details": {
                    key: f"{value}_{i}" for key, value in tabular_source["table_details"].items()
                },
            }
            yield payload

    @pytest.mark.asyncio
    async def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""
        super().test_create_201(test_api_client_persistent, create_success_response, user_id)
        response_dict = create_success_response.json()
        assert response_dict["readiness"] == "DRAFT"

        # check feature namespace
        test_api_client, persistent = test_api_client_persistent
        feat_namespace_docs, match_count = await persistent.find(
            collection_name="feature_namespace",
            query_filter={"name": self.payload["name"]},
        )
        assert match_count == 1
        assert feat_namespace_docs[0]["name"] == self.payload["name"]
        assert feat_namespace_docs[0]["version_ids"] == [ObjectId(self.payload["_id"])]
        assert feat_namespace_docs[0]["readiness"] == "DRAFT"
        assert feat_namespace_docs[0]["default_version_id"] == ObjectId(self.payload["_id"])
        assert feat_namespace_docs[0]["default_version_mode"] == "AUTO"
        assert feat_namespace_docs[0]["created_at"] >= datetime.fromisoformat(
            response_dict["created_at"]
        )
        assert feat_namespace_docs[0]["updated_at"] is None

        # create a new feature version with the same namespace
        new_payload = self.payload.copy()
        new_payload["_id"] = str(ObjectId())
        new_payload["version"] = f'{self.payload["version"]}_1'
        new_response = test_api_client.post("/feature", json=new_payload)
        new_response_dict = new_response.json()
        assert new_response.status_code == HTTPStatus.CREATED
        assert new_response_dict.items() >= new_payload.items()

        # check feature namespace with the new feature version
        feat_namespace_docs, match_count = await persistent.find(
            collection_name="feature_namespace",
            query_filter={"name": self.payload["name"]},
        )
        assert match_count == 1
        assert feat_namespace_docs[0]["name"] == self.payload["name"]
        assert feat_namespace_docs[0]["version_ids"] == [
            ObjectId(self.payload["_id"]),
            ObjectId(new_payload["_id"]),
        ]
        assert feat_namespace_docs[0]["readiness"] == "DRAFT"
        assert feat_namespace_docs[0]["default_version_id"] == ObjectId(new_payload["_id"])
        assert feat_namespace_docs[0]["default_version_mode"] == "AUTO"
        assert feat_namespace_docs[0]["created_at"] >= datetime.fromisoformat(
            response_dict["created_at"]
        )
        assert feat_namespace_docs[0]["updated_at"] > feat_namespace_docs[0]["created_at"]


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_fixture(feature_model_dict):
    """
    Feature model dict fixture
    """
    feature_model_dict["_id"] = str(ObjectId())
    feature_model_dict["tabular_source"] = {
        "feature_store_id": str(ObjectId()),
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": "sf_table",
        },
    }
    return feature_model_dict


@pytest.mark.asyncio
@patch("featurebyte.session.base.BaseSession.execute_query")
async def test_insert_feature_registry(
    mock_execute_query,
    feature_model_dict,
    snowflake_connector,
    snowflake_feature_store,
    get_credential,
):
    """
    Test insert_feature_registry
    """
    _ = snowflake_connector
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
        document=feature,
        get_credential=get_credential,
    )

    match_count = 0
    expected_partial_query = "INSERT INTO FEATURE_REGISTRY"
    for call_args in mock_execute_query.call_args_list:
        if expected_partial_query in call_args.args[0]:
            match_count += 1
    assert match_count > 0


@pytest.fixture(name="sqlite_feature_store")
def sqlite_feature_store_fixture(mock_get_persistent):
    """
    Sqlite source fixture
    """
    return FeatureStore(
        name="sqlite_datasource",
        type="sqlite",
        details=SQLiteDetails(filename="some_filename"),
    )


@pytest.mark.asyncio
@patch("featurebyte.session.base.BaseSession.execute_query")
async def test_insert_feature_registry__non_snowflake_feature_store(
    mock_execute_query, feature_model_dict, get_credential, sqlite_feature_store
):
    """
    Test insert_feature_registry function (when feature store is not snowflake)
    """
    feature_store = ExtendedFeatureStoreModel(
        name="sq_feature_store",
        type=SourceType.SQLITE,
        details=SQLiteDetails(filename="some_filename"),
    )
    feature_model_dict["tabular_source"] = {
        "feature_store_id": feature_store.id,
        "table_details": TableDetails(table_name="some_table"),
    }
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=sqlite_feature_store)
    await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
        document=feature, get_credential=get_credential
    )
    assert mock_execute_query.call_count == 0


@pytest.mark.asyncio
@patch("featurebyte.service.feature.FeatureManagerSnowflake")
async def test_insert_feature_registry__duplicated_feature_registry_exception(
    mock_feature_manager,
    feature_model_dict,
    get_credential,
    snowflake_connector,
    snowflake_feature_store,
):
    """
    Test insert_feature_registry with duplicated_registry exception
    """
    _ = snowflake_connector
    mock_feature_manager.return_value.insert_feature_registry.side_effect = DuplicatedRegistryError
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    with pytest.raises(DocumentConflictError) as exc:
        await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
            document=feature, get_credential=get_credential
        )
    expected_msg = (
        'Feature (name: "sum_30m") has been registered by other feature '
        "at Snowflake feature store."
    )
    assert expected_msg in str(exc)
    assert not mock_feature_manager.return_value.remove_feature_registry.called


@pytest.mark.asyncio
@patch("featurebyte.service.feature.FeatureManagerSnowflake")
async def test_insert_feature_registry__other_exception(
    mock_feature_manager,
    feature_model_dict,
    get_credential,
    snowflake_feature_store,
    snowflake_connector,
):
    """
    Test insert_feature_registry with non duplicated feature registry exception
    """
    _ = snowflake_connector
    mock_feature_manager.return_value.insert_feature_registry.side_effect = ValueError
    feature = ExtendedFeatureModel(**feature_model_dict, feature_store=snowflake_feature_store)
    with pytest.raises(ValueError):
        await FeatureService(user=Mock(), persistent=Mock())._insert_feature_registry(
            document=feature,
            get_credential=get_credential,
        )
    assert mock_feature_manager.return_value.remove_feature_registry.called
