"""
Tests for FeatureList route
"""
import json
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import SourceType
from featurebyte.exception import DocumentConflictError, DuplicatedRegistryError
from featurebyte.feature_manager.model import ExtendedFeatureListModel
from featurebyte.models.feature_store import SQLiteDetails
from featurebyte.service.feature_list import FeatureListService
from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureListApi(BaseApiTestSuite):
    """
    TestFeatureListApi class
    """

    class_name = "FeatureList"
    base_route = "/feature_list"
    payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_list_single.json"
    )
    payload_multi = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_list_multi.json"
    )
    object_id = str(ObjectId())
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'FeatureList (id: "{payload["_id"]}") already exists. '
            'Get the existing object by `FeatureList.get(name="sf_feature_list")`.',
        ),
        (
            {**payload, "_id": object_id},
            'FeatureList (name: "sf_feature_list", version: "V220906") already exists. '
            'Get the existing object by `FeatureList.get_by_id(id="6317467bb72b797bd08f7300")`.',
        ),
        (
            {**payload, "_id": object_id, "name": "other_name"},
            "FeatureList (feature_ids: \"[ObjectId('6317467bb72b797bd08f72fa')]\") already exists. "
            'Get the existing object by `FeatureList.get(name="sf_feature_list")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "_id": object_id, "name": "random_name", "feature_ids": [object_id]},
            f'Feature (id: "{object_id}") not found. ' "Please save the Feature object first.",
        ),
        (
            payload_multi,
            'Feature (id: "6317467bb72b797bd08f72fc") not found. Please save the Feature object first.',
        ),
        (
            {**payload, "feature_ids": []},
            [
                {
                    "loc": ["body", "feature_ids"],
                    "msg": "ensure this value has at least 1 items",
                    "type": "value_error.list.min_items",
                    "ctx": {"limit_value": 1},
                }
            ],
        ),
    ]

    @pytest.fixture(autouse=True)
    def mock_insert_feature_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch("featurebyte.service.feature.FeatureService._insert_feature_registry") as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def mock_insert_feature_list_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch(
            "featurebyte.service.feature_list.FeatureListService._insert_feature_list_registry"
        ) as mock:
            yield mock

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_data", "event_data"),
            ("feature", "feature_sum_30m"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        for i in range(3):
            # make a new feature from feature_sum_30m & create a new feature_ids
            feature_payload = self.load_payload(
                "tests/fixtures/request_payloads/feature_sum_30m.json"
            )
            new_feature_id = str(ObjectId())
            response = api_client.post(
                "/feature",
                json={
                    **feature_payload,
                    "_id": new_feature_id,
                    "version": f'{feature_payload["version"]}_{i}',
                },
            )
            assert response.status_code == HTTPStatus.CREATED

            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["feature_ids"] = [new_feature_id]
            yield payload

    @pytest.mark.asyncio
    async def test_create_201__with_existing_feature_list_namespace(
        self, test_api_client_persistent, create_success_response, user_id
    ):
        """Test create (success) - with existing feature list namespace"""
        assert create_success_response.status_code == HTTPStatus.CREATED
        result = create_success_response.json()

        test_api_client, persistent = test_api_client_persistent
        # create a new feature
        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        feature_payload["version"] = f'{feature_payload["version"]}_1'
        feature_id = await persistent.insert_one(
            collection_name="feature",
            document={
                **feature_payload,
                "_id": ObjectId(),
                "user_id": ObjectId(user_id),
                "readiness": "PRODUCTION_READY",
            },
        )

        # prepare a new payload with existing feature list namespace
        new_payload = self.payload.copy()
        new_payload["_id"] = str(ObjectId())
        new_payload["version"] = f'{new_payload["version"]}_1'
        new_payload["feature_ids"] = [str(feature_id)]
        new_payload["feature_list_namespace_id"] = result["feature_list_namespace_id"]
        expected_readiness_dist = [{"count": 1, "readiness": "PRODUCTION_READY"}]
        response = test_api_client.post(f"{self.base_route}", json=new_payload)
        new_fl_dict = response.json()
        assert new_fl_dict["readiness_distribution"] == expected_readiness_dist
        assert new_fl_dict["feature_list_namespace_id"] == result["feature_list_namespace_id"]

        # check feature list namespace
        namespace_response = test_api_client.get(
            f"/feature_list_namespace/{result['feature_list_namespace_id']}"
        )
        namespace_response_dict = namespace_response.json()
        assert namespace_response_dict["feature_list_ids"] == [result["_id"], new_fl_dict["_id"]]
        assert namespace_response_dict["readiness_distribution"] == expected_readiness_dist
        assert namespace_response_dict["default_version_mode"] == "AUTO"
        assert namespace_response_dict["default_feature_list_id"] == new_fl_dict["_id"]

    def test_create_201_multiple_features(self, test_api_client_persistent, user_id):
        """Create feature list with multiple features"""
        _ = user_id
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # save another feature
        payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        response = test_api_client.post("/feature", json=payload)
        assert response.status_code == HTTPStatus.CREATED

        # make sure the payload feature_ids is in non-sorted order
        payload_multi = self.payload_multi.copy()
        payload_multi["feature_ids"] = list(reversed(payload_multi["feature_ids"]))
        assert payload_multi["feature_ids"] != sorted(payload_multi["feature_ids"])

        # check that feature_ids in post response are sorted
        response = test_api_client.post("/feature_list", json=payload_multi)
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["feature_ids"] == sorted(payload_multi["feature_ids"])

    def test_create_422__different_feature_stores(self, test_api_client_persistent):
        """
        Test feature list with different feature stores
        """
        test_api_client, _ = test_api_client_persistent
        # create feature_store, event_data & feature
        self.setup_creation_route(api_client=test_api_client)

        # create another feature_store, event_data & feature with different feature_store
        feature_store = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        feature_store["_id"] = str(ObjectId())
        feature_store["name"] = f'new_{feature_store["name"]}'
        feature_store["details"] = {
            key: f"{value}_1" for key, value in feature_store["details"].items()
        }

        event_data = self.load_payload("tests/fixtures/request_payloads/event_data.json")
        event_data["_id"] = str(ObjectId())
        event_data["name"] = f'new_{event_data["name"]}'
        tabular_source = {
            "feature_store_id": feature_store["_id"],
            "table_details": event_data["tabular_source"]["table_details"],
        }
        event_data["tabular_source"] = tabular_source

        feature = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        feature["tabular_source"] = tabular_source

        payload_api_object_pairs = [
            (feature_store, "feature_store"),
            (event_data, "event_data"),
            (feature, "feature"),
        ]
        for payload, api_object in payload_api_object_pairs:
            response = test_api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

        # test feature list post route
        response = test_api_client.post(f"{self.base_route}", json=self.payload_multi)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == (
            "All the Feature objects within the same FeatureList object must be from the same feature store."
        )

    def test_create_422__duplicated_feature_name(self, test_api_client_persistent):
        """
        Test feature list with different feature stores
        """
        test_api_client, _ = test_api_client_persistent
        # create feature_store, event_data & feature
        self.setup_creation_route(api_client=test_api_client)

        # create another feature with the same name
        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        new_feature_id = str(ObjectId())
        feature_payload["_id"] = new_feature_id
        feature_payload["version"] = f"{feature_payload['version']}_1"
        response = test_api_client.post("/feature", json=feature_payload)
        assert response.status_code == HTTPStatus.CREATED, response.text

        payload = self.load_payload("tests/fixtures/request_payloads/feature_list_single.json")
        payload["feature_ids"].append(new_feature_id)

        # check that feature_ids in post response are sorted
        response = test_api_client.post("/feature_list", json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.text
        assert response.json()["detail"] == (
            "Two Feature objects must not share the same name in a FeatureList object."
        )

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert (
            response_dict.items()
            > {
                "name": "sf_feature_list",
                "updated_at": None,
                "entities": {
                    "data": [{"name": "customer", "serving_names": ["cust_id"]}],
                    "page": 1,
                    "page_size": 10,
                    "total": 1,
                },
                "event_data": {
                    "data": [{"name": "sf_event_data", "status": "DRAFT"}],
                    "page": 1,
                    "page_size": 10,
                    "total": 1,
                },
                "default_version_mode": "AUTO",
                "dtype_distribution": [{"count": 1, "dtype": "FLOAT"}],
                "version_count": 1,
                "feature_count": 1,
                "version": {"this": "V220906", "default": "V220906"},
                "production_ready_fraction": {"this": 0, "default": 0},
            }.items()
        )
        assert "created_at" in response_dict


@pytest.fixture(name="feature_list_model")
def feature_list_model_fixture():
    """FeatureList model fixture"""
    with open("tests/fixtures/request_payloads/feature_sum_30m.json") as fhandle:
        feature_dict = json.loads(fhandle.read())

    with open("tests/fixtures/request_payloads/feature_list_single.json") as fhandle:
        feature_list_dict = json.loads(fhandle.read())
        feature_list_dict["feature_signatures"] = [
            {
                "id": feature_dict["_id"],
                "name": feature_list_dict["name"],
                "version": feature_list_dict["version"],
            }
        ]
        feature_list = ExtendedFeatureListModel(**feature_list_dict)
    return feature_list


@pytest.mark.asyncio
@patch("featurebyte.session.base.BaseSession.execute_query")
async def test_insert_feature_list_registry(
    mock_execute_query,
    snowflake_connector,
    snowflake_feature_store,
    get_credential,
    feature_list_model,
):
    """
    Test insert_feature_list_registry
    """
    _ = snowflake_connector
    await FeatureListService(user=Mock(), persistent=Mock())._insert_feature_list_registry(
        document=feature_list_model,
        feature_store=snowflake_feature_store,
        get_credential=get_credential,
    )

    match_count = 0
    expected_partial_query = "INSERT INTO FEATURE_LIST_REGISTRY"
    for call_args in mock_execute_query.call_args_list:
        if expected_partial_query in call_args.args[0]:
            match_count += 1
    assert match_count > 0


@pytest.mark.asyncio
@patch("featurebyte.session.base.BaseSession.execute_query")
async def test_insert_feature_list_registry__non_snowflake_feature_store(
    mock_execute_query, feature_list_model, get_credential
):
    """
    Test insert_feature_registry function (when feature store is not snowflake)
    """
    feature_store = ExtendedFeatureStoreModel(
        name="sq_feature_store",
        type=SourceType.SQLITE,
        details=SQLiteDetails(filename="some_filename"),
    )
    await FeatureListService(user=Mock(), persistent=Mock())._insert_feature_list_registry(
        document=feature_list_model,
        feature_store=feature_store,
        get_credential=get_credential,
    )
    assert mock_execute_query.call_count == 0


@pytest.mark.asyncio
@patch("featurebyte.service.feature_list.FeatureListManagerSnowflake")
async def test_insert_feature_registry__duplicated_feature_registry_exception(
    mock_feature_list_manager,
    feature_list_model,
    get_credential,
    snowflake_connector,
    snowflake_feature_store,
):
    """
    Test insert_feature_list_registry with duplicated_registry exception
    """
    _ = snowflake_connector
    mock_feature_list_manager.return_value.insert_feature_list_registry.side_effect = (
        DuplicatedRegistryError
    )
    with pytest.raises(DocumentConflictError) as exc:
        await FeatureListService(user=Mock(), persistent=Mock())._insert_feature_list_registry(
            document=feature_list_model,
            feature_store=snowflake_feature_store,
            get_credential=get_credential,
        )
    expected_msg = (
        'FeatureList (name: "sf_feature_list") has been registered by other feature list '
        "at Snowflake feature list store."
    )
    assert expected_msg in str(exc.value)
