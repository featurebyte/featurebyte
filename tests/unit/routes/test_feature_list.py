"""
Tests for FeatureList route
"""
import json
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import SourceType
from featurebyte.exception import DuplicatedRegistryError
from featurebyte.feature_manager.model import ExtendedFeatureListModel
from featurebyte.models.feature_store import SQLiteDetails, TableDetails
from featurebyte.routes.feature_list.controller import FeatureListController
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
            'FeatureList (name: "sf_feature_list") already exists. '
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
            'Feature (id: "62ef015b940d152708e1e5dc") not found. Please save the Feature object first.',
        ),
    ]

    @pytest.fixture(autouse=True)
    def mock_insert_feature_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch(
            "featurebyte.routes.feature.controller.FeatureController._insert_feature_registry"
        ) as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def mock_insert_feature_list_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch(
            "featurebyte.routes.feature_list.controller.FeatureListController._insert_feature_list_registry"
        ) as mock:
            yield mock

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("event_data", "event_data"),
            ("feature", "feature_sum_30m"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

    def test_create_422_different_feature_stores(self, test_api_client_persistent):
        """
        Test feature list with different feature stores
        """
        test_api_client, persistent = test_api_client_persistent
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
        _, table_detail = event_data["tabular_source"]
        tabular_source = [feature_store["_id"], table_detail]
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


@pytest.fixture(name="feature_list_model")
def feature_list_model_fixture():
    """FeatureList model fixture"""
    with open("tests/fixtures/request_payloads/feature_sum_30m.json") as fhandle:
        feature_dict = json.loads(fhandle.read())

    with open("tests/fixtures/request_payloads/feature_list_single.json") as fhandle:
        feature_list_dict = json.loads(fhandle.read())
        feature_list_dict["features"] = [
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
    user = Mock()

    await FeatureListController._insert_feature_list_registry(
        user=user,
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


@patch("featurebyte.session.base.BaseSession.execute_query")
def test_insert_feature_list_registry__non_snowflake_feature_store(
    mock_execute_query, feature_list_model
):
    """
    Test insert_feature_registry function (when feature store is not snowflake)
    """
    feature_store = ExtendedFeatureStoreModel(
        name="sq_feature_store",
        type=SourceType.SQLITE,
        details=SQLiteDetails(filename="some_filename"),
    )

    user, get_credential = Mock(), Mock()
    FeatureListController._insert_feature_list_registry(
        user=user,
        document=feature_list_model,
        feature_store=feature_store,
        get_credential=get_credential,
    )
    assert mock_execute_query.call_count == 0


@pytest.mark.asyncio
@patch("featurebyte.routes.feature_list.controller.FeatureListManagerSnowflake")
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
    user = Mock()
    with pytest.raises(HTTPException) as exc:
        await FeatureListController._insert_feature_list_registry(
            user=user,
            document=feature_list_model,
            feature_store=snowflake_feature_store,
            get_credential=get_credential,
        )
    expected_msg = (
        'FeatureList (name: "sf_feature_list") has been registered by other feature list '
        "at Snowflake feature list store."
    )
    assert expected_msg in str(exc.value.detail)
