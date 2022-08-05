"""
Tests for Feature route
"""
from datetime import datetime
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.exception import DuplicatedFeatureRegistryError
from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.models.feature_store import SourceType, SQLiteDetails, TableDetails
from featurebyte.routes.feature.controller import FeatureController
from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureApi(BaseApiTestSuite):
    """
    TestFeatureApi class
    """

    class_name = "Feature"
    base_route = "/feature"
    payload_filename = "tests/fixtures/request_payloads/feature.json"
    payload = BaseApiTestSuite.load_payload(payload_filename)
    object_id = str(ObjectId())
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'Feature (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `Feature.get_by_id(id="{payload["_id"]}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'Feature (name: "sum_1d") already exists. '
            f'Get the existing object by `Feature.get_by_id(id="{payload["_id"]}")`.',
        ),
        (
            {**payload, "_id": object_id, "parent_id": payload["_id"], "name": "random_name"},
            f'Feature (id: "{object_id}", name: "random_name") '
            f'has invalid parent feature (id: "{payload["_id"]}", name: "sum_1d")!',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "_id": object_id, "parent_id": object_id},
            f'The original feature (id: "{object_id}") not found. '
            f"Please save the Feature object first.",
        ),
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
    ]

    @pytest.fixture(autouse=True)
    def mock_insert_feature_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch(
            "featurebyte.routes.feature.controller.FeatureController.insert_feature_registry"
        ) as mock:
            yield mock

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        # save feature store
        feature_store_payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_store.json"
        )
        response = api_client.post("/feature_store", json=feature_store_payload)
        assert response.status_code == HTTPStatus.CREATED

        # save event data
        feature_store_payload = self.load_payload("tests/fixtures/request_payloads/event_data.json")
        response = api_client.post("/event_data", json=feature_store_payload)
        assert response.status_code == HTTPStatus.CREATED

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
        assert feat_namespace_docs[0]["description"] is None
        assert feat_namespace_docs[0]["versions"] == [response_dict["version"]]
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
        new_payload["parent_id"] = self.payload["_id"]
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
        assert feat_namespace_docs[0]["description"] is None
        assert feat_namespace_docs[0]["versions"] == [
            response_dict["version"],
            f"{response_dict['version']}_1",
        ]
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
    feature_model_dict["tabular_source"] = (
        str(ObjectId()),
        {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": "sf_table",
        },
    )
    return feature_model_dict


@pytest.mark.parametrize(
    "version_ids,versions,readiness,default_version_mode,default_version,doc_id,doc_version,expected",
    [
        [
            # for auto mode, update default version to the best readiness of the most recent version
            # the best readiness before update is DRAFT, new version will replace the default version
            ["feature_id1"],
            ["V220710"],
            FeatureReadiness.DRAFT,
            "AUTO",
            "feature_id1",
            "feature_id2",
            "V220711",
            {
                "versions": ["V220710", "V220711"],
                "version_ids": ["feature_id1", "feature_id2"],
                "readiness": "DRAFT",
                "default_version_id": "feature_id2",
            },
        ],
        [
            # for manual mode, won't update default feature version
            ["feature_id1"],
            ["V220710"],
            FeatureReadiness.DRAFT,
            "MANUAL",
            "feature_id1",
            "feature_id2",
            "V220710",
            {
                "versions": ["V220710", "V220710_1"],
                "version_ids": ["feature_id1", "feature_id2"],
                "readiness": "DRAFT",
                "default_version_id": "feature_id1",
            },
        ],
        [
            # for auto mode, update default version to the best readiness of the most recent version
            # the best readiness before update is PRODUCTION_READY, new version will not replace the default version
            ["feature_id1"],
            ["V220710"],
            FeatureReadiness.PRODUCTION_READY,
            "AUTO",
            "feature_id1",
            "feature_id2",
            "V220710",
            {
                "versions": ["V220710", "V220710_1"],
                "version_ids": ["feature_id1", "feature_id2"],
                "readiness": "PRODUCTION_READY",
                "default_version_id": "feature_id1",
            },
        ],
    ],
)
def test_prepare_feature_namespace_payload(
    version_ids,
    versions,
    readiness,
    default_version_mode,
    default_version,
    doc_id,
    doc_version,
    expected,
):
    """
    Test prepare_feature_namespace_payload function
    """
    feature_namespace = Mock()
    feature_namespace.version_ids = version_ids
    feature_namespace.versions = versions
    feature_namespace.readiness = readiness
    feature_namespace.default_version_mode = default_version_mode
    feature_namespace.default_version_id = default_version
    feature = Mock()
    feature.id = doc_id
    feature.version = doc_version
    assert (
        FeatureController.prepare_feature_namespace_payload(
            document=feature, feature_namespace=feature_namespace
        )
        == expected
    )


@pytest.fixture(name="get_credential")
def get_credential_fixture(config):
    """
    get_credential fixture
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return config.credentials.get(feature_store_name)

    return get_credential


@pytest.mark.asyncio
@patch("featurebyte.session.base.BaseSession.execute_query")
async def test_insert_feature_register(
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
    user = Mock()
    feature = FeatureModel(**feature_model_dict)
    await FeatureController.insert_feature_registry(
        user=user,
        document=feature,
        feature_store=snowflake_feature_store,
        get_credential=get_credential,
    )

    match_count = 0
    expected_partial_query = "INSERT INTO FEATURE_REGISTRY"
    for call_args in mock_execute_query.call_args_list:
        if expected_partial_query in call_args.args[0]:
            match_count += 1
    assert match_count > 0


@patch("featurebyte.session.base.BaseSession.execute_query")
def test_insert_feature_registry__non_snowflake_feature_store(
    mock_execute_query, feature_model_dict
):
    """
    Test insert_feature_registry function (when feature store is not snowflake)
    """
    feature_store = ExtendedFeatureStoreModel(
        name="sq_feature_store",
        type=SourceType.SQLITE,
        details=SQLiteDetails(filename="some_filename"),
    )
    feature_model_dict["tabular_source"] = (feature_store.id, TableDetails(table_name="some_table"))
    feature = FeatureModel(**feature_model_dict)
    user, get_credential = Mock(), Mock()
    FeatureController.insert_feature_registry(
        user=user, document=feature, feature_store=feature_store, get_credential=get_credential
    )
    assert mock_execute_query.call_count == 0


@pytest.mark.asyncio
@patch("featurebyte.routes.feature.controller.FeatureManagerSnowflake")
async def test_insert_feature_registry__duplicated_feature_registry_exception(
    mock_feature_manager,
    feature_model_dict,
    get_credential,
    snowflake_connector,
    snowflake_feature_store,
):
    """
    Test insert_feature_registry with duplicated_feature_registry exception
    """
    _ = snowflake_connector
    mock_feature_manager.return_value.insert_feature_registry.side_effect = (
        DuplicatedFeatureRegistryError
    )
    feature = FeatureModel(**feature_model_dict)
    user = Mock()
    with pytest.raises(HTTPException) as exc:
        await FeatureController.insert_feature_registry(
            user=user,
            document=feature,
            feature_store=snowflake_feature_store,
            get_credential=get_credential,
        )
    expected_msg = (
        'Feature (feature.name: "sum_30m") has been registered by other feature '
        "at Snowflake feature store."
    )
    assert expected_msg in str(exc.value.detail)
    assert not mock_feature_manager.return_value.remove_feature_registry.called


@pytest.mark.asyncio
@patch("featurebyte.routes.feature.controller.FeatureManagerSnowflake")
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
    feature = FeatureModel(**feature_model_dict)
    user = Mock()
    with pytest.raises(ValueError):
        await FeatureController.insert_feature_registry(
            user=user,
            document=feature,
            feature_store=snowflake_feature_store,
            get_credential=get_credential,
        )
    assert mock_feature_manager.return_value.remove_feature_registry.called
