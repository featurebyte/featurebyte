"""
Tests for DimensionData routes
"""
from http import HTTPStatus
from unittest import mock

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.models.feature_store import DataStatus
from featurebyte.schema.dimension_data import DimensionDataCreate
from featurebyte.service.semantic import SemanticService
from tests.unit.routes.base import BaseDataApiTestSuite


class TestDimensionDataApi(BaseDataApiTestSuite):
    """
    TestDimensionDataApi class
    """

    class_name = "DimensionData"
    base_route = "/dimension_data"
    data_create_schema_class = DimensionDataCreate
    payload = BaseDataApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/dimension_data.json"
    )
    document_name = "sf_dimension_data"
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'DimensionData (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `DimensionData.get(name="{document_name}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            f'DimensionData (name: "{document_name}") already exists. '
            f'Get the existing object by `DimensionData.get(name="{document_name}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId()), "name": "other_name"},
            f"DimensionData (tabular_source: \"{{'feature_store_id': "
            f'ObjectId(\'{payload["tabular_source"]["feature_store_id"]}\'), \'table_details\': '
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'sf_table'}}\") "
            f'already exists. Get the existing object by `DimensionData.get(name="{document_name}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "tabular_source": ("Some other source", "other table")},
            [
                {
                    "ctx": {"object_type": "TabularSource"},
                    "loc": ["body", "tabular_source"],
                    "msg": "value is not a valid TabularSource type",
                    "type": "type_error.featurebytetype",
                }
            ],
        )
    ]
    update_unprocessable_payload_expected_detail_pairs = []

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(self, tabular_source, columns_info, user_id):
        """Fixture for a Dimension Data dict"""
        dimension_data_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": columns_info,
            "record_creation_date_column": "created_at",
            "status": "PUBLISHED",
            "user_id": str(user_id),
            "dimension_data_id_column": "primary_key_column",
        }
        output = DimensionDataModel(**dimension_data_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        Dimension data update dict object
        """
        return {
            "record_creation_date_column": "created_at",
        }

    def test_update_success(
        self,
        test_api_client_persistent,
        data_response,
        data_update_dict,
        data_model_dict,
    ):
        """
        Update Dimension Data
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        insert_id = response_dict["_id"]

        response = test_api_client.patch(f"{self.base_route}/{insert_id}", json=data_update_dict)
        assert response.status_code == HTTPStatus.OK
        update_response_dict = response.json()
        assert update_response_dict["_id"] == insert_id
        update_response_dict.pop("created_at")
        update_response_dict.pop("updated_at")

        # the other fields should be unchanged
        data_model_dict["status"] = DataStatus.DRAFT
        assert update_response_dict == data_model_dict

        # test get audit records
        response = test_api_client.get(f"/dimension_data/audit/{insert_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 3
        assert [record["action_type"] for record in results["data"]] == [
            "UPDATE",
            "UPDATE",
            "INSERT",
        ]

    def test_update_excludes_unsupported_fields(
        self,
        test_api_client_persistent,
        data_response,
        data_update_dict,
        data_model_dict,
    ):
        """
        Update Dimension Data only updates job settings even if other fields are provided
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        insert_id = response_dict["_id"]
        assert insert_id

        # expect status to be draft
        assert response_dict["status"] == DataStatus.DRAFT

        data_update_dict["name"] = "Some other name"
        data_update_dict["source"] = "Some other source"
        data_update_dict["status"] = DataStatus.PUBLISHED.value
        response = test_api_client.patch(f"/dimension_data/{insert_id}", json=data_update_dict)
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["_id"] == insert_id
        data.pop("created_at")
        data.pop("updated_at")

        # the other fields should be unchanged
        assert data == data_model_dict

        # expect status to be updated to published
        assert data["status"] == DataStatus.PUBLISHED

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        expected_info_response = {
            "name": f"{self.document_name}",
            "record_creation_date_column": "created_at",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
            "dimension_data_id_column": "primary_key_column",
            "entities": [{"name": "customer", "serving_names": ["cust_id"]}],
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        assert response_dict["columns_info"] is None

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        verbose_response_dict = verbose_response.json()
        assert verbose_response_dict.items() > expected_info_response.items(), verbose_response.text
        assert "created_at" in verbose_response_dict
        assert verbose_response_dict["columns_info"] is not None
