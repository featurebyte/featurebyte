"""
Tests for ObservationTable routes
"""
import json
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson.objectid import ObjectId
from requests import Response

from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.service.observation_table import ObservationTableService
from tests.unit.routes.base import BaseApiTestSuite


class MockResponse(Response):
    """
    Mock response object
    """

    def __init__(self, content: str, status_code: int):
        super().__init__()
        self.encoding = "utf-8"
        self._content = content.encode(self.encoding)
        self.status_code = status_code


def get_observation_table_payload():
    feature_store_id = ObjectId()
    observation_table_dict = {
        "name": "My Observation Table",
        "location": {
            "feature_store_id": feature_store_id,
            "table_details": {
                "database_name": "my_db",
                "schema_name": "my_schema",
                "table_name": "my_obs_table",
            },
        },
        "catalog_id": DEFAULT_CATALOG_ID,
        "observation_input": {
            "source": {
                "feature_store_id": feature_store_id,
                "table_details": {
                    "database_name": "my_db",
                    "schema_name": "my_schema",
                    "table_name": "input_table",
                },
            }
        },
        "column_names": ["a", "b", "c"],
    }
    return ObservationTableModel(**observation_table_dict).dict()


class TestObservationTableApi(BaseApiTestSuite):
    """
    Tests for ObservationTable route
    """

    class_name = "ObservationTable"
    base_route = "/observation_table"
    payload = get_observation_table_payload()
    unknown_id = ObjectId()

    @pytest.mark.skip("Test not applicable")
    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        pass

    @pytest.mark.skip("Test not applicable")
    def test_create_201__without_specifying_id_field(self, test_api_client_persistent):
        pass

    @pytest.mark.skip("Test not applicable")
    def test_create_201__id_is_none(self, test_api_client_persistent):
        pass

    @pytest.mark.skip("Test not applicable")
    def test_create_201_non_default_catalog(
        self,
        catalog_id,
        create_success_response_non_default_catalog,
    ):
        pass

    @pytest.mark.skip("Test not applicable")
    def test_list_audit_422(
        self,
        test_api_client_persistent,
        create_multiple_success_responses,
        list_unprocessable_params_expected_detail,
    ):
        pass

    @pytest.mark.skip("Test not applicable")
    def test_list_audit_422__invalid_id_value(self, test_api_client_persistent):
        pass

    @pytest_asyncio.fixture()
    async def create_success_response(
        self, test_api_client_persistent, user_id
    ):  # pylint: disable=arguments-differ
        """Post route success response object"""
        _, persistent = test_api_client_persistent
        service = ObservationTableService(
            user=User(id=user_id), persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
        )
        document = await service.create_document(data=ObservationTableModel(**self.payload))
        return MockResponse(
            content=json.dumps(document.json_dict()), status_code=HTTPStatus.CREATED
        )

    # @pytest_asyncio.fixture()
    # async def create_multiple_success_responses(
    #     self, test_api_client_persistent, user_id
    # ):  # pylint: disable=arguments-differ
    #     """Post multiple success responses"""
    #     _, persistent = test_api_client_persistent
    #     output = []
    #     periodic_task_service = PeriodicTaskService(
    #         user=User(id=user_id), persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
    #     )
    #     for payload in self.multiple_success_payload_generator(None):
    #         document = await periodic_task_service.create_document(data=PeriodicTask(**payload))
    #         output.append(
    #             MockResponse(
    #                 content=json.dumps(document.json_dict()), status_code=HTTPStatus.CREATED
    #             )
    #         )
    #     return output
    #
    # def multiple_success_payload_generator(self, api_client):
    #     """Create multiple payload for setting up create_multiple_success_responses fixture"""
    #     _ = api_client
    #
    #     # default catalog
    #     payload = self.payload.copy()
    #     yield payload
    #
    #     for i in range(2):
    #         data = self.payload.copy()
    #         data["_id"] = ObjectId()
    #         data["name"] = f'{self.payload["name"]}_{i}'
    #         yield PeriodicTask(**data).json_dict()
