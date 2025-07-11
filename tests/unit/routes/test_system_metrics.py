"""
Test system metrics route.
"""

import json
from http import HTTPStatus

import pytest
import pytest_asyncio

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.models.system_metrics import SqlQueryMetrics, SqlQueryType, SystemMetricsType


class TestSystemMetricsApi:
    """Test suite for Task API"""

    # class variables to be set at metaclass
    base_route = "/system_metrics"

    @pytest_asyncio.fixture(autouse=True)
    async def metrics_populated_fixture(self, app_container):
        """Fixture to populate system metrics"""
        metrics_data = SqlQueryMetrics(
            query="SELECT * FROM table",
            total_seconds=600.12,
            query_type=SqlQueryType.FEATURE_COMPUTE,
        )
        service = app_container.system_metrics_service
        service.catalog_id = DEFAULT_CATALOG_ID
        for _ in range(10):
            await service.create_metrics(metrics_data)

    @pytest.fixture(name="test_api_client")
    def test_api_client(self, api_client_persistent):
        """Test API client fixture"""
        api_client, _ = api_client_persistent
        api_client.headers["active-catalog-id"] = str(DEFAULT_CATALOG_ID)
        return api_client

    def test_get_200(self, test_api_client, user_id):
        """Test get (success)"""
        response = test_api_client.get(f"{self.base_route}")
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict["total"] == 10
        assert response_dict["data"][0] == {
            "_id": response_dict["data"][0]["_id"],
            "name": None,
            "description": None,
            "is_deleted": False,
            "created_at": response_dict["data"][0]["created_at"],
            "updated_at": response_dict["data"][0]["updated_at"],
            "catalog_id": str(DEFAULT_CATALOG_ID),
            "block_modification_by": [],
            "metrics_data": {
                "query_id": None,
                "query": "SELECT * FROM table",
                "total_seconds": 600.12,
                "query_type": SqlQueryType.FEATURE_COMPUTE.value,
                "feature_names": None,
                "observation_table_id": None,
                "batch_request_table_id": None,
                "metrics_type": SystemMetricsType.SQL_QUERY.value,
            },
            "user_id": str(user_id),
        }

    def test_download_200(self, test_api_client, user_id):
        """Test download (success)"""
        response = test_api_client.get(f"{self.base_route}/download")
        assert response.status_code == HTTPStatus.OK
        assert (
            response.headers["Content-Disposition"]
            == f"attachment; filename=system_metrics_{DEFAULT_CATALOG_ID}.json"
        )
        results = json.loads(response.content.decode("utf-8"))
        assert len(results) == 10
        assert results[0] == {
            "_id": results[0]["_id"],
            "name": None,
            "description": None,
            "is_deleted": False,
            "created_at": results[0]["created_at"],
            "updated_at": results[0]["updated_at"],
            "catalog_id": str(DEFAULT_CATALOG_ID),
            "block_modification_by": [],
            "metrics_data": {
                "query_id": None,
                "query": "SELECT * FROM table",
                "total_seconds": 600.12,
                "query_type": SqlQueryType.FEATURE_COMPUTE.value,
                "feature_names": None,
                "observation_table_id": None,
                "batch_request_table_id": None,
                "metrics_type": SystemMetricsType.SQL_QUERY.value,
            },
            "user_id": str(user_id),
        }
