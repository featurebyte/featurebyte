"""
Tests functions/methods in api_object.py
"""
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest

from featurebyte.api.api_object import ApiGetObject


@pytest.fixture(name="mock_configuration")
def mock_configuration_fixture(request):
    """Mock configuration (page_size is parametrized)"""

    def fake_get(url, params):
        _ = url
        page = params["page"]
        page_size, total = request.param, 11
        data = [
            {"name": f"item_{i + (page - 1) * page_size}"}
            for i in range(page_size)
            if (i + (page - 1) * page_size) < total
        ]
        response_dict = {"page": page, "page_size": page_size, "total": total, "data": data}
        response = Mock()
        response.json.return_value = response_dict
        response.status_code = HTTPStatus.OK
        return response

    with patch("featurebyte.api.api_object.Configurations") as mock_config:
        mock_client = mock_config.return_value.get_client.return_value
        mock_client.get = fake_get
        yield mock_config


@pytest.mark.parametrize("mock_configuration", [2, 3, 5], indirect=True)
def test_list(mock_configuration):
    """Test pagination list logic"""
    output = ApiGetObject.list()
    assert output == [f"item_{i}" for i in range(11)]
