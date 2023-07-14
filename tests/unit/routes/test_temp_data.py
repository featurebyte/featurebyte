"""
Test for Temp Data route
"""
from http import HTTPStatus

import pytest


class TestTempDataApi:
    """Test suite for Temp Data API"""

    # class variables to be set at metaclass
    base_route = "/temp_data"

    @pytest.mark.asyncio
    async def test_retrieve_html(self, api_client_persistent, temp_storage):
        """
        Retrieve temp html file
        """
        test_api_client, _ = api_client_persistent

        source_file = "tests/fixtures/feature_job_setting_analysis/backtest.html"
        dest_path = "feature_job_setting_analysis/backtest/62f301e841b9a757c9ff871b.html"
        await temp_storage.put(source_file, dest_path)

        # retrieve profile picture
        response = test_api_client.get(f"/temp_data?path={dest_path}")
        assert response.status_code == HTTPStatus.OK
        assert response.headers["content-type"] == "text/html; charset=utf-8"

        with open(source_file, "r") as file_obj:
            expected_content = file_obj.read()
        assert response.content.decode("utf-8") == expected_content

    @pytest.mark.asyncio
    async def test_retrieve_parquet(self, api_client_persistent, temp_storage):
        """
        Retrieve temp parquet file
        """
        test_api_client, _ = api_client_persistent

        source_file = "tests/fixtures/feature_job_setting_analysis/backtest.parquet"
        dest_path = "feature_job_setting_analysis/backtest/62f301e841b9a757c9ff871b.parquet"
        await temp_storage.put(source_file, dest_path)

        # retrieve profile picture
        with test_api_client.stream("GET", f"/temp_data?path={dest_path}") as response:
            assert response.status_code == HTTPStatus.OK
            assert response.headers["content-type"] == "application/octet-stream"
            assert (
                response.headers["content-disposition"]
                == "filename=62f301e841b9a757c9ff871b.parquet"
            )
            content = b""
            for chunk in response.iter_bytes():
                content += chunk

            with open(source_file, "rb") as file_obj:
                expected_content = file_obj.read()
            assert content == expected_content
